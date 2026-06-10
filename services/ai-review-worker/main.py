import json
import os
import pathlib

import anthropic
import httpx
from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

HEALTHY_FILE = pathlib.Path("/tmp/healthy")  # liveness: 循环还在跑
READY_FILE = pathlib.Path("/tmp/ready")      # readiness: 已连上 Kafka

anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "ai-review-group",        # 独立 consumer group
    "auto.offset.reset": "earliest",
})

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def fetch_pr_diff(diff_url: str) -> str:
    """通过 GitHub API 获取 PR diff 内容"""
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3.diff",
    }
    response = httpx.get(diff_url, headers=headers, follow_redirects=True)
    response.raise_for_status()
    # diff 可能很长，截取前 8000 字符避免超出 token 限制
    return response.text[:8000]


def analyze_with_ai(pr_title: str, diff: str) -> dict:
    """调用 Claude 分析 PR diff，返回结构化 review"""
    prompt = f"""You are a senior engineer doing a practical code review. Be direct and pragmatic — your goal is to help ship good code, not to find as many issues as possible.

PR Title: {pr_title}

Diff (only lines starting with + are new code):
{diff}

Rules:
- Only report issues you are CERTAIN about from the diff. Do NOT speculate about runtime behavior you cannot verify.
- Only flag lines that are actually in the diff (new code added). Do not comment on existing unchanged code.
- HIGH severity: real bugs, security vulnerabilities, data loss risks.
- MEDIUM severity: clear logic errors or missing error handling that will likely cause problems.
- LOW severity: only if it's a concrete maintainability issue, not just style preference.
- If the code is clean and correct, return an empty issues array. It is perfectly fine to have 0 issues.
- Suggestions should be actionable and specific. Max 3 suggestions.
- Score 7-10 if code is solid. Only score below 5 if there are HIGH severity bugs.

Respond with ONLY valid JSON (no markdown, no extra text):
{{
  "summary": "One sentence: what this PR does and overall quality assessment",
  "score": <1-10>,
  "issues": [
    {{"severity": "high|medium|low", "line": "filename:linenum", "comment": "specific, certain issue"}}
  ],
  "suggestions": ["concrete suggestion 1", "concrete suggestion 2"],
  "approved": <true if score >= 6, false otherwise>
}}"""

    message = anthropic_client.messages.create(
        model="claude-haiku-4-5",
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )

    response_text = message.content[0].text.strip()
    if response_text.startswith("```"):
        response_text = response_text.split("```")[1]
        if response_text.startswith("json"):
            response_text = response_text[4:]
    response_text = response_text.strip()
    return json.loads(response_text)


def delivery_report(err, msg):
    if err:
        print(f"[Kafka] Delivery failed: {err}")
    else:
        print(f"[Kafka] Delivered to {msg.topic()} [{msg.partition()}]")


def process_event(event: dict):
    pr_number = event["pr_number"]
    repo = event["repo_full_name"]
    print(f"[AI Worker] Processing PR #{pr_number} from {repo}")

    diff = fetch_pr_diff(event["diff_url"])
    if not diff.strip():
        print(f"[AI Worker] PR #{pr_number} has no diff, skipping")
        return

    review = analyze_with_ai(event["title"], diff)
    print(f"[AI Worker] PR #{pr_number} score: {review.get('score')}/10")

    result = {
        "type": "ai_review",
        "pr_number": pr_number,
        "repo_full_name": repo,
        "head_sha": event["head_sha"],
        "html_url": event["html_url"],
        "review": review,
    }

    producer.produce(
        "ai-results",
        key=str(pr_number),
        value=json.dumps(result),
        callback=delivery_report,
    )
    producer.flush()


def main():
    consumer.subscribe(["pr-events"])
    READY_FILE.touch()   # Readiness: 成功订阅 Kafka topic，可以接收消息了
    print("[AI Worker] Started, waiting for PR events...")

    try:
        while True:
            HEALTHY_FILE.touch()  # Liveness: 每次 poll 前更新时间戳，证明循环没卡死
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[AI Worker] Consumer error: {msg.error()}")
                continue

            event = json.loads(msg.value().decode("utf-8"))
            try:
                process_event(event)
            except Exception as e:
                print(f"[AI Worker] Error processing PR #{event.get('pr_number')}: {e}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
