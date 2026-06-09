import json
import os

import anthropic
import httpx
from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

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
    prompt = f"""You are an expert code reviewer. Analyze the following pull request and provide a structured review.

PR Title: {pr_title}

Diff:
{diff}

Respond with ONLY valid JSON in the following format (no markdown, no extra text):
{{
  "summary": "Brief summary of what this PR does",
  "score": <1-10 quality score>,
  "issues": [
    {{"severity": "high|medium|low", "line": "filename:linenum or general", "comment": "specific issue description"}}
  ],
  "suggestions": ["improvement suggestion 1", "suggestion 2"],
  "approved": <true if code is good enough to merge, false otherwise>
}}

Focus on: correctness, performance, readability, and best practices. Be concise and actionable."""

    message = anthropic_client.messages.create(
        model="claude-haiku-4-5",
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )

    response_text = message.content[0].text.strip()
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
    print("[AI Worker] Started, waiting for PR events...")

    try:
        while True:
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
