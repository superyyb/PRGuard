import base64
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


def fetch_pr_files(repo: str, pr_number: int) -> list:
    """获取 PR 改动的文件列表，每个文件包含 diff patch 和 contents_url"""
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/files"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    response = httpx.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def fetch_file_content(contents_url: str) -> str:
    """通过 contents_url 拿完整文件内容（base64 解码）"""
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    response = httpx.get(contents_url, headers=headers)
    response.raise_for_status()
    data = response.json()
    return base64.b64decode(data["content"]).decode("utf-8")


def build_review_context(files: list) -> str:
    """
    为每个改动的文件拼接：完整文件内容 + diff。
    总长度上限 50000 字符，每个文件完整内容上限 8000 字符。
    """
    parts = []
    total = 0
    MAX_TOTAL = 50000
    MAX_PER_FILE = 8000

    for file in files:
        if total >= MAX_TOTAL:
            break

        filename = file["filename"]
        patch = file.get("patch", "")
        status = file.get("status", "modified")

        # 跳过删除的文件和没有 diff 的文件（如二进制文件）
        if status == "removed" or not patch:
            continue

        part = f"=== {filename} ===\n"

        # 对于修改的文件，额外获取完整内容提供上下文
        if status == "modified" and file.get("contents_url"):
            try:
                content = fetch_file_content(file["contents_url"])
                part += f"Full file:\n{content[:MAX_PER_FILE]}\n\n"
            except Exception as e:
                print(f"[AI Worker] Could not fetch full content for {filename}: {e}")

        part += f"Changes:\n{patch}\n"

        # 超出总上限则截断
        remaining = MAX_TOTAL - total
        if len(part) > remaining:
            part = part[:remaining]

        parts.append(part)
        total += len(part)

    return "\n".join(parts)


def analyze_with_ai(pr_title: str, context: str) -> dict:
    """调用 Claude 分析 PR，传入完整文件内容 + diff，返回结构化 review"""
    prompt = f"""You are a senior engineer doing a practical code review. Be direct and pragmatic — your goal is to help ship good code, not to find as many issues as possible.

PR Title: {pr_title}

Changed files (each section shows the full file content followed by the specific changes made):
{context}

CRITICAL — how to read this context:
- Each file section shows the FULL file content first, then the changes (diff).
- Only flag issues in the new code shown in the "Changes" section.
- The full file is provided so you have complete context — use it to understand imports, existing functions, and structure.
- Do NOT flag things that are already handled elsewhere in the full file.

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

    files = fetch_pr_files(repo, pr_number)
    context = build_review_context(files)
    if not context.strip():
        print(f"[AI Worker] PR #{pr_number} has no reviewable changes, skipping")
        return

    review = analyze_with_ai(event["title"], context)
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
