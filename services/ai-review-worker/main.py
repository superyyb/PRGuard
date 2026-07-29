import base64
import json
import logging
import os
import pathlib
import time

import anthropic
import httpx
from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv
from prometheus_client import Counter, Histogram, start_http_server
from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from kafka_reliability import process_with_retry, send_to_dlq

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("ai-review-worker")

STAGE_DURATION = Histogram(
    "pr_stage_duration_seconds", "Duration of each pipeline stage",
    ["service", "stage"],
    buckets=(0.1, 0.25, 0.5, 1, 2, 3, 5, 7.5, 10, 15, 20, 30),
)
STAGE_TOTAL = Counter(
    "pr_stage_total", "Count of pipeline stage outcomes",
    ["service", "stage", "outcome"],
)

OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
provider = TracerProvider(resource=Resource.create({"service.name": "ai-review-worker"}))
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{OTEL_ENDPOINT}/v1/traces")))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("ai-review-worker")
HTTPXClientInstrumentor().instrument()


def log_event(stage: str, trace_id: str, **fields):
    logger.info(json.dumps({"service": "ai-review-worker", "trace_id": trace_id, "stage": stage, **fields}))


def record_stage(stage: str, duration_ms: float, outcome: str = "success"):
    STAGE_DURATION.labels(service="ai-review-worker", stage=stage).observe(duration_ms / 1000)
    STAGE_TOTAL.labels(service="ai-review-worker", stage=stage, outcome=outcome).inc()


def extract_trace_id(msg) -> str:
    for key, value in msg.headers() or []:
        if key == "trace_id":
            return value.decode()
    return "unknown"


def extract_otel_context(msg):
    carrier = {k: v.decode() for k, v in (msg.headers() or [])}
    return propagate.extract(carrier)


def inject_otel_headers(headers: list) -> list:
    carrier = {}
    propagate.inject(carrier)
    headers.extend((k, v.encode()) for k, v in carrier.items())
    return headers


_has_assignment = False


def on_assign(consumer, partitions):
    global _has_assignment
    _has_assignment = True
    print(f"[AI Worker] Partitions assigned: {partitions}")


def on_revoke(consumer, partitions):
    global _has_assignment
    _has_assignment = False
    print(f"[AI Worker] Partitions revoked: {partitions}")

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
    "enable.auto.commit": False,          # 手动 commit：只有处理到终态才移动 offset
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
        timeout=45.0,
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


def process_event(event: dict, trace_id: str = "unknown", otel_context=None):
    with tracer.start_as_current_span("ai_review", context=otel_context) as span:
        span.set_attribute("prguard.trace_id", trace_id)

        t0 = time.perf_counter()
        pr_number = event["pr_number"]
        repo = event["repo_full_name"]
        print(f"[AI Worker] Processing PR #{pr_number} from {repo}")

        with tracer.start_as_current_span("build_context"):
            t_context = time.perf_counter()
            files = fetch_pr_files(repo, pr_number)
            context = build_review_context(files)
            context_ms = (time.perf_counter() - t_context) * 1000
            log_event("build_context", trace_id, pr_number=pr_number, duration_ms=round(context_ms, 2))
            record_stage("build_context", context_ms)

        if not context.strip():
            print(f"[AI Worker] PR #{pr_number} has no reviewable changes, skipping")
            return

        with tracer.start_as_current_span("claude_call"):
            t_ai = time.perf_counter()
            review = analyze_with_ai(event["title"], context)
            ai_ms = (time.perf_counter() - t_ai) * 1000
            log_event("claude_call", trace_id, pr_number=pr_number, duration_ms=round(ai_ms, 2))
            record_stage("claude_call", ai_ms)
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
            headers=inject_otel_headers([("trace_id", trace_id.encode())]),
            callback=delivery_report,
        )
        producer.flush()

        total_ms = (time.perf_counter() - t0) * 1000
        log_event("ai_review", trace_id, pr_number=pr_number, duration_ms=round(total_ms, 2))
        record_stage("ai_review", total_ms)


def main():
    start_http_server(9100)
    consumer.subscribe(["pr-events"], on_assign=on_assign, on_revoke=on_revoke)
    READY_FILE.touch()   # Readiness: 成功订阅 Kafka topic，可以接收消息了
    print("[AI Worker] Started, waiting for PR events...")

    try:
        while True:
            # Liveness: 只有真的持有 partition 分配时才更新时间戳——
            # 之前只要循环还在转就 touch，consumer 卡在 rejoin group 时探针也测不出来
            if _has_assignment:
                HEALTHY_FILE.touch()
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[AI Worker] Consumer error: {msg.error()}")
                continue

            raw = msg.value().decode("utf-8")
            trace_id = extract_trace_id(msg)
            otel_context = extract_otel_context(msg)

            try:
                event = json.loads(raw)
            except json.JSONDecodeError as e:
                print(f"[AI Worker] Invalid JSON: {e}")
                send_to_dlq(KAFKA_BOOTSTRAP_SERVERS, "ai-review-worker", raw, str(e),
                            "PermanentFailure", 1, trace_id)
                consumer.commit(msg)
                continue

            done = process_with_retry(
                "ai-review-worker", KAFKA_BOOTSTRAP_SERVERS,
                process_event, event, raw, trace_id, otel_context,
            )
            if done:
                consumer.commit(msg)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
