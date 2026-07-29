import json
import logging
import os
import pathlib
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from confluent_kafka import Consumer
from dotenv import load_dotenv
from prometheus_client import Counter, Histogram, start_http_server
from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from database import init_db, is_email_sent, mark_email_sent
from kafka_reliability import process_with_retry, send_to_dlq

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("notifier")

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
provider = TracerProvider(resource=Resource.create({"service.name": "notifier"}))
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{OTEL_ENDPOINT}/v1/traces")))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("notifier")


def log_event(stage: str, trace_id: str, **fields):
    logger.info(json.dumps({"service": "notifier", "trace_id": trace_id, "stage": stage, **fields}))


def record_stage(stage: str, duration_ms: float, outcome: str = "success"):
    STAGE_DURATION.labels(service="notifier", stage=stage).observe(duration_ms / 1000)
    STAGE_TOTAL.labels(service="notifier", stage=stage, outcome=outcome).inc()


def extract_trace_id(msg) -> str:
    for key, value in msg.headers() or []:
        if key == "trace_id":
            return value.decode()
    return "unknown"


def extract_otel_context(msg):
    carrier = {k: v.decode() for k, v in (msg.headers() or [])}
    return propagate.extract(carrier)


_has_assignment = False


def on_assign(consumer, partitions):
    global _has_assignment
    _has_assignment = True
    print(f"[Notifier] Partitions assigned: {partitions}")


def on_revoke(consumer, partitions):
    global _has_assignment
    _has_assignment = False
    print(f"[Notifier] Partitions revoked: {partitions}")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GMAIL_USER = os.getenv("GMAIL_USER", "")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")
NOTIFY_EMAIL = os.getenv("NOTIFY_EMAIL", "")

HEALTHY_FILE = pathlib.Path("/tmp/healthy")  # liveness: 是否持有 partition 分配
READY_FILE = pathlib.Path("/tmp/ready")      # readiness: 已连上 Kafka

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "notifier-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,   # 手动 commit：只有处理到终态才移动 offset
})


def should_notify(review: dict) -> bool:
    """只在 score <= 6 或有 HIGH 问题时发通知"""
    if review.get("score", 10) <= 6:
        return True
    issues = review.get("issues", [])
    if any(issue.get("severity") == "high" for issue in issues):
        return True
    return False


def format_score_emoji(score: int) -> str:
    if score >= 8:
        return "🟢"
    elif score >= 6:
        return "🟡"
    else:
        return "🔴"


def build_email_body(pr_number: int, repo: str, html_url: str, review: dict) -> str:
    score = review.get("score", 0)
    summary = review.get("summary", "No summary available")
    issues = review.get("issues", [])
    suggestions = review.get("suggestions", [])
    approved = review.get("approved", False)

    score_emoji = format_score_emoji(score)
    status = "✅ Approved" if approved else "❌ Changes Requested"

    # 构建 issues 部分
    issues_text = ""
    if issues:
        for issue in issues:
            severity = issue.get("severity", "low").upper()
            line = issue.get("line", "general")
            comment = issue.get("comment", "")
            if severity == "HIGH":
                emoji = "🔴"
            elif severity == "MEDIUM":
                emoji = "🟡"
            else:
                emoji = "🔵"
            issues_text += f"{emoji} {severity}   {line}\n         {comment}\n\n"
    else:
        issues_text = "No issues found.\n"

    # 构建 suggestions 部分
    suggestions_text = ""
    if suggestions:
        for s in suggestions:
            suggestions_text += f"💡 {s}\n"
    else:
        suggestions_text = "No suggestions.\n"

    body = f"""
────────────────────────────────
🤖 PRGuard AI Code Review Report
────────────────────────────────

PR Title : PR #{pr_number}
Repo     : {repo}
Score    : {score_emoji} {score}/10
Status   : {status}

Summary:
{summary}

─── Issues Found ──────────────────
{issues_text}
─── Suggestions ───────────────────
{suggestions_text}
─── Action Required ───────────────
👉 View PR: {html_url}

────────────────────────────────
Generated by PRGuard
"""
    return body


def send_email(subject: str, body: str):
    """通过 Gmail SMTP 发送邮件；失败交给外层 process_with_retry 统一重试"""
    msg = MIMEMultipart()
    msg["From"] = GMAIL_USER
    msg["To"] = NOTIFY_EMAIL
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        server.sendmail(GMAIL_USER, NOTIFY_EMAIL, msg.as_string())
    print(f"[Notifier] Email sent to {NOTIFY_EMAIL}")


def process_event(event: dict, trace_id: str = "unknown", otel_context=None):
    with tracer.start_as_current_span("notify", context=otel_context) as span:
        span.set_attribute("prguard.trace_id", trace_id)

        t0 = time.perf_counter()
        pr_number = event["pr_number"]
        repo = event.get("repo_full_name", "")
        head_sha = event.get("head_sha", "")
        html_url = event.get("html_url", "")
        review = event.get("review", {})

        if not should_notify(review):
            print(f"[Notifier] PR #{pr_number} score {review.get('score')}/10 — no notification needed")
            duration_ms = (time.perf_counter() - t0) * 1000
            log_event("notify", trace_id, pr_number=pr_number, sent=False, duration_ms=round(duration_ms, 2))
            record_stage("notify", duration_ms)
            return

        # 幂等性检查：这个 commit 的报警邮件已发过则跳过，避免重复消费导致重复报警
        if is_email_sent(repo, pr_number, head_sha):
            print(f"[Notifier] PR #{pr_number} ({head_sha[:7]}) already notified, skipping")
            duration_ms = (time.perf_counter() - t0) * 1000
            log_event("notify", trace_id, pr_number=pr_number, sent=False, duration_ms=round(duration_ms, 2))
            record_stage("notify", duration_ms)
            return

        score = review.get("score", 0)
        score_emoji = format_score_emoji(score)
        subject = f"[PRGuard] ⚠️ PR #{pr_number} needs attention — Score: {score_emoji} {score}/10"
        body = build_email_body(pr_number, repo, html_url, review)

        send_email(subject, body)
        mark_email_sent(repo, pr_number, head_sha)
        print(f"[Notifier] PR #{pr_number} — notified {NOTIFY_EMAIL} (score: {score}/10)")
        duration_ms = (time.perf_counter() - t0) * 1000
        log_event("notify", trace_id, pr_number=pr_number, sent=True, duration_ms=round(duration_ms, 2))
        record_stage("notify", duration_ms)


def main():
    start_http_server(9100)
    init_db()
    consumer.subscribe(["ai-results"], on_assign=on_assign, on_revoke=on_revoke)
    READY_FILE.touch()   # Readiness: 成功订阅 Kafka topic，可以接收消息了
    print("[Notifier] Started, waiting for AI review results...")

    try:
        while True:
            # Liveness: 只有真的持有 partition 分配时才更新时间戳
            if _has_assignment:
                HEALTHY_FILE.touch()
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[Notifier] Consumer error: {msg.error()}")
                continue

            raw = msg.value().decode("utf-8")
            trace_id = extract_trace_id(msg)
            otel_context = extract_otel_context(msg)

            try:
                event = json.loads(raw)
            except json.JSONDecodeError as e:
                print(f"[Notifier] Invalid JSON: {e}")
                send_to_dlq(KAFKA_BOOTSTRAP_SERVERS, "notifier", raw, str(e),
                            "PermanentFailure", 1, trace_id)
                consumer.commit(msg)
                continue

            done = process_with_retry(
                "notifier", KAFKA_BOOTSTRAP_SERVERS,
                process_event, event, raw, trace_id, otel_context,
            )
            if done:
                consumer.commit(msg)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
