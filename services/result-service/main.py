import json
import logging
import os
import threading
import time

from confluent_kafka import Consumer
from dotenv import load_dotenv
from prometheus_client import Counter, Histogram, start_http_server
from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from database import (
    init_db,
    save_ai_review,
    save_security_scan,
    is_ai_comment_posted,
    is_security_comment_posted,
    mark_ai_comment_posted,
    mark_security_comment_posted,
)
from github_client import (
    format_ai_comment,
    format_security_comment,
    post_pr_comment,
)
from kafka_reliability import process_with_retry, send_to_dlq

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("result-service")

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
provider = TracerProvider(resource=Resource.create({"service.name": "result-service"}))
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{OTEL_ENDPOINT}/v1/traces")))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("result-service")


def log_event(stage: str, trace_id: str, **fields):
    logger.info(json.dumps({"service": "result-service", "trace_id": trace_id, "stage": stage, **fields}))


def record_stage(stage: str, duration_ms: float, outcome: str = "success"):
    STAGE_DURATION.labels(service="result-service", stage=stage).observe(duration_ms / 1000)
    STAGE_TOTAL.labels(service="result-service", stage=stage, outcome=outcome).inc()


def extract_trace_id(msg) -> str:
    for key, value in msg.headers() or []:
        if key == "trace_id":
            return value.decode()
    return "unknown"


def extract_otel_context(msg):
    carrier = {k: v.decode() for k, v in (msg.headers() or [])}
    return propagate.extract(carrier)


def make_consumer(group_id: str) -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,   # 手动 commit：只有处理到终态才移动 offset
    })


def consume_loop(topic: str, group_id: str, handler):
    consumer = make_consumer(group_id)
    consumer.subscribe([topic])
    print(f"[Result Service] Listening on {topic} ({group_id})")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[Result Service] Consumer error: {msg.error()}")
                continue

            raw = msg.value().decode("utf-8")
            trace_id = extract_trace_id(msg)
            otel_context = extract_otel_context(msg)

            # JSON 解析失败 = 永久失败，直接进 DLQ
            try:
                data = json.loads(raw)
            except json.JSONDecodeError as e:
                print(f"[Result Service] Invalid JSON: {e}")
                send_to_dlq(KAFKA_BOOTSTRAP_SERVERS, "result-service", raw, str(e),
                            "PermanentFailure", 1, trace_id)
                consumer.commit(msg)
                continue

            done = process_with_retry(
                "result-service", KAFKA_BOOTSTRAP_SERVERS,
                handler, data, raw, trace_id, otel_context,
            )
            if done:
                consumer.commit(msg)
    finally:
        consumer.close()


def handle_ai_result(data: dict, trace_id: str = "unknown", otel_context=None):
    with tracer.start_as_current_span("result_publish_ai", context=otel_context) as span:
        span.set_attribute("prguard.trace_id", trace_id)

        t0 = time.perf_counter()
        pr_number = data["pr_number"]
        repo = data["repo_full_name"]
        head_sha = data["head_sha"]
        review = data["review"]

        # 幂等性检查：comment 已发过则跳过
        if is_ai_comment_posted(repo, pr_number, head_sha):
            print(f"[Result Service] PR #{pr_number} ({head_sha[:7]}) already reviewed, skipping")
            return

        # 先存数据库，再发 comment
        save_ai_review(repo, pr_number, head_sha, review)
        comment = format_ai_comment(review)
        post_pr_comment(repo, pr_number, comment)
        mark_ai_comment_posted(repo, pr_number, head_sha)
        print(f"[Result Service] AI review posted and saved for PR #{pr_number}")
        duration_ms = (time.perf_counter() - t0) * 1000
        log_event("result_publish_ai", trace_id, pr_number=pr_number, duration_ms=round(duration_ms, 2))
        record_stage("result_publish_ai", duration_ms)


def handle_security_result(data: dict, trace_id: str = "unknown", otel_context=None):
    with tracer.start_as_current_span("result_publish_security", context=otel_context) as span:
        span.set_attribute("prguard.trace_id", trace_id)

        t0 = time.perf_counter()
        pr_number = data["pr_number"]
        repo = data["repo_full_name"]
        head_sha = data["head_sha"]
        findings = data["findings"]
        passed = data["passed"]

        # 幂等性检查：comment 已发过则跳过
        if is_security_comment_posted(repo, pr_number, head_sha):
            print(f"[Result Service] PR #{pr_number} ({head_sha[:7]}) security scan already posted, skipping")
            return

        # 先存数据库，再发 comment
        save_security_scan(repo, pr_number, head_sha, findings, passed)
        comment = format_security_comment(findings, passed)
        post_pr_comment(repo, pr_number, comment)
        mark_security_comment_posted(repo, pr_number, head_sha)
        print(f"[Result Service] Security scan posted and saved for PR #{pr_number}")
        duration_ms = (time.perf_counter() - t0) * 1000
        log_event("result_publish_security", trace_id, pr_number=pr_number, duration_ms=round(duration_ms, 2))
        record_stage("result_publish_security", duration_ms)


def main():
    start_http_server(9100)
    # 两个 topic 各用一个线程并发消费
    ai_thread = threading.Thread(
        target=consume_loop,
        args=("ai-results", "result-ai-group", handle_ai_result),
        daemon=True,
    )
    security_thread = threading.Thread(
        target=consume_loop,
        args=("security-results", "result-security-group", handle_security_result),
        daemon=True,
    )

    init_db()

    ai_thread.start()
    security_thread.start()

    print("[Result Service] Started both consumers")
    ai_thread.join()
    security_thread.join()


if __name__ == "__main__":
    main()
