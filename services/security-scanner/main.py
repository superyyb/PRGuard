import json
import logging
import os
import pathlib
import re
import time

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
logger = logging.getLogger("security-scanner")

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
provider = TracerProvider(resource=Resource.create({"service.name": "security-scanner"}))
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{OTEL_ENDPOINT}/v1/traces")))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("security-scanner")
HTTPXClientInstrumentor().instrument()


def log_event(stage: str, trace_id: str, **fields):
    logger.info(json.dumps({"service": "security-scanner", "trace_id": trace_id, "stage": stage, **fields}))


def record_stage(stage: str, duration_ms: float, outcome: str = "success"):
    STAGE_DURATION.labels(service="security-scanner", stage=stage).observe(duration_ms / 1000)
    STAGE_TOTAL.labels(service="security-scanner", stage=stage, outcome=outcome).inc()


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
    print(f"[Security Scanner] Partitions assigned: {partitions}")


def on_revoke(consumer, partitions):
    global _has_assignment
    _has_assignment = False
    print(f"[Security Scanner] Partitions revoked: {partitions}")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")

HEALTHY_FILE = pathlib.Path("/tmp/healthy")  # liveness: 是否持有 partition 分配
READY_FILE = pathlib.Path("/tmp/ready")      # readiness: 已连上 Kafka

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "security-scanner-group",   # 独立 consumer group，与 AI Worker 并行消费
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,            # 手动 commit：只有处理到终态才移动 offset
})

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

# 安全规则：(规则名, 正则, 严重级别, 说明)
SECURITY_RULES = [
    ("hardcoded_password", r'(?i)(password|passwd|pwd)\s*=\s*["\'][^"\']{4,}["\']', "high", "Hardcoded password detected"),
    ("hardcoded_secret",   r'(?i)(secret|api_key|apikey|token)\s*=\s*["\'][^"\']{8,}["\']', "high", "Hardcoded secret or API key detected"),
    ("hardcoded_aws_key",  r'AKIA[0-9A-Z]{16}', "high", "Hardcoded AWS access key detected"),
    ("sql_injection",      r'(?i)(execute|cursor\.execute)\s*\(\s*["\'].*%s', "high", "Potential SQL injection via string formatting"),
    ("eval_usage",         r'\beval\s*\(', "medium", "Use of eval() is dangerous"),
    ("shell_injection",    r'(?i)(os\.system|subprocess\.call|subprocess\.Popen)\s*\(.*\+', "medium", "Potential shell injection via string concatenation"),
    ("debug_enabled",      r'(?i)DEBUG\s*=\s*True', "low", "Debug mode enabled in code"),
    ("print_sensitive",    r'(?i)print\s*\(.*(?:password|token|secret)', "low", "Potentially printing sensitive data"),
]


def fetch_pr_diff(diff_url: str) -> str:
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3.diff",
    }
    response = httpx.get(diff_url, headers=headers, follow_redirects=True)
    response.raise_for_status()
    return response.text[:8000]


def scan_diff(diff: str) -> list[dict]:
    """对 diff 的新增行（+ 开头）做安全规则扫描"""
    findings = []
    added_lines = [
        (i + 1, line[1:])
        for i, line in enumerate(diff.splitlines())
        if line.startswith("+") and not line.startswith("+++")
    ]

    for line_num, line in added_lines:
        for rule_name, pattern, severity, message in SECURITY_RULES:
            if re.search(pattern, line):
                findings.append({
                    "rule": rule_name,
                    "severity": severity,
                    "message": message,
                    "line": line_num,
                    "content": line.strip()[:120],
                })

    return findings


def delivery_report(err, msg):
    if err:
        print(f"[Kafka] Delivery failed: {err}")
    else:
        print(f"[Kafka] Delivered to {msg.topic()} [{msg.partition()}]")


def process_event(event: dict, trace_id: str = "unknown", otel_context=None):
    with tracer.start_as_current_span("security_scan", context=otel_context) as span:
        span.set_attribute("prguard.trace_id", trace_id)

        t0 = time.perf_counter()
        pr_number = event["pr_number"]
        repo = event["repo_full_name"]
        print(f"[Security Scanner] Scanning PR #{pr_number} from {repo}")

        t_diff = time.perf_counter()
        diff = fetch_pr_diff(event["diff_url"])
        diff_ms = (time.perf_counter() - t_diff) * 1000
        log_event("fetch_diff", trace_id, pr_number=pr_number, duration_ms=round(diff_ms, 2))
        record_stage("fetch_diff", diff_ms)

        findings = scan_diff(diff)

        high_count = sum(1 for f in findings if f["severity"] == "high")
        print(f"[Security Scanner] PR #{pr_number}: {len(findings)} findings ({high_count} high)")

        result = {
            "type": "security_scan",
            "pr_number": pr_number,
            "repo_full_name": repo,
            "head_sha": event["head_sha"],
            "html_url": event["html_url"],
            "findings": findings,
            "passed": high_count == 0,
        }

        producer.produce(
            "security-results",
            key=str(pr_number),
            value=json.dumps(result),
            headers=inject_otel_headers([("trace_id", trace_id.encode())]),
            callback=delivery_report,
        )
        producer.flush()

        total_ms = (time.perf_counter() - t0) * 1000
        log_event("security_scan", trace_id, pr_number=pr_number, duration_ms=round(total_ms, 2))
        record_stage("security_scan", total_ms)


def main():
    start_http_server(9100)
    consumer.subscribe(["pr-events"], on_assign=on_assign, on_revoke=on_revoke)
    READY_FILE.touch()   # Readiness: 成功订阅 Kafka topic，可以接收消息了
    print("[Security Scanner] Started, waiting for PR events...")

    try:
        while True:
            # Liveness: 只有真的持有 partition 分配时才更新时间戳
            if _has_assignment:
                HEALTHY_FILE.touch()
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[Security Scanner] Consumer error: {msg.error()}")
                continue

            raw = msg.value().decode("utf-8")
            trace_id = extract_trace_id(msg)
            otel_context = extract_otel_context(msg)

            try:
                event = json.loads(raw)
            except json.JSONDecodeError as e:
                print(f"[Security Scanner] Invalid JSON: {e}")
                send_to_dlq(KAFKA_BOOTSTRAP_SERVERS, "security-scanner", raw, str(e),
                            "PermanentFailure", 1, trace_id)
                consumer.commit(msg)
                continue

            done = process_with_retry(
                "security-scanner", KAFKA_BOOTSTRAP_SERVERS,
                process_event, event, raw, trace_id, otel_context,
            )
            if done:
                consumer.commit(msg)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
