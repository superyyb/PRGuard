import hashlib
import hmac
import json
import logging
import os
import time
import uuid

from confluent_kafka import Producer
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, make_asgi_app
from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

load_dotenv()

app = FastAPI(title="Webhook Service")
app.mount("/metrics", make_asgi_app())

WEBHOOK_SECRET = os.getenv("GITHUB_WEBHOOK_SECRET", "")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("webhook-service")

provider = TracerProvider(resource=Resource.create({"service.name": "webhook-service"}))
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{OTEL_ENDPOINT}/v1/traces")))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("webhook-service")
FastAPIInstrumentor.instrument_app(app)


def inject_otel_headers(headers: list) -> list:
    carrier = {}
    propagate.inject(carrier)
    headers.extend((k, v.encode()) for k, v in carrier.items())
    return headers

STAGE_DURATION = Histogram(
    "pr_stage_duration_seconds", "Duration of each pipeline stage",
    ["service", "stage"],
    buckets=(0.1, 0.25, 0.5, 1, 2, 3, 5, 7.5, 10, 15, 20, 30),
)
STAGE_TOTAL = Counter(
    "pr_stage_total", "Count of pipeline stage outcomes",
    ["service", "stage", "outcome"],
)


def log_event(stage: str, trace_id: str, **fields):
    logger.info(json.dumps({"service": "webhook-service", "trace_id": trace_id, "stage": stage, **fields}))


def record_stage(stage: str, duration_ms: float, outcome: str = "success"):
    STAGE_DURATION.labels(service="webhook-service", stage=stage).observe(duration_ms / 1000)
    STAGE_TOTAL.labels(service="webhook-service", stage=stage, outcome=outcome).inc()


def verify_signature(payload: bytes, signature: str) -> bool:
    """验证 GitHub Webhook HMAC-SHA256 签名"""
    if not WEBHOOK_SECRET:
        return True  # 本地开发跳过验证
    expected = "sha256=" + hmac.new(
        WEBHOOK_SECRET.encode(), payload, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


def delivery_report(err, msg):
    if err:
        print(f"[Kafka] Delivery failed: {err}")
    else:
        print(f"[Kafka] Delivered to {msg.topic()} [{msg.partition()}]")


@app.post("/webhook")
async def github_webhook(
    request: Request,
    x_github_event: str = Header(None),
    x_hub_signature_256: str = Header(None),
    x_github_delivery: str = Header(None),
):
    t0 = time.perf_counter()
    trace_id = x_github_delivery or str(uuid.uuid4())
    payload = await request.body()

    # 验签
    if x_hub_signature_256 and not verify_signature(payload, x_hub_signature_256):
        raise HTTPException(status_code=401, detail="Invalid signature")

    # 只处理 pull_request 事件
    if x_github_event != "pull_request":
        return JSONResponse({"status": "ignored", "event": x_github_event})

    data = json.loads(payload)
    action = data.get("action")

    # 只在 PR 打开或更新时触发审查
    if action not in ("opened", "synchronize", "reopened"):
        return JSONResponse({"status": "ignored", "action": action})

    pr = data["pull_request"]
    event = {
        "pr_number": pr["number"],
        "title": pr["title"],
        "repo_full_name": data["repository"]["full_name"],
        "head_sha": pr["head"]["sha"],
        "base_sha": pr["base"]["sha"],
        "diff_url": f"https://api.github.com/repos/{data['repository']['full_name']}/pulls/{pr['number']}",
        "html_url": pr["html_url"],
        "action": action,
    }

    current_span = trace.get_current_span()
    current_span.set_attribute("prguard.trace_id", trace_id)

    producer.produce(
        "pr-events",
        key=str(pr["number"]),
        value=json.dumps(event),
        headers=inject_otel_headers([("trace_id", trace_id.encode())]),
        callback=delivery_report,
    )
    producer.flush()

    duration_ms = (time.perf_counter() - t0) * 1000
    log_event(
        "webhook_publish",
        trace_id,
        pr_number=pr["number"],
        repo=data["repository"]["full_name"],
        duration_ms=round(duration_ms, 2),
    )
    record_stage("webhook_publish", duration_ms)
    return JSONResponse({"status": "published", "pr_number": pr["number"]})


@app.get("/health")
def health():
    return {"status": "ok"}
