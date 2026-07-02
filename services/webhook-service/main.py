import hashlib
import hmac
import json
import os

from confluent_kafka import Producer
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

load_dotenv()

app = FastAPI(title="Webhook Service")

WEBHOOK_SECRET = os.getenv("GITHUB_WEBHOOK_SECRET", "")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


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
):
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

    producer.produce(
        "pr-events",
        key=str(pr["number"]),
        value=json.dumps(event),
        callback=delivery_report,
    )
    producer.flush()

    print(f"[Webhook] Published PR #{pr['number']} from {data['repository']['full_name']}")
    return JSONResponse({"status": "published", "pr_number": pr["number"]})


@app.get("/health")
def health():
    return {"status": "ok"}
