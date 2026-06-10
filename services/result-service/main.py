import datetime
import json
import os
import threading
import time

from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

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

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DLQ_TOPIC = "pr-events-dlq"

# Lazy-initialized DLQ producer, shared across threads
_dlq_producer = None
_dlq_lock = threading.Lock()


def get_dlq_producer() -> Producer:
    global _dlq_producer
    with _dlq_lock:
        if _dlq_producer is None:
            _dlq_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    return _dlq_producer


def send_to_dlq(raw_message: str, error: str, error_type: str, attempts: int):
    """把失败的消息发到 Dead Letter Queue，保留原始内容 + 错误信息。"""
    producer = get_dlq_producer()
    dlq_payload = json.dumps({
        "original_message": raw_message,
        "error": error,
        "error_type": error_type,
        "attempts": attempts,
        "failed_at": datetime.datetime.utcnow().isoformat(),
    })
    producer.produce(DLQ_TOPIC, dlq_payload.encode("utf-8"))
    producer.flush()
    print(f"[Result Service] 📨 Sent to DLQ ({error_type}, {attempts} attempts): {error}")


def process_with_retry(handler, data: dict, raw: str, max_retries: int = 3):
    """
    区分暂时失败和永久失败：
    - KeyError / ValueError → 永久失败（消息结构错误），直接进 DLQ，不重试
    - 其他 Exception      → 暂时失败（网络/API 超时），指数退避重试，耗尽后进 DLQ
    """
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[Result Service] Processing PR #{data.get('pr_number', '?')} "
                  f"attempt {attempt}/{max_retries}")
            handler(data)
            print(f"[Result Service] ✅ PR #{data.get('pr_number', '?')} processed successfully")
            return
        except (KeyError, ValueError) as e:
            # 永久失败：消息本身有问题，重试也没用
            print(f"[Result Service] ❌ Permanent failure: {e}")
            send_to_dlq(raw, str(e), "PermanentFailure", attempt)
            return
        except Exception as e:
            if attempt < max_retries:
                wait = 2 ** attempt  # 2s → 4s
                print(f"[Result Service] ⚠️ Attempt {attempt} failed: {e}, "
                      f"retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"[Result Service] ❌ All {max_retries} retries exhausted: {e}")
                send_to_dlq(raw, str(e), "TemporaryFailure", attempt)


def make_consumer(group_id: str) -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
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

            # JSON 解析失败 = 永久失败，直接进 DLQ
            try:
                data = json.loads(raw)
            except json.JSONDecodeError as e:
                print(f"[Result Service] ❌ Invalid JSON: {e}")
                send_to_dlq(raw, str(e), "PermanentFailure", 1)
                continue

            process_with_retry(handler, data, raw)
    finally:
        consumer.close()


def handle_ai_result(data: dict):
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


def handle_security_result(data: dict):
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


def main():
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
