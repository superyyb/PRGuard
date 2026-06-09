import json
import os
import threading

from confluent_kafka import Consumer
from dotenv import load_dotenv

from database import init_db, save_ai_review, save_security_scan
from github_client import (
    format_ai_comment,
    format_security_comment,
    post_pr_comment,
)

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


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

            data = json.loads(msg.value().decode("utf-8"))
            try:
                handler(data)
            except Exception as e:
                print(f"[Result Service] Error handling message: {e}")
    finally:
        consumer.close()


def handle_ai_result(data: dict):
    pr_number = data["pr_number"]
    repo = data["repo_full_name"]
    review = data["review"]

    print(f"[Result Service] Posting AI review for PR #{pr_number} in {repo}")
    save_ai_review(repo, pr_number, data["head_sha"], review)
    comment = format_ai_comment(review)
    post_pr_comment(repo, pr_number, comment)
    print(f"[Result Service] AI review posted and saved for PR #{pr_number}")


def handle_security_result(data: dict):
    pr_number = data["pr_number"]
    repo = data["repo_full_name"]
    findings = data["findings"]
    passed = data["passed"]

    print(f"[Result Service] Posting security scan for PR #{pr_number} in {repo}")
    save_security_scan(repo, pr_number, data["head_sha"], findings, passed)
    comment = format_security_comment(findings, passed)
    post_pr_comment(repo, pr_number, comment)
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
