import json
import os
import threading

from confluent_kafka import Consumer
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
