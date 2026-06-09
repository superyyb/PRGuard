import json
import os
import re

import httpx
from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "security-scanner-group",   # 独立 consumer group，与 AI Worker 并行消费
    "auto.offset.reset": "earliest",
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


def process_event(event: dict):
    pr_number = event["pr_number"]
    repo = event["repo_full_name"]
    print(f"[Security Scanner] Scanning PR #{pr_number} from {repo}")

    diff = fetch_pr_diff(event["diff_url"])
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
        callback=delivery_report,
    )
    producer.flush()


def main():
    consumer.subscribe(["pr-events"])
    print("[Security Scanner] Started, waiting for PR events...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[Security Scanner] Consumer error: {msg.error()}")
                continue

            event = json.loads(msg.value().decode("utf-8"))
            try:
                process_event(event)
            except Exception as e:
                print(f"[Security Scanner] Error processing PR #{event.get('pr_number')}: {e}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
