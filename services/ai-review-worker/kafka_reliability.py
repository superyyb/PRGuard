import datetime
import json
import threading
import time

from confluent_kafka import Producer
from prometheus_client import Counter

DLQ_TOPIC = "pr-events-dlq"

DLQ_MESSAGES = Counter(
    "dlq_messages_total", "Messages sent to the dead letter queue",
    ["service", "error_type"],
)

_dlq_producer = None
_dlq_lock = threading.Lock()


def get_dlq_producer(bootstrap_servers: str) -> Producer:
    global _dlq_producer
    with _dlq_lock:
        if _dlq_producer is None:
            _dlq_producer = Producer({"bootstrap.servers": bootstrap_servers})
    return _dlq_producer


def send_to_dlq(bootstrap_servers: str, service: str, raw_message: str, error: str,
                 error_type: str, attempts: int, trace_id: str = "unknown"):
    """把失败的消息发到 Dead Letter Queue，保留原始内容 + 错误信息。"""
    producer = get_dlq_producer(bootstrap_servers)
    dlq_payload = json.dumps({
        "service": service,
        "original_message": raw_message,
        "error": error,
        "error_type": error_type,
        "attempts": attempts,
        "trace_id": trace_id,
        "failed_at": datetime.datetime.utcnow().isoformat(),
    })
    producer.produce(DLQ_TOPIC, dlq_payload.encode("utf-8"))
    producer.flush()
    print(f"[{service}] Sent to DLQ ({error_type}, {attempts} attempts): {error}")
    DLQ_MESSAGES.labels(service=service, error_type=error_type).inc()


def process_with_retry(service: str, bootstrap_servers: str, handler, data: dict, raw: str,
                        trace_id: str = "unknown", otel_context=None, max_retries: int = 3) -> bool:
    """
    区分暂时失败和永久失败：
    - KeyError / ValueError -> 永久失败（消息结构错误），直接进 DLQ，不重试
    - 其他 Exception       -> 暂时失败（网络/API 超时），指数退避重试，耗尽后进 DLQ

    返回 True 表示消息已到达终态（处理成功，或已送入 DLQ），调用方此时才能安全地
    commit offset —— 只要还在重试中途进程崩溃，offset 不会被提前提交。
    """
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[{service}] Processing PR #{data.get('pr_number', '?')} "
                  f"attempt {attempt}/{max_retries}")
            handler(data, trace_id, otel_context)
            print(f"[{service}] PR #{data.get('pr_number', '?')} processed successfully")
            return True
        except (KeyError, ValueError) as e:
            print(f"[{service}] Permanent failure: {e}")
            send_to_dlq(bootstrap_servers, service, raw, str(e), "PermanentFailure", attempt, trace_id)
            return True
        except Exception as e:
            if attempt < max_retries:
                wait = 2 ** attempt  # 2s -> 4s
                print(f"[{service}] Attempt {attempt} failed: {e}, retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"[{service}] All {max_retries} retries exhausted: {e}")
                send_to_dlq(bootstrap_servers, service, raw, str(e), "TemporaryFailure", attempt, trace_id)
                return True
    return True
