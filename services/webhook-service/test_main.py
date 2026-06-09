import hashlib
import hmac
import json
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from main import app, WEBHOOK_SECRET

client = TestClient(app)


def make_signature(payload: bytes) -> str:
    secret = WEBHOOK_SECRET or "test-secret"
    sig = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
    return f"sha256={sig}"


PR_OPENED_PAYLOAD = {
    "action": "opened",
    "pull_request": {
        "number": 42,
        "title": "Add feature X",
        "head": {"sha": "abc123"},
        "base": {"sha": "def456"},
        "diff_url": "https://github.com/owner/repo/pull/42.diff",
        "html_url": "https://github.com/owner/repo/pull/42",
    },
    "repository": {"full_name": "owner/repo"},
}


@patch("main.producer")
def test_webhook_pr_opened(mock_producer):
    mock_producer.produce = MagicMock()
    mock_producer.flush = MagicMock()

    payload = json.dumps(PR_OPENED_PAYLOAD).encode()
    response = client.post(
        "/webhook",
        content=payload,
        headers={
            "X-GitHub-Event": "pull_request",
            "X-Hub-Signature-256": make_signature(payload),
            "Content-Type": "application/json",
        },
    )
    assert response.status_code == 200
    assert response.json()["status"] == "published"
    mock_producer.produce.assert_called_once()


@patch("main.producer")
def test_webhook_ignores_non_pr_events(mock_producer):
    payload = json.dumps({"action": "created"}).encode()
    response = client.post(
        "/webhook",
        content=payload,
        headers={
            "X-GitHub-Event": "push",
            "X-Hub-Signature-256": make_signature(payload),
            "Content-Type": "application/json",
        },
    )
    assert response.status_code == 200
    assert response.json()["status"] == "ignored"
    mock_producer.produce.assert_not_called()


@patch("main.producer")
def test_webhook_ignores_closed_action(mock_producer):
    payload = json.dumps({**PR_OPENED_PAYLOAD, "action": "closed"}).encode()
    response = client.post(
        "/webhook",
        content=payload,
        headers={
            "X-GitHub-Event": "pull_request",
            "X-Hub-Signature-256": make_signature(payload),
            "Content-Type": "application/json",
        },
    )
    assert response.status_code == 200
    assert response.json()["status"] == "ignored"


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
