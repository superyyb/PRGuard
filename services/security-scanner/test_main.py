import json
from unittest.mock import MagicMock, patch

from main import scan_diff, process_event


MOCK_EVENT = {
    "pr_number": 5,
    "title": "Update config",
    "repo_full_name": "owner/repo",
    "head_sha": "abc123",
    "diff_url": "https://github.com/owner/repo/pull/5.diff",
    "html_url": "https://github.com/owner/repo/pull/5",
}


def test_scan_detects_hardcoded_password():
    diff = '+password = "supersecret123"'
    findings = scan_diff(diff)
    assert any(f["rule"] == "hardcoded_password" for f in findings)
    assert any(f["severity"] == "high" for f in findings)


def test_scan_detects_aws_key():
    diff = "+aws_key = AKIAIOSFODNN7EXAMPLE"
    findings = scan_diff(diff)
    assert any(f["rule"] == "hardcoded_aws_key" for f in findings)


def test_scan_ignores_removed_lines():
    # 删除行（- 开头）不应该被扫描
    diff = '-password = "oldsecret"'
    findings = scan_diff(diff)
    assert len(findings) == 0


def test_scan_clean_diff():
    diff = '+def add(a, b):\n+    return a + b'
    findings = scan_diff(diff)
    assert len(findings) == 0


@patch("main.producer")
@patch("main.fetch_pr_diff", return_value='+password = "hardcoded123"')
def test_process_event_finds_issues(mock_diff, mock_producer):
    mock_producer.produce = MagicMock()
    mock_producer.flush = MagicMock()

    process_event(MOCK_EVENT)

    mock_producer.produce.assert_called_once()
    call_kwargs = mock_producer.produce.call_args
    value = json.loads(call_kwargs[1]["value"])

    assert value["type"] == "security_scan"
    assert value["passed"] is False
    assert len(value["findings"]) > 0


@patch("main.producer")
@patch("main.fetch_pr_diff", return_value="+def hello():\n+    return 'world'")
def test_process_event_passes_clean_code(mock_diff, mock_producer):
    mock_producer.produce = MagicMock()
    mock_producer.flush = MagicMock()

    process_event(MOCK_EVENT)

    value = json.loads(mock_producer.produce.call_args[1]["value"])
    assert value["passed"] is True
    assert len(value["findings"]) == 0
