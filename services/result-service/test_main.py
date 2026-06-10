from unittest.mock import MagicMock, patch

from github_client import format_ai_comment, format_security_comment
from main import handle_ai_result, handle_security_result, process_with_retry


AI_DATA = {
    "type": "ai_review",
    "pr_number": 10,
    "repo_full_name": "owner/repo",
    "head_sha": "abc123",
    "html_url": "https://github.com/owner/repo/pull/10",
    "review": {
        "summary": "Adds authentication endpoint",
        "score": 8,
        "issues": [
            {"severity": "medium", "line": "auth.py:10", "comment": "Missing input validation"}
        ],
        "suggestions": ["Add rate limiting"],
        "approved": True,
    },
}

SECURITY_DATA = {
    "type": "security_scan",
    "pr_number": 10,
    "repo_full_name": "owner/repo",
    "head_sha": "abc123",
    "html_url": "https://github.com/owner/repo/pull/10",
    "findings": [
        {"severity": "high", "rule": "hardcoded_password", "message": "Hardcoded password", "line": 5, "content": 'password = "secret"'}
    ],
    "passed": False,
}


def test_format_ai_comment_approved():
    comment = format_ai_comment(AI_DATA["review"])
    assert "✅ Approved" in comment
    assert "8/10" in comment
    assert "Missing input validation" in comment
    assert "Add rate limiting" in comment


def test_format_ai_comment_rejected():
    review = {**AI_DATA["review"], "score": 3, "approved": False}
    comment = format_ai_comment(review)
    assert "❌ Changes Requested" in comment
    assert "🔴" in comment  # low score emoji


def test_format_security_comment_with_findings():
    comment = format_security_comment(SECURITY_DATA["findings"], passed=False)
    assert "⚠️" in comment
    assert "Hardcoded password" in comment
    assert "🔴" in comment


def test_format_security_comment_passed():
    comment = format_security_comment([], passed=True)
    assert "✅ No issues found" in comment


@patch("main.mark_ai_comment_posted")
@patch("main.is_ai_comment_posted", return_value=False)
@patch("main.save_ai_review", return_value=1)
@patch("main.post_pr_comment")
def test_handle_ai_result_posts_comment(mock_post, mock_save, mock_check, mock_mark):
    handle_ai_result(AI_DATA)
    mock_post.assert_called_once()
    mock_save.assert_called_once()
    mock_mark.assert_called_once()
    args = mock_post.call_args[0]
    assert args[0] == "owner/repo"
    assert args[1] == 10
    assert "AI Code Review" in args[2]


@patch("main.mark_ai_comment_posted")
@patch("main.is_ai_comment_posted", return_value=True)
@patch("main.save_ai_review", return_value=1)
@patch("main.post_pr_comment")
def test_handle_ai_result_skips_duplicate(mock_post, mock_save, mock_check, mock_mark):
    handle_ai_result(AI_DATA)
    mock_post.assert_not_called()
    mock_save.assert_not_called()


@patch("main.mark_security_comment_posted")
@patch("main.is_security_comment_posted", return_value=False)
@patch("main.save_security_scan", return_value=1)
@patch("main.post_pr_comment")
def test_handle_security_result_posts_comment(mock_post, mock_save, mock_check, mock_mark):
    handle_security_result(SECURITY_DATA)
    mock_post.assert_called_once()
    mock_save.assert_called_once()
    mock_mark.assert_called_once()
    args = mock_post.call_args[0]
    assert args[0] == "owner/repo"
    assert args[1] == 10
    assert "Security Scan" in args[2]


@patch("main.mark_security_comment_posted")
@patch("main.is_security_comment_posted", return_value=True)
@patch("main.save_security_scan", return_value=1)
@patch("main.post_pr_comment")
def test_handle_security_result_skips_duplicate(mock_post, mock_save, mock_check, mock_mark):
    handle_security_result(SECURITY_DATA)
    mock_post.assert_not_called()
    mock_save.assert_not_called()


# ── DLQ / retry tests ────────────────────────────────────────────────────────

@patch("main.send_to_dlq")
def test_process_with_retry_success(mock_dlq):
    """Handler 成功 → 不发 DLQ"""
    handler = MagicMock()
    process_with_retry(handler, AI_DATA, "{}")
    handler.assert_called_once()
    mock_dlq.assert_not_called()


@patch("main.send_to_dlq")
def test_process_with_retry_permanent_failure(mock_dlq):
    """KeyError → 永久失败，只尝试一次，直接进 DLQ"""
    handler = MagicMock(side_effect=KeyError("pr_number"))
    process_with_retry(handler, AI_DATA, "{}")
    handler.assert_called_once()          # 不重试
    mock_dlq.assert_called_once()
    assert mock_dlq.call_args[0][2] == "PermanentFailure"


@patch("main.time.sleep")
@patch("main.send_to_dlq")
def test_process_with_retry_exhausted(mock_dlq, mock_sleep):
    """Exception 重试 3 次后仍失败 → 进 DLQ"""
    handler = MagicMock(side_effect=Exception("GitHub API timeout"))
    process_with_retry(handler, AI_DATA, "{}")
    assert handler.call_count == 3        # 重试了 3 次
    assert mock_sleep.call_count == 2     # 只在第 1、2 次失败后 sleep
    mock_dlq.assert_called_once()
    assert mock_dlq.call_args[0][2] == "TemporaryFailure"


@patch("main.time.sleep")
@patch("main.send_to_dlq")
def test_process_with_retry_succeeds_on_second_attempt(mock_dlq, mock_sleep):
    """第一次失败，第二次成功 → 不进 DLQ"""
    handler = MagicMock(side_effect=[Exception("timeout"), None])
    process_with_retry(handler, AI_DATA, "{}")
    assert handler.call_count == 2
    mock_dlq.assert_not_called()
    mock_sleep.assert_called_once()       # 只在第 1 次失败后 sleep
