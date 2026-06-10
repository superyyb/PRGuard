from unittest.mock import MagicMock, patch

from main import should_notify, build_email_body, process_event

MOCK_EVENT = {
    "pr_number": 5,
    "repo_full_name": "superyyb/ssh-testkit",
    "html_url": "https://github.com/superyyb/ssh-testkit/pull/5",
    "review": {
        "summary": "Adds login endpoint",
        "score": 4,
        "issues": [
            {"severity": "high", "line": "auth.py:42", "comment": "Password in plaintext"}
        ],
        "suggestions": ["Use bcrypt"],
        "approved": False,
    },
}


def test_should_notify_low_score():
    review = {"score": 4, "issues": []}
    assert should_notify(review) is True


def test_should_notify_high_issue():
    review = {"score": 8, "issues": [{"severity": "high", "line": "x", "comment": "bug"}]}
    assert should_notify(review) is True


def test_should_not_notify_good_code():
    review = {"score": 8, "issues": [{"severity": "low", "line": "x", "comment": "style"}]}
    assert should_notify(review) is False


def test_build_email_body_contains_pr_info():
    body = build_email_body(5, "superyyb/repo", "https://github.com/pull/5", MOCK_EVENT["review"])
    assert "PR #5" in body
    assert "superyyb/repo" in body
    assert "https://github.com/pull/5" in body
    assert "4/10" in body


@patch("main.send_email")
def test_process_event_sends_email_for_low_score(mock_send):
    process_event(MOCK_EVENT)
    mock_send.assert_called_once()
    subject = mock_send.call_args[0][0]
    assert "PR #5" in subject
    assert "4/10" in subject


@patch("main.send_email")
def test_process_event_no_email_for_good_pr(mock_send):
    good_event = {
        "pr_number": 6,
        "repo_full_name": "superyyb/repo",
        "html_url": "https://github.com/pull/6",
        "review": {"score": 9, "issues": [], "suggestions": [], "approved": True, "summary": "LGTM"},
    }
    process_event(good_event)
    mock_send.assert_not_called()
