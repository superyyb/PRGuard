import json
from unittest.mock import MagicMock, patch

from main import analyze_with_ai, process_event


MOCK_REVIEW = {
    "summary": "Adds a new login endpoint",
    "score": 7,
    "issues": [
        {"severity": "high", "line": "auth.py:42", "comment": "Password stored in plaintext"}
    ],
    "suggestions": ["Use bcrypt for password hashing"],
    "approved": False,
}

MOCK_EVENT = {
    "pr_number": 1,
    "title": "Add login endpoint",
    "repo_full_name": "owner/repo",
    "head_sha": "abc123",
    "html_url": "https://github.com/owner/repo/pull/1",
}

MOCK_FILES = [
    {
        "filename": "auth.py",
        "status": "modified",
        "patch": "@@ -1,3 +1,5 @@\n+def login():\n+    pass",
        "contents_url": "https://api.github.com/repos/owner/repo/contents/auth.py?ref=abc123",
    }
]


@patch("main.anthropic_client")
def test_analyze_with_ai(mock_anthropic):
    mock_content = MagicMock()
    mock_content.text = json.dumps(MOCK_REVIEW)
    mock_anthropic.messages.create.return_value = MagicMock(content=[mock_content])

    result = analyze_with_ai("Add login endpoint", "context content here")

    assert result["score"] == 7
    assert result["approved"] is False
    assert len(result["issues"]) == 1
    mock_anthropic.messages.create.assert_called_once()


@patch("main.producer")
@patch("main.analyze_with_ai", return_value=MOCK_REVIEW)
@patch("main.fetch_file_content", return_value="def existing(): pass")
@patch("main.fetch_pr_files", return_value=MOCK_FILES)
def test_process_event(mock_files, mock_content, mock_analyze, mock_producer):
    mock_producer.produce = MagicMock()
    mock_producer.flush = MagicMock()

    process_event(MOCK_EVENT)

    mock_files.assert_called_once_with("owner/repo", 1)
    mock_analyze.assert_called_once()
    mock_producer.produce.assert_called_once()

    call_kwargs = mock_producer.produce.call_args
    topic = call_kwargs[0][0]
    value = json.loads(call_kwargs[1]["value"])

    assert topic == "ai-results"
    assert value["type"] == "ai_review"
    assert value["pr_number"] == 1
    assert value["review"]["score"] == 7


@patch("main.producer")
@patch("main.fetch_pr_files", return_value=[])   # 没有改动文件
def test_process_event_skips_empty_diff(mock_files, mock_producer):
    mock_producer.produce = MagicMock()

    process_event(MOCK_EVENT)

    mock_producer.produce.assert_not_called()
