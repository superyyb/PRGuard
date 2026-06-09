from unittest.mock import MagicMock, patch, call
import psycopg2


# Mock 整个数据库连接，不需要真实 PostgreSQL
def make_mock_conn(fetchone_values):
    """构造一个返回指定值的 mock 数据库连接"""
    mock_cur = MagicMock()
    mock_cur.fetchone.side_effect = fetchone_values
    mock_cur.__enter__ = lambda s: s
    mock_cur.__exit__ = MagicMock(return_value=False)

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_conn.__enter__ = lambda s: s
    mock_conn.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cur


@patch("database.get_connection")
def test_save_ai_review_new_record(mock_get_conn):
    """新记录插入成功，返回 review id"""
    mock_conn, mock_cur = make_mock_conn(fetchone_values=[{"id": 1}])
    mock_get_conn.return_value = mock_conn

    from database import save_ai_review
    review = {"score": 8, "approved": True, "summary": "Looks good"}
    result = save_ai_review("owner/repo", 42, "abc123", review)

    assert result == 1
    mock_cur.execute.assert_called()


@patch("database.get_connection")
def test_save_ai_review_existing_record(mock_get_conn):
    """记录已存在时走 UPDATE 分支"""
    # 第一次 fetchone 返回 None（INSERT ON CONFLICT DO NOTHING 没插入）
    # 第二次 fetchone 返回 id（UPDATE 成功）
    mock_conn, mock_cur = make_mock_conn(fetchone_values=[None, {"id": 5}])
    mock_get_conn.return_value = mock_conn

    from database import save_ai_review
    review = {"score": 6, "approved": False, "summary": "Needs work"}
    result = save_ai_review("owner/repo", 42, "abc123", review)

    assert result == 5
    # 应该执行了两次 execute（INSERT + UPDATE）
    assert mock_cur.execute.call_count == 2


@patch("database.get_connection")
def test_save_security_scan_with_findings(mock_get_conn):
    """安全扫描有 findings 时正确存储"""
    mock_conn, mock_cur = make_mock_conn(fetchone_values=[{"id": 3}])
    mock_get_conn.return_value = mock_conn

    from database import save_security_scan
    findings = [
        {"rule": "hardcoded_password", "severity": "high",
         "message": "Hardcoded password", "line": 5, "content": 'password="secret"'}
    ]
    result = save_security_scan("owner/repo", 10, "def456", findings, passed=False)

    assert result == 3
    # 应该有：INSERT pr_reviews + DELETE old findings + INSERT finding
    assert mock_cur.execute.call_count == 3


@patch("database.get_connection")
def test_save_security_scan_no_findings(mock_get_conn):
    """没有安全问题时 passed=True，不插 findings"""
    mock_conn, mock_cur = make_mock_conn(fetchone_values=[{"id": 7}])
    mock_get_conn.return_value = mock_conn

    from database import save_security_scan
    result = save_security_scan("owner/repo", 10, "def456", findings=[], passed=True)

    assert result == 7
    # INSERT + DELETE，没有 finding INSERT
    assert mock_cur.execute.call_count == 2
