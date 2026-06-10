import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/pr_reviews")


def get_connection():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def init_db():
    """创建表结构（如果不存在）"""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS pr_reviews (
                    id              SERIAL PRIMARY KEY,
                    repo_full_name  TEXT NOT NULL,
                    pr_number       INTEGER NOT NULL,
                    head_sha        TEXT NOT NULL,
                    ai_score        INTEGER,
                    ai_approved     BOOLEAN,
                    ai_summary      TEXT,
                    ai_comment_posted    BOOLEAN DEFAULT FALSE,
                    security_passed BOOLEAN,
                    security_findings_count INTEGER,
                    security_comment_posted BOOLEAN DEFAULT FALSE,
                    created_at      TIMESTAMP DEFAULT NOW(),
                    UNIQUE(repo_full_name, pr_number, head_sha)
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS security_findings (
                    id        SERIAL PRIMARY KEY,
                    review_id INTEGER REFERENCES pr_reviews(id) ON DELETE CASCADE,
                    rule      TEXT NOT NULL,
                    severity  TEXT NOT NULL,
                    message   TEXT NOT NULL,
                    line      INTEGER,
                    content   TEXT
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_pr_reviews_repo_pr
                ON pr_reviews(repo_full_name, pr_number);
            """)
        conn.commit()
    print("[DB] Tables initialized")


def is_ai_comment_posted(repo_full_name: str, pr_number: int, head_sha: str) -> bool:
    """检查这个 commit 的 AI review comment 是否已经发过"""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ai_comment_posted FROM pr_reviews
                WHERE repo_full_name = %s AND pr_number = %s AND head_sha = %s
            """, (repo_full_name, pr_number, head_sha))
            row = cur.fetchone()
            return row is not None and row["ai_comment_posted"] is True


def is_security_comment_posted(repo_full_name: str, pr_number: int, head_sha: str) -> bool:
    """检查这个 commit 的 Security comment 是否已经发过"""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT security_comment_posted FROM pr_reviews
                WHERE repo_full_name = %s AND pr_number = %s AND head_sha = %s
            """, (repo_full_name, pr_number, head_sha))
            row = cur.fetchone()
            return row is not None and row["security_comment_posted"] is True


def mark_ai_comment_posted(repo_full_name: str, pr_number: int, head_sha: str):
    """标记 AI comment 已发送"""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE pr_reviews SET ai_comment_posted = TRUE
                WHERE repo_full_name = %s AND pr_number = %s AND head_sha = %s
            """, (repo_full_name, pr_number, head_sha))
        conn.commit()


def mark_security_comment_posted(repo_full_name: str, pr_number: int, head_sha: str):
    """标记 Security comment 已发送"""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE pr_reviews SET security_comment_posted = TRUE
                WHERE repo_full_name = %s AND pr_number = %s AND head_sha = %s
            """, (repo_full_name, pr_number, head_sha))
        conn.commit()


def save_ai_review(repo_full_name: str, pr_number: int, head_sha: str, review: dict) -> int:
    """
    插入或更新 AI review 结果，返回 review id。
    同一个 PR 的同一个 commit 重复收到时直接覆盖。
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pr_reviews
                    (repo_full_name, pr_number, head_sha, ai_score, ai_approved, ai_summary)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING id
            """, (
                repo_full_name,
                pr_number,
                head_sha,
                review.get("score"),
                review.get("approved"),
                review.get("summary"),
            ))
            row = cur.fetchone()

            if row is None:
                # 记录已存在，更新 AI 字段
                cur.execute("""
                    UPDATE pr_reviews
                    SET ai_score = %s, ai_approved = %s, ai_summary = %s
                    WHERE repo_full_name = %s AND pr_number = %s AND head_sha = %s
                    RETURNING id
                """, (
                    review.get("score"),
                    review.get("approved"),
                    review.get("summary"),
                    repo_full_name,
                    pr_number,
                    head_sha,
                ))
                row = cur.fetchone()

        conn.commit()
    return row["id"]


def save_security_scan(repo_full_name: str, pr_number: int, head_sha: str,
                       findings: list, passed: bool) -> int:
    """
    插入或更新安全扫描结果，返回 review id。
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pr_reviews
                    (repo_full_name, pr_number, head_sha, security_passed, security_findings_count)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING id
            """, (repo_full_name, pr_number, head_sha, passed, len(findings)))
            row = cur.fetchone()

            if row is None:
                cur.execute("""
                    UPDATE pr_reviews
                    SET security_passed = %s, security_findings_count = %s
                    WHERE repo_full_name = %s AND pr_number = %s AND head_sha = %s
                    RETURNING id
                """, (passed, len(findings), repo_full_name, pr_number, head_sha))
                row = cur.fetchone()

            review_id = row["id"]

            # 先删旧的 findings，再插新的（保证数据最新）
            cur.execute("DELETE FROM security_findings WHERE review_id = %s", (review_id,))
            for f in findings:
                cur.execute("""
                    INSERT INTO security_findings (review_id, rule, severity, message, line, content)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (review_id, f["rule"], f["severity"], f["message"], f["line"], f["content"]))

        conn.commit()
    return review_id
