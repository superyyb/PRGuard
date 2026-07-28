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
                CREATE TABLE IF NOT EXISTS notifications_sent (
                    id              SERIAL PRIMARY KEY,
                    repo_full_name  TEXT NOT NULL,
                    pr_number       INTEGER NOT NULL,
                    head_sha        TEXT NOT NULL,
                    sent_at         TIMESTAMP DEFAULT NOW(),
                    UNIQUE(repo_full_name, pr_number, head_sha)
                );
            """)
        conn.commit()
    print("[DB] Notifier tables initialized")


def is_email_sent(repo_full_name: str, pr_number: int, head_sha: str) -> bool:
    """检查这个 commit 的报警邮件是否已经发过，避免消息重复投递导致重复报警"""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM notifications_sent
                WHERE repo_full_name = %s AND pr_number = %s AND head_sha = %s
            """, (repo_full_name, pr_number, head_sha))
            return cur.fetchone() is not None


def mark_email_sent(repo_full_name: str, pr_number: int, head_sha: str):
    """标记报警邮件已发送"""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO notifications_sent (repo_full_name, pr_number, head_sha)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (repo_full_name, pr_number, head_sha))
        conn.commit()
