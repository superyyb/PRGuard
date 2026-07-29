import os
import pathlib

import psycopg2
from alembic import command
from alembic.config import Config
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/pr_reviews")

_SERVICE_DIR = pathlib.Path(__file__).parent


def get_connection():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def run_migrations():
    """用 Alembic 把数据库 schema 收敛到当前代码期望的状态（幂等，对任意历史状态安全）"""
    cfg = Config(str(_SERVICE_DIR / "alembic.ini"))
    cfg.set_main_option("script_location", str(_SERVICE_DIR / "alembic"))
    command.upgrade(cfg, "head")
    print("[DB] Migrations applied")


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
