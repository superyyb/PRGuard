"""Baseline: converge any existing pr_reviews/security_findings schema to the
current expected shape, regardless of what state the database is already in
(fresh install, or one of the manually-patched databases missing the
ai_comment_posted/security_comment_posted columns and the unique constraint).

Revision ID: 0001_baseline
Revises:
Create Date: 2026-07-28

"""
from alembic import op

revision = "0001_baseline"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS pr_reviews (
            id              SERIAL PRIMARY KEY,
            repo_full_name  TEXT NOT NULL,
            pr_number       INTEGER NOT NULL,
            head_sha        TEXT NOT NULL,
            ai_score        INTEGER,
            ai_approved     BOOLEAN,
            ai_summary      TEXT,
            security_passed BOOLEAN,
            security_findings_count INTEGER,
            created_at      TIMESTAMP DEFAULT NOW()
        );
    """)
    op.execute("""
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
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_pr_reviews_repo_pr
        ON pr_reviews(repo_full_name, pr_number);
    """)

    # 老库缺的两个字段，新库这里也是第一次加
    op.execute("ALTER TABLE pr_reviews ADD COLUMN IF NOT EXISTS ai_comment_posted BOOLEAN DEFAULT FALSE;")
    op.execute("ALTER TABLE pr_reviews ADD COLUMN IF NOT EXISTS security_comment_posted BOOLEAN DEFAULT FALSE;")

    # 去重：一直没有唯一约束，ON CONFLICT DO NOTHING 从来没生效过，
    # 同一个 (repo, pr_number, head_sha) 可能已经堆了好几行，只保留 id 最大（最新）的一行
    op.execute("""
        DELETE FROM pr_reviews a
        USING pr_reviews b
        WHERE a.repo_full_name = b.repo_full_name
          AND a.pr_number = b.pr_number
          AND a.head_sha = b.head_sha
          AND a.id < b.id;
    """)

    # 去重完再加约束，按名字检查是否已存在，新库/老库都安全、可重复执行
    op.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.table_constraints
                WHERE table_name = 'pr_reviews'
                  AND constraint_name = 'pr_reviews_unique_repo_pr_sha'
            ) THEN
                ALTER TABLE pr_reviews
                ADD CONSTRAINT pr_reviews_unique_repo_pr_sha
                UNIQUE (repo_full_name, pr_number, head_sha);
            END IF;
        END $$;
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE pr_reviews DROP CONSTRAINT IF EXISTS pr_reviews_unique_repo_pr_sha;")
    op.execute("ALTER TABLE pr_reviews DROP COLUMN IF EXISTS security_comment_posted;")
    op.execute("ALTER TABLE pr_reviews DROP COLUMN IF EXISTS ai_comment_posted;")
