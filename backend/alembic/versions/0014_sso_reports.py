"""add credential-free SSO reports

Revision ID: 5c8f1a2d9e40
Revises: 2f6a9c1d4e80
Create Date: 2026-08-13 15:30:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "5c8f1a2d9e40"
down_revision: str | None = "2f6a9c1d4e80"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "sso_reports",
        sa.Column("id", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False, server_default=""),
        sa.Column(
            "status",
            sa.String(length=24),
            nullable=False,
            server_default="queued",
        ),
        sa.Column("total", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("completed_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("valid_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("clean_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("flagged_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("invalid_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("elapsed_seconds", sa.Float(), nullable=False, server_default="0"),
        sa.Column("summary", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("results", sa.JSON(), nullable=False, server_default=sa.text("'[]'")),
        sa.Column("proxy_used", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("concurrency", sa.Integer(), nullable=False, server_default="8"),
        sa.Column(
            "request_timeout_seconds",
            sa.Integer(),
            nullable=False,
            server_default="20",
        ),
        sa.Column("error", sa.Text(), nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_sso_reports_created_at", "sso_reports", ["created_at"])


def downgrade() -> None:
    op.drop_index("ix_sso_reports_created_at", table_name="sso_reports")
    op.drop_table("sso_reports")
