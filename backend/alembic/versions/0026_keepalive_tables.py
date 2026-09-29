"""add keep-alive tables

Keep-alive warms upstream accounts with low-cost, randomized chat traffic. It
is intentionally isolated from the probe tables so warm requests never feed
degradation scoring or account health.

Revision ID: d5f2a8c1b740
Revises: b2d9e4a7c813
Create Date: 2026-09-29 13:10:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "d5f2a8c1b740"
down_revision: str | None = "b2d9e4a7c813"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "keepalive_accounts",
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("account_name", sa.String(length=160), nullable=False, server_default=""),
        sa.Column("account_email", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("last_sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_due_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("success_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("failure_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("skip_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("account_id"),
    )
    op.create_index(
        "ix_keepalive_accounts_next_due_at",
        "keepalive_accounts",
        ["next_due_at"],
    )
    op.create_index(
        "ix_keepalive_accounts_skip_until",
        "keepalive_accounts",
        ["skip_until"],
    )

    op.create_table(
        "keepalive_runs",
        sa.Column("id", sa.String(length=64), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("account_name", sa.String(length=160), nullable=False, server_default=""),
        sa.Column("account_email", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("request_id", sa.String(length=100), nullable=False, server_default=""),
        sa.Column("audit_id", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_code", sa.String(length=120), nullable=False, server_default=""),
        sa.Column("prompt", sa.Text(), nullable=False, server_default=""),
        sa.Column("temperature", sa.Float(), nullable=False, server_default="0"),
        sa.Column("output_tokens", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("duration_ms", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error", sa.Text(), nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_keepalive_runs_account_id", "keepalive_runs", ["account_id"])
    op.create_index("ix_keepalive_runs_status", "keepalive_runs", ["status"])
    op.create_index(
        "ix_keepalive_run_account_created",
        "keepalive_runs",
        ["account_id", "created_at"],
    )
    op.create_index(
        "ix_keepalive_run_status_created",
        "keepalive_runs",
        ["status", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_keepalive_run_status_created", table_name="keepalive_runs")
    op.drop_index("ix_keepalive_run_account_created", table_name="keepalive_runs")
    op.drop_index("ix_keepalive_runs_status", table_name="keepalive_runs")
    op.drop_index("ix_keepalive_runs_account_id", table_name="keepalive_runs")
    op.drop_table("keepalive_runs")
    op.drop_index("ix_keepalive_accounts_skip_until", table_name="keepalive_accounts")
    op.drop_index("ix_keepalive_accounts_next_due_at", table_name="keepalive_accounts")
    op.drop_table("keepalive_accounts")
