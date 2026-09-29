"""add local request audit projection

Revision ID: d4a7c9e2f601
Revises: c7a1e5d9b203
Create Date: 2026-08-16 12:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "d4a7c9e2f601"
down_revision: str | Sequence[str] | None = "c7a1e5d9b203"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "request_audit_records",
        sa.Column("upstream_id", sa.String(length=120), nullable=False),
        sa.Column("request_id", sa.String(length=255), nullable=False),
        sa.Column("day_key", sa.String(length=16), nullable=False),
        sa.Column("provider", sa.String(length=40), nullable=False),
        sa.Column("operation", sa.String(length=32), nullable=False),
        sa.Column("model_public_id", sa.String(length=255), nullable=False),
        sa.Column("model_upstream_model", sa.String(length=255), nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=True),
        sa.Column("account_name", sa.String(length=255), nullable=False),
        sa.Column("egress_node_id", sa.Integer(), nullable=True),
        sa.Column("egress_node_name", sa.String(length=255), nullable=False),
        sa.Column("egress_ip", sa.String(length=255), nullable=False),
        sa.Column("egress_mode", sa.String(length=24), nullable=False),
        sa.Column("egress_scope", sa.String(length=48), nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=False),
        sa.Column("streaming", sa.Boolean(), nullable=False),
        sa.Column("input_tokens", sa.BigInteger(), nullable=False),
        sa.Column("output_tokens", sa.BigInteger(), nullable=False),
        sa.Column("reasoning_tokens", sa.BigInteger(), nullable=False),
        sa.Column("total_tokens", sa.BigInteger(), nullable=False),
        sa.Column("first_token_ms", sa.BigInteger(), nullable=True),
        sa.Column("duration_ms", sa.BigInteger(), nullable=False),
        sa.Column("tps", sa.Float(), nullable=True),
        sa.Column("risk_level", sa.String(length=24), nullable=False),
        sa.Column("risk_reasons", sa.JSON(), nullable=False),
        sa.Column("raw", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("fetched_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("upstream_id"),
    )
    op.create_index(
        "ix_request_audit_created_at",
        "request_audit_records",
        ["created_at"],
        unique=False,
    )
    op.create_index(
        "ix_request_audit_day_account",
        "request_audit_records",
        ["day_key", "account_id"],
        unique=False,
    )
    op.create_index(
        "ix_request_audit_day_created",
        "request_audit_records",
        ["day_key", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_request_audit_day_egress_ip",
        "request_audit_records",
        ["day_key", "egress_ip"],
        unique=False,
    )
    op.create_index(
        "ix_request_audit_day_egress_node",
        "request_audit_records",
        ["day_key", "egress_node_id"],
        unique=False,
    )
    op.create_index(
        "ix_request_audit_day_tps",
        "request_audit_records",
        ["day_key", "tps"],
        unique=False,
    )

    op.create_table(
        "request_audit_scan_states",
        sa.Column("scope", sa.String(length=80), nullable=False),
        sa.Column("day_key", sa.String(length=16), nullable=False),
        sa.Column("newest_upstream_id", sa.String(length=120), nullable=False),
        sa.Column("newest_created_at", sa.DateTime(), nullable=True),
        sa.Column("initial_cursor", sa.Text(), nullable=False),
        sa.Column("initial_complete", sa.Boolean(), nullable=False),
        sa.Column("last_scan_at", sa.DateTime(), nullable=True),
        sa.Column("last_success_at", sa.DateTime(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=False),
        sa.Column("last_pages", sa.Integer(), nullable=False),
        sa.Column("last_new_records", sa.Integer(), nullable=False),
        sa.Column("last_seen_records", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("scope"),
    )


def downgrade() -> None:
    op.drop_table("request_audit_scan_states")
    op.drop_table("request_audit_records")
