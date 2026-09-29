"""add request-audit SSO actions and reasoning-zero counters

Revision ID: e7b2c4d9a610
Revises: d4a7c9e2f601
Create Date: 2026-08-19 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "e7b2c4d9a610"
down_revision: str | Sequence[str] | None = "d4a7c9e2f601"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("account_assessments") as batch_op:
        batch_op.add_column(
            sa.Column(
                "reasoning_zero_count",
                sa.Integer(),
                nullable=False,
                server_default="0",
            )
        )

    with op.batch_alter_table("request_audit_records") as batch_op:
        batch_op.add_column(
            sa.Column(
                "media_input_images",
                sa.Integer(),
                nullable=False,
                server_default="0",
            )
        )
    op.execute(
        "UPDATE request_audit_records "
        "SET media_input_images = MAX(0, CAST(json_extract(raw, '$.mediaInputImages') AS INTEGER)) "
        "WHERE media_input_images = 0 AND json_valid(raw) "
        "AND COALESCE(json_extract(raw, '$.mediaInputImages'), 0) > 0"
    )

    with op.batch_alter_table("probe_samples") as batch_op:
        batch_op.add_column(
            sa.Column(
                "risk_rule_id",
                sa.String(length=100),
                nullable=False,
                server_default="",
            )
        )
        batch_op.add_column(
            sa.Column(
                "risk_rule_ids",
                sa.JSON(),
                nullable=False,
                server_default=sa.text("'[]'"),
            )
        )
        batch_op.add_column(
            sa.Column(
                "risk_reasons",
                sa.JSON(),
                nullable=False,
                server_default=sa.text("'[]'"),
            )
        )
    op.create_index(
        "ix_probe_samples_risk_rule_id",
        "probe_samples",
        ["risk_rule_id"],
        unique=False,
    )
    op.execute(
        "UPDATE probe_samples SET risk_rule_id = CASE classification "
        "WHEN 'elevated' THEN 'elevated_tps' "
        "WHEN 'buffered_soft' THEN 'buffered_soft' "
        "WHEN 'buffered_hard' THEN 'buffered_hard' "
        "WHEN 'fast_risk' THEN 'fast_risk' "
        "WHEN 'marker_miss' THEN 'marker_miss' "
        "WHEN 'reasoning_zero' THEN 'reasoning_zero' "
        "WHEN 'error' THEN 'http_error' "
        "ELSE risk_rule_id END"
    )

    op.create_table(
        "request_audit_account_verifications",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("audit_upstream_id", sa.String(length=120), nullable=False),
        sa.Column("audit_created_at", sa.DateTime(), nullable=False),
        sa.Column("audit_tps", sa.Float(), nullable=False, server_default="0"),
        sa.Column(
            "status",
            sa.String(length=32),
            nullable=False,
            server_default="pending",
        ),
        sa.Column(
            "sso_verdict",
            sa.String(length=40),
            nullable=False,
            server_default="",
        ),
        sa.Column(
            "bot_flag",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
        sa.Column(
            "proxy_used", sa.Boolean(), nullable=False, server_default=sa.false()
        ),
        sa.Column("valid_session", sa.Boolean(), nullable=True),
        sa.Column("email_match", sa.Boolean(), nullable=True),
        sa.Column("status_code", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("response_ms", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("check_error", sa.Text(), nullable=False, server_default=""),
        sa.Column(
            "action_status",
            sa.String(length=40),
            nullable=False,
            server_default="pending",
        ),
        sa.Column("action_error", sa.Text(), nullable=False, server_default=""),
        sa.Column(
            "egress_recommendation",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
        sa.Column("previous_priority", sa.Integer(), nullable=True),
        sa.Column("applied_priority", sa.Integer(), nullable=True),
        sa.Column("checked_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["audit_upstream_id"],
            ["request_audit_records.upstream_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "audit_upstream_id",
            name="uq_request_audit_account_verification_audit",
        ),
    )
    op.create_index(
        "ix_request_audit_verification_account_updated",
        "request_audit_account_verifications",
        ["account_id", "updated_at"],
        unique=False,
    )
    op.create_index(
        "ix_request_audit_verification_status",
        "request_audit_account_verifications",
        ["status"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_request_audit_verification_status",
        table_name="request_audit_account_verifications",
    )
    op.drop_index(
        "ix_request_audit_verification_account_updated",
        table_name="request_audit_account_verifications",
    )
    op.drop_table("request_audit_account_verifications")
    with op.batch_alter_table("request_audit_records") as batch_op:
        batch_op.drop_column("media_input_images")
    op.drop_index(
        "ix_probe_samples_risk_rule_id",
        table_name="probe_samples",
    )
    with op.batch_alter_table("probe_samples") as batch_op:
        batch_op.drop_column("risk_reasons")
        batch_op.drop_column("risk_rule_ids")
        batch_op.drop_column("risk_rule_id")
    with op.batch_alter_table("account_assessments") as batch_op:
        batch_op.drop_column("reasoning_zero_count")
