"""add durable grok-register webhook inbox

Revision ID: 9a4e7c2d1f80
Revises: 6b9d2e4f7a10
Create Date: 2026-08-11 18:30:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "9a4e7c2d1f80"
down_revision: str | None = "6b9d2e4f7a10"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "probe_runs",
        sa.Column("source_event_id", sa.String(length=120), nullable=True),
    )
    op.create_index(
        "ix_probe_runs_source_event_id",
        "probe_runs",
        ["source_event_id"],
        unique=False,
    )
    op.create_table(
        "register_webhook_events",
        sa.Column("event_id", sa.String(length=120), nullable=False),
        sa.Column("event_type", sa.String(length=80), nullable=False, server_default=""),
        sa.Column("registration_id", sa.String(length=120), nullable=False, server_default=""),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("grok2api_account_id", sa.Integer(), nullable=True),
        sa.Column("bot_risk", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("bfs", sa.String(length=120), nullable=False, server_default=""),
        sa.Column("occurred_at", sa.String(length=80), nullable=False, server_default=""),
        sa.Column("status", sa.String(length=24), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=False, server_default=""),
        sa.Column("resolved_account_id", sa.Integer(), nullable=True),
        sa.Column("run_ids", sa.JSON(), nullable=False, server_default=sa.text("'[]'")),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("event_id"),
    )
    op.create_index(
        "ix_register_webhook_events_email",
        "register_webhook_events",
        ["email"],
        unique=False,
    )
    op.create_index(
        "ix_register_webhook_due",
        "register_webhook_events",
        ["status", "next_attempt_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_register_webhook_due", table_name="register_webhook_events")
    op.drop_index(
        "ix_register_webhook_events_email", table_name="register_webhook_events"
    )
    op.drop_table("register_webhook_events")
    op.drop_index("ix_probe_runs_source_event_id", table_name="probe_runs")
    op.drop_column("probe_runs", "source_event_id")
