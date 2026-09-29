"""track register SSO receipt time

Revision ID: c7a1e5d9b203
Revises: b8e2c4d6f901
Create Date: 2026-08-16 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "c7a1e5d9b203"
down_revision: str | Sequence[str] | None = "b8e2c4d6f901"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("register_webhook_events") as batch_op:
        batch_op.add_column(sa.Column("sso_received_at", sa.DateTime(), nullable=True))
    op.execute(
        "UPDATE register_webhook_events SET sso_received_at = updated_at "
        "WHERE sso != '' AND sso_received_at IS NULL"
    )
    op.create_index(
        "ix_register_webhook_resolved_sso_received",
        "register_webhook_events",
        ["resolved_account_id", "sso_received_at"],
        unique=False,
    )
    op.create_index(
        "ix_register_webhook_upstream_sso_received",
        "register_webhook_events",
        ["grok2api_account_id", "sso_received_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_register_webhook_upstream_sso_received",
        table_name="register_webhook_events",
    )
    op.drop_index(
        "ix_register_webhook_resolved_sso_received",
        table_name="register_webhook_events",
    )
    with op.batch_alter_table("register_webhook_events") as batch_op:
        batch_op.drop_column("sso_received_at")
