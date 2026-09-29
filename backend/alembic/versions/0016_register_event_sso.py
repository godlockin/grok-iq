"""add raw SSO to register webhook events

Revision ID: b8e2c4d6f901
Revises: 8c1f4a9b2d70
Create Date: 2026-08-16 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "b8e2c4d6f901"
down_revision: str | None = "8c1f4a9b2d70"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("register_webhook_events") as batch_op:
        batch_op.add_column(
            sa.Column("sso", sa.Text(), nullable=False, server_default="")
        )


def downgrade() -> None:
    with op.batch_alter_table("register_webhook_events") as batch_op:
        batch_op.drop_column("sso")
