"""add probe run account created at

Revision ID: 8c1f4a9b2d70
Revises: 5c8f1a2d9e40
Create Date: 2026-08-14 12:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "8c1f4a9b2d70"
down_revision: str | None = "5c8f1a2d9e40"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("probe_runs") as batch_op:
        batch_op.add_column(
            sa.Column("account_created_at", sa.DateTime(timezone=True), nullable=True)
        )


def downgrade() -> None:
    with op.batch_alter_table("probe_runs") as batch_op:
        batch_op.drop_column("account_created_at")
