"""add probe execution mode

Revision ID: b731e11e3a20
Revises: 8953b4278bf6
Create Date: 2026-08-09 18:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "b731e11e3a20"
down_revision: str | None = "8953b4278bf6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("probe_plans") as batch_op:
        batch_op.add_column(
            sa.Column("execution_mode", sa.String(length=24), nullable=False, server_default="chat")
        )
    with op.batch_alter_table("probe_runs") as batch_op:
        batch_op.add_column(
            sa.Column("execution_mode", sa.String(length=24), nullable=False, server_default="chat")
        )


def downgrade() -> None:
    with op.batch_alter_table("probe_runs") as batch_op:
        batch_op.drop_column("execution_mode")
    with op.batch_alter_table("probe_plans") as batch_op:
        batch_op.drop_column("execution_mode")
