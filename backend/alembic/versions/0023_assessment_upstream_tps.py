"""add upstream TPS aggregates to account assessments

Revision ID: d7f2b4a6c801
Revises: c5e8a1f3d702
Create Date: 2026-08-27 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "d7f2b4a6c801"
down_revision: str | Sequence[str] | None = "c5e8a1f3d702"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("account_assessments") as batch_op:
        batch_op.add_column(
            sa.Column("avg_upstream_tps", sa.Float(), nullable=False, server_default="0")
        )
        batch_op.add_column(
            sa.Column("max_upstream_tps", sa.Float(), nullable=False, server_default="0")
        )
        batch_op.add_column(
            sa.Column(
                "latest_upstream_tps", sa.Float(), nullable=False, server_default="0"
            )
        )


def downgrade() -> None:
    with op.batch_alter_table("account_assessments") as batch_op:
        batch_op.drop_column("latest_upstream_tps")
        batch_op.drop_column("max_upstream_tps")
        batch_op.drop_column("avg_upstream_tps")
