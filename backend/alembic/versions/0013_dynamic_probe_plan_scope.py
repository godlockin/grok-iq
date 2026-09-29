"""add dynamic probe plan account scope

Revision ID: 2f6a9c1d4e80
Revises: 9a4e7c2d1f80
Create Date: 2026-08-12 14:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "2f6a9c1d4e80"
down_revision: str | None = "9a4e7c2d1f80"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("probe_plans") as batch_op:
        batch_op.add_column(
            sa.Column(
                "account_scope",
                sa.String(length=24),
                nullable=False,
                server_default="fixed",
            )
        )


def downgrade() -> None:
    with op.batch_alter_table("probe_plans") as batch_op:
        batch_op.drop_column("account_scope")
