"""add renderable expected probe output

Revision ID: f1a6c9d2e4b7
Revises: 7d2e4f1c8a90, c2d7e9f4a1b3
Create Date: 2026-08-10 08:30:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "f1a6c9d2e4b7"
down_revision: str | Sequence[str] | None = (
    "7d2e4f1c8a90",
    "c2d7e9f4a1b3",
)
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("probe_profiles") as batch_op:
        batch_op.add_column(
            sa.Column("expected_output", sa.Text(), nullable=False, server_default="")
        )


def downgrade() -> None:
    with op.batch_alter_table("probe_profiles") as batch_op:
        batch_op.drop_column("expected_output")
