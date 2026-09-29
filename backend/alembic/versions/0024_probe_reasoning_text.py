"""store probe reasoning text with sample responses

Revision ID: e8c3a1f6d904
Revises: d7f2b4a6c801
Create Date: 2026-08-27 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "e8c3a1f6d904"
down_revision: str | Sequence[str] | None = "d7f2b4a6c801"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("probe_samples") as batch_op:
        batch_op.add_column(
            sa.Column("reasoning_text", sa.Text(), nullable=False, server_default="")
        )


def downgrade() -> None:
    with op.batch_alter_table("probe_samples") as batch_op:
        batch_op.drop_column("reasoning_text")
