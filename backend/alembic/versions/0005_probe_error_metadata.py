"""add structured probe error metadata

Revision ID: 7d2e4f1c8a90
Revises: 4c52f0ce6118
Create Date: 2026-08-10 01:05:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "7d2e4f1c8a90"
down_revision: str | None = "4c52f0ce6118"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("probe_samples") as batch_op:
        batch_op.add_column(
            sa.Column("error_code", sa.String(length=100), nullable=False, server_default="")
        )
        batch_op.add_column(sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"))
        batch_op.add_column(
            sa.Column("retry_after_seconds", sa.Float(), nullable=False, server_default="0")
        )
        batch_op.create_index("ix_probe_samples_error_code", ["error_code"], unique=False)


def downgrade() -> None:
    with op.batch_alter_table("probe_samples") as batch_op:
        batch_op.drop_index("ix_probe_samples_error_code")
        batch_op.drop_column("retry_after_seconds")
        batch_op.drop_column("retry_count")
        batch_op.drop_column("error_code")
