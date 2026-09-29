"""preserve upstream TPS for probe overrides

Revision ID: c5e8a1f3d702
Revises: a1c3e5f7b902
Create Date: 2026-08-27 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "c5e8a1f3d702"
down_revision: str | Sequence[str] | None = "a1c3e5f7b902"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("probe_samples") as batch_op:
        batch_op.add_column(sa.Column("upstream_tps", sa.Float()))
    op.execute("UPDATE probe_samples SET upstream_tps = tps WHERE upstream_tps IS NULL")


def downgrade() -> None:
    with op.batch_alter_table("probe_samples") as batch_op:
        batch_op.drop_column("upstream_tps")
