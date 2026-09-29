"""add indexes for probe run pagination

Revision ID: c2d7e9f4a1b3
Revises: 4c52f0ce6118
Create Date: 2026-08-10 03:00:00
"""

from collections.abc import Sequence

from alembic import op

revision: str = "c2d7e9f4a1b3"
down_revision: str | None = "4c52f0ce6118"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_probe_run_status_created "
        "ON probe_runs (status, created_at)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_probe_run_created_at "
        "ON probe_runs (created_at)"
    )


def downgrade() -> None:
    op.drop_index("ix_probe_run_created_at", table_name="probe_runs")
    op.drop_index("ix_probe_run_status_created", table_name="probe_runs")
