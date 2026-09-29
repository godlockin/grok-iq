"""add incremental probe duration estimates

Revision ID: 3f8c2d7a9e10
Revises: e4b7c1d9a203
Create Date: 2026-08-11 09:30:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "3f8c2d7a9e10"
down_revision: str | None = "e4b7c1d9a203"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

BACKFILL_KEY = "probe_duration_estimates_backfill_v1"


def upgrade() -> None:
    op.create_table(
        "probe_duration_estimates",
        sa.Column("profile_id", sa.String(length=64), nullable=False),
        sa.Column("execution_mode", sa.String(length=24), nullable=False),
        sa.Column("sample_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_duration_ms", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["profile_id"],
            ["probe_profiles.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("profile_id", "execution_mode"),
    )

    connection = op.get_bind()
    connection.execute(
        sa.text(
            """
            INSERT INTO probe_duration_estimates (
                profile_id,
                execution_mode,
                sample_count,
                total_duration_ms,
                created_at,
                updated_at
            )
            SELECT
                probe_runs.profile_id,
                probe_runs.execution_mode,
                COUNT(probe_samples.id),
                SUM(probe_samples.duration_ms),
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
            FROM probe_samples
            JOIN probe_runs ON probe_runs.id = probe_samples.run_id
            WHERE probe_samples.duration_ms > 0
            GROUP BY probe_runs.profile_id, probe_runs.execution_mode
            """
        )
    )
    connection.execute(
        sa.text(
            "INSERT INTO metadata (key, value) VALUES (:key, CURRENT_TIMESTAMP) "
            "ON CONFLICT(key) DO NOTHING"
        ),
        {"key": BACKFILL_KEY},
    )


def downgrade() -> None:
    op.execute(sa.text("DELETE FROM metadata WHERE key = :key").bindparams(key=BACKFILL_KEY))
    op.drop_table("probe_duration_estimates")
