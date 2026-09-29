"""add probe account restore audit

Revision ID: 4c52f0ce6118
Revises: b731e11e3a20
Create Date: 2026-08-09 20:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "4c52f0ce6118"
down_revision: str | None = "b731e11e3a20"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("probe_runs") as batch_op:
        batch_op.add_column(sa.Column("original_account_enabled", sa.Boolean(), nullable=True))
        batch_op.add_column(sa.Column("original_account_priority", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("original_account_max_concurrent", sa.Integer(), nullable=True))
        batch_op.add_column(
            sa.Column("account_settings_snapshot_at", sa.DateTime(timezone=True), nullable=True)
        )
        batch_op.add_column(sa.Column("diagnostic_priority", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("diagnostic_max_concurrent", sa.Integer(), nullable=True))
        batch_op.add_column(
            sa.Column(
                "diagnostic_activation_active",
                sa.Boolean(),
                nullable=False,
                server_default=sa.false(),
            )
        )
        batch_op.add_column(
            sa.Column(
                "account_restore_status",
                sa.String(length=32),
                nullable=False,
                server_default="not_recorded",
            )
        )
        batch_op.add_column(
            sa.Column("account_restore_source", sa.String(length=32), nullable=False, server_default="")
        )
        batch_op.add_column(
            sa.Column("account_restore_attempts", sa.Integer(), nullable=False, server_default="0")
        )
        batch_op.add_column(sa.Column("account_restore_error", sa.Text(), nullable=False, server_default=""))
        batch_op.add_column(
            sa.Column("account_restore_attempted_at", sa.DateTime(timezone=True), nullable=True)
        )
        batch_op.add_column(sa.Column("account_restored_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("probe_runs") as batch_op:
        batch_op.drop_column("account_restored_at")
        batch_op.drop_column("account_restore_attempted_at")
        batch_op.drop_column("account_restore_error")
        batch_op.drop_column("account_restore_attempts")
        batch_op.drop_column("account_restore_source")
        batch_op.drop_column("account_restore_status")
        batch_op.drop_column("diagnostic_activation_active")
        batch_op.drop_column("diagnostic_max_concurrent")
        batch_op.drop_column("diagnostic_priority")
        batch_op.drop_column("account_settings_snapshot_at")
        batch_op.drop_column("original_account_max_concurrent")
        batch_op.drop_column("original_account_priority")
        batch_op.drop_column("original_account_enabled")
