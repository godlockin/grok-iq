"""add multi-profile Cron plan selection

Revision ID: d6a4c9f2e781
Revises: a8c4e2f7b1d9
Create Date: 2026-08-10 11:30:00.000000
"""

from __future__ import annotations

import json
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "d6a4c9f2e781"
down_revision: str | None = "a8c4e2f7b1d9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("probe_plans") as batch_op:
        batch_op.add_column(
            sa.Column(
                "profile_ids",
                sa.JSON(),
                nullable=False,
                server_default=sa.text("'[]'"),
            )
        )

    connection = op.get_bind()
    plans = list(
        connection.execute(
            sa.text("SELECT id, profile_id FROM probe_plans")
        ).mappings()
    )
    for plan in plans:
        connection.execute(
            sa.text(
                "UPDATE probe_plans SET profile_ids = :profile_ids WHERE id = :plan_id"
            ),
            {
                "profile_ids": json.dumps([str(plan["profile_id"])]),
                "plan_id": plan["id"],
            },
        )


def downgrade() -> None:
    with op.batch_alter_table("probe_plans") as batch_op:
        batch_op.drop_column("profile_ids")
