"""add quarantine recovery guard

Revision ID: 6b9d2e4f7a10
Revises: 3f8c2d7a9e10
Create Date: 2026-08-11 10:30:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "6b9d2e4f7a10"
down_revision: str | None = "3f8c2d7a9e10"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "account_assessments",
        sa.Column(
            "recovery_guarded",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.create_index(
        op.f("ix_account_assessments_recovery_guarded"),
        "account_assessments",
        ["recovery_guarded"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_account_assessments_recovery_guarded"),
        table_name="account_assessments",
    )
    op.drop_column("account_assessments", "recovery_guarded")
