"""store request-audit client key identity

Revision ID: a1c3e5f7b902
Revises: f4c8a2d7e901
Create Date: 2026-08-25 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "a1c3e5f7b902"
down_revision: str | Sequence[str] | None = "f4c8a2d7e901"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("request_audit_records") as batch_op:
        batch_op.add_column(
            sa.Column(
                "client_key_id",
                sa.String(length=64),
                nullable=False,
                server_default="",
            )
        )
        batch_op.add_column(
            sa.Column(
                "client_key_name",
                sa.String(length=160),
                nullable=False,
                server_default="",
            )
        )


def downgrade() -> None:
    with op.batch_alter_table("request_audit_records") as batch_op:
        batch_op.drop_column("client_key_name")
        batch_op.drop_column("client_key_id")
