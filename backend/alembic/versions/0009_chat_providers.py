"""add playground chat providers

Revision ID: e4b7c1d9a203
Revises: d6a4c9f2e781
Create Date: 2026-08-10 12:30:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "e4b7c1d9a203"
down_revision: str | None = "d6a4c9f2e781"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "chat_providers",
        sa.Column("id", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("base_url", sa.String(length=2000), nullable=False),
        sa.Column("api_key_ciphertext", sa.Text(), nullable=False, server_default=""),
        sa.Column("models", sa.JSON(), nullable=False, server_default=sa.text("'[]'")),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("is_default", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_chat_providers_enabled", "chat_providers", ["enabled"])
    op.create_index("ix_chat_providers_is_default", "chat_providers", ["is_default"])


def downgrade() -> None:
    op.drop_index("ix_chat_providers_is_default", table_name="chat_providers")
    op.drop_index("ix_chat_providers_enabled", table_name="chat_providers")
    op.drop_table("chat_providers")
