"""track explicit reasoning-token presence

Revision ID: f4c8a2d7e901
Revises: e7b2c4d9a610
Create Date: 2026-08-21 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "f4c8a2d7e901"
down_revision: str | Sequence[str] | None = "e7b2c4d9a610"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("request_audit_records") as batch_op:
        batch_op.add_column(
            sa.Column(
                "reasoning_tokens_reported",
                sa.Boolean(),
                nullable=False,
                server_default=sa.false(),
            )
        )
    op.execute(
        "UPDATE request_audit_records SET reasoning_tokens_reported = 1 "
        "WHERE reasoning_tokens_reported = 0 AND json_valid(raw) "
        "AND json_type(raw, '$.reasoningTokens') IS NOT NULL"
    )

    with op.batch_alter_table("probe_samples") as batch_op:
        batch_op.add_column(
            sa.Column(
                "reasoning_tokens_reported",
                sa.Boolean(),
                nullable=False,
                server_default=sa.false(),
            )
        )
    op.execute(
        "UPDATE probe_samples SET reasoning_tokens_reported = 1 "
        "WHERE reasoning_tokens_reported = 0 AND ("
        "reasoning_tokens > 0 "
        "OR (json_valid(usage) AND ("
        "json_type(usage, '$.completion_tokens_details.reasoning_tokens') IS NOT NULL "
        "OR json_type(usage, '$.completionTokensDetails.reasoningTokens') IS NOT NULL"
        "))"
        ")"
    )


def downgrade() -> None:
    with op.batch_alter_table("probe_samples") as batch_op:
        batch_op.drop_column("reasoning_tokens_reported")
    with op.batch_alter_table("request_audit_records") as batch_op:
        batch_op.drop_column("reasoning_tokens_reported")
