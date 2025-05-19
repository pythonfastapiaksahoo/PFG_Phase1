"""grant select on corp_mail to powerbi

Revision ID: 6b38df3cdb5f
Revises: f004527005f3
Create Date: 2025-05-15 18:11:41.777765

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6b38df3cdb5f'
down_revision: Union[str, None] = 'f004527005f3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("GRANT SELECT ON pfg_schema.corp_mail TO powerbi;")

def downgrade() -> None:
    op.execute("REVOKE SELECT ON pfg_schema.corp_mail FROM powerbi;")
