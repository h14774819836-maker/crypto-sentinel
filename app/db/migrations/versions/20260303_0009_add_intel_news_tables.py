"""add_intel_news_tables

Revision ID: 20260303_0009
Revises: 20260301_0008
Create Date: 2026-03-03 00:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260303_0009"
down_revision = "20260301_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "news_items",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source", sa.String(length=128), nullable=False),
        sa.Column("category", sa.String(length=32), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("title_hash", sa.String(length=64), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("url_hash", sa.String(length=64), nullable=False),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("raw_text", sa.Text(), nullable=True),
        sa.Column("region", sa.String(length=32), nullable=True),
        sa.Column("topics_json", sa.JSON(), nullable=True),
        sa.Column("alert_keyword", sa.String(length=64), nullable=True),
        sa.Column("severity", sa.Integer(), nullable=False),
        sa.Column("entities_json", sa.JSON(), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("url_hash", name="uq_news_items_url_hash"),
    )
    with op.batch_alter_table("news_items", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_news_items_ts_utc"), ["ts_utc"], unique=False)
        batch_op.create_index(batch_op.f("ix_news_items_source"), ["source"], unique=False)
        batch_op.create_index(batch_op.f("ix_news_items_category"), ["category"], unique=False)
        batch_op.create_index(batch_op.f("ix_news_items_title_hash"), ["title_hash"], unique=False)
        batch_op.create_index(batch_op.f("ix_news_items_url_hash"), ["url_hash"], unique=False)
        batch_op.create_index(batch_op.f("ix_news_items_region"), ["region"], unique=False)
        batch_op.create_index(batch_op.f("ix_news_items_alert_keyword"), ["alert_keyword"], unique=False)
        batch_op.create_index(batch_op.f("ix_news_items_severity"), ["severity"], unique=False)

    op.create_table(
        "intel_digest_cache",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(length=20), nullable=False),
        sa.Column("lookback_hours", sa.Integer(), nullable=False),
        sa.Column("digest_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("intel_digest_cache", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_intel_digest_cache_symbol"), ["symbol"], unique=False)
        batch_op.create_index(batch_op.f("ix_intel_digest_cache_created_at"), ["created_at"], unique=False)


def downgrade() -> None:
    with op.batch_alter_table("intel_digest_cache", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_intel_digest_cache_created_at"))
        batch_op.drop_index(batch_op.f("ix_intel_digest_cache_symbol"))
    op.drop_table("intel_digest_cache")

    with op.batch_alter_table("news_items", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_news_items_severity"))
        batch_op.drop_index(batch_op.f("ix_news_items_alert_keyword"))
        batch_op.drop_index(batch_op.f("ix_news_items_region"))
        batch_op.drop_index(batch_op.f("ix_news_items_url_hash"))
        batch_op.drop_index(batch_op.f("ix_news_items_title_hash"))
        batch_op.drop_index(batch_op.f("ix_news_items_category"))
        batch_op.drop_index(batch_op.f("ix_news_items_source"))
        batch_op.drop_index(batch_op.f("ix_news_items_ts_utc"))
    op.drop_table("news_items")
