"""add_youtube_mvp_tables

Revision ID: 20260223_0003
Revises: 20260220_0002
Create Date: 2026-02-23 00:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260223_0003"
down_revision = "20260220_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- youtube_channels ---
    op.create_table(
        "youtube_channels",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("channel_id", sa.String(length=64), nullable=False),
        sa.Column("channel_url", sa.Text(), nullable=False, server_default=""),
        sa.Column("channel_title", sa.String(length=256)),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("channel_id", name="uq_youtube_channels_channel_id"),
    )
    op.create_index("ix_youtube_channels_channel_id", "youtube_channels", ["channel_id"])

    # --- youtube_videos ---
    op.create_table(
        "youtube_videos",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("video_id", sa.String(length=32), nullable=False),
        sa.Column("channel_id", sa.String(length=64), nullable=False),
        sa.Column("channel_title", sa.String(length=256)),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("transcript_text", sa.Text()),
        sa.Column("transcript_lang", sa.String(length=20)),
        sa.Column("needs_asr", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("processed_at", sa.DateTime(timezone=True)),
        sa.Column("asr_backend", sa.String(length=64)),
        sa.Column("asr_model", sa.String(length=64)),
        sa.Column("asr_processed_at", sa.DateTime(timezone=True)),
        sa.Column("last_error", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("video_id", name="uq_youtube_videos_video_id"),
    )
    op.create_index("ix_youtube_videos_video_id", "youtube_videos", ["video_id"])
    op.create_index("ix_youtube_videos_channel_id", "youtube_videos", ["channel_id"])
    op.create_index("ix_youtube_videos_published_at", "youtube_videos", ["published_at"])

    # --- youtube_insights ---
    op.create_table(
        "youtube_insights",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("video_id", sa.String(length=32), nullable=False),
        sa.Column("symbol", sa.String(length=20), nullable=False, server_default="BTCUSDT"),
        sa.Column("analyst_view_json", sa.JSON()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("video_id", name="uq_youtube_insights_video_id"),
    )
    op.create_index("ix_youtube_insights_video_id", "youtube_insights", ["video_id"])
    op.create_index("ix_youtube_insights_symbol", "youtube_insights", ["symbol"])

    # --- youtube_consensus ---
    op.create_table(
        "youtube_consensus",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("symbol", sa.String(length=20), nullable=False),
        sa.Column("lookback_hours", sa.Integer(), nullable=False),
        sa.Column("consensus_json", sa.JSON()),
        sa.Column("source_video_ids", sa.JSON()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_youtube_consensus_symbol", "youtube_consensus", ["symbol"])


def downgrade() -> None:
    op.drop_index("ix_youtube_consensus_symbol", table_name="youtube_consensus")
    op.drop_table("youtube_consensus")

    op.drop_index("ix_youtube_insights_symbol", table_name="youtube_insights")
    op.drop_index("ix_youtube_insights_video_id", table_name="youtube_insights")
    op.drop_table("youtube_insights")

    op.drop_index("ix_youtube_videos_published_at", table_name="youtube_videos")
    op.drop_index("ix_youtube_videos_channel_id", table_name="youtube_videos")
    op.drop_index("ix_youtube_videos_video_id", table_name="youtube_videos")
    op.drop_table("youtube_videos")

    op.drop_index("ix_youtube_channels_channel_id", table_name="youtube_channels")
    op.drop_table("youtube_channels")
