"""add_youtube_explicit_status

Revision ID: 20260304_0013
Revises: 20260304_0012
Create Date: 2026-03-04 18:00:00

"""

from alembic import op
import sqlalchemy as sa


revision = "20260304_0013"
down_revision = "20260304_0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("youtube_videos", schema=None) as batch_op:
        batch_op.add_column(sa.Column("status", sa.String(length=32), nullable=True))
        batch_op.add_column(sa.Column("status_updated_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.add_column(sa.Column("asr_queued_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.create_index(batch_op.f("ix_youtube_videos_status"), ["status"], unique=False)
        batch_op.create_index(batch_op.f("ix_youtube_videos_status_updated_at"), ["status_updated_at"], unique=False)

    # Backfill: derive initial status from existing fields
    conn = op.get_bind()
    dialect = conn.dialect.name
    if dialect == "sqlite":
        has_valid_insight = "EXISTS (SELECT 1 FROM youtube_insights i WHERE i.video_id = youtube_videos.video_id AND i.analyst_view_json IS NOT NULL)"
        needs_asr_expr = "needs_asr = 1"
    else:
        has_valid_insight = "EXISTS (SELECT 1 FROM youtube_insights i WHERE i.video_id = youtube_videos.video_id AND i.analyst_view_json IS NOT NULL)"
        needs_asr_expr = "needs_asr = true"
    conn.execute(sa.text(f"""
        UPDATE youtube_videos SET
            status = CASE
                WHEN transcript_text IS NOT NULL AND {has_valid_insight} THEN 'completed'
                WHEN transcript_text IS NOT NULL AND LOWER(COALESCE(analysis_runtime_status, '')) IN ('queued','running') THEN 'analyzing'
                WHEN transcript_text IS NOT NULL AND LOWER(COALESCE(analysis_runtime_status, '')) IN ('failed_paused','failed') THEN 'failed'
                WHEN transcript_text IS NOT NULL THEN 'pending_analysis'
                WHEN {needs_asr_expr} AND asr_processed_at IS NOT NULL AND last_error IS NOT NULL THEN 'asr_failed'
                WHEN {needs_asr_expr} THEN 'queued_asr'
                ELSE 'pending_subtitle'
            END,
            status_updated_at = COALESCE(analysis_updated_at, asr_processed_at, processed_at, created_at)
        WHERE status IS NULL
    """))

    with op.batch_alter_table("youtube_videos", schema=None) as batch_op:
        batch_op.alter_column("status", nullable=False, server_default="pending_subtitle")


def downgrade() -> None:
    with op.batch_alter_table("youtube_videos", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_youtube_videos_status_updated_at"), if_exists=True)
        batch_op.drop_index(batch_op.f("ix_youtube_videos_status"), if_exists=True)
        batch_op.drop_column("asr_queued_at")
        batch_op.drop_column("status_updated_at")
        batch_op.drop_column("status")
