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
    conn = op.get_bind()
    existing_columns = {
        str(col.get("name") or "")
        for col in sa.inspect(conn).get_columns("youtube_videos")
    }

    with op.batch_alter_table("youtube_videos", schema=None) as batch_op:
        if "analysis_runtime_status" not in existing_columns:
            batch_op.add_column(sa.Column("analysis_runtime_status", sa.String(length=32), nullable=True))
        if "analysis_stage" not in existing_columns:
            batch_op.add_column(sa.Column("analysis_stage", sa.String(length=32), nullable=True))
        if "analysis_started_at" not in existing_columns:
            batch_op.add_column(sa.Column("analysis_started_at", sa.DateTime(timezone=True), nullable=True))
        if "analysis_updated_at" not in existing_columns:
            batch_op.add_column(sa.Column("analysis_updated_at", sa.DateTime(timezone=True), nullable=True))
        if "analysis_finished_at" not in existing_columns:
            batch_op.add_column(sa.Column("analysis_finished_at", sa.DateTime(timezone=True), nullable=True))
        if "analysis_retry_count" not in existing_columns:
            batch_op.add_column(
                sa.Column("analysis_retry_count", sa.Integer(), nullable=False, server_default=sa.text("0"))
            )
        if "analysis_next_retry_at" not in existing_columns:
            batch_op.add_column(sa.Column("analysis_next_retry_at", sa.DateTime(timezone=True), nullable=True))
        if "analysis_last_error_type" not in existing_columns:
            batch_op.add_column(sa.Column("analysis_last_error_type", sa.String(length=64), nullable=True))
        if "analysis_last_error_code" not in existing_columns:
            batch_op.add_column(sa.Column("analysis_last_error_code", sa.String(length=64), nullable=True))
        if "analysis_last_error_message" not in existing_columns:
            batch_op.add_column(sa.Column("analysis_last_error_message", sa.Text(), nullable=True))
        if "status" not in existing_columns:
            batch_op.add_column(sa.Column("status", sa.String(length=32), nullable=True))
        if "status_updated_at" not in existing_columns:
            batch_op.add_column(sa.Column("status_updated_at", sa.DateTime(timezone=True), nullable=True))
        if "asr_queued_at" not in existing_columns:
            batch_op.add_column(sa.Column("asr_queued_at", sa.DateTime(timezone=True), nullable=True))
        if "status" not in existing_columns:
            batch_op.create_index(batch_op.f("ix_youtube_videos_status"), ["status"], unique=False)
        if "status_updated_at" not in existing_columns:
            batch_op.create_index(batch_op.f("ix_youtube_videos_status_updated_at"), ["status_updated_at"], unique=False)

    # Backfill: derive initial status from existing fields
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
        batch_op.drop_column("analysis_last_error_message")
        batch_op.drop_column("analysis_last_error_code")
        batch_op.drop_column("analysis_last_error_type")
        batch_op.drop_column("analysis_next_retry_at")
        batch_op.drop_column("analysis_retry_count")
        batch_op.drop_column("analysis_finished_at")
        batch_op.drop_column("analysis_updated_at")
        batch_op.drop_column("analysis_started_at")
        batch_op.drop_column("analysis_stage")
        batch_op.drop_column("analysis_runtime_status")
