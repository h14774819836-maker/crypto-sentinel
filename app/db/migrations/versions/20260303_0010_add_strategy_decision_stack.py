"""add_strategy_decision_stack

Revision ID: 20260303_0010
Revises: 20260303_0009
Create Date: 2026-03-03 00:10:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260303_0010"
down_revision = "20260303_0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("ai_signals", schema=None) as batch_op:
        batch_op.add_column(sa.Column("manifest_id", sa.String(length=128), nullable=True))
        batch_op.add_column(sa.Column("blob_ref", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("blob_sha256", sa.String(length=64), nullable=True))
        batch_op.add_column(sa.Column("blob_size_bytes", sa.Integer(), nullable=True))
        batch_op.create_index(batch_op.f("ix_ai_signals_manifest_id"), ["manifest_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_ai_signals_blob_sha256"), ["blob_sha256"], unique=False)

    op.create_table(
        "strategy_decisions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(length=20), nullable=False),
        sa.Column("exchange", sa.String(length=32), nullable=False),
        sa.Column("market_type", sa.String(length=16), nullable=False),
        sa.Column("base_timeframe", sa.String(length=10), nullable=False),
        sa.Column("decision_ts", sa.BigInteger(), nullable=False),
        sa.Column("manifest_id", sa.String(length=128), nullable=True),
        sa.Column("analysis_id", sa.Integer(), nullable=True),
        sa.Column("account_equity", sa.Float(), nullable=True),
        sa.Column("capital_alloc", sa.Float(), nullable=True),
        sa.Column("leverage", sa.Float(), nullable=True),
        sa.Column("margin_mode", sa.String(length=16), nullable=True),
        sa.Column("position_side", sa.String(length=10), nullable=False),
        sa.Column("qty", sa.Float(), nullable=True),
        sa.Column("notional", sa.Float(), nullable=True),
        sa.Column("entry_mode", sa.String(length=16), nullable=False),
        sa.Column("entry_price", sa.Float(), nullable=True),
        sa.Column("take_profit", sa.Float(), nullable=True),
        sa.Column("stop_loss", sa.Float(), nullable=True),
        sa.Column("expiration_ts", sa.BigInteger(), nullable=True),
        sa.Column("max_hold_bars", sa.Integer(), nullable=True),
        sa.Column("fee_bps_assumption", sa.Float(), nullable=True),
        sa.Column("slippage_bps_assumption", sa.Float(), nullable=True),
        sa.Column("liq_price_est", sa.Float(), nullable=True),
        sa.Column("risk_notes", sa.Text(), nullable=True),
        sa.Column("regime_calc_mode", sa.String(length=16), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("reason_brief", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["analysis_id"], ["ai_signals.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("analysis_id", name="uq_strategy_decisions_analysis_id"),
    )
    with op.batch_alter_table("strategy_decisions", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_strategy_decisions_symbol"), ["symbol"], unique=False)
        batch_op.create_index(batch_op.f("ix_strategy_decisions_exchange"), ["exchange"], unique=False)
        batch_op.create_index(batch_op.f("ix_strategy_decisions_market_type"), ["market_type"], unique=False)
        batch_op.create_index(batch_op.f("ix_strategy_decisions_decision_ts"), ["decision_ts"], unique=False)
        batch_op.create_index(batch_op.f("ix_strategy_decisions_manifest_id"), ["manifest_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_strategy_decisions_analysis_id"), ["analysis_id"], unique=False)
        batch_op.create_index("ix_strategy_decisions_symbol_decision_ts", ["symbol", "decision_ts"], unique=False)
        batch_op.create_index(
            "ix_strategy_decisions_manifest_id_decision_ts",
            ["manifest_id", "decision_ts"],
            unique=False,
        )

    op.create_table(
        "decision_executions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("decision_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("filled", sa.Boolean(), nullable=False),
        sa.Column("filled_ts", sa.BigInteger(), nullable=True),
        sa.Column("filled_price", sa.Float(), nullable=True),
        sa.Column("filled_qty", sa.Float(), nullable=True),
        sa.Column("avg_fill_price", sa.Float(), nullable=True),
        sa.Column("source", sa.String(length=20), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["decision_id"], ["strategy_decisions.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("decision_id", name="uq_decision_executions_decision_id"),
    )
    with op.batch_alter_table("decision_executions", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_decision_executions_decision_id"), ["decision_id"], unique=False)

    op.create_table(
        "decision_evals",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("decision_id", sa.Integer(), nullable=False),
        sa.Column("eval_replay_tf", sa.String(length=10), nullable=False),
        sa.Column("intrabar_flag", sa.String(length=16), nullable=False),
        sa.Column("tp_hit", sa.Boolean(), nullable=False),
        sa.Column("sl_hit", sa.Boolean(), nullable=False),
        sa.Column("first_hit_ts", sa.BigInteger(), nullable=True),
        sa.Column("exit_ts", sa.BigInteger(), nullable=True),
        sa.Column("exit_price", sa.Float(), nullable=True),
        sa.Column("outcome_raw", sa.String(length=20), nullable=False),
        sa.Column("r_multiple_raw", sa.Float(), nullable=True),
        sa.Column("mfe", sa.Float(), nullable=True),
        sa.Column("mae", sa.Float(), nullable=True),
        sa.Column("bars_to_outcome", sa.Integer(), nullable=True),
        sa.Column("evaluated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["decision_id"], ["strategy_decisions.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("decision_id", name="uq_decision_evals_decision_id"),
    )
    with op.batch_alter_table("decision_evals", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_decision_evals_decision_id"), ["decision_id"], unique=False)

    op.create_table(
        "strategy_scores",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("manifest_id", sa.String(length=128), nullable=False),
        sa.Column("window_start_ts", sa.BigInteger(), nullable=False),
        sa.Column("window_end_ts", sa.BigInteger(), nullable=False),
        sa.Column("split_type", sa.String(length=20), nullable=False),
        sa.Column("scoring_mode", sa.String(length=20), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("n_trades", sa.Integer(), nullable=False),
        sa.Column("n_resolved", sa.Integer(), nullable=False),
        sa.Column("n_ambiguous", sa.Integer(), nullable=False),
        sa.Column("n_timeout", sa.Integer(), nullable=False),
        sa.Column("win_rate", sa.Float(), nullable=True),
        sa.Column("avg_r", sa.Float(), nullable=True),
        sa.Column("win_rate_ci_low", sa.Float(), nullable=True),
        sa.Column("win_rate_ci_high", sa.Float(), nullable=True),
        sa.Column("avg_r_ci_low", sa.Float(), nullable=True),
        sa.Column("avg_r_ci_high", sa.Float(), nullable=True),
        sa.Column("timeout_rate", sa.Float(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "manifest_id",
            "window_start_ts",
            "window_end_ts",
            "split_type",
            "scoring_mode",
            name="uq_strategy_scores_window_mode",
        ),
    )
    with op.batch_alter_table("strategy_scores", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_strategy_scores_manifest_id"), ["manifest_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_strategy_scores_window_start_ts"), ["window_start_ts"], unique=False)
        batch_op.create_index(batch_op.f("ix_strategy_scores_window_end_ts"), ["window_end_ts"], unique=False)

    op.create_table(
        "strategy_feature_stats",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("manifest_id", sa.String(length=128), nullable=False),
        sa.Column("window_start_ts", sa.BigInteger(), nullable=False),
        sa.Column("window_end_ts", sa.BigInteger(), nullable=False),
        sa.Column("split_type", sa.String(length=20), nullable=False),
        sa.Column("regime_id", sa.String(length=64), nullable=False),
        sa.Column("scoring_mode", sa.String(length=20), nullable=False),
        sa.Column("feature_key", sa.String(length=64), nullable=False),
        sa.Column("bucket_key", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("n", sa.Integer(), nullable=False),
        sa.Column("win_rate", sa.Float(), nullable=True),
        sa.Column("avg_r", sa.Float(), nullable=True),
        sa.Column("ci_low", sa.Float(), nullable=True),
        sa.Column("ci_high", sa.Float(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "manifest_id",
            "window_start_ts",
            "window_end_ts",
            "split_type",
            "regime_id",
            "scoring_mode",
            "feature_key",
            "bucket_key",
            name="uq_strategy_feature_stats_bucket",
        ),
    )
    with op.batch_alter_table("strategy_feature_stats", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_strategy_feature_stats_manifest_id"), ["manifest_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_strategy_feature_stats_window_start_ts"), ["window_start_ts"], unique=False)
        batch_op.create_index(batch_op.f("ix_strategy_feature_stats_window_end_ts"), ["window_end_ts"], unique=False)
        batch_op.create_index(batch_op.f("ix_strategy_feature_stats_regime_id"), ["regime_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_strategy_feature_stats_feature_key"), ["feature_key"], unique=False)


def downgrade() -> None:
    with op.batch_alter_table("strategy_feature_stats", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_strategy_feature_stats_feature_key"))
        batch_op.drop_index(batch_op.f("ix_strategy_feature_stats_regime_id"))
        batch_op.drop_index(batch_op.f("ix_strategy_feature_stats_window_end_ts"))
        batch_op.drop_index(batch_op.f("ix_strategy_feature_stats_window_start_ts"))
        batch_op.drop_index(batch_op.f("ix_strategy_feature_stats_manifest_id"))
    op.drop_table("strategy_feature_stats")

    with op.batch_alter_table("strategy_scores", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_strategy_scores_window_end_ts"))
        batch_op.drop_index(batch_op.f("ix_strategy_scores_window_start_ts"))
        batch_op.drop_index(batch_op.f("ix_strategy_scores_manifest_id"))
    op.drop_table("strategy_scores")

    with op.batch_alter_table("decision_evals", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_decision_evals_decision_id"))
    op.drop_table("decision_evals")

    with op.batch_alter_table("decision_executions", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_decision_executions_decision_id"))
    op.drop_table("decision_executions")

    with op.batch_alter_table("strategy_decisions", schema=None) as batch_op:
        batch_op.drop_index("ix_strategy_decisions_manifest_id_decision_ts")
        batch_op.drop_index("ix_strategy_decisions_symbol_decision_ts")
        batch_op.drop_index(batch_op.f("ix_strategy_decisions_analysis_id"))
        batch_op.drop_index(batch_op.f("ix_strategy_decisions_manifest_id"))
        batch_op.drop_index(batch_op.f("ix_strategy_decisions_decision_ts"))
        batch_op.drop_index(batch_op.f("ix_strategy_decisions_market_type"))
        batch_op.drop_index(batch_op.f("ix_strategy_decisions_exchange"))
        batch_op.drop_index(batch_op.f("ix_strategy_decisions_symbol"))
    op.drop_table("strategy_decisions")

    with op.batch_alter_table("ai_signals", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_ai_signals_blob_sha256"))
        batch_op.drop_index(batch_op.f("ix_ai_signals_manifest_id"))
        batch_op.drop_column("blob_size_bytes")
        batch_op.drop_column("blob_sha256")
        batch_op.drop_column("blob_ref")
        batch_op.drop_column("manifest_id")
