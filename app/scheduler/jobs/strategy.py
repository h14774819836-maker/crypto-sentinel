"""Strategy jobs: decision eval, scores, research."""
from __future__ import annotations

import json
import math
import random
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.db.models import DecisionEval, DecisionExecution, StrategyDecision
from app.db.repository import (
    list_manifest_ids_for_strategy,
    list_ohlcv_range,
    list_strategy_decisions_for_eval,
    list_strategy_rows_for_window,
    upsert_decision_eval,
    upsert_decision_execution,
    upsert_strategy_feature_stat,
    upsert_strategy_score,
)
from app.logging import logger
from app.scheduler.jobs.common import _epoch_seconds
from app.scheduler.runtime import WorkerRuntime

_STRATEGY_EVAL_CHECKPOINT_FILE = Path("data/decision_eval_checkpoint.json")
_FEATURE_WHITELIST_BUCKETS: dict[str, set[str]] = {
    "signal_confidence": {"CONF_Q1", "CONF_Q2", "CONF_Q3", "CONF_Q4"},
    "position_side": {"LONG", "SHORT", "HOLD"},
    "market_regime": {"TRENDING_UP", "TRENDING_DOWN", "RANGING", "VOLATILE", "UNCERTAIN"},
}


def _load_eval_checkpoint_map() -> dict[str, int]:
    try:
        if not _STRATEGY_EVAL_CHECKPOINT_FILE.exists():
            return {}
        data = json.loads(_STRATEGY_EVAL_CHECKPOINT_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {}
        out: dict[str, int] = {}
        for k, v in data.items():
            try:
                out[str(k)] = int(v)
            except (TypeError, ValueError):
                continue
        return out
    except Exception:
        return {}


def _save_eval_checkpoint_map(payload: dict[str, int]) -> None:
    _STRATEGY_EVAL_CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    _STRATEGY_EVAL_CHECKPOINT_FILE.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )


class _OhlcvSliceCache:
    def __init__(self, max_size: int = 16):
        self.max_size = max(1, int(max_size))
        self._cache: OrderedDict[tuple[str, int, int], list[Any]] = OrderedDict()

    def get(
        self,
        session,
        *,
        symbol: str,
        start_ts: int,
        end_ts: int,
    ) -> list[Any]:
        from datetime import timedelta

        key = (symbol.upper(), int(start_ts), int(end_ts))
        hit = self._cache.get(key)
        if hit is not None:
            self._cache.move_to_end(key)
            return hit
        start_dt = datetime.fromtimestamp(int(start_ts), tz=timezone.utc)
        end_dt = datetime.fromtimestamp(int(end_ts), tz=timezone.utc)
        rows = list_ohlcv_range(
            session,
            symbol=symbol.upper(),
            timeframe="1m",
            start_ts=start_dt,
            end_ts=end_dt + timedelta(minutes=1),
        )
        self._cache[key] = rows
        self._cache.move_to_end(key)
        while len(self._cache) > self.max_size:
            self._cache.popitem(last=False)
        return rows


def _decision_expiration_ts(decision: StrategyDecision) -> int:
    base_ts = int(decision.decision_ts)
    hold_ts = base_ts + max(1, int(decision.max_hold_bars or 60)) * 60
    if decision.expiration_ts is None:
        return hold_ts
    return min(int(decision.expiration_ts), hold_ts)


def _calc_r_tp(decision: StrategyDecision) -> float | None:
    if decision.entry_price is None or decision.take_profit is None or decision.stop_loss is None:
        return None
    risk = abs(float(decision.entry_price) - float(decision.stop_loss))
    if risk <= 0:
        return None
    reward = abs(float(decision.take_profit) - float(decision.entry_price))
    return reward / risk


def _simulate_fill_for_decision(
    decision: StrategyDecision,
    *,
    bars: list[Any],
    decision_ts: int,
    expiration_ts: int,
) -> dict[str, Any]:
    side = str(decision.position_side or "HOLD").upper()
    entry_mode = str(decision.entry_mode or "MARKET").upper()
    if side == "HOLD":
        return {
            "status": "NO_FILL",
            "filled": False,
            "filled_ts": None,
            "filled_price": None,
            "filled_qty": decision.qty,
            "avg_fill_price": None,
            "source": "SIMULATED",
            "notes": "HOLD decision, no execution.",
        }

    candidate_bars = []
    for bar in bars:
        ts = _epoch_seconds(getattr(bar, "ts", None))
        if ts is None:
            continue
        if ts < decision_ts or ts > expiration_ts:
            continue
        candidate_bars.append((ts, bar))
    if not candidate_bars:
        return {
            "status": "PENDING",
            "filled": False,
            "filled_ts": None,
            "filled_price": None,
            "filled_qty": None,
            "avg_fill_price": None,
            "source": "SIMULATED",
            "notes": "No replay bars available yet.",
        }

    if entry_mode == "MARKET":
        ts, bar = candidate_bars[0]
        fill_price = float(getattr(bar, "open", None) or getattr(bar, "close", None) or decision.entry_price or 0.0)
        return {
            "status": "FILLED",
            "filled": True,
            "filled_ts": ts,
            "filled_price": fill_price,
            "filled_qty": decision.qty,
            "avg_fill_price": fill_price,
            "source": "SIMULATED",
            "notes": "MARKET filled at first 1m bar open.",
        }

    if decision.entry_price is None:
        return {
            "status": "NO_FILL",
            "filled": False,
            "filled_ts": None,
            "filled_price": None,
            "filled_qty": None,
            "avg_fill_price": None,
            "source": "SIMULATED",
            "notes": "LIMIT missing entry_price.",
        }

    target = float(decision.entry_price or 0.0)
    for ts, bar in candidate_bars:
        low = float(getattr(bar, "low", target))
        high = float(getattr(bar, "high", target))
        touched = (side == "LONG" and low <= target) or (side == "SHORT" and high >= target)
        if touched:
            return {
                "status": "FILLED",
                "filled": True,
                "filled_ts": ts,
                "filled_price": target,
                "filled_qty": decision.qty,
                "avg_fill_price": target,
                "source": "SIMULATED",
                "notes": "LIMIT touched by 1m replay.",
            }
    if candidate_bars and candidate_bars[-1][0] >= expiration_ts:
        return {
            "status": "NO_FILL",
            "filled": False,
            "filled_ts": None,
            "filled_price": None,
            "filled_qty": None,
            "avg_fill_price": None,
            "source": "SIMULATED",
            "notes": "LIMIT not touched before expiration.",
        }
    return {
        "status": "PENDING",
        "filled": False,
        "filled_ts": None,
        "filled_price": None,
        "filled_qty": None,
        "avg_fill_price": None,
        "source": "SIMULATED",
        "notes": "LIMIT pending, waiting replay bars.",
    }


def _simulate_post_fill_eval(
    decision: StrategyDecision,
    *,
    bars: list[Any],
    filled_ts: int,
    scan_start_ts: int,
    expiration_ts: int,
    max_scan_bars: int,
    now_ts: int,
) -> tuple[dict[str, Any], int | None]:
    side = str(decision.position_side or "HOLD").upper()
    tp = decision.take_profit
    sl = decision.stop_loss
    entry = decision.entry_price
    r_tp = _calc_r_tp(decision)

    selected: list[tuple[int, Any]] = []
    for bar in bars:
        ts = _epoch_seconds(getattr(bar, "ts", None))
        if ts is None:
            continue
        if ts < scan_start_ts or ts < filled_ts or ts > expiration_ts:
            continue
        selected.append((ts, bar))
        if len(selected) >= max(1, int(max_scan_bars)):
            break

    mfe = None
    mae = None
    if entry is not None and selected:
        highs = [float(getattr(b, "high", entry)) for _, b in selected]
        lows = [float(getattr(b, "low", entry)) for _, b in selected]
        if side == "LONG":
            mfe = max(highs) - float(entry)
            mae = min(lows) - float(entry)
        elif side == "SHORT":
            mfe = float(entry) - min(lows)
            mae = float(entry) - max(highs)

    base_payload = {
        "eval_replay_tf": "1m",
        "intrabar_flag": "NONE",
        "tp_hit": False,
        "sl_hit": False,
        "first_hit_ts": None,
        "exit_ts": None,
        "exit_price": None,
        "outcome_raw": "OPEN",
        "r_multiple_raw": None,
        "mfe": mfe,
        "mae": mae,
        "bars_to_outcome": len(selected),
        "evaluated_at": datetime.now(timezone.utc),
    }

    if side == "HOLD":
        base_payload["outcome_raw"] = "NO_FILL"
        return base_payload, None

    for ts, bar in selected:
        high = float(getattr(bar, "high", getattr(bar, "close", 0.0)))
        low = float(getattr(bar, "low", getattr(bar, "close", 0.0)))
        tp_hit = False
        sl_hit = False
        if side == "LONG":
            tp_hit = tp is not None and high >= float(tp)
            sl_hit = sl is not None and low <= float(sl)
        elif side == "SHORT":
            tp_hit = tp is not None and low <= float(tp)
            sl_hit = sl is not None and high >= float(sl)

        if tp_hit and sl_hit:
            base_payload.update(
                {
                    "intrabar_flag": "BOTH_HIT",
                    "tp_hit": True,
                    "sl_hit": True,
                    "first_hit_ts": ts,
                    "outcome_raw": "AMBIGUOUS",
                    "exit_ts": ts,
                    "exit_price": None,
                    "r_multiple_raw": None,
                    "bars_to_outcome": (ts - filled_ts) // 60,
                }
            )
            return base_payload, None
        if tp_hit:
            base_payload.update(
                {
                    "tp_hit": True,
                    "first_hit_ts": ts,
                    "exit_ts": ts,
                    "exit_price": tp,
                    "outcome_raw": "TP",
                    "r_multiple_raw": r_tp,
                    "bars_to_outcome": (ts - filled_ts) // 60,
                }
            )
            return base_payload, None
        if sl_hit:
            base_payload.update(
                {
                    "sl_hit": True,
                    "first_hit_ts": ts,
                    "exit_ts": ts,
                    "exit_price": sl,
                    "outcome_raw": "SL",
                    "r_multiple_raw": -1.0,
                    "bars_to_outcome": (ts - filled_ts) // 60,
                }
            )
            return base_payload, None

    if selected:
        last_ts = selected[-1][0]
        if last_ts >= expiration_ts and now_ts >= expiration_ts:
            base_payload.update({"outcome_raw": "TIMEOUT", "bars_to_outcome": (expiration_ts - filled_ts) // 60})
            return base_payload, None
        if len(selected) >= max(1, int(max_scan_bars)):
            return base_payload, int(last_ts + 60)
        return base_payload, int(last_ts + 60)

    if now_ts >= expiration_ts:
        base_payload.update({"outcome_raw": "TIMEOUT", "bars_to_outcome": max(0, (expiration_ts - filled_ts) // 60)})
        return base_payload, None
    return base_payload, int(scan_start_ts)


def _wilson_ci(wins: float, n: int, z: float = 1.96) -> tuple[float | None, float | None]:
    if n <= 0:
        return None, None
    p = max(0.0, min(1.0, float(wins) / float(n)))
    denom = 1.0 + (z * z) / n
    center = (p + (z * z) / (2 * n)) / denom
    margin = (z / denom) * math.sqrt((p * (1 - p) / n) + ((z * z) / (4 * n * n)))
    return max(0.0, center - margin), min(1.0, center + margin)


def _bootstrap_mean_ci(values: list[float], *, samples: int = 200, alpha: float = 0.05) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    if len(values) == 1:
        return values[0], values[0]
    rng = random.Random(42)
    means: list[float] = []
    n = len(values)
    for _ in range(max(50, int(samples))):
        sample = [values[rng.randrange(0, n)] for _ in range(n)]
        means.append(sum(sample) / n)
    means.sort()
    lo_idx = int((alpha / 2) * len(means))
    hi_idx = int((1 - alpha / 2) * len(means)) - 1
    lo_idx = max(0, min(len(means) - 1, lo_idx))
    hi_idx = max(0, min(len(means) - 1, hi_idx))
    return means[lo_idx], means[hi_idx]


def _score_trade_outcome(
    *,
    outcome: str,
    mode: str,
    r_tp: float | None,
    realistic_p: float,
) -> tuple[float, float, bool, bool]:
    out = str(outcome or "OPEN").upper()
    mode_u = str(mode or "STRICT").upper()
    if out == "TP":
        return 1.0, float(r_tp if r_tp is not None else 1.0), True, True
    if out == "SL":
        return 0.0, -1.0, True, True
    if out == "AMBIGUOUS":
        if mode_u == "OPTIMISTIC":
            return 1.0, float(r_tp if r_tp is not None else 1.0), True, False
        if mode_u == "REALISTIC":
            p = max(0.0, min(1.0, float(realistic_p)))
            r = (p * float(r_tp if r_tp is not None else 1.0)) + ((1.0 - p) * -1.0)
            return p, r, True, False
        return 0.0, -1.0, True, False
    return 0.0, 0.0, False, False


def _compute_window_score(
    rows: list[tuple[StrategyDecision, DecisionExecution | None, DecisionEval | None, Any]],
    *,
    scoring_mode: str,
    realistic_p: float,
    min_trades: int,
) -> dict[str, Any]:
    n_trades = 0
    n_resolved = 0
    n_ambiguous = 0
    n_timeout = 0
    wins = 0.0
    r_values: list[float] = []
    r_sum = 0.0

    for decision, execution, eval_row, _signal in rows:
        exec_status = str((execution.status if execution else "PENDING") or "PENDING").upper()
        outcome = str((eval_row.outcome_raw if eval_row else "OPEN") or "OPEN").upper()
        if exec_status == "NO_FILL" or outcome == "NO_FILL":
            continue
        if outcome == "TIMEOUT":
            n_timeout += 1
            continue
        if exec_status != "FILLED":
            continue
        if outcome not in {"TP", "SL", "AMBIGUOUS"}:
            continue

        r_tp = _calc_r_tp(decision)
        win_c, r_c, counts_trade, resolved = _score_trade_outcome(
            outcome=outcome,
            mode=scoring_mode,
            r_tp=r_tp,
            realistic_p=realistic_p,
        )
        if counts_trade:
            n_trades += 1
            wins += win_c
            r_sum += r_c
            r_values.append(r_c)
        if resolved:
            n_resolved += 1
        if outcome == "AMBIGUOUS":
            n_ambiguous += 1

    win_rate = (wins / n_trades) if n_trades > 0 else None
    avg_r = (r_sum / n_trades) if n_trades > 0 else None
    win_ci_low, win_ci_high = _wilson_ci(wins, n_trades) if n_trades > 0 else (None, None)
    avg_ci_low, avg_ci_high = _bootstrap_mean_ci(r_values) if r_values else (None, None)
    timeout_rate = (n_timeout / max(1, (n_trades + n_timeout))) if (n_trades + n_timeout) > 0 else None
    status = "OK" if n_trades >= max(1, int(min_trades)) else "INSUFFICIENT_DATA"
    return {
        "status": status,
        "n_trades": n_trades,
        "n_resolved": n_resolved,
        "n_ambiguous": n_ambiguous,
        "n_timeout": n_timeout,
        "win_rate": win_rate,
        "avg_r": avg_r,
        "win_rate_ci_low": win_ci_low,
        "win_rate_ci_high": win_ci_high,
        "avg_r_ci_low": avg_ci_low,
        "avg_r_ci_high": avg_ci_high,
        "timeout_rate": timeout_rate,
    }


def _strategy_split_windows(now_ts: int) -> list[tuple[str, int, int]]:
    oos_days = 14
    is_days = 60
    oos_end = int(now_ts)
    oos_start = oos_end - (oos_days * 24 * 3600)
    is_end = oos_start
    is_start = is_end - (is_days * 24 * 3600)
    return [
        ("IN_SAMPLE", is_start, is_end),
        ("OUT_OF_SAMPLE", oos_start, oos_end),
    ]


def _confidence_bucket(conf: float | None) -> str:
    c = 0.0 if conf is None else max(0.0, min(1.0, float(conf)))
    if c < 0.25:
        return "CONF_Q1"
    if c < 0.50:
        return "CONF_Q2"
    if c < 0.75:
        return "CONF_Q3"
    return "CONF_Q4"


def _regime_bucket(raw: str | None) -> str:
    r = str(raw or "").strip().upper()
    if r in {"TRENDING_UP", "UP"}:
        return "TRENDING_UP"
    if r in {"TRENDING_DOWN", "DOWN"}:
        return "TRENDING_DOWN"
    if r in {"RANGING", "SIDE"}:
        return "RANGING"
    if r in {"VOLATILE", "VOL"}:
        return "VOLATILE"
    return "UNCERTAIN"


def _derive_regime_id(raw: str | None) -> str:
    b = _regime_bucket(raw)
    if b == "TRENDING_UP":
        return "UP_MID_NORMAL"
    if b == "TRENDING_DOWN":
        return "DOWN_MID_NORMAL"
    if b == "VOLATILE":
        return "SIDE_HIGH_SHOCK"
    if b == "RANGING":
        return "SIDE_LOW_NORMAL"
    return "SIDE_MID_NORMAL"


async def decision_eval_job(runtime: WorkerRuntime) -> dict[str, Any]:
    settings = runtime.settings
    batch_size = max(1, int(getattr(settings, "strategy_eval_batch_size", 120) or 120))
    max_scan_bars = max(10, int(getattr(settings, "strategy_eval_max_bars_per_decision", 720) or 720))
    cache_size = max(2, int(getattr(settings, "strategy_eval_ohlcv_cache_size", 16) or 16))
    now_ts = int(datetime.now(timezone.utc).timestamp())
    checkpoints = _load_eval_checkpoint_map()
    cache = _OhlcvSliceCache(max_size=cache_size)

    rows_written = 0
    rows_read = 0
    pending = 0
    resolved = 0
    backlog = 0

    with runtime.session_factory() as session:
        candidates = list_strategy_decisions_for_eval(session, limit=batch_size)
        backlog = len(candidates)
        for decision, execution, eval_row in candidates:
            rows_read += 1
            expiration_ts = _decision_expiration_ts(decision)
            bars_fill = cache.get(
                session,
                symbol=decision.symbol,
                start_ts=int(decision.decision_ts),
                end_ts=expiration_ts,
            )

            if execution and str(execution.status or "").upper() in {"FILLED", "NO_FILL"}:
                fill = {
                    "status": str(execution.status or "PENDING").upper(),
                    "filled": bool(execution.filled),
                    "filled_ts": execution.filled_ts,
                    "filled_price": execution.filled_price,
                    "filled_qty": execution.filled_qty,
                    "avg_fill_price": execution.avg_fill_price,
                    "source": execution.source or "SIMULATED",
                    "notes": execution.notes,
                }
            else:
                fill = _simulate_fill_for_decision(
                    decision,
                    bars=bars_fill,
                    decision_ts=int(decision.decision_ts),
                    expiration_ts=expiration_ts,
                )
                upsert_decision_execution(session, decision_id=decision.id, payload=fill, commit=False)
                rows_written += 1

            fill_status = str(fill.get("status") or "PENDING").upper()
            if fill_status == "NO_FILL":
                upsert_decision_eval(
                    session,
                    decision_id=decision.id,
                    payload={
                        "eval_replay_tf": "1m",
                        "intrabar_flag": "NONE",
                        "tp_hit": False,
                        "sl_hit": False,
                        "outcome_raw": "NO_FILL",
                        "first_hit_ts": None,
                        "exit_ts": None,
                        "exit_price": None,
                        "r_multiple_raw": None,
                        "mfe": None,
                        "mae": None,
                        "bars_to_outcome": 0,
                    },
                    commit=False,
                )
                checkpoints.pop(str(decision.id), None)
                rows_written += 1
                resolved += 1
                continue
            if fill_status != "FILLED":
                pending += 1
                continue

            filled_ts = int(fill.get("filled_ts") or decision.decision_ts)
            scan_start_ts = max(filled_ts, int(checkpoints.get(str(decision.id), filled_ts)))
            scan_end_ts = min(expiration_ts, scan_start_ts + max_scan_bars * 60)
            bars_post = cache.get(
                session,
                symbol=decision.symbol,
                start_ts=scan_start_ts,
                end_ts=scan_end_ts,
            )
            eval_payload, next_checkpoint = _simulate_post_fill_eval(
                decision,
                bars=bars_post,
                filled_ts=filled_ts,
                scan_start_ts=scan_start_ts,
                expiration_ts=expiration_ts,
                max_scan_bars=max_scan_bars,
                now_ts=now_ts,
            )
            upsert_decision_eval(session, decision_id=decision.id, payload=eval_payload, commit=False)
            rows_written += 1
            if next_checkpoint is not None and str(eval_payload.get("outcome_raw") or "").upper() == "OPEN":
                checkpoints[str(decision.id)] = int(next_checkpoint)
                pending += 1
            else:
                checkpoints.pop(str(decision.id), None)
                resolved += 1

        session.commit()

    _save_eval_checkpoint_map(checkpoints)
    return {
        "rows_read": rows_read,
        "rows_written": rows_written,
        "backlog": backlog,
        "resolved": resolved,
        "pending": pending,
        "checkpoint_size": len(checkpoints),
    }


async def strategy_scores_job(runtime: WorkerRuntime) -> dict[str, Any]:
    settings = runtime.settings
    min_trades = max(1, int(getattr(settings, "strategy_research_min_trades", 50) or 50))
    realistic_p = max(0.0, min(1.0, float(getattr(settings, "strategy_ambiguous_realistic_p", 0.5) or 0.5)))
    now_ts = int(datetime.now(timezone.utc).timestamp())
    rows_read = 0
    rows_written = 0

    with runtime.session_factory() as session:
        manifests = list_manifest_ids_for_strategy(session, limit=1000)
        for manifest_id in manifests:
            for split_type, ws, we in _strategy_split_windows(now_ts):
                rows = list_strategy_rows_for_window(
                    session,
                    manifest_id=manifest_id,
                    window_start_ts=ws,
                    window_end_ts=we,
                )
                rows_read += len(rows)
                for mode in ("STRICT", "REALISTIC", "OPTIMISTIC"):
                    stats = _compute_window_score(
                        rows,
                        scoring_mode=mode,
                        realistic_p=realistic_p,
                        min_trades=min_trades,
                    )
                    upsert_strategy_score(
                        session,
                        {
                            "manifest_id": manifest_id,
                            "window_start_ts": ws,
                            "window_end_ts": we,
                            "split_type": split_type,
                            "scoring_mode": mode,
                            **stats,
                        },
                        commit=False,
                    )
                    rows_written += 1
        session.commit()

    return {"rows_read": rows_read, "rows_written": rows_written}


async def strategy_research_job(runtime: WorkerRuntime) -> dict[str, Any]:
    settings = runtime.settings
    min_trades = max(1, int(getattr(settings, "strategy_research_min_trades", 50) or 50))
    realistic_p = max(0.0, min(1.0, float(getattr(settings, "strategy_ambiguous_realistic_p", 0.5) or 0.5)))
    now_ts = int(datetime.now(timezone.utc).timestamp())
    rows_read = 0
    rows_written = 0
    tested_features: set[str] = set()

    with runtime.session_factory() as session:
        manifests = list_manifest_ids_for_strategy(session, limit=1000)
        for manifest_id in manifests:
            for split_type, ws, we in _strategy_split_windows(now_ts):
                rows = list_strategy_rows_for_window(
                    session,
                    manifest_id=manifest_id,
                    window_start_ts=ws,
                    window_end_ts=we,
                )
                rows_read += len(rows)
                for scoring_mode in ("STRICT", "REALISTIC", "OPTIMISTIC"):
                    agg: dict[tuple[str, str, str], dict[str, Any]] = {}
                    for decision, execution, eval_row, signal in rows:
                        exec_status = str((execution.status if execution else "PENDING") or "PENDING").upper()
                        outcome = str((eval_row.outcome_raw if eval_row else "OPEN") or "OPEN").upper()
                        if exec_status != "FILLED" or outcome not in {"TP", "SL", "AMBIGUOUS"}:
                            continue
                        r_tp = _calc_r_tp(decision)
                        win_c, r_c, counts_trade, _resolved = _score_trade_outcome(
                            outcome=outcome,
                            mode=scoring_mode,
                            r_tp=r_tp,
                            realistic_p=realistic_p,
                        )
                        if not counts_trade:
                            continue

                        market_regime = getattr(signal, "market_regime", None) if signal is not None else None
                        regime_id = _derive_regime_id(market_regime)
                        features = {
                            "signal_confidence": _confidence_bucket(decision.confidence),
                            "position_side": str(decision.position_side or "HOLD").upper(),
                            "market_regime": _regime_bucket(market_regime),
                        }
                        for feature_key, bucket_key in features.items():
                            tested_features.add(feature_key)
                            allowed = _FEATURE_WHITELIST_BUCKETS.get(feature_key)
                            if not allowed or bucket_key not in allowed:
                                logger.error(
                                    "strategy_research reject feature write feature=%s bucket=%s manifest=%s",
                                    feature_key,
                                    bucket_key,
                                    manifest_id,
                                )
                                continue
                            k = (regime_id, feature_key, bucket_key)
                            item = agg.setdefault(k, {"n": 0, "wins": 0.0, "r_values": []})
                            item["n"] += 1
                            item["wins"] += win_c
                            item["r_values"].append(float(r_c))

                    for (regime_id, feature_key, bucket_key), item in agg.items():
                        n = int(item["n"])
                        wins = float(item["wins"])
                        r_values = list(item["r_values"])
                        win_rate = (wins / n) if n > 0 else None
                        avg_r = (sum(r_values) / n) if n > 0 else None
                        ci_low, ci_high = _wilson_ci(wins, n) if n > 0 else (None, None)
                        avg_ci_low, avg_ci_high = _bootstrap_mean_ci(r_values) if r_values else (None, None)
                        status = "OK" if n >= min_trades else "INSUFFICIENT_DATA"
                        upsert_strategy_feature_stat(
                            session,
                            {
                                "manifest_id": manifest_id,
                                "window_start_ts": ws,
                                "window_end_ts": we,
                                "split_type": split_type,
                                "regime_id": regime_id,
                                "scoring_mode": scoring_mode,
                                "feature_key": feature_key,
                                "bucket_key": bucket_key,
                                "status": status,
                                "n": n,
                                "win_rate": win_rate,
                                "avg_r": avg_r,
                                "ci_low": ci_low if ci_low is not None else avg_ci_low,
                                "ci_high": ci_high if ci_high is not None else avg_ci_high,
                            },
                            commit=False,
                        )
                        rows_written += 1

        session.commit()

    return {
        "rows_read": rows_read,
        "rows_written": rows_written,
        "tested_features": sorted(tested_features),
    }
