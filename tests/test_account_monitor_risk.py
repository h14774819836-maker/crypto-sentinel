from __future__ import annotations

from app.scheduler.jobs import _calc_dynamic_liq_threshold_pct, _liquidation_distance_pct


def test_liquidation_distance_long_and_short_direction():
    long_pct = _liquidation_distance_pct(mark_price=100.0, liq_price=90.0, position_amt=0.01)
    short_pct = _liquidation_distance_pct(mark_price=100.0, liq_price=110.0, position_amt=-0.01)
    assert long_pct is not None and abs(long_pct - 10.0) < 1e-9
    assert short_pct is not None and abs(short_pct - 10.0) < 1e-9


def test_liquidation_distance_invalid_inputs_are_skipped():
    assert _liquidation_distance_pct(mark_price=100.0, liq_price=0.0, position_amt=0.01) is None
    assert _liquidation_distance_pct(mark_price=100.0, liq_price=90.0, position_amt=0.0) is None
    # Wrong-side liquidation values should not emit misleading risk alerts.
    assert _liquidation_distance_pct(mark_price=100.0, liq_price=110.0, position_amt=0.01) is None


def test_dynamic_liq_threshold_uses_atr_when_available():
    threshold, dynamic = _calc_dynamic_liq_threshold_pct(
        mark_price=100.0,
        atr_14=3.0,
        atr_multiplier=1.5,
        static_floor_pct=2.0,
    )
    assert dynamic is not None and abs(dynamic - 4.5) < 1e-9
    assert abs(threshold - 4.5) < 1e-9


def test_dynamic_liq_threshold_falls_back_to_static_floor():
    threshold, dynamic = _calc_dynamic_liq_threshold_pct(
        mark_price=100.0,
        atr_14=None,
        atr_multiplier=1.5,
        static_floor_pct=5.0,
    )
    assert dynamic is None
    assert abs(threshold - 5.0) < 1e-9
