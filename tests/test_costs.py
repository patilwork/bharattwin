"""Tests for the India transaction-cost engine."""

import pytest

from src.costs import RATES, breakeven_move_pct, round_trip_cost


def test_all_segments_priced():
    for seg in RATES:
        c = round_trip_cost(1_000_000, seg)
        assert c.total > 0
        assert c.total_bps > 0
        # breakeven is total_bps/100, both fields independently rounded
        assert c.breakeven_move_pct == pytest.approx(c.total_bps / 100, abs=1e-3)


def test_futures_cost_in_expected_band():
    # The operating guide's key claim: liquid Nifty futures ~5-8 bps round trip.
    c = round_trip_cost(1_000_000, "index_futures")
    assert 4.0 <= c.total_bps <= 9.0


def test_delivery_costs_more_than_futures():
    # 0.1% STT on both legs makes delivery far pricier than futures.
    assert round_trip_cost(1_000_000, "equity_delivery").total_bps > round_trip_cost(
        1_000_000, "index_futures"
    ).total_bps


def test_bps_scale_invariant():
    # bps cost should be ~flat in notional once brokerage cap stops binding.
    a = round_trip_cost(5_000_000, "index_futures").total_bps
    b = round_trip_cost(10_000_000, "index_futures").total_bps
    assert abs(a - b) < 0.5


def test_breakeven_helper_matches():
    assert breakeven_move_pct("index_futures") == round_trip_cost(
        1_000_000, "index_futures"
    ).breakeven_move_pct


def test_unknown_segment_raises():
    with pytest.raises(ValueError):
        round_trip_cost(1_000_000, "crypto_perp")
