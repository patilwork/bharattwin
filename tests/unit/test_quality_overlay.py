"""Tests for the Tier-2 deep-fundamental quality/forensic metric+flag logic.

compute_metrics is pure (no DB) — these pin the forensic red-flag rules and the
financial-sector suppression that stops CFO/leverage flags firing on banks/NBFCs.
"""
from src.quality import compute_metrics, is_financial


def _row(**kw):
    base = dict(revenue=100.0, pat=10.0, operating_profit=15.0, cash_from_ops=12.0,
                total_assets=200.0, total_debt=40.0, net_worth=80.0)
    base.update(kw)
    return base


def test_healthy_company_no_flags():
    m = compute_metrics(_row(), financial=False)
    assert m["red_flags"] == []
    assert m["roa"] == 10.0 / 200.0
    assert m["cash_conversion"] == 12.0 / 10.0
    assert m["leverage"] == 40.0 / 80.0


def test_pat_exceeds_revenue_flag():
    m = compute_metrics(_row(revenue=20.0, pat=50.0), financial=False)
    assert any("PAT>revenue" in f for f in m["red_flags"])


def test_negative_net_worth_flag():
    m = compute_metrics(_row(net_worth=-30.0), financial=False)
    assert any("negative net worth" in f for f in m["red_flags"])
    assert m["leverage"] is None            # undefined when net worth <= 0


def test_negative_cfo_flag_nonfinancial():
    m = compute_metrics(_row(cash_from_ops=-5.0), financial=False)
    assert any("negative operating cash flow" in f for f in m["red_flags"])


def test_weak_cash_conversion_flag():
    m = compute_metrics(_row(cash_from_ops=2.0, pat=10.0), financial=False)  # CFO/PAT=0.2
    assert any("weak cash conversion" in f for f in m["red_flags"])


def test_high_leverage_flag():
    m = compute_metrics(_row(total_debt=300.0, net_worth=80.0), financial=False)  # D/E=3.75
    assert any("high leverage" in f for f in m["red_flags"])


def test_financial_suppresses_cashflow_and_leverage_flags():
    # A bank with negative CFO and huge leverage: those flags must be suppressed,
    # but sector-agnostic flags (negative net worth) still fire.
    m = compute_metrics(_row(cash_from_ops=-50.0, total_debt=1000.0, net_worth=-10.0),
                        financial=True)
    assert not any("cash flow" in f or "leverage" in f or "cash conversion" in f
                   for f in m["red_flags"])
    assert any("negative net worth" in f for f in m["red_flags"])


def test_is_financial_handles_missing_and_nan():
    import numpy as np
    assert is_financial("Financial Services") is True
    assert is_financial("Oil Gas & Consumable Fuels") is False
    assert is_financial(None) is False
    assert is_financial(float("nan")) is False
    assert is_financial(np.nan) is False
