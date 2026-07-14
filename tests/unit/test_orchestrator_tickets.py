"""Tests for the Phase A order-ticket diff (pure, no I/O).

build_order_tickets turns (currently-held book, new target book) into placeable
BUY/SELL/HOLD tickets. These pin the diff logic, incl. the Tier-1.2 risk-off case
where the new book is all-cash and everything must be sold.
"""
from src.orchestrator import build_order_tickets

N = 100_000.0


def _h(sym, w, price=100.0):
    return {"symbol": sym, "weight": w, "entry_price": price}


def test_fresh_entry_all_buy():
    new = [_h("A", 0.5), _h("B", 0.5)]
    o = build_order_tickets([], new, N, target_exposure=1.0)
    assert o["n_buy"] == 2 and o["n_sell"] == 0 and o["n_hold"] == 0
    assert o["cash_value"] == 0.0
    assert {t["symbol"]: t["target_value"] for t in o["tickets"]} == {"A": 50_000.0, "B": 50_000.0}


def test_rotation_buy_sell_hold():
    held = [_h("A", 0.5), _h("B", 0.5)]
    new = [_h("A", 0.5), _h("C", 0.5)]          # keep A, drop B, add C
    o = build_order_tickets(held, new, N, target_exposure=1.0)
    acts = {t["symbol"]: t["action"] for t in o["tickets"]}
    assert acts == {"A": "HOLD", "B": "SELL", "C": "BUY"}
    assert o["turnover_names"] == 2


def test_rebalance_when_weight_changes():
    held = [_h("A", 0.4), _h("B", 0.6)]
    new = [_h("A", 0.6), _h("B", 0.4)]
    o = build_order_tickets(held, new, N, target_exposure=1.0)
    acts = {t["symbol"]: t["action"] for t in o["tickets"]}
    assert acts == {"A": "REBALANCE", "B": "REBALANCE"}
    assert o["n_hold"] == 2                      # HOLD/REBAL bucket


def test_risk_off_sells_everything_to_cash():
    held = [_h("A", 0.5), _h("B", 0.5)]
    # Tier-1.2 de-risk: new target weights are all 0, exposure 0
    new = [_h("A", 0.0), _h("B", 0.0)]
    o = build_order_tickets(held, new, N, target_exposure=0.0)
    assert o["n_sell"] == 2 and o["n_buy"] == 0
    assert all(t["action"] == "SELL" for t in o["tickets"])
    assert o["cash_value"] == N and o["cash_weight"] == 1.0


def test_partial_exposure_leaves_cash():
    held = []
    new = [_h("A", 0.3), _h("B", 0.3)]          # 60% invested, 40% cash
    o = build_order_tickets(held, new, N, target_exposure=0.6)
    assert o["cash_value"] == 40_000.0
    assert o["cash_weight"] == 0.4
