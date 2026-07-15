"""Tests for the Phase B risk guardrails (pure — no DB). Confirms each limit fires
when breached and stays quiet when satisfied, and that the kill-switch latches."""
import json

import pytest

from src import guardrails as g


def _holdings(n, w=0.04, price=100.0, mcap=5000.0):
    return [{"symbol": f"S{i}", "weight": w, "entry_price": price, "mktcap_cr": mcap} for i in range(n)]


# diversified sector map (6 sectors, ~evenly split) so a clean book has no one
# sector > 45%; concentration tests use their own all-one-sector map.
_SECS = ["Financials", "Energy", "Tech", "FMCG", "Pharma", "Auto"]
SECTORS = {f"S{i}": _SECS[i % len(_SECS)] for i in range(30)}


# ---- data quality ----
def test_stale_data_blocks():
    v = g.check_data_quality({"stale_days": 12}, 900)
    assert [c["code"] for c in v] == ["stale_data"] and v[0]["severity"] == g.BLOCK


def test_fresh_and_broad_passes():
    assert g.check_data_quality({"stale_days": 1}, 900) == []


def test_thin_universe_blocks():
    assert g.check_data_quality({"stale_days": 0}, 50)[0]["code"] == "thin_universe"


# ---- composition ----
def test_clean_equal_weight_book_passes():
    assert g.check_composition(_holdings(25), 1.0, SECTORS) == []


def test_too_few_names_blocks():
    codes = [c["code"] for c in g.check_composition(_holdings(10), 0.4, SECTORS)]
    assert "too_few_names" in codes


def test_oversized_position_blocks():
    h = _holdings(20) + [{"symbol": "BIG", "weight": 0.15, "entry_price": 100, "mktcap_cr": 5000}]
    v = g.check_composition(h, 0.95, {**SECTORS, "BIG": "Tech"})
    assert any(c["code"] == "position_size" and c["severity"] == g.BLOCK for c in v)


def test_leverage_blocks():
    v = g.check_composition(_holdings(25, w=0.05), 1.25, SECTORS)  # gross 1.25
    assert any(c["code"] == "leverage" for c in v)


def test_sector_concentration_warns():
    allfin = {f"S{i}": "Financial Services" for i in range(25)}
    v = g.check_composition(_holdings(25), 1.0, allfin)
    assert any(c["code"] == "sector_concentration" and c["severity"] == g.WARN for c in v)


def test_price_and_mcap_floors_warn():
    h = [{"symbol": "P", "weight": 0.04, "entry_price": 5.0, "mktcap_cr": 800.0}] + _holdings(20)
    codes = [c["code"] for c in g.check_composition(h, 0.84, {**SECTORS, "P": "Tech"})]
    assert "price_floor" in codes and "mcap_floor" in codes


# ---- drawdown ----
def test_drawdown_halt_trips():
    assert g.check_drawdown(-30.0)[0]["severity"] == g.HALT
    assert g.check_drawdown(-10.0) == []
    assert g.check_drawdown(None) == []


# ---- kill-switch latch ----
def test_killswitch_latches(tmp_path, monkeypatch):
    monkeypatch.setattr(g, "_KILL_PATH", tmp_path / "ks.json")
    assert g.read_killswitch()["tripped"] is False
    g.trip_killswitch("drawdown -30%", "2026-07-14T00:00:00Z")
    assert g.read_killswitch()["tripped"] is True
    g.reset_killswitch()
    assert g.read_killswitch()["tripped"] is False


# ---- aggregate ----
def test_evaluate_blocks_and_reports(tmp_path, monkeypatch):
    monkeypatch.setattr(g, "_KILL_PATH", tmp_path / "ks.json")
    pf = {"universe_size": 900, "target_exposure": 1.0, "holdings": _holdings(25)}
    r = g.evaluate(pf, {"stale_days": 1}, SECTORS, book_ret_pct=0.0)
    assert r["status"] == g.OK and r["ok_to_record"] is True

    bad = {"universe_size": 50, "target_exposure": 1.0, "holdings": _holdings(8)}
    r2 = g.evaluate(bad, {"stale_days": 20}, SECTORS, book_ret_pct=-30.0)
    assert r2["ok_to_record"] is False and r2["status"] == g.HALT
    assert r2["should_trip"] is True
