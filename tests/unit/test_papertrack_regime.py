"""Tests for the Tier-1.2 trend/tail-risk overlay wired into papertrack.

The overlay is the one adoptable upgrade from the TIER 1 sweep: de-risk the book
to cash when the equal-weight universe index is below its 200d MA. These tests
pin the risk-on/risk-off switch and the insufficient-history default. No DB.
"""
import numpy as np
import pandas as pd

from src.papertrack import _market_regime


def _panel(daily_drift: float, n: int = 320, cols: int = 5) -> pd.DataFrame:
    """Synthetic price panel with a constant daily drift (no noise), so the
    equal-weight index is a clean up/down trend for a deterministic regime call."""
    idx = pd.bdate_range("2020-01-01", periods=n)
    base = 100.0 * np.cumprod(1 + daily_drift * np.ones(n))
    return pd.DataFrame({f"SYM{i}": base for i in range(cols)}, index=idx)


def test_uptrend_is_risk_on():
    px = _panel(daily_drift=0.001)          # steady climb -> above its own MA
    r = _market_regime(px, px.index.max())
    assert r["risk_on"] is True
    assert r["target_exposure"] == 1.0
    assert r["index_level"] > r["index_ma"]


def test_downtrend_is_risk_off():
    px = _panel(daily_drift=-0.001)         # steady fall -> below its own MA
    r = _market_regime(px, px.index.max())
    assert r["risk_on"] is False
    assert r["target_exposure"] == 0.0
    assert r["index_level"] < r["index_ma"]


def test_insufficient_history_defaults_risk_on():
    px = _panel(daily_drift=-0.001, n=50)   # fewer than ma_days rows
    r = _market_regime(px, px.index.max())
    assert r["risk_on"] is True             # fail-safe: stay invested, don't guess
    assert r["target_exposure"] == 1.0
    assert r["index_level"] is None


def test_regime_uses_only_data_up_to_t():
    """A crash strictly AFTER t must not affect the regime call at t."""
    px = _panel(daily_drift=0.001)
    t = px.index[260]
    calm = _market_regime(px, t)
    # slam prices to zero after t; the call at t must be unchanged
    px.loc[px.index > t] = 0.01
    assert _market_regime(px, t) == calm
