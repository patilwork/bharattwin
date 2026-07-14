"""
TIER 2 — deep-fundamental quality / forensic overlay (LIVE snapshot).

Dawn's deep fields (revenue, pat, operating_profit, cash_from_ops, total_assets,
total_debt, net_worth) are populated for the latest annual snapshot only (~1083
names, period_end 2026-03-31) — a current cross-section, NOT a history. So this is
a LIVE OVERLAY that annotates the momentum+value picks with a quality score and
forensic red flags; it is deliberately NOT a backtested factor (that needs paid
history — PROJECT_STATUS §7 Tier 3).

Purpose: a "cheap-AND-good-AND-clean" screen. Momentum can chase names whose run
is driven by an accounting one-off (PAT > revenue), a distressed balance sheet
(negative net worth), or earnings that don't convert to cash (weak CFO/PAT). This
surfaces those so a human can veto or size down.

Sector caveat: banks/NBFCs/insurers ("Financial Services") have non-comparable
cash-flow and leverage accounting, so CFO/accruals/leverage flags are suppressed
for them and their quality score falls back to ROA only.
"""
from __future__ import annotations

import os

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

DAWN_URL = os.environ.get("DAWN_URL", "postgresql://localhost:5432/dawn")

FINANCIAL_INDUSTRIES = {"Financial Services"}
# ratio metrics that make up the quality composite (higher = better after signing)
_QUALITY_METRICS = ["roa", "op_profitability", "roic", "neg_accruals", "cash_conversion"]


def is_financial(nse_industry: str | None) -> bool:
    if nse_industry is None or (isinstance(nse_industry, float) and np.isnan(nse_industry)):
        return False
    return str(nse_industry).strip() in FINANCIAL_INDUSTRIES


def compute_metrics(row: dict, financial: bool) -> dict:
    """Per-name fundamental ratios + forensic red flags from one deep-fundamental
    row. Pure (no I/O). `row` uses Dawn column names; missing values -> None/NaN."""
    def g(k):
        v = row.get(k)
        return float(v) if v is not None and not (isinstance(v, float) and np.isnan(v)) else None

    rev, pat, op = g("revenue"), g("pat"), g("operating_profit")
    cfo, ta, debt, nw = g("cash_from_ops"), g("total_assets"), g("total_debt"), g("net_worth")

    roa = pat / ta if (pat is not None and ta) else None
    op_profitability = op / ta if (op is not None and ta) else None
    roic = op / (debt + nw) if (op is not None and debt is not None and nw is not None and (debt + nw) > 0) else None
    accruals = (pat - cfo) / ta if (pat is not None and cfo is not None and ta) else None
    cash_conversion = cfo / pat if (cfo is not None and pat and pat > 0) else None
    leverage = debt / nw if (debt is not None and nw is not None and nw > 0) else None

    flags: list[str] = []
    # sector-agnostic red flags
    if rev is not None and pat is not None and rev > 0 and pat > rev:
        flags.append("PAT>revenue (exceptional/one-off item?)")
    if nw is not None and nw < 0:
        flags.append("negative net worth")
    # ratio-based flags — meaningless for financials, so suppressed there
    if not financial:
        if cfo is not None and cfo < 0:
            flags.append("negative operating cash flow")
        elif cash_conversion is not None and cash_conversion < 0.5:
            flags.append(f"weak cash conversion (CFO/PAT={cash_conversion:.2f})")
        if leverage is not None and leverage > 3:
            flags.append(f"high leverage (D/E={leverage:.1f})")
        elif nw is not None and nw <= 0 and debt and debt > 0:
            flags.append("leverage undefined (net worth <= 0)")

    return {
        "roa": roa, "op_profitability": op_profitability, "roic": roic,
        "accruals": accruals, "neg_accruals": (-accruals if accruals is not None else None),
        "cash_conversion": (min(cash_conversion, 3.0) if cash_conversion is not None else None),
        "leverage": leverage, "financial": financial,
        "red_flags": flags, "n_flags": len(flags),
    }


def _z(s: pd.Series) -> pd.Series:
    s = s.replace([np.inf, -np.inf], np.nan)
    lo, hi = s.quantile(0.02), s.quantile(0.98)
    s = s.clip(lo, hi)
    return (s - s.mean()) / s.std() if s.std() else s * 0.0


def load_quality(as_of: str | None = None) -> pd.DataFrame:
    """Load the latest deep-fundamental snapshot (consolidated preferred) joined
    with sector, compute per-name metrics and a cross-sectional quality score.
    Returns a DataFrame indexed by symbol. Quality score is z-scored across the
    NON-financial universe (financials scored on ROA only, clearly labelled)."""
    eng = create_engine(DAWN_URL)
    try:
        cutoff = as_of or "9999-12-31"
        f = pd.read_sql(text("""
            SELECT DISTINCT ON (symbol) symbol, period_end, basis,
                   revenue, pat, operating_profit, cash_from_ops,
                   total_assets, total_debt, net_worth
            FROM security_fundamentals
            WHERE revenue IS NOT NULL AND period_end <= :c
              AND symbol ~ '^[A-Z][A-Z0-9&-]{1,}$'
            ORDER BY symbol, period_end DESC, (basis='consolidated') DESC
        """), eng, params={"c": cutoff})
        sec = pd.read_sql(text("""
            SELECT DISTINCT ON (nse_symbol) nse_symbol AS symbol, nse_industry
            FROM security_sector WHERE nse_symbol IS NOT NULL
            ORDER BY nse_symbol, nse_industry NULLS LAST
        """), eng)
    finally:
        eng.dispose()

    df = f.merge(sec, on="symbol", how="left")
    if df.empty:
        return pd.DataFrame()

    recs = []
    for _, r in df.iterrows():
        fin = is_financial(r.get("nse_industry"))
        m = compute_metrics(r.to_dict(), fin)
        m["symbol"] = r["symbol"]
        m["nse_industry"] = r.get("nse_industry")
        m["period_end"] = r["period_end"]
        recs.append(m)
    q = pd.DataFrame(recs).set_index("symbol")

    # composite score: z-mean of the quality metrics over NON-financials
    nonfin = q[~q["financial"]]
    zsum = pd.DataFrame(index=q.index)
    for col in _QUALITY_METRICS:
        z = _z(nonfin[col].astype(float))
        zsum[col] = z.reindex(q.index)
    q["quality_score"] = zsum.mean(axis=1, skipna=True)
    # financials: fall back to ROA z over financials only
    fin_roa_z = _z(q.loc[q["financial"], "roa"].astype(float))
    q.loc[q["financial"], "quality_score"] = fin_roa_z
    q["quality_rank_pct"] = q["quality_score"].rank(pct=True)
    return q


def annotate(symbols: list[str], as_of: str | None = None) -> dict:
    """Quality/forensic annotation for a specific pick list. Returns per-symbol
    score/flags plus a rollup, for the orchestrator report."""
    q = load_quality(as_of)
    out = {}
    for s in symbols:
        if s in q.index:
            r = q.loc[s]
            out[s] = {
                "quality_score": None if pd.isna(r["quality_score"]) else round(float(r["quality_score"]), 3),
                "quality_rank_pct": None if pd.isna(r["quality_rank_pct"]) else round(float(r["quality_rank_pct"]), 3),
                "roa": None if pd.isna(r["roa"]) else round(float(r["roa"]), 4),
                "cash_conversion": None if pd.isna(r["cash_conversion"]) else round(float(r["cash_conversion"]), 2),
                "leverage": None if pd.isna(r["leverage"]) else round(float(r["leverage"]), 2),
                "financial": bool(r["financial"]),
                "red_flags": list(r["red_flags"]),
            }
        else:
            out[s] = {"quality_score": None, "red_flags": [], "no_data": True}
    flagged = [s for s, v in out.items() if v.get("red_flags")]
    covered = [s for s, v in out.items() if not v.get("no_data")]
    return {"by_symbol": out, "n_covered": len(covered), "n_missing": len(symbols) - len(covered),
            "flagged": flagged, "n_flagged": len(flagged)}
