"""
The R -> B -> I promotion gate.

Research proposes; STATISTICS decide. Nothing reaches the paper track by looking
good on a backtest — it has to survive a fixed battery whose bar RISES as more
candidates are tried.

Why the registry is the whole point
-----------------------------------
`deflated_sharpe` (Bailey & Lopez de Prado) deflates an observed Sharpe by the
expected maximum Sharpe under `n_trials` independent attempts. Run 5 experiments
and a 1.2 Sharpe is interesting; run 500 and the best of them is *expected* to
look that good by luck alone. So `n_trials` must be the number of candidates ever
tested, not the number in the current batch.

TrialRegistry is that memory: an append-only log of every spec ever evaluated.
The gate reads its length and feeds it into the DSR. Consequence: an agent that
sprays 200 hypotheses automatically raises its own bar and mostly fails. That is
the intended behaviour — it converts "LLM proposes strategies" from a p-hacking
engine into a multiple-testing-corrected search.

The gate is deliberately strict and the thresholds are explicit constants below,
not tuned. Tuning the gate on its own outcomes would reintroduce exactly the bias
the gate exists to remove. If you change a threshold, change it for a stated
reason and record it in the docstring history.

Usage:
    from src.promotion import CandidateSpec, evaluate, TrialRegistry
    v = evaluate(CandidateSpec(legs={"momentum": 1.0, "value_ey": 1.0}))
    print(v.verdict, v.reasons)

CLI:
    DAWN_URL=... python3 -m src.promotion --incumbent      # score the live Core spec
    DAWN_URL=... python3 -m src.promotion --registry       # show trial history
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

REGISTRY_PATH = Path(os.environ.get("TRIAL_REGISTRY", REPO / "logs" / "trials.jsonl"))

# ── Gate thresholds. Fixed, not tuned. ───────────────────────────────────────
DSR_MIN = 0.95           # Bailey/LdP convention: P(true SR>0) after deflation
PBO_MAX = 0.50           # >=50% means picking on the backtest does not generalise
CPCV_POS_MIN = 0.60      # fraction of purged OOS paths with positive Sharpe
PERM_P_MAX = 0.01        # permutation test on the signal's IC
NET_SHARPE_MIN = 0.50    # after costs; a floor, not a target
TURNOVER_MAX = 0.60      # mean monthly turnover — cost blows up past this
MIN_MONTHS = 48          # never judge a spec on less than 4 years

# CPCV configuration (matches scripts/tier1_purged_cv.py)
N_GROUPS, K_TEST, EMBARGO = 8, 2, 1

# The factor vocabulary a candidate may draw on. Constrained ON PURPOSE: a spec is
# declarative data, never executable code, so a proposal can be logged, diffed,
# replayed and rejected without ever running untrusted text.
LEGS = ("momentum", "value_ey", "value_pb", "low_vol", "size", "reversal")


@dataclass(frozen=True)
class CandidateSpec:
    """A declarative strategy proposal. Frozen + hashable => stable trial id."""
    legs: dict = field(default_factory=lambda: {"momentum": 1.0})
    formation: int = 252          # momentum formation window (trading days)
    skip: int = 21                # skip most-recent N days (reversal guard)
    min_mcap_cr: float = 2000.0
    max_mcap_cr: float | None = None
    min_price: float = 10.0
    top_quantile: int = 5
    max_holdings: int = 25
    note: str = ""                # free text from the proposer; never executed

    def __post_init__(self):
        bad = set(self.legs) - set(LEGS)
        if bad:
            raise ValueError(f"unknown legs {sorted(bad)}; allowed: {list(LEGS)}")
        if not self.legs:
            raise ValueError("spec needs at least one leg")
        if self.skip >= self.formation:
            raise ValueError("skip must be < formation")

    def key(self) -> str:
        d = asdict(self)
        d.pop("note")                       # note is commentary, not identity
        d["legs"] = sorted(self.legs.items())
        return hashlib.sha256(json.dumps(d, sort_keys=True, default=str).encode()).hexdigest()[:12]


@dataclass
class Verdict:
    spec_key: str
    verdict: str                  # PROMOTE | REJECT
    reasons: list
    metrics: dict
    n_trials: int


class TrialRegistry:
    """Append-only log of every spec ever evaluated. Its length is the multiple-
    testing correction — that is the only reason it exists, so never prune it to
    make a candidate look better."""

    def __init__(self, path: Path = REGISTRY_PATH):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def all(self) -> list:
        if not self.path.exists():
            return []
        out = []
        for line in self.path.read_text().splitlines():
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return out

    def n_trials(self) -> int:
        """Distinct specs ever tested. Re-testing the same spec is not a new trial."""
        return len({r.get("spec_key") for r in self.all()}) or 0

    def sr_variance(self, default: float | None = None) -> float:
        """Cross-trial variance of observed MONTHLY Sharpes — the dispersion term
        the DSR needs. Prefer the registry once it has enough history; otherwise
        measure it from the reference menu (what single-leg factors actually
        disperse like). Only falls back to a constant if both are unavailable.

        This matters more than it looks: sr0 (the hurdle the observed Sharpe must
        clear) scales with sqrt(trial_sr_var), so an invented dispersion silently
        sets the entire bar. The old 0.25 constant implies monthly trial Sharpes
        with SD 0.5 (~1.7 annualised) — far wider than real factors disperse, and
        it rejects everything."""
        srs = [r["metrics"].get("sharpe_monthly") for r in self.all()
               if r.get("metrics", {}).get("sharpe_monthly") is not None
               and np.isfinite(r["metrics"]["sharpe_monthly"])]
        if len(srs) >= 5:
            return float(np.var(srs, ddof=1))
        if default is not None:
            return default
        menu = reference_menu()
        msr = [float(s.mean() / s.std()) for s in menu.values() if s.std()]
        return float(np.var(msr, ddof=1)) if len(msr) > 1 else 0.25

    def record(self, v: Verdict, spec: CandidateSpec) -> None:
        row = {"ts": datetime.now(timezone.utc).isoformat(), "spec_key": v.spec_key,
               "verdict": v.verdict, "spec": asdict(spec), "metrics": v.metrics,
               "reasons": v.reasons, "n_trials_at_eval": v.n_trials}
        with self.path.open("a") as fh:
            fh.write(json.dumps(row, default=str) + "\n")


# ── statistics ───────────────────────────────────────────────────────────────

def deflated_sharpe(returns: pd.Series, n_trials: int, trial_sr_var: float) -> dict:
    """Bailey & Lopez de Prado DSR. Identical formula to scripts/xsection_backtest.py
    (kept here so the gate has no import-time dependency on a CLI script)."""
    r = pd.Series(returns).dropna()
    n = len(r)
    if n < 10 or r.std() == 0:
        return {"sr": np.nan, "sr0": np.nan, "dsr": np.nan}
    sr = r.mean() / r.std()
    sk = stats.skew(r)
    ku = stats.kurtosis(r, fisher=False)
    gamma, e = 0.5772156649, np.e
    n_trials = max(int(n_trials), 2)          # ppf(1-1/1) is undefined
    z1 = stats.norm.ppf(1 - 1.0 / n_trials)
    z2 = stats.norm.ppf(1 - 1.0 / (n_trials * e))
    sr0 = np.sqrt(max(trial_sr_var, 1e-12)) * ((1 - gamma) * z1 + gamma * z2)
    denom = np.sqrt(max(1 - sk * sr + ((ku - 1) / 4) * sr**2, 1e-9))
    return {"sr": float(sr), "sr0": float(sr0),
            "dsr": float(stats.norm.cdf((sr - sr0) * np.sqrt(n - 1) / denom))}


def _sharpe(r) -> float:
    r = pd.Series(r).dropna()
    return float(r.mean() / r.std() * np.sqrt(12)) if len(r) >= 4 and r.std() else np.nan


def cpcv_paths(ret: pd.Series) -> np.ndarray:
    """Purged/embargoed combinatorial CV — the OOS Sharpe distribution for ONE
    spec. (PBO needs a menu of specs and is computed separately.)"""
    n = len(ret)
    bounds = np.linspace(0, n, N_GROUPS + 1).astype(int)
    groups = [list(range(bounds[g], bounds[g + 1])) for g in range(N_GROUPS)]
    out = []
    for combo in combinations(range(N_GROUPS), K_TEST):
        test_idx = sorted(sum((groups[g] for g in combo), []))
        out.append(_sharpe(ret.iloc[test_idx]))
    return np.array([x for x in out if np.isfinite(x)])


def pbo_logit(menu: pd.DataFrame) -> float:
    """Probability of Backtest Overfitting across a menu of competing specs.
    Needs >=2 columns; returns NaN otherwise (a lone spec cannot be 'selected')."""
    if menu.shape[1] < 2:
        return float("nan")
    n = len(menu)
    bounds = np.linspace(0, n, N_GROUPS + 1).astype(int)
    groups = [list(range(bounds[g], bounds[g + 1])) for g in range(N_GROUPS)]
    logits = []
    for combo in combinations(range(N_GROUPS), K_TEST):
        test_idx = sorted(sum((groups[g] for g in combo), []))
        test_set = set(test_idx)
        emb = {j + e for j in test_idx for e in range(-EMBARGO, EMBARGO + 1)}
        train_idx = [j for j in range(n) if j not in test_set and j not in emb]
        tr, te = menu.iloc[train_idx].apply(_sharpe), menu.iloc[test_idx].apply(_sharpe)
        if tr.notna().sum() < 2 or te.notna().sum() < 2:
            continue
        best = tr.idxmax()
        omega = min(max(te.rank()[best] / (te.notna().sum() + 1), 1e-3), 1 - 1e-3)
        logits.append(np.log(omega / (1 - omega)))
    return float((np.array(logits) < 0).mean()) if logits else float("nan")


# ── evaluation ───────────────────────────────────────────────────────────────

_PANEL = None


def _panel():
    """Load and cache the Dawn panel once per process (it is ~5M rows)."""
    global _PANEL
    if _PANEL is None:
        from xsection_montecarlo import load
        _PANEL = load()
    return _PANEL


def _z(s: pd.Series) -> pd.Series:
    s = s.replace([np.inf, -np.inf], np.nan)
    return (s - s.mean()) / s.std() if s.std() else s * 0.0


_MENU = None


def reference_menu() -> dict:
    """{leg: monthly return Series} for every single-leg spec in the vocabulary.

    Serves two jobs the gate cannot do without:
      · PBO needs a MENU of competing specs — "did picking the backtest winner
        generalise?" is meaningless against a field of one.
      · trial_sr_var needs a real measurement of how much Sharpe disperses across
        the things one might have tried.
    Cached per process; costs one backtest per leg."""
    global _MENU
    if _MENU is None:
        px, f = _panel()
        _MENU = {}
        for leg in LEGS:
            try:
                r = backtest(CandidateSpec(legs={leg: 1.0}), px, f)["returns"]
                if len(r) >= MIN_MONTHS:
                    _MENU[leg] = r
            except Exception as e:                       # a leg may lack data depth
                print(f"[promotion] reference leg {leg} unavailable: {e}", file=sys.stderr)
    return _MENU


def backtest(spec: CandidateSpec, px=None, f=None, n_perm: int = 0) -> dict:
    """Walk-forward monthly returns for a spec. Same construction as the live
    book (PIT fundamentals, mcap floor, top-quantile, equal weight, costed) so a
    promoted spec is directly comparable to the incumbent."""
    from xsection_montecarlo import pit
    from src.costs import round_trip_cost
    COST = round_trip_cost(1_000_000, "equity_delivery").total_bps / 1e4 + 0.0025

    if px is None or f is None:
        px, f = _panel()
    rebal = px.resample("ME").last().index
    pxf = px.ffill()
    pxm = pxf.reindex(pxf.index.union(rebal)).ffill().reindex(rebal)
    bvps, eps, shr = (pit(f, c, rebal) for c in ["bvps", "ttm_eps", "shares_outstanding"])
    dret = px.pct_change()

    rng = np.random.default_rng(42)
    rets, turns, ics, perm_ics, prev = [], [], [], [], set()
    for i in range(len(rebal) - 1):
        t, t1 = rebal[i], rebal[i + 1]
        win = px.loc[t - pd.Timedelta(days=15):t]
        live = win.columns[win.notna().any()]
        price_t = pxm.loc[t].reindex(live)
        if t not in shr.index:
            continue
        mcap = (price_t * shr.loc[t].reindex(live) / 1e7).dropna()
        sel = mcap[(price_t.reindex(mcap.index) >= spec.min_price) & (mcap >= spec.min_mcap_cr)]
        if spec.max_mcap_cr:
            sel = sel[sel <= spec.max_mcap_cr]
        liquid = sel.index
        if len(liquid) < 30:
            continue
        fwd = (pxm.loc[t1].reindex(liquid) / price_t.reindex(liquid) - 1.0).clip(-0.40, 0.80)

        hist = px.loc[:t]
        vals = {}
        if "momentum" in spec.legs:
            seg = hist.iloc[-spec.formation:-spec.skip] if spec.skip else hist.iloc[-spec.formation:]
            vals["momentum"] = seg.apply(lambda c: c.dropna().iloc[-1] / c.dropna().iloc[0] - 1
                                         if c.dropna().shape[0] > spec.formation * 0.8 else np.nan)
        if "reversal" in spec.legs:
            seg = hist.iloc[-spec.skip:] if spec.skip else hist.iloc[-21:]
            vals["reversal"] = -seg.apply(lambda c: c.dropna().iloc[-1] / c.dropna().iloc[0] - 1
                                          if c.dropna().shape[0] > 5 else np.nan)
        if "low_vol" in spec.legs:
            vals["low_vol"] = -dret.loc[:t].iloc[-252:].std()
        if "size" in spec.legs:
            vals["size"] = -np.log(mcap.reindex(liquid))
        if "value_ey" in spec.legs:
            vals["value_ey"] = eps.loc[t] / price_t
        if "value_pb" in spec.legs:
            vals["value_pb"] = -(price_t / bvps.loc[t])

        parts, wts = [], []
        for leg, w in spec.legs.items():
            v = vals.get(leg)
            if v is None:
                continue
            parts.append(_z(v.reindex(liquid)) * float(w)); wts.append(abs(float(w)))
        if not parts:
            continue
        comp = (pd.concat(parts, axis=1).sum(axis=1, min_count=1) / (sum(wts) or 1)).dropna()
        comp = comp[comp.index.isin(fwd.dropna().index)]
        if len(comp) < 30:
            continue

        d = pd.concat([comp, fwd.reindex(comp.index)], axis=1).dropna()
        if len(d) >= 30:
            fr, rr = d.iloc[:, 0].rank(), d.iloc[:, 1].rank()
            ics.append(fr.corr(rr))
            # Null IC for the SAME rebalance: shuffle the forward returns across
            # the cross-section, breaking any factor->return link while keeping
            # both marginal distributions and the panel's shape intact. Building
            # the null inline costs one pass; doing it by re-running the whole
            # backtest (as a naive sign-flip test would) costs a full reload and
            # tests the wrong thing — whether mean return beats zero, which for a
            # long-only book is mostly market beta, not signal.
            rv = rr.values
            perm_ics.append([fr.corr(pd.Series(rng.permutation(rv), index=fr.index))
                             for _ in range(n_perm)])
        k = min(max(len(comp) // spec.top_quantile, 1), spec.max_holdings)
        hold = set(comp.sort_values(ascending=False).head(k).index)
        turn = 1.0 - len(hold & prev) / len(hold) if prev else 1.0
        rets.append({"m": t1, "r": float(fwd.reindex(list(hold)).mean() - turn * COST)})
        turns.append(turn); prev = hold

    if not rets:
        return {"returns": pd.Series(dtype=float), "turnover": np.nan,
                "ic": np.nan, "perm_p": np.nan}
    s = pd.Series([x["r"] for x in rets], index=[x["m"] for x in rets])
    obs_ic = float(np.nanmean(ics)) if ics else np.nan
    perm_p = np.nan
    if n_perm and perm_ics and np.isfinite(obs_ic):
        # null distribution of the MEAN IC across rebalances
        null = np.nanmean(np.array(perm_ics, dtype=float), axis=0)
        perm_p = float((np.sum(np.abs(null) >= abs(obs_ic)) + 1) / (len(null) + 1))
    return {"returns": s, "turnover": float(np.mean(turns)),
            "ic": obs_ic, "perm_p": perm_p}


def evaluate(spec: CandidateSpec, menu: dict | None = None,
             registry: TrialRegistry | None = None, record: bool = True,
             n_perm: int = 200) -> Verdict:
    """Run the full battery and return PROMOTE/REJECT with every number shown.

    `menu` is an optional {name: monthly-return-Series} of competing specs used
    for PBO — without it PBO is undefined (you cannot overfit a selection of one)
    and the gate REJECTS rather than silently passing that check.
    """
    reg = registry or TrialRegistry()
    bt = backtest(spec, n_perm=n_perm)
    r = bt["returns"]
    m = {"months": len(r), "turnover": bt["turnover"], "ic": bt["ic"]}

    if len(r) < MIN_MONTHS:
        m["sharpe"] = None
        v = Verdict(spec.key(), "REJECT",
                    [f"only {len(r)} months of history (need {MIN_MONTHS})"], m, reg.n_trials())
        if record:
            reg.record(v, spec)
        return v

    # Trials counted = distinct specs in the registry + the reference menu we
    # implicitly searched + this candidate. Undercounting trials is the classic
    # way a DSR flatters a strategy, so count generously.
    ref = reference_menu()
    n_trials = reg.n_trials() + len(ref) + 1
    sr_var = reg.sr_variance()
    d = deflated_sharpe(r, n_trials, sr_var)
    cp = cpcv_paths(r)
    menu_df = pd.DataFrame({**ref, **(menu or {}), spec.key(): r}).dropna(how="all")
    pbo = pbo_logit(menu_df)

    m.update({"sharpe": _sharpe(r), "sharpe_monthly": d["sr"], "dsr": d["dsr"],
              "sr0_hurdle": d["sr0"], "trial_sr_var": sr_var, "cpcv_paths": int(len(cp)),
              "cpcv_pos_frac": float((cp > 0).mean()) if len(cp) else None,
              "cpcv_p5": float(np.percentile(cp, 5)) if len(cp) else None,
              "pbo": pbo, "total_pct": float(((1 + r).prod() - 1) * 100),
              "maxdd_pct": float(((1 + r).cumprod() / (1 + r).cumprod().cummax() - 1).min() * 100),
              "perm_p": bt["perm_p"]})

    reasons = []
    if not (m["sharpe"] is not None and m["sharpe"] >= NET_SHARPE_MIN):
        reasons.append(f"net Sharpe {m['sharpe']:.2f} < {NET_SHARPE_MIN}")
    if not (np.isfinite(m["dsr"]) and m["dsr"] >= DSR_MIN):
        reasons.append(f"DSR {m['dsr']:.3f} < {DSR_MIN} at {n_trials} trials "
                       f"(hurdle SR {d['sr0']:.3f}/mo)")
    if m["cpcv_pos_frac"] is None or m["cpcv_pos_frac"] < CPCV_POS_MIN:
        reasons.append(f"CPCV OOS positive {(m['cpcv_pos_frac'] or 0)*100:.0f}% < {CPCV_POS_MIN*100:.0f}%")
    if not np.isfinite(pbo):
        reasons.append("PBO undefined — need a menu of >=2 competing specs")
    elif pbo > PBO_MAX:
        reasons.append(f"PBO {pbo*100:.0f}% > {PBO_MAX*100:.0f}%")
    if np.isfinite(m.get("perm_p", np.nan)) and m["perm_p"] > PERM_P_MAX:
        reasons.append(f"permutation p {m['perm_p']:.3f} > {PERM_P_MAX}")
    if np.isfinite(m["turnover"]) and m["turnover"] > TURNOVER_MAX:
        reasons.append(f"turnover {m['turnover']*100:.0f}%/mo > {TURNOVER_MAX*100:.0f}%")

    v = Verdict(spec.key(), "PROMOTE" if not reasons else "REJECT",
                reasons or ["cleared every gate"], m, n_trials)
    if record:
        reg.record(v, spec)
    return v


INCUMBENT = CandidateSpec(legs={"momentum": 1.0, "value_ey": 1.0, "value_pb": 1.0},
                          note="live Core book (momentum_value_composite_v1)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--incumbent", action="store_true", help="score the live Core spec")
    ap.add_argument("--registry", action="store_true", help="print the trial log")
    ap.add_argument("--no-record", action="store_true", help="do not append to the registry")
    args = ap.parse_args()

    reg = TrialRegistry()
    if args.registry:
        rows = reg.all()
        print(f"{reg.path} — {len(rows)} evaluations, {reg.n_trials()} distinct specs")
        print(f"{'when':<21}{'key':<14}{'verdict':<10}{'sharpe':>8}{'dsr':>8}  legs")
        for r in rows:
            mm = r.get("metrics", {})
            sh = mm.get("sharpe"); ds = mm.get("dsr")
            print(f"{r['ts'][:19]:<21}{r['spec_key']:<14}{r['verdict']:<10}"
                  f"{(f'{sh:.2f}' if sh is not None else 'n/a'):>8}"
                  f"{(f'{ds:.3f}' if ds is not None else 'n/a'):>8}  "
                  f"{','.join(r['spec']['legs'])}")
        return

    spec = INCUMBENT
    print(f"Evaluating {spec.key()} — {spec.note or 'candidate'}")
    print(f"legs={spec.legs} formation={spec.formation} skip={spec.skip} "
          f"mcap>={spec.min_mcap_cr:.0f}cr top1/{spec.top_quantile} max={spec.max_holdings}")
    print("loading panel (slow) ...", file=sys.stderr)
    v = evaluate(spec, record=not args.no_record)
    m = v.metrics
    print("=" * 72)
    print(f"VERDICT: {v.verdict}   (trial #{v.n_trials} in the registry)")
    print("=" * 72)
    for k, lab in [("months", "months"), ("sharpe", "net Sharpe (ann)"),
                   ("total_pct", "total return %"), ("maxdd_pct", "max drawdown %"),
                   ("dsr", "deflated Sharpe"), ("sr0_hurdle", "DSR hurdle (SR/mo)"),
                   ("cpcv_pos_frac", "CPCV OOS positive"), ("cpcv_p5", "CPCV 5th pct Sharpe"),
                   ("pbo", "PBO"), ("perm_p", "permutation p"), ("ic", "mean IC"),
                   ("turnover", "turnover /mo")]:
        val = m.get(k)
        print(f"  {lab:<22}{'n/a' if val is None or (isinstance(val,float) and not np.isfinite(val)) else (f'{val:.3f}' if isinstance(val,float) else val)}")
    print("-" * 72)
    for why in v.reasons:
        print(f"  · {why}")


if __name__ == "__main__":
    main()
