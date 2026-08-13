"""
Research agent — proposes strategy HYPOTHESES. It never trades.

This is the "R" in R -> B -> I. The agent's entire output is a `CandidateSpec`:
declarative JSON drawn from a fixed factor vocabulary. It cannot emit code, size a
position, pick a stock, or touch the paper book. Everything it proposes goes to
`src.promotion.evaluate`, and the statistics decide.

Why it is shaped this way
-------------------------
The obvious version of "AI for trading" — an LLM that looks at a chart and says
buy — is unfalsifiable and sits outside every control this project has. So the
agent is confined to the one job an LLM is actually good at here: generating
*diverse hypotheses* over a search space a human would explore too slowly.

Two properties keep that safe:

  1. DECLARATIVE OUTPUT. A proposal is data, not executable text. It is validated
     against the `LEGS` vocabulary before anything runs, so a malformed or
     adversarial response fails closed at parse time rather than executing.

  2. THE TRIAL COUNTER IS THE PRICE. Every proposal is logged to the registry, and
     the DSR hurdle rises with the trial count. An agent that sprays 200 ideas
     makes the bar for all of them harder — searching more costs more. That is
     the correct incentive and it is enforced by arithmetic, not by good manners.

So the agent cannot p-hack by volume. If it proposes junk, the junk raises the
hurdle and then fails it.

Proposers
---------
  llm     — Claude via src.agents.llm_providers (needs an API key)
  grid    — deterministic enumeration of the vocabulary; no key required, and the
            default, so the pipeline is runnable and testable out of the box

Usage:
  DAWN_URL=... python3 -m src.research_agent --n 5                 # grid proposer
  DAWN_URL=... python3 -m src.research_agent --n 5 --proposer llm  # needs API key
  DAWN_URL=... python3 -m src.research_agent --dry-run             # propose only
"""
from __future__ import annotations

import argparse
import itertools
import json
import re
import sys
from dataclasses import asdict

from src.promotion import (LEGS, CandidateSpec, TrialRegistry, evaluate,
                           reference_menu)

MODEL = "claude-sonnet-5"          # structured proposal generation; cheap enough to fan out

SYSTEM = """You are a quantitative research assistant for an Indian equity (NSE) \
cross-sectional factor book. You propose STRATEGY HYPOTHESES ONLY.

You never make trade decisions, never name individual stocks, and never write code.
Your entire output is JSON.

Available factor legs (use ONLY these):
  momentum   price momentum over the formation window, skipping the most recent `skip` days
  reversal   negated short-horizon return (mean reversion)
  low_vol    negated trailing volatility (low-volatility anomaly)
  size       negated log market cap (small-cap tilt)
  value_ey   earnings yield, TTM EPS / price
  value_pb   negated price-to-book

Spec fields:
  legs          object mapping leg name -> weight (float, may be negative)
  formation     int trading days for the momentum formation window (60-504)
  skip          int trading days skipped at the near end (0-63, must be < formation)
  min_mcap_cr   float minimum market cap in rupee crore (>=500)
  max_mcap_cr   float or null maximum market cap in crore
  min_price     float minimum share price in rupees
  top_quantile  int; the book takes the top 1/N of the scored universe (3-10)
  max_holdings  int number of names held (10-50)
  note          one sentence stating the ECONOMIC RATIONALE for this combination

Rules:
- Propose hypotheses with a real economic rationale, not random parameter jitter.
- Vary the STRUCTURE (which legs, what weights, what universe), not just numbers.
- Each proposal must be meaningfully different from the others.
- The incumbent is {momentum:1, value_ey:1, value_pb:1}, formation 252, skip 21,
  min_mcap_cr 2000, top_quantile 5, max_holdings 25. Do not re-propose it.

Return ONLY a JSON array of spec objects. No prose, no markdown fence."""


# ── proposers ────────────────────────────────────────────────────────────────

def propose_grid(n: int, seen: set) -> list:
    """Deterministic enumeration — no API key needed. Walks structurally distinct
    leg combinations in a fixed order so runs are reproducible and the registry
    stays meaningful across sessions."""
    rationale = {
        ("momentum",): "momentum alone — the leg with the strongest standalone IC",
        ("momentum", "low_vol"): "momentum with a volatility brake; low-vol is the classic momentum crash hedge",
        ("momentum", "value_ey"): "momentum plus earnings yield — trend confirmed by cheapness",
        ("momentum", "size"): "momentum concentrated in smaller caps where factor efficacy is highest",
        ("momentum", "value_ey", "low_vol"): "momentum+value with a volatility brake on the drawdown",
        ("momentum", "value_ey", "value_pb", "low_vol"): "the incumbent plus a low-vol leg",
        ("value_ey", "value_pb"): "pure value — tests whether the value legs carry any weight alone",
        ("momentum", "reversal"): "momentum with a short-horizon reversal overlay",
        ("low_vol", "value_ey"): "defensive value — low-vol and cheapness without trend",
        ("momentum", "value_pb"): "momentum plus book value only",
    }
    out = []
    combos = sorted(rationale, key=lambda c: (len(c), c))
    variants = [(252, 21, 2000.0, None), (252, 21, 2000.0, 8000.0), (126, 21, 2000.0, None)]
    for combo, (form, skip, floor, cap) in itertools.product(combos, variants):
        if len(out) >= n:
            break
        spec = CandidateSpec(legs={k: 1.0 for k in combo}, formation=form, skip=skip,
                             min_mcap_cr=floor, max_mcap_cr=cap,
                             note=rationale[combo] + (f"; capped at ₹{cap:.0f}cr" if cap else "")
                                  + (f"; {form}d formation" if form != 252 else ""))
        if spec.key() in seen:
            continue
        seen.add(spec.key()); out.append(spec)
    return out


def propose_llm(n: int, seen: set) -> list:
    """Ask Claude for n structurally distinct hypotheses. Fails closed: anything
    that does not parse into a valid CandidateSpec is dropped with a warning
    rather than coerced, so a bad response can never reach the backtest."""
    from src.agents.llm_providers import call_llm

    raw = call_llm(system=SYSTEM,
                   user=f"Propose {n} distinct strategy hypotheses as a JSON array.",
                   model=MODEL, max_tokens=2048)
    txt = raw.strip()
    m = re.search(r"\[.*\]", txt, re.S)          # tolerate a stray fence or preamble
    if not m:
        print(f"[agent] no JSON array in response: {txt[:200]}", file=sys.stderr)
        return []
    try:
        items = json.loads(m.group(0))
    except json.JSONDecodeError as e:
        print(f"[agent] unparseable JSON ({e})", file=sys.stderr)
        return []

    out = []
    for it in items:
        if not isinstance(it, dict):
            continue
        try:
            legs = {k: float(v) for k, v in (it.get("legs") or {}).items() if k in LEGS}
            if not legs:
                raise ValueError(f"no valid legs in {it.get('legs')!r}")
            spec = CandidateSpec(
                legs=legs,
                formation=int(it.get("formation", 252)),
                skip=int(it.get("skip", 21)),
                min_mcap_cr=float(it.get("min_mcap_cr", 2000.0)),
                max_mcap_cr=(float(it["max_mcap_cr"]) if it.get("max_mcap_cr") else None),
                min_price=float(it.get("min_price", 10.0)),
                top_quantile=int(it.get("top_quantile", 5)),
                max_holdings=int(it.get("max_holdings", 25)),
                note=str(it.get("note", ""))[:300])
        except (ValueError, TypeError, KeyError) as e:
            print(f"[agent] rejected malformed proposal: {e}", file=sys.stderr)
            continue
        if spec.key() in seen:
            continue
        seen.add(spec.key()); out.append(spec)
    return out


PROPOSERS = {"grid": propose_grid, "llm": propose_llm}


def run(n: int, proposer: str = "grid", dry_run: bool = False) -> list:
    reg = TrialRegistry()
    seen = {r["spec_key"] for r in reg.all()}
    specs = PROPOSERS[proposer](n, seen)
    if not specs:
        print("no new proposals (all already in the registry?)")
        return []

    print(f"{len(specs)} proposal(s) from the '{proposer}' proposer "
          f"| registry currently holds {reg.n_trials()} distinct specs")
    for s in specs:
        print(f"  · {s.key()}  {'+'.join(s.legs)}  — {s.note}")
    if dry_run:
        print("\n[dry run] nothing evaluated, nothing recorded.")
        return specs

    print("\nloading panel + reference menu (slow, once) ...", file=sys.stderr)
    menu = reference_menu()
    results = []
    for i, s in enumerate(specs, 1):
        print(f"\n[{i}/{len(specs)}] evaluating {s.key()} ({'+'.join(s.legs)}) ...", file=sys.stderr)
        v = evaluate(s, menu=menu)
        results.append((s, v))
        mm = v.metrics
        sh = mm.get("sharpe"); ds = mm.get("dsr")
        print(f"  {v.verdict:<8} sharpe={sh if sh is None else round(sh,2)} "
              f"dsr={ds if ds is None else round(ds,3)} "
              f"pbo={mm.get('pbo') if mm.get('pbo') is None else round(mm['pbo'],2)} "
              f"turnover={mm.get('turnover') if mm.get('turnover') is None else round(mm['turnover'],2)}")
        for why in v.reasons:
            print(f"      · {why}")

    promoted = [(s, v) for s, v in results if v.verdict == "PROMOTE"]
    print("\n" + "=" * 74)
    print(f"{len(promoted)}/{len(results)} promoted | registry now {TrialRegistry().n_trials()} "
          f"distinct specs (every trial raises the DSR bar for the next one)")
    for s, v in promoted:
        print(f"  PROMOTE {s.key()} {'+'.join(s.legs)} — sharpe {v.metrics['sharpe']:.2f}, "
              f"dsr {v.metrics['dsr']:.3f}")
    if not promoted:
        print("  nothing cleared the gate. That is the expected outcome most of the time —")
        print("  a gate that promotes often is not a gate.")
    print("=" * 74)
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=5, help="how many hypotheses to propose")
    ap.add_argument("--proposer", choices=list(PROPOSERS), default="grid")
    ap.add_argument("--dry-run", action="store_true", help="propose only; no backtest, no registry write")
    args = ap.parse_args()
    run(args.n, args.proposer, args.dry_run)


if __name__ == "__main__":
    main()
