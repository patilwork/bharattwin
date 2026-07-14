# Second-Signal Hunt (long-short / beta-stripped) + Survivorship Fix

## Correcting the earlier error
The earlier "reversal failed → no 2nd signal" conclusion was WRONG: it compared
LONG-ONLY returns, which all correlate ~0.9 via market beta. The correct test is
on LONG-SHORT (top-minus-bottom) returns, which strip beta and isolate the signal.

## Long-short signal quality + correlation to momentum (2017-2026, survivorship-aware)
| signal | LS Sharpe | t | corr w/ momentum | usable diversifier? |
|---|---|---|---|---|
| momentum | 0.60 | 1.84 | — | (base) |
| value_pb | 0.95 | 2.01 | **+0.14** | **YES** — real + uncorrelated |
| value_ey | 0.84 | 1.68 | **+0.25** | **YES** — uncorrelated |
| low_beta | −1.11 | −3.39 | +0.07 | no — uncorrelated but loses this regime |
| low_idvol | −1.41 | −4.34 | +0.05 | no — uncorrelated but loses this regime |
| reversal | −0.68 | −2.10 | −0.56 | no — just anti-momentum, loses |

## The second uncorrelated signal is VALUE (and we already have it)
- momentum & value are genuinely uncorrelated in signal space (+0.14 / +0.25).
- Blending improves risk-adjusted return:
  - momentum + value_pb: Sharpe **0.81 → 1.16** (same window)
  - momentum + value_ey: Sharpe **0.90 → 1.08**
- The momentum+value composite the paper book already runs IS the diversified
  two-signal strategy. Confirmed, not hypothetical.
- low-beta / low-vol are uncorrelated but unprofitable in the 2021-26 bull
  (high-beta/high-vol won) — could matter in another regime, not reliable now.

## Survivorship fix — confirmed and locked into the core
The liveness filter (only names that traded within 15d of t; delisted-mid-hold
realises loss to last price) is now in scripts/xsection_backtest.py, not just the
extended script. Impact on long-only momentum/composite: ~0 (momentum holds
winners; delisted names are losers). Composite DSR steady at ~0.82.
