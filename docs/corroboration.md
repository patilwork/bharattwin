# Do our empirical findings corroborate the quant knowledge base?

Cross-check of this session's hands-on results against the 3-layer crawl catalog
(docs/quant_resources_catalog.md) + operating guide (docs/superquant_operating_guide.md).

## ✅ Strong corroboration (independent re-derivation of the canon)
| Our empirical finding | Canonical source (our KB) | Match |
|---|---|---|
| momentum + value uncorrelated (+0.14), blend raises Sharpe | **Asness-Moskowitz-Pedersen, "Value and Momentum Everywhere" (2013)**: the two most robust anomalies, negatively correlated, combine to beat either alone | exact |
| concentrated ~25-name long-only book | Gray & Carlisle/Vogel (Quantitative Value/Momentum, Alpha Architect): ~40-50 high-conviction names, diluted funds capture little | yes |
| equal-weight beats inv-vol/signal-tilt | DeMiguel-Garlappi-Uppal, "Optimal vs Naive Diversification" (1/N) | exact |
| used Deflated Sharpe + permutation to fight overfitting | López de Prado / mlfinlab: DSR, PSR, CPCV, min track-record length for false-discovery control | exact (his tools) |
| cost engine first; weekly's cost drag kills it | Narang, "Inside the Black Box": txn costs eat 20-50% of gross; cost model is first-class | yes |
| leverage/concentration lower median + raise ruin (Kelly/vol drag) | Chan (Kelly, drawdown discipline); Thorp | yes |
| size factor not significant (t 0.42) | Guide §2.2: **SMB ≈ 0 in India (Agarwalla-Jacob-Varma)** | exact |
| stayed long-only (no single-stock short) | Guide §2.2: SLB thin, borrow scarce, shorting Indian quality is a widow-maker | exact |
| next-day INDEX momentum = no edge, but CROSS-SECTIONAL momentum works | Guide: "edge does NOT live in next-day OHLCV/momentum" (index level) | consistent |

## ⚠️ Divergences (where we differ — and why)
- **Low-vol / low-beta LOST in our window** (t −3.39 / −4.34), but the canon
  (Frazzini-Pedersen "Betting Against Beta"; the low-vol anomaly; NSE's Low-Vol 30
  ETF) says they WIN long-term. Explanation: 2021-26 was a speculative bull where
  high-beta/high-vol won; low-vol underperforms in such regimes + our window is
  short. The guide half-agrees ("Low-Vol 30 crowded, thin alpha"). Honest tension —
  revisit in a bear/normal regime.
- **We used P/B; the canon prefers EBIT/EV.** Our value_pb worked (t 2.01) but
  Gray/Carlisle + the guide both say EV/EBITDA & FCF-yield are more robust than
  P/B ("weak in asset-light India"). Our value is real but sub-optimally specified.
- **Weighting:** we tested equal vs inv-vol vs signal; the canon (López de Prado)
  offers HRP/HERC/covariance-denoising as risk-based schemes we did NOT test. 1/N
  (DeMiguel) says they're hard to beat, but HRP is a fairer challenger than inv-vol.

## 🕳️ Gaps (canon says do it; we haven't — mostly data-blocked)
- **Quality factor** (ROIC, gross-profitability à la Novy-Marx, Piotroski F, Sloan
  accruals) — the guide's "cheap-AND-good"; blocked by sparse Dawn fundamentals (~3.7%).
- **Forensic/governance exclusion overlay** (promoter pledge, auditor/CFO exits,
  CFO/PAT divergence, Beneish) — the guide's *strongest surviving edge*; data-blocked.
- **12-2 momentum + Frog-in-the-Pan path-quality** (Gray/Vogel) vs our plain 12-1.
- **Combinatorial Purged CV** (López de Prado) — we did permutation + DSR, not CPCV.
- **Trend-following / GVMT overlay** for tail-risk (Alpha Architect) — not built.

## Verdict
Our independent empirical work RE-DERIVED the core canon (momentum+value, 1/N,
DSR discipline, cost-first, long-only India, SMB≈0, index-momentum dead). That
convergence is a strong validity check — we didn't overfit our way to a private
conclusion; we landed where the literature is. Divergences are regime/spec
explained; gaps are a clear, mostly data-gated roadmap.
