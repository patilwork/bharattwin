# BharatTwin Super-Quant Operating Guide

*Lead quant synthesis of 14 verified domain packets. Written to be run, not admired. Where the evidence and the adversarial verifiers disagreed with the original findings, I have sided with the verifiers — 10 of the 14 packets had their headline "edge" downgraded to hygiene, risk-control, or infrastructure on adversarial review, and that pattern is the single most important input to this document.*

---

## 1. Executive Thesis — Where Post-Cost Edge Actually Lives

**The blunt version: this system has no demonstrated next-day directional alpha, and 46% is not a rounding error away from one — it is below both the 50% coin-flip and the ~53–54% Nifty up-day base rate.** A model that is *below the naive always-long hit rate* on direction, and that the team already concluded "is not edge after costs," should stop being marketed, internally or externally, as a next-day Nifty predictor. Every packet that touched the next-day-index question independently reached this conclusion. The +226pp cumulative PnL proxy is the most dangerous artifact in the repo: in a market that closes up ~53–54% of days, any long-biased or magnitude-weighted rule *mechanically* prints positive cumulative PnL. That is beta and drift wearing a skill costume.

So where does durable, post-cost value realistically live? In four places, none of which is "beat 46%":

1. **Data & PIT hygiene (real, durable, defensive).** A bitemporal, filing-date-keyed, revision-aware fundamentals store + exchange-dissemination-timestamped news/filings ingestion. This *prevents fake alpha*; it produces none on its own. Against the retail tier (Screener/Trendlyne/Tijori latest-restated data) it is a genuine moat. Against real institutional competitors (Prowess vintage, Worldscope PIT, in-house filing-date stores) it is table stakes. Bank it as *reliability*, not *alpha*.

2. **Risk, sizing & survival (real, durable, but a multiplier not a source).** Filtered Historical Simulation for regime-conditional CVaR, block-bootstrap of full equity curves for drawdown/ruin, fractional-Kelly/vol-targeting sizing off the MC distribution with costs *inside* expectancy. This keeps a book alive through India's violent regime breaks. Critical honesty check: **on a negative-post-cost signal, correct sizing outputs ~zero. That is the right answer, not a bug — and it means this layer generates zero PnL until a positive-expectancy signal is found elsewhere.**

3. **A genuinely different product: cross-sectional, long-horizon stock selection (thin, capacity-limited, real).** This is where the *only* candidate positive edges live — and they are NOT the next-day index model. Specifically: a **forensic/governance exclusion overlay** (promoter pledge, auditor/CFO resignations, CFO/PAT divergence, accruals) as a left-tail filter; a **quality-conditioned value tilt** (cheap-AND-good, long-only, 12–36 month hold); and a **strictly ADV-sized PEAD earnings-surprise overlay** in liquid mid-caps. All are cross-sectional, multi-week-to-multi-year, and measured as drawdown/blow-up avoidance and rank-IC, not next-day hit rate. All are decaying and cost-eroded. None is scalable.

4. **The operational/epistemic moat: a public, timestamped, calibrated track record.** Non-gameable (if externally hash-committed), expensive to replicate (6–12 months of discipline), and aligned with the stated "transparent scoring" moat. It is credibility, not PnL.

**Where edge does NOT live, stated without hedging:** next-day Nifty direction from OHLCV/momentum/VIX-level; news-sentiment→direction; FII/DII cash flow as a next-day predictor (the causal arrow runs returns→flows); the LLM swarm's directional votes; PCR/max-pain; the raw GIFT-Nifty gap; factor-*timing* rotation; and anything validated on smoothed regime states or in-sample-selected parameters. The autoresearch findings (momentum weight ~0.44, VIX crisis ~22, mean-reversion 2.5–3σ) are **unproven hypotheses from a ~10,000-trial search**, not established facts, until they survive strictly out-of-sample CPCV.

The honest strategic pivot: **BharatTwin is not a next-day index-timing engine with an accuracy problem. It is (a) a PIT-clean India data platform, (b) a survival-grade risk engine, and (c) a candidate cross-sectional stock-selection book — wrapped in a transparent, auditable track record.** Run it that way.

---

## 2. The Signal Stack

For each sleeve: the durable edge, what to ignore, the concrete technique, the India data source, and how it wires into the existing factor engine / swarm / bootstrap evaluator.

### 2.1 XBRL Fundamentals

- **Durable edge:** PIT *hygiene*, not alpha. A bitemporal fact store keyed by `(isin, canonical_concept, period_start, period_end, consolidation_basis, filing_date, taxonomy_version, revision_seq)` with a `fact_asof(entity, concept, period, asof_date)` query. `filing_date` = exchange submission timestamp, never `period_end`.
- **Ignore:** MCA21 AOC-4 annual Ind-AS XBRL (captcha-gated, pay-per-doc, ~9–10mo late — unusable systematically). Naive P/B value. Any factor computed on Screener/Trendlyne/Tijori (all latest-restated, all inject look-ahead). The belief that quarterly fundamentals inform next-day index direction — wrong horizon entirely.
- **Technique:** Scrape NSE/BSE Reg-33 quarterly results XBRL → parse with **Arelle** (offline, local taxonomy cache) or pragmatic lxml + hand-built qname map. **Ingest ONLY the current-period fact from each filing** — discard the embedded prior-period comparatives (they are restated; harvesting them is the #1 look-ahead contamination in Indian fundamentals). Add a unit test that fails if a comparative is ingested as a period's PIT value. Sector-aware tag map from day one (non-financial / bank / NBFC / insurance — ~15–20% of the universe is financials and uses interest-income schedules, not RevenueFromOperations). Flag the FY2016-17 Ind-AS break as a hard structural discontinuity. **Verifier correction to bank: BS *and* cash flow are half-yearly under Reg 33(3)(f), not "cash flow annual only" — coverage is slightly better than the original finding claimed.**
- **India source:** NSE/BSE corporate-announcements results XBRL (free, scrape at ~3 req/s, cookie/session handling; `BennyThadikaran/NseIndiaApi`). Arelle (free). Benchmark against the **IIMA Fama-French-Momentum library** (free, survivorship-corrected).
- **Wiring:** Do **not** touch the next-day index model. Stand up a *separate* cross-sectional/monthly stock-selection sleeve + a PEAD event overlay. The single highest-value deliverable here is the **look-ahead-quantification study**: build one value/quality factor on Screener-style restated data vs your filing-date store and report the backtest-edge gap. That study *monetizes the hygiene moat* by proving how much reported "alpha" was restatement look-ahead — and it is the most defensible narrative for the stated moat.

### 2.2 Fundamental Factors (Value / Quality / Momentum / Low-Vol / Size)

- **Durable edge:** A **forensic/governance EXCLUSION overlay** as a left-tail filter (the strongest surviving item on adversarial review), and a **quality-conditioned value tilt** (cheap-AND-good beats cheap alone; QMJ has documented India alpha). Measured as blow-up/drawdown avoidance and rank-IC, not excess return.
- **Ignore:** The size premium as a *harvestable* factor — SMB is ~0 in India (the canonical Agarwalla-Jacob-Varma figure is ~zero, NOT the −3.71% the original finding overstated); do not bet on small-cap-as-risk-premium. Vanilla single-factor large-cap tilts (NSE has productized Momentum 30 / Quality 30 / Low-Vol 30 / Value 20 / Alpha 50 into ETFs — crowded, thin net alpha). Raw P/B (weak in asset-light India — but note the internal tension: HML *is* a P/B-family premium, so use cash-flow/EV metrics as *more robust*, don't claim P/B is "dead"). The mid/small-cap quality+momentum *return-harvesting* leg — that is an illiquidity-capacity trap that dies to impact cost at scale.
- **Technique:** Cross-sectional z-score composite: winsorize ±3σ, sector- and cap-neutralize, blend quality (ROIC, gross profitability GP/assets à la Novy-Marx, Sloan accruals, Piotroski F) + value (EV/EBITDA, FCF yield, earnings yield) + 12-1 momentum. Forensic exclusion: Beneish M-score **EM-recalibrated** (~−1.78, treat as one weak input with a ~17.5%+ false-positive tax, never standalone), CFO/PAT<1 sustained, receivable/inventory-day spikes, promoter pledged-share %, auditor/CFO resignations, qualified opinions. **Long-only or long-tilt; no active single-name short leg** (SLB is thin, borrow scarce/expensive, squeeze risk — shorting expensive Indian quality is a documented widow-maker).
- **India source:** IIMA/Prowess factor library (free benchmark, the honesty gate); NSE/BSE XBRL for line items; Prowess/Capitaline (paid) for depth; SEBI forensic disclosures (separate from results XBRL).
- **Wiring:** Separate long-horizon sleeve with its own scorecard (rolling 1/3/5yr decile spreads, rank-IC). Gate the value leg's weight on *measured incremental IC after RMW/CMA*, autoresearch-style. Also cap the price paid for quality (quality gets crowded-expensive — the 2015-20 "quality bubble" then de-rated hard). Feed the forensic exclusion as a hard filter on any long book and as a fragility indicator into the index-product regime overlay. **Honesty gate: if the composite doesn't beat plain IIMA HML+WML net of realistic Indian costs (STT, impact, post-Budget-2024 STCG 20%/LTCG 12.5%), you have no edge.**

### 2.3 News & Events (PEAD, event studies, LLM extraction)

- **Durable edge:** **Point-in-time ingestion with exchange dissemination-timestamp discipline** (`dissemination_ts <= decision_ts`) on BSE/NSE LODR Reg-30 filings — backtest integrity, not a signal. Secondary: **event-window regime gating** (calendar-known catalysts widen the MC return range and cut confidence). Speculative: **second-order LLM entity linking** (event → supplier/customer/competitor/input-cost tickers) — an *unproven* hypothesis, not an edge.
- **Ignore:** Headline polarity / FinBERT sentiment → next-day index direction (news-sentiment IR collapsed ~66% 2003-07 vs 2008-17; fully priced by day 3-4). Trading news already on Moneycontrol/ET (stale-news penalty). Naive PEAD on Nifty-50 large caps (drift concentrates in illiquid small caps where costs eat 70–100% of it). FinTwit/Telegram tips (sub-hour half-life, operator/dabba pump-and-dump).
- **Technique:** Store the exchange broadcast timestamp (not scrape time, not filing "date" — Indian results file after hours; the tradable reaction is the next open). Separate SURPRISE (drifts) from SENTIMENT (fast-priced) from NOVELTY (gates any reaction). Exponential decay kernel (half-life ~1 trading day for headline sentiment). Deduplicate/cluster wire reprints (keep earliest timestamp). SUE construction: seasonal-random-walk baseline where consensus is absent (consensus covers only top ~100-150 names — exactly where PEAD is weakest). Strict-JSON LLM extraction (`event_type, entities, direction, magnitude_bucket, horizon, novelty`), validated against a benchmark you must build (the FinBERT 71% vs GPT 63% number is on generic English PhraseBank — irrelevant to Indian filings).
- **India source:** NSE/BSE corporate announcements (free, timestamped); SEBI curation page; Sarvam/Hindi-FinBERT for code-mixed retail chatter (route to *manipulation-risk flagging*, never direction).
- **Wiring:** Build a PIT announcement ingester under `src/ingestion` parallel to `nse/fii_dii.py` — this closes the "news auto-ingestion not built" gap and is the load-bearing moat. Reframe the `event_news`/`corp_earnings` agents from "predict direction" to "characterize event → gate/calibrate the MC p10." Add a scheduled-event calendar (results, RBI MPC, Budget Feb 1, F&O expiry, index rebalance, elections) as a first-class regime flag. Concentrate index-relevant extraction on the top-5 names (~40% of Nifty weight) — but treat their earnings as the *least* likely place for residual next-day edge (most-watched, fastest-priced) and the *best* place for gating effort.

### 2.4 Valuation & Reverse-DCF

- **Durable edge:** A slow, **long-only, quality-conditioned value tilt** on a 6–36 month horizon, with reverse-DCF used only to flag implied-growth *extremity*, not to produce tradable fair values. QMJ's downturn behavior gives *conditional* (not reliable) downside diversification.
- **Ignore:** Reverse-DCF point estimates (output is dominated by WACC/terminal-growth/starting-FCF — illusory precision). Any use of valuation in the next-day accuracy loop (~zero IC at 1-day horizon). Shorting "priced-for-perfection" names (Nestle/Titan/Pidilite/Divi's compounded for a decade screening as "unachievable growth"). Morningstar star rating as a short-term predictor.
- **Technique:** Reverse-DCF to extract the market's implied growth/margin, signal = gap vs an achievable band. **Probabilistic (Monte Carlo) DCF with correlated input sampling** (Cholesky/copula — never independent marginals) → output P(undervalued), not a number. Morningstar-style uncertainty-scaled margin-of-safety bands (20/30/40/50/75%) for *position sizing*, not just gating. Sector-segment: residual-income/P-B-ROE for financials (~35% of index), not FCF-DCF.
- **India source:** The **76-stock Morningstar fundamentals already in the DB but not wired** — highest-leverage idle asset. Damodaran India ERP/cost-of-capital pages (free) for defensible discount-rate inputs. RBI repo + USDINR (already in DB).
- **Wiring:** Feed the fair-value *distribution* into the existing regime-conditional block bootstrap so valuation informs the p10 objective, not the mean. New long-horizon scorecard module. Note the accessible-in-universe edge is *small*: the real premium lives in mid/small-caps outside both BharatTwin's 96-stock panel and Morningstar's large-cap-biased 76-name coverage.

### 2.5 Sentiment & Flows (FII/DII, F&O positioning, GIFT-Nifty, global cues)

- **Durable edge:** Honestly, a **negative result**: in India returns Granger-cause flows (FIIs are positive-feedback/trend-chasing traders), so daily FII cash net-buy is a *lagged echo of price* with weak, endogenous next-day content. The only residual worth keeping is **regime-conditional, tail-only positioning context** (extreme FII index-futures long/short ratio or FII-vs-Client OI divergence) as a *weak, low-weight contrarian risk flag*.
- **Ignore:** "FII bought X cr → market up tomorrow" (causal arrow backwards, endogenous, most-quoted-least-useful number in Indian media). PCR/max-pain as level predictors (tiny/wrong-signed; max-pain near-tautological). Raw GIFT-Nifty gap as capturable alpha (already-priced; you'd trade the gapped open). DII buying as bullish (DIIs are contrarian absorbers). Stacking S&P/DXY/crude/A50 as separate features once GIFT is in (collinear — collapse to one global-risk factor via PCA; keep crude separate since India imports it).
- **Technique:** Never regress next-day return on same-day flow — use strictly prior-session, lagged flow only. Positioning-percentile z-scores over rolling 1–3yr; act only at |z|>~1.5–2 tails. GIFT-gap residualization (regress implied gap on overnight cues, trade the residual + systematic gap-fade). Timestamp discipline: participant OI publishes ~18:00-19:00 IST post-close (usable next day), GIFT gap knowable ~09:00 IST.
- **India source:** NSE participant-wise OI (`fao_participant_oi`, free, EOD) — the flagged "highest-impact missing signal." **But verifiers flatly contradict the "under-arbitraged" framing:** the FII F&O long/short ratio is one of the most-watched, freely-published, retail-narrated numbers in India (Sensibull, Opstra, StockEdge, dozens of dashboards). It is crowded, EOD-latent, contemporaneous, and multi-week in useful horizon — not a clean next-day edge.
- **Wiring:** Demote daily FII/DII cash from predictor to context; add a hard rule that no model uses contemporaneous flow. Wire participant-wise OI as *positioning* percentile z-scores feeding only the tails to the swarm's FII-quant/DII archetypes. Every flow feature must clear the full gauntlet (strictly-lagged, purged CV, deflated Sharpe, net-of-cost, p10 block bootstrap) before promotion. Expected honest outcome: most clear as marginal-to-null.

### 2.6 LLM Agent Swarm

- **Durable edge:** NOT directional. Three narrow, legitimate roles: **(1) Stage-1 structured extraction** from unstructured Indian text (concalls, vernacular news, filings) into clean features for the numeric engine; **(2) scenario/narrative generation** for the MC evaluator (brainstorming, not calibrated prediction); **(3) auditable explanation** as a product surface.
- **Ignore:** Any claim the swarm adds directional accuracy over the numeric model (unproven, and the 2024-26 literature — "Alpha Illusion," "Profit Mirage" — shows 50–72% return/Sharpe collapse once the backtest crosses the model's knowledge cutoff). Multi-agent "debate" (wins <20% of configs; same-base-model agents share priors, so 10 archetypes are NOT 10 independent votes). Raw verbalized LLM confidence for sizing (systematically overconfident, large ECE). MiroFish's financial module (does not exist).
- **Technique:** **Confine LLMs to Stage 1**; the deterministic numeric engine owns prediction and sizing. Contamination guard: reserve a strict *post-training-cutoff* walk-forward window as the ONLY headline evaluation for any LLM-involving component; label all 20yr replays contaminated/debug-only. Run a memorization probe (date-stamped Nifty/Adani/COVID Q&A). Post-hoc calibration (isotonic/temperature) fit OOS; report ECE/Brier; feed only the calibrated probability downstream. Measure cross-agent correlation; if high, assign different base models per archetype or hard-wire contrarian evidence sets. RAG-ground every persona on your own India DB (US-centric priors otherwise dominate and hallucinate operator/dabba dynamics). **First-order constraint: every base-model upgrade re-contaminates history and resets your clean OOS window — you can never compound a long clean LLM track record.**
- **Wiring:** Rewrite the swarm's charter in repo docs to extraction + scenario + explanation. Wire XBRL/Morningstar/FII-DII into the *numeric* engine first, using the LLM only as the extraction/normalization layer.

### 2.7 Monte Carlo & the Bootstrap Evaluator

- **Durable edge:** Honest, fat-tail-conditional, net-of-state-dependent-cost **risk numbers** — the arbiter that confirms 46% is untradable and gates any future candidate. Produces zero PnL on its own; it is correctness/survival infrastructure.
- **Ignore:** Treating a nicer p10 as "alpha" (category error). Gaussian/GBM VaR (understates Indian fat tails badly). IID bootstrap for any path/drawdown metric (deletes the serial dependence that *creates* drawdowns). Optimizing the **p10 point quantile** (non-convex, dominated by 1-2 order statistics, chases noise). Independent-input probabilistic DCF. In-sample worst-case tuning on the full 20yr sample (overfits fastest exactly in the tail).
- **Technique:** **Replace the p10-point objective with CVaR** (Rockafellar-Uryasev — coherent, convex, LP-solvable via cvxpy/Riskfolio-Lib; a small code change that materially cuts overfitting). Add **Filtered Historical Simulation** alongside the block bootstrap: GJR-GARCH(1,1)-t filter → IID-bootstrap standardized residuals → re-inflate by the forecast vol path (conditions the tail on *today's* regime without fragmenting history into explicit regimes). Use `arch` (StationaryBootstrap, `optimal_block_length` on **squared** returns — autocorrelation lives in the squares; expect ~10-20 day mean block). **Vectorize to 5,000-10,000 scenarios** for tail objectives (the 10% tail of 200 draws is ~20 points; the p10 is ~1-2). Attach a nested-bootstrap confidence band to the objective itself. EVT/Generalized-Pareto (POT, `pyextremes`) for p1/p0.5. Split overnight-gap vs intraday (circuit breakers truncate sessions; GIFT/US-close drives gaps). **Fold a state-dependent cost distribution inside the PnL simulation** — this is what operationalizes "46% is not edge after costs."
- **India source:** 20yr Nifty/BankNifty + India VIX (regime conditioner, crisis ~22 not 30, per the team's own autoresearch); local free data.
- **Wiring / urgent bug:** **Investigate the suspiciously narrow composite** (`observations_and_roadmap.md` p10=30.76 / median=31.68 / p90=32.48, spread 1.72). A forward Nifty return/PnL distribution *cannot* be that tight — this is almost certainly the sampling distribution of an aggregate score/accuracy statistic (narrows by CLT), not a return distribution. **Confirm exactly what quantity is being resampled and re-express it in return/PnL units before any p10/CVaR objective is trusted.** This is a real bug that invalidates any sizing built on it.

---

## 3. Combination & Sizing

### 3.1 Fusing signals — low-DOF, shrinkage-heavy, meta-labeled

The combiner is where multiple-testing bias concentrates and where the small-sample problem is most acute: 20yr daily = ~5,000 rows but only *dozens of independent regime-episodes*. Any expressive stacker (GBM, deep net) will overfit and deflate OOS.

- **Measure signal correlation FIRST and prune.** Combined IC benefits from adding signals only when they are uncorrelated; if the 10-agent swarm consensus correlates >0.6 with the factor engine, collapse it to one effective signal — do not count 10 correlated agents as 10 votes. (Verifier note on the finding's stated "1/sqrt(M)" scaling: it was garbled — combining M *uncorrelated* equal-IC signals *raises* combined IC ≈ sqrt(M)·IC; the actionable point — prune redundancy — stands.)
- **Default to inverse-variance / equal-weight z-scored blend** as the hard baseline. Adopt a regime-conditional shrunk logistic combiner **only if it beats that baseline under CPCV + deflated Sharpe.**
- **Meta-labeling done correctly:** primary model (factor engine) decides side; a *separate* secondary model decides act/abstain and size, trained on **genuinely orthogonal** features (lagged flow, regime, VIX term structure, event proximity) — never the primary's own features ("you can't squeeze the same orange twice"). The "regime-features-only-to-the-meta-model" trick is a leakage artifact — avoid.
- **Abstain is a first-class output.** For a ~46-53% base, tune the meta-filter to trade recall for precision and act only on high-precision days. Report trade frequency and precision-on-acted-days as headline metrics. Honest framing: meta-labeling is sizing/abstention *hygiene*, not alpha — it cannot manufacture edge where the primary has none.
- **Shrink everything** (ridge-logistic / Bayesian model averaging / James-Stein toward equal weights). Keep the combiner transparent and monotone (signed linear/logistic coefficients) to preserve the "transparent scoring" moat.
- **Embargo the combiner from the autoresearch data** that tuned the primary factor weights — otherwise compounded overfitting.

### 3.2 Sizing — fractional Kelly / vol-targeting / CVaR off the MC distribution

Position sizing is NOT alpha and cannot manufacture edge. Its job is geometric-return preservation and ruin-avoidance. Build **one sizing module**: `position = min(fractionalKelly, volTarget, cvarCap) × direction`, taking the *minimum* of three independent caps.

1. **Fractional Kelly, solved numerically over the MC scenarios:** `f* = argmax mean(log(1 + f·r_i))`, capped at 0.25 (start there) to 0.5, plus a ~1.5x leverage limit. This natively handles skew/fat tails that closed-form μ/σ² Kelly ignores. **Never full Kelly on estimated parameters** (it amplifies estimation error). Correction to the finding: half-Kelly gives ~75% of growth at ~50% of *volatility* (~25% of variance) — the tradeoff is even better than stated, but variance and volatility were conflated.
2. **Volatility targeting as RISK CONTROL, not a Sharpe engine.** Scale by target/realized vol (EWMA λ=0.94), target ~10-15% annualized book vol, cap leverage ~1.5-2x, hard de-gear above India VIX ~22 (conditional vol targeting). Be explicit internally: Cederburg et al. (JFE 2020) show vol-managed timing does *not* reliably beat the unmanaged base OOS — it stabilizes risk, it does not raise returns.
3. **Empirical CVaR gate** (95%/99%) computed from the same MC scenarios as a hard pre-trade block; CVaR over VaR (coherent, tail-aware).
4. **Drawdown-control overlay:** size ∝ max(0, 1 − currentDD/DD_budget).
5. **Calibrate before sizing** (isotonic/Platt on held-out data, then shrink confidence toward 0.5). Size off the calibrated probability, never raw softmax/agent-vote confidence.

**Costs inside expectancy.** Compute per-trade edge net of STT, exchange charges, GST, stamp, and modeled impact *before* Kelly, so f*→0 organically when the edge can't clear the hurdle. **Verify the sizer outputs ~0 on the current 46% signal — that is the correct result, and it becomes the regression test that the sizing layer is not fabricating edge.** Any positive size the sizer emits must never be read as signal validation.

**Express direction in Nifty/BankNifty near-month futures** (~5-8 bps round-trip vs ~25-35 bps single-stock cash delivery), and surface F&O lot-size quantization rather than silently rounding small accounts up into oversizing.

---

## 4. Validation Protocol

This is the highest-integrity artifact in the whole program. Gate every "edge" claim behind it.

1. **Benchmark against always-long AND buy-and-hold, never zero.** Report excess return over always-long, net of costs. Run this on the existing 20yr backtest *immediately* — it is the fastest way to confirm the 226pp proxy is drift, not skill, and to stop citing it.
2. **Deflated Sharpe Ratio (Bailey & Lopez de Prado 2014), require DSR > 0.95.** Feed the *honest trials count* from the autoresearch sweep (momentum weight, VIX weight, σ 2.5-3.0, asymmetric thresholds, VIX cutoff — easily hundreds-to-thousands of effective trials) into the expected-max-Sharpe null. A t-stat of 2.0 is invalid here; Harvey-Liu implies t>3.0+.
3. **Probability of Backtest Overfitting (PBO via CSCV), reject if >0.5.**
4. **Combinatorial Purged Cross-Validation + embargo** (1-5 day embargo for next-day labels) replaces plain k-fold / single walk-forward. Re-validate every autoresearch finding on folds it never selected on; drop anything that doesn't replicate.
5. **Nested search:** outer walk-forward for honest OOS, inner CPCV for hyperparameter selection. Never select parameters on data that also reports performance.
6. **One explicit India cost engine, per instrument:** STT (0.1% ×2 cash delivery; **0.02% futures sell-side, 0.1%-of-premium options sell-side** — post-Oct-2024, verified; ignore the fabricated "0.05%/0.15% Apr-2026" figures), stamp (buy-side 0.015% delivery / 0.002% futures), exchange true-to-label flat charges, SEBI ₹10/cr, **18% GST on brokerage+txn+SEBI** (GST-on-SEBI-fee added Apr 2025), DP ₹15.34/scrip on cash sells, plus a cap-tier impact term (square-root law calibrated to NSE published impact cost; ~1-5 bps Nifty futures, 30-100+ bps sub-Nifty, widening when VIX>22). Reconcile against a live Zerodha contract note before trusting any net number.
7. **Regime-segment the backtest** at **Oct-1-2024** (STT doubling) and **Nov-20-2024** (single weekly expiry, ₹15-20L lots, ~29% notional drop). Never report a single metric averaged across these incompatible regimes.
8. **Survivorship:** reconstruct Nifty membership as-of each date from the composition history already in the repo (semi-annual Mar/Sep reconstitution + delistings). The ~4.9pp/yr, ~0.10 Sharpe inflation figure is a **Smallcap-250** result — it does NOT transfer to the Nifty-50 index-direction signal (rare delistings); the real survivorship exposure is in the 96-stock single-stock universe.
9. **PIT / corporate-action hazard (India-specific, load-bearing):** Kite/Zerodha historical OHLC is retroactively split/bonus/rights-adjusted (dividends not), so "the same date" returns different prices depending on *when* you query. NSE bhavcopy is unadjusted as-traded. **Snapshot the exact feature vector at prediction time; do not rebuild `market_state` with `force=True` at score time.** Store unadjusted close + a separate adjustment-factor series.
10. **Fix the scoring-threshold bug** in `src/scoring.py`: the label uses a 0.25% dead-band but a HOLD is credited correct when |actual|<1.0%. This double standard silently inflates `direction_correct`. Use ONE symmetric dead-band and re-score history. Add Brier and log score on the confidence field — calibration is the sellable artifact.
11. **Optimize CVaR/p10 net-of-cost under walk-forward, never in-sample mean PnL**; multiple seeds / nested resampling so you don't overfit one bootstrap realization.

---

## 5. Production & Compliance

### 5.1 The #1 bottleneck: zero scored live predictions

Nothing this system claims about edge is falsifiable until an append-only forecast ledger exists and accumulates rows. **Everything downstream is blocked on this.**

- **Append-only, event-sourced ledger.** A **predict job ~19:00 IST** INSERTs `(prediction_date, target_date, direction, return_range, confidence, model_version_hash, input_snapshot_hash, created_at)` and can never UPDATE. A **separate score job ~09:30 IST** INSERTs into an outcomes table joined on `(date, model_version)`. Split the jobs so they can never accidentally share future data. Idempotent upsert keyed on `(session_date, model_version, run_type)`.
- **Non-gameable requires EXTERNAL commitment.** Revoking UPDATE/DELETE is not enough (the operator controls the DB). Hash-commit each prediction publicly (public git, OpenTimestamps/third-party timestamp) *before* outcome.
- **Version everything** into `model_version`: data snapshot, feature code, agent prompts, factor weights, **and LLM model id** (Sarvam vs Claude). Any change forks the track record and resets the credibility clock — do NOT blend scores across versions.
- **Dead-man's-switch alerting** on job non-run, missing bhavcopy, NaN/stale features, prediction-count mismatch, score-job lag. A daily pipeline that silently stops is the most common production failure.
- **Live-vs-backtest reconciliation:** re-run the backtest engine on the frozen live inputs and assert identical output; any gap is a code/data bug (calendar, adjusted-price drift, cost double-count) — the literature's dominant divergence source, not alpha decay.
- **Paper-trade with frozen parameters.** ~60 scored days for a first calibration read; ~250 before *any* directional claim — **but be honest that 250 days cannot statistically confirm a ~3.5-4pp directional edge**; distinguishing 46% from 42.5%/50% at daily frequency needs ~1,200-1,500+ observations (5-6 years). 250 days supports a *calibration* sanity check only.

### 5.2 Model decay & retraining

Retrain on a *fixed cadence tied to horizon*, not on a whim: quarterly-to-semi-annual walk-forward re-optimization, 2-4yr IS window, 3-6mo OOS step. Re-optimizing on <100 live points chases noise and manufactures false "decay" signals. Every re-optimization forks `model_version` — do not run Phase-2 autoresearch again until live scored days exist.

### 5.3 SEBI retail-algo compliance

Stay in the safe harbor and it is a non-event; step outside and it is a violation.

- **Sub-10-OPS, white-box, self-use (or narrow family: self/spouse/dependent children/dependent parents).** A next-day strategy firing a handful of batched orders is nowhere near 10 orders/sec/segment/exchange → **no exchange algo *registration*, no RA registration, no empanelment.**
- **Mandatory regardless of speed:** broker-issued client-specific API key, static-IP whitelisting, OAuth+2FA per session, and **generic algo-ID order tagging** (the tagging obligation persists *below* threshold — do not treat it as optional; only the *unique-ID registration* is threshold-gated).
- **Firewall research from execution.** Generating signals is unregulated research; only automated *order placement* is in scope.
- **Do NOT distribute or sell signals.** That flips you into exchange-empaneled Algo-Provider territory, and publishing next-day calls could independently trigger the SEBI RA/IA regime. Don't burst all 50 constituents at once (momentary >10 OPS reclassifies you).
- **Timeline (verified, corrected):** full framework effective **1 April 2026** with penalty milestones through early 2026 — reconfirm the live state with your broker at go-live. The broker is principal and can kill your algo-ID; build graceful halt handling. Correction to the finding: black-box → RA-registration is a *provider-side* trigger; it does NOT automatically attach to a solo user running a black-box algo on their own account (staying white-box is still the right conservative call).
- **Frame this honestly: it is a license-to-operate, available identically to every retail quant — not an edge. Keep it as a compliance checklist; delete it from the edge inventory.**

---

## 6. Sequenced Roadmap

Ranked by leverage, tailored to current state.

**🎯 THE SINGLE HIGHEST-LEVERAGE NEXT STEP — Ship the append-only, externally-committed forecast ledger (predict job + score job + dead-man's-switch).** Until this exists and accumulates rows, *nothing* about edge is falsifiable and every other improvement is unmeasurable. It is literally item #1 in the team's own roadmap. Do it first, freeze parameters, start the clock.

Then, in order:

1. **Fix the two latent integrity bugs before collecting any track record:** (a) the `scoring.py` 0.25%-vs-1.0% HOLD-threshold mismatch; (b) the PIT/corporate-action hole (`build(d, force=True)` at score time rebuilding from retroactively-adjusted Kite data). Snapshot inputs at prediction time. A contaminated ledger is worse than none.

2. **Run the honest-benchmark pass on the existing 20yr backtest:** always-long + buy-and-hold, net of the new India cost engine, regime-split at Oct/Nov-2024. Re-express the 226pp proxy as excess-over-always-long. Publish the DSR (with real trials count) and PBO alongside 46%. This retires the false-edge narrative with numbers.

3. **Retarget the direction model from the untradeable Nifty index level to the roll-adjusted Nifty/BankNifty FUTURES return.** Compute per-vehicle breakeven hit rate and test the ensemble's *conditional* accuracy in its top-confidence decile — the real go/no-go is whether any confidence-gated subset clears ~51% futures breakeven by more than its ~5-6pp standard error on post-Oct-2024 OOS data.

4. **Swap the MC objective from p10-point to CVaR, fix the narrow-composite bug, add FHS, vectorize to 5-10k scenarios.** Fold the state-dependent cost distribution inside the PnL sim. Ship the min-of-three sizing module and verify it outputs ~0 on the 46% signal (the regression test).

5. **Wire participant-wise OI (FII/DII/Client/Pro) as lagged positioning percentile z-scores;** demote FII/DII cash to context. Add the scheduled-event calendar as a regime flag. Cut the statistical regime count from 7 to 2-4 (keep the 7 archetypes as an in-regime behavioral ensemble); audit the existing regime evaluation for **smoothed-state look-ahead** and re-run with filtered/online states (expect the regime-conditional edge to shrink materially).

6. **Stand up the SEPARATE cross-sectional stock-selection sleeve** (not the index model): PIT XBRL bitemporal store + the look-ahead-quantification study; forensic exclusion overlay; quality-conditioned value composite gated against IIMA HML+WML net of cost; ADV-sized PEAD overlay in liquid mid-caps. Wire the idle 76-stock Morningstar data + probabilistic DCF here.

7. **Confine the LLM swarm to Stage-1 extraction + scenario + explanation;** add the contamination guard (post-cutoff-only OOS window), memorization probe, and post-hoc confidence calibration. Rewrite the swarm charter in repo docs.

8. **Document SEBI safe-harbor compliance** (sub-10-OPS, white-box, self-use, static-IP/OAuth/generic-tag) before any live order placement via Kite.

---

## 7. Hard Truths & Open Questions

**Hard truths (say them out loud):**

- **The core product premise is wrong-horizon.** Next-day Nifty direction is the hardest, most-efficient, most-crowded question this data can ask, and 46% is below the naive base rate. The system's real value is elsewhere (data hygiene, risk, cross-sectional selection, track record). Keep pointing the flagship metric at the one thing the system is worst at and you will keep concluding it has no edge — correctly.
- **Almost nothing here is alpha.** Across 14 packets, the durable items were overwhelmingly *hygiene, risk-control, and infrastructure*. Sizing is a multiplier on edge, not a source. Regime overlays cut drawdown but do not raise OOS Sharpe. PIT stores prevent fake alpha but produce none. The "structural authenticity" moat (modeling FII/DII/dealer/operator/dabba) is a real narrative and data-assembly moat — but it is **unvalidated as a source of realized VaR/return improvement** and must not be booked as edge until backtested.
- **The autoresearch numbers are hypotheses, not findings.** 46.2%, momentum 0.44, VIX-22, σ 2.5-3.0 all came out of a ~10,000-trial search on ~5yr of data — far past the ~45-trial ceiling where a high in-sample Sharpe is expected to be zero OOS. Treat them as unproven until forward-scored.
- **The contamination treadmill is structural.** Any LLM-in-the-loop component can never compound a long clean OOS record, because every model upgrade re-contaminates history. Plan the LLM's role around this permanently.
- **Costs are lower than feared on the right vehicle, which makes the real problem starker.** On liquid Nifty futures the cost hurdle is ~5-8 bps — so "costs kill it" is *not* the honest story for the index product. The honest story is that the directional edge itself is too thin and too regime-dependent to trade on the mean. Costs *are* lethal on single-stock cash / options, which is exactly where the cross-sectional sleeve must be ADV- and impact-disciplined.

**Open questions that need empirical answers (not more backtesting):**

1. **What is that narrow composite actually resampling?** (p10=30.76 / spread 1.72) — until re-expressed in return/PnL units, no p10/CVaR objective is trustworthy. *Resolve first.*
2. **Does ANY confidence-gated futures subset clear ~51% breakeven OOS post-Oct-2024, by more than its standard error?** This is the true go/no-go for the index product. Most likely answer: no — but it must be *run*, not assumed.
3. **How much of the reported fundamental "alpha" is restatement look-ahead?** The Screener-vs-filing-date gap study monetizes the hygiene moat and is the most defensible single deliverable.
4. **Does the forensic exclusion overlay measurably reduce left-tail drawdown / avoid de-ratings OOS?** Events are rare; statistical proof is weak; this needs a long-horizon, blow-up-avoidance scorecard, not a Sharpe number.
5. **After the full gauntlet (lagged, CPCV, DSR, net-of-cost, p10), does the FII positioning tail signal survive as anything above null?** Expect marginal-to-null; design to be unsurprised.
6. **Is the "structural authenticity" agent modeling worth its engineering cost** in realized risk/return terms, or is it narrative? Requires a backtested VaR-exception / attribution test before it earns its place.

**Bottom line for the book:** run BharatTwin as a PIT-clean India data platform + a survival-grade risk engine + a candidate long-horizon cross-sectional selection book, wrapped in an externally-committed, calibrated public track record. Stop selling next-day index direction. The edge that survives adversarial scrutiny is the edge of *not fooling yourself and not blowing up* — which, in a market where 91% of retail F&O traders lose money, is a more valuable and more defensible position than a fake 46%.

---

*Strongest sources underpinning this guide: Bailey & López de Prado, "The Deflated Sharpe Ratio" (SSRN 2460551) and the PBO/CSCV work (SSRN 2326253); Harvey & Liu, "Backtesting" (t>3.0 haircut); Rockafellar & Uryasev, "Optimization of Conditional Value-at-Risk" (2000); Politis-Romano stationary bootstrap & Politis-White block length; Barone-Adesi Filtered Historical Simulation; Cederburg et al., "On the Performance of Volatility-Managed Portfolios" (JFE 2020); Moreira-Muir (JF 2017); the IIMA/Agarwalla-Jacob-Varma "Indian Fama-French-Momentum" library; Asness-Frazzini-Pedersen QMJ; McLean-Pontiff (JF 2016) and Jacobs-Müller on post-publication anomaly decay; the 2024-26 LLM-agent contamination literature ("The Alpha Illusion" 2605.16895, "Profit Mirage" 2510.07920); SEBI circular SEBI/HO/MIRSD/MIRSD-PoD/P/CIR/2025/0000013 and NSE retail-algo FAQs; and NSE/Zerodha primary cost/corporate-action documentation.*