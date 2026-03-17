"""
Session 011 — In-context replay of exit poll euphoria (positive event).

Claude Code acts as all 8 agents. First BUY case.
Actual: Nifty +3.25% on Jun 3, 2024.
"""

from __future__ import annotations

import json
import logging
from datetime import date

from src.agents.personas import PERSONA_BY_ID
from src.agents.runner import _aggregate
from src.agents.schemas import AgentDecision, ConsensusResult
from src.replay.cases.exit_poll_june2024 import ACTUAL_NIFTY_RETURN_PCT, evaluate

logger = logging.getLogger(__name__)

# ─── In-context agent responses ─────────────────────────────────────────────
# Context: Nifty 22,531 (May 31 close). Weak breadth (36% up), momentum_1d -0.45%,
# VIX elevated at 24.60 (election uncertainty). FII net sellers -2,222 cr.
# EVENT: Exit polls predict NDA 350-370 seats, BJP >300. Landslide = reform mandate.
# All this over the weekend — Monday Jun 3 is the first session to react.

AGENT_RESPONSES: dict[str, dict] = {

    "fii_quant": {
        "direction": "BUY",
        "confidence_pct": 88,
        "nifty_return": {
            "low_pct": +1.0,
            "base_pct": +3.0,
            "high_pct": +5.0
        },
        "sector_views": {
            "FINBK": {"direction": "BUY", "reasoning": "Banking reform acceleration under strong mandate — privatization, insurance FDI. BankNifty +4-5%."},
            "IT": {"direction": "HOLD", "reasoning": "IT is reform-neutral; some buying on broad risk-on but not the primary beneficiary."},
            "ENERGY_ONG": {"direction": "BUY", "reasoning": "PSU divestment accelerates under strong mandate — BPCL, ONGC get re-rated."}
        },
        "thesis": "This is the single most bullish political event for Indian equities. A 350-370 NDA win signals the strongest reform mandate since 2014. FIIs have been cautious ahead of elections (net sellers -2,222 cr) — now they'll reverse aggressively. Expect ₹10,000-15,000 cr of FII buying on Jun 3 alone. The VIX at 24.60 reflects election uncertainty that evaporates completely with a landslide result — expect VIX to collapse to 15-17. The 'Modi premium' gets a massive upgrade. Nifty should gap up 2-3% at open and add more through the day.",
        "key_factors": [
            "Exit polls consensus: NDA 350-370 — removes political uncertainty completely",
            "FII positioning was cautious (-2,222 cr net sellers) — reversal incoming",
            "VIX at 24.60 collapses to 15-17 as uncertainty premium evaporates",
            "'Modi premium' upgrade — strongest mandate = fastest reform execution",
            "Historical: 2014 and 2019 Modi wins saw 5-7% rallies"
        ],
        "risks": [
            "Exit polls can be wrong — actual results on Jun 4 may differ",
            "Gap-up may attract profit-taking from traders who bought pre-election",
            "Global risk-off could dampen the rally"
        ],
        "conviction": 5
    },

    "retail_momentum": {
        "direction": "BUY",
        "confidence_pct": 92,
        "nifty_return": {
            "low_pct": +1.5,
            "base_pct": +3.5,
            "high_pct": +5.5
        },
        "sector_views": {
            "FINBK": {"direction": "BUY", "reasoning": "Banks lead every election rally — SBIN, ICICI, HDFC all gap up 4-6%."},
            "CONAUT": {"direction": "BUY", "reasoning": "Auto stocks rally on reform optimism and consumer confidence."},
            "IT": {"direction": "BUY", "reasoning": "Everything rallies in a gap-up euphoria — IT joins the party."}
        },
        "thesis": "FOMO is about to explode. Every retail trader who was sitting on the sidelines during election uncertainty will rush in at open. The momentum signals were weak going into this (breadth 36%, mom_1d -0.45%) but exit polls change EVERYTHING. This is a regime break — from uncertainty to euphoria in one weekend. Nifty will blast through 23,000 resistance and test 23,500. The chart pattern shifts from bearish to ultra-bullish overnight. Volume will be the highest of the year.",
        "key_factors": [
            "Regime break: election uncertainty → euphoria in one weekend",
            "Retail FOMO — sideline cash rushes in at open",
            "23,000 breakout level becomes the first target on gap-up",
            "Volume spike confirms momentum shift — highest of the year"
        ],
        "risks": [
            "Gap-up at open may mean the easy money is already made",
            "Profit-taking from pre-election longs could cap the rally"
        ],
        "conviction": 5
    },

    "dealer_hedging": {
        "direction": "BUY",
        "confidence_pct": 85,
        "nifty_return": {
            "low_pct": +1.0,
            "base_pct": +2.5,
            "high_pct": +4.5
        },
        "sector_views": {
            "FINBK": {"direction": "BUY", "reasoning": "Call OI will explode at 23,000-24,000 strikes; dealer gamma hedging pushes prices higher."},
            "IT": {"direction": "HOLD", "reasoning": "Lower beta to political events from a derivatives perspective."}
        },
        "thesis": "VIX at 24.60 is pricing in a binary election outcome. With exit polls removing the downside scenario, VIX collapses 30-40% to the 15-17 range. This has two effects: (1) put sellers collect massive premium as puts go to zero, freeing up cash, (2) dealers who sold calls must delta-hedge by buying futures, amplifying the rally. The options market was pricing ±4-5% — the move will be at the upper end. Dealer gamma is now positive, meaning hedging flows AMPLIFY the upside. This is a vol crush + delta hedging double-whammy to the upside.",
        "key_factors": [
            "VIX collapse from 24.60 to 15-17 — massive vol crush",
            "Put OI goes to zero — put sellers freed to buy calls/futures",
            "Dealer gamma flips positive — hedging amplifies upside",
            "Options were pricing ±4-5%; actual move at upper end (+3-5%)"
        ],
        "risks": [
            "If VIX doesn't collapse as expected (exit polls doubted), rally is capped",
            "Very high opening gap may lead to 'buy the rumor, sell the news' intraday"
        ],
        "conviction": 4
    },

    "dii_mf": {
        "direction": "BUY",
        "confidence_pct": 72,
        "nifty_return": {
            "low_pct": +0.5,
            "base_pct": +2.0,
            "high_pct": +3.5
        },
        "sector_views": {
            "FINBK": {"direction": "BUY", "reasoning": "Policy continuity is positive for banking; reform acceleration supportive for NIMs."},
            "CONPFMCG": {"direction": "BUY", "reasoning": "Consumption story strengthens under stable government."},
            "INDMFG": {"direction": "BUY", "reasoning": "Infrastructure capex accelerates with strong mandate."}
        },
        "thesis": "Political stability is the best macro gift for long-term investors. A strong NDA mandate means policy continuity, capex acceleration, and reform execution. From a valuation standpoint, Nifty at ~22,500 with a PE of ~21x is fair, and the mandate upgrade pushes fair value higher. Our SIP book will see even more inflows as retail confidence surges. We'll participate in the rally but we're more moderate than the fast-money crowd — a 2-3% move is reasonable, not 5%+. The real story is the medium-term: Modi 3.0 = 3 more years of capex-led growth.",
        "key_factors": [
            "Policy continuity and reform acceleration under strong mandate",
            "Valuation support at ~21x PE with earnings upgrade potential",
            "SIP inflows will accelerate as retail confidence surges",
            "Historical: post-election rallies sustain for 3-6 months"
        ],
        "risks": [
            "Exit polls can be significantly wrong (2004 precedent)",
            "Market may have already priced in some BJP-win probability",
            "Global macro headwinds (Fed rates, crude) unchanged"
        ],
        "conviction": 3
    },

    "macro": {
        "direction": "BUY",
        "confidence_pct": 82,
        "nifty_return": {
            "low_pct": +1.0,
            "base_pct": +2.8,
            "high_pct": +4.5
        },
        "sector_views": {
            "FINBK": {"direction": "BUY", "reasoning": "Banking reform pipeline (privatization, digital lending, insurance) gets accelerated."},
            "ENERGY_ONG": {"direction": "BUY", "reasoning": "PSU divestment (BPCL, HPCL) back on track with strong mandate."},
            "INDMFG": {"direction": "BUY", "reasoning": "Gati Shakti, PLI, defense capex all accelerate under reform continuity."},
            "IT": {"direction": "HOLD", "reasoning": "IT is FX/global-driven, not domestic reform beneficiary."}
        },
        "thesis": "The macro implications of a 350+ NDA mandate are significant: (1) fiscal consolidation stays on track — 5.1% deficit target credible, (2) divestment pipeline accelerates (BPCL, LIC), (3) infrastructure capex of ₹11 lakh crore continues, (4) labor/land reforms get another shot. The 'India premium' in EM allocations gets a boost — expect rating agencies to turn more positive. INR strengthens to sub-83 as FII inflows accelerate. India's weight in MSCI EM is already rising; this accelerates it. The medium-term growth narrative gets the strongest possible endorsement.",
        "key_factors": [
            "Fiscal consolidation credible under strong mandate",
            "Divestment pipeline accelerates (BPCL, LIC further tranches)",
            "Infrastructure capex ₹11 lakh crore continues and accelerates",
            "India's MSCI EM weight rises as FII inflows accelerate",
            "INR strengthens below 83 on FII inflows"
        ],
        "risks": [
            "Exit polls wrong → devastating reversal on Jun 4",
            "Global risk factors unchanged (Fed, crude, China slowdown)"
        ],
        "conviction": 4
    },

    "sector_rotation": {
        "direction": "BUY",
        "confidence_pct": 85,
        "nifty_return": {
            "low_pct": +1.5,
            "base_pct": +3.0,
            "high_pct": +5.0
        },
        "sector_views": {
            "FINBK": {"direction": "BUY", "reasoning": "Banks are THE election trade — SBIN, ICICI, HDFC all +4-6%. BankNifty outperforms."},
            "ENERGY_ONG": {"direction": "BUY", "reasoning": "PSU energy re-rates on divestment hopes. BPCL, ONGC +5-8%."},
            "INDMFG": {"direction": "BUY", "reasoning": "Capex/infra plays (LT, NTPC, Adani Ports) rally on Gati Shakti acceleration."},
            "IT": {"direction": "HOLD", "reasoning": "IT participates marginally; not the primary beneficiary of domestic political events."},
            "CONPFMCG": {"direction": "HOLD", "reasoning": "Defensive FMCG underperforms in a risk-on rally."},
            "CONPPHARM": {"direction": "HOLD", "reasoning": "Pharma is reform-neutral; relative underperformer on a risk-on day."}
        },
        "thesis": "The sector rotation map for a BJP landslide is clear: overweight banks, PSUs, infra; underweight defensives. BankNifty should outperform Nifty by 1-2%. PSU basket (BPCL, ONGC, Coal India) is the highest-beta play — these are pure divestment and reform proxies. The rotation is: sell defensive (pharma, FMCG, IT) on a relative basis, buy cyclical (banks, infra, PSU). Nifty's 35% financial weight means banks alone contribute 1.5% to the index rally.",
        "key_factors": [
            "Banks are #1 election play — BankNifty outperforms by 1-2%",
            "PSU re-rating on divestment acceleration",
            "Infra/capex names (LT, NTPC) rally on policy continuity",
            "Defensives (pharma, FMCG) underperform on relative basis"
        ],
        "risks": [
            "If rally is indiscriminate (everything up), rotation thesis is muted",
            "PSU re-rating may be premature if divestment timeline extends"
        ],
        "conviction": 4
    },

    "corp_earnings": {
        "direction": "BUY",
        "confidence_pct": 70,
        "nifty_return": {
            "low_pct": +0.5,
            "base_pct": +2.0,
            "high_pct": +3.5
        },
        "sector_views": {
            "FINBK": {"direction": "BUY", "reasoning": "Bank heavyweights (HDFC +0.34% drag, ICICI -0.89%) will reverse sharply — these are reform proxies."},
            "IT": {"direction": "HOLD", "reasoning": "INFY/TCS earnings are global — election outcome doesn't change their numbers."},
            "ENERGY_ONG": {"direction": "BUY", "reasoning": "RELIANCE benefits from reform continuity (telecom, retail, energy transition)."}
        },
        "thesis": "From a bottom-up perspective, the exit poll result is about PE multiple expansion, not earnings. FY25 Nifty EPS doesn't change because of the election — it's the MULTIPLE the market is willing to pay. A strong mandate pushes the 'fair PE' from 21x to 22-23x as reform certainty commands a premium. HDFCBANK (-0.34% on May 31) and ICICIBANK (-0.89%) are the biggest beneficiaries — together they're ~15% of Nifty. RELIANCE (10%+ weight) benefits from Jio/retail reform continuity. These three names alone drive 1.5-2% of the index move.",
        "key_factors": [
            "PE multiple expansion from 21x to 22-23x on reform premium",
            "HDFCBANK + ICICIBANK (~15% Nifty weight) rally 3-5%",
            "RELIANCE benefits from reform continuity narrative",
            "FY25 EPS unchanged — this is a valuation re-rating event"
        ],
        "risks": [
            "PE multiple already elevated at 21x — limited expansion room",
            "Q1 earnings in July may not support higher multiples"
        ],
        "conviction": 3
    },

    "event_news": {
        "direction": "BUY",
        "confidence_pct": 95,
        "nifty_return": {
            "low_pct": +2.0,
            "base_pct": +3.5,
            "high_pct": +6.0
        },
        "sector_views": {
            "FINBK": {"direction": "BUY", "reasoning": "BankNifty is THE election play. Expect +4-5% on banks alone."},
            "IT": {"direction": "BUY", "reasoning": "Everything rallies in euphoria — even IT gets a 1-2% bid."},
            "ENERGY_ONG": {"direction": "BUY", "reasoning": "PSU energy names are the highest-beta election plays."}
        },
        "thesis": "This is THE definitive positive event for Indian markets. Historical precedent is overwhelming: 2014 Modi landslide → Nifty +6.4%. 2019 Modi return → Nifty +3.8%. Exit polls predicting 350-370 NDA seats is even stronger than what was expected in 2019 (300-340). The gap-up will be massive (2-3% at open) and the rally continues through the day as FIIs pile in. Every major broker will upgrade India to overweight. VIX crashes 30%+. This is a one-day, front-page, risk-on party. The only caveat: exit polls were wrong in 2004, so some hedging is prudent.",
        "key_factors": [
            "Historical precedent: 2014 Modi win +6.4%, 2019 +3.8%",
            "Exit poll consensus 350-370 — strongest mandate prediction since 2014",
            "VIX crash from 24.60 to sub-17 — fear premium evaporates",
            "FII buying wave — ₹10,000+ cr expected on Jun 3",
            "Every sell-side desk will upgrade India on Monday morning"
        ],
        "risks": [
            "Exit polls can be wrong — 2004 BJP was predicted to win, lost",
            "Gap-up may exhaust day-1 buying power; consolidation follows",
            "Actual results on Jun 4 are the real test — this is still 'exit poll' not 'results'"
        ],
        "conviction": 5
    },
}


def run_exitpoll_replay() -> ConsensusResult:
    """Parse in-context agent responses and aggregate."""
    decisions: list[AgentDecision] = []

    for agent_id, response_data in AGENT_RESPONSES.items():
        persona = PERSONA_BY_ID[agent_id]
        response_data["agent_id"] = agent_id
        response_data["agent_role"] = persona.role
        response_data["raw_response"] = json.dumps(response_data)

        decision = AgentDecision(**response_data)
        decisions.append(decision)
        print(f"  {agent_id:20s} | {decision.direction.value:4s} | "
              f"conv={decision.conviction} | base={decision.nifty_return.base_pct:+.2f}%")

    consensus = _aggregate(date(2024, 5, 31), decisions)
    return consensus


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("\n" + "=" * 60)
    print("SESSION 011 — EXIT POLL REPLAY: June 3, 2024")
    print("Claude Code acting as all 8 agents (POSITIVE event)")
    print("=" * 60 + "\n")
    consensus = run_exitpoll_replay()
    print()
    evaluate(consensus)
