"""
Session 010 — In-context replay of 2024 Election results.

Claude Code acts as all 8 agents against the Jun 3 2024 state +
election result surprise. Actual: Nifty -5.93%.
"""

from __future__ import annotations

import json
import logging
from datetime import date

from src.agents.personas import PERSONA_BY_ID
from src.agents.runner import _aggregate
from src.agents.schemas import AgentDecision, ConsensusResult
from src.replay.cases.election_june2024 import (
    ACTUAL_NIFTY_RETURN_PCT,
    evaluate,
)

logger = logging.getLogger(__name__)

# ─── In-context agent responses ─────────────────────────────────────────────
# Context: Nifty at 23,264 after +3.3% exit-poll euphoria on Jun 3.
# All breadth metrics at extreme bullish levels (90% up, mom_1d +3.25%).
# FII net buyers at +7,444 cr. VIX 20.94 (elevated for the event).
# EVENT: BJP 240 seats (vs 350+ expected), loses majority. Massive negative surprise.

AGENT_RESPONSES: dict[str, dict] = {

    "fii_quant": {
        "direction": "SELL",
        "confidence_pct": 95,
        "nifty_return": {
            "low_pct": -8.0,
            "base_pct": -5.0,
            "high_pct": -2.5
        },
        "sector_views": {
            "FINBK": {"direction": "SELL", "reasoning": "Banks rallied hardest on exit polls (+3.5-5.7%) — maximum reversion risk on policy uncertainty."},
            "IT": {"direction": "SELL", "reasoning": "Even defensive IT will sell off in a broad risk-off panic of this magnitude."},
            "ENERGY_ONG": {"direction": "SELL", "reasoning": "PSU energy names rallied on reform expectations; coalition govt = reform deceleration."}
        },
        "thesis": "This is the most extreme positioning reversal setup I've seen. FIIs bought ₹7,444 cr on Jun 3 on exit poll euphoria. Now the actual result is a complete negation of that thesis — BJP at 240 vs expected 350+. Every FII position put on in the last week needs to be unwound. Expect ₹15,000-20,000 cr of FII selling on Jun 4. The exit poll premium in Nifty was 800-1000 points — that gets wiped immediately at open. Coalition government means slower reforms, weaker mandate, policy compromises. This is a 5-8% event.",
        "key_factors": [
            "BJP 240 vs 350+ expected — 100+ seat shortfall is a 5-sigma surprise",
            "FII net bought +7,444 cr on Jun 3 — all positions need unwinding",
            "Exit poll premium of 800-1000 Nifty points gets wiped at open",
            "Coalition govt = weaker reform mandate (divestment, labor, land)",
            "VIX at 20.94 will spike to 30+ on this magnitude of surprise"
        ],
        "risks": [
            "NDA at 292 seats still forms government — PM continuity limits tail risk",
            "If circuit breaker triggers at -10%, forced pause could create a bottom",
            "Historically, election surprises in India recover within 2-4 weeks"
        ],
        "conviction": 5
    },

    "retail_momentum": {
        "direction": "SELL",
        "confidence_pct": 90,
        "nifty_return": {
            "low_pct": -8.0,
            "base_pct": -5.5,
            "high_pct": -3.0
        },
        "sector_views": {
            "FINBK": {"direction": "SELL", "reasoning": "SBIN +5.67% and AXISBANK +3.92% on Jun 3 — those gains evaporate and then some."},
            "CONAUT": {"direction": "SELL", "reasoning": "Auto stocks rallied on reform hopes; all reversion candidates."},
            "IT": {"direction": "SELL", "reasoning": "Everything sells in a panic — no safe havens on day 1."}
        },
        "thesis": "Pure panic. Every retail trader who bought the exit poll rally on Jun 3 is sitting on a massive loss at open. The momentum was extremely bullish — 90% breadth, +3.25% in a single day — and now it reverses violently. 23,000 was the breakout level; we're going to slice through it and test 22,000 or lower. F&O traders with long positions face margin calls. The psychology is FOMO turning to panic in 12 hours. I'm expecting circuit breaker territory.",
        "key_factors": [
            "90% breadth on Jun 3 = maximum long positioning, maximum pain on reversal",
            "23,000 breakout level becomes breakdown — stops trigger cascade",
            "Retail F&O longs face margin calls — forced liquidation accelerates selling",
            "Momentum_1d was +3.25% — this reverses to -5% or worse"
        ],
        "risks": [
            "Intraday recovery if NDA govt formation becomes certain",
            "Circuit breaker at lower levels could halt the cascade"
        ],
        "conviction": 5
    },

    "dealer_hedging": {
        "direction": "SELL",
        "confidence_pct": 95,
        "nifty_return": {
            "low_pct": -10.0,
            "base_pct": -6.0,
            "high_pct": -3.5
        },
        "sector_views": {
            "FINBK": {"direction": "SELL", "reasoning": "BankNifty options will see the most violent repricing — put skew will explode."},
            "IT": {"direction": "SELL", "reasoning": "Correlation goes to 1 in a panic — everything sells together."}
        },
        "thesis": "This is a gamma nuclear event. VIX at 20.94 was already elevated for the election, but the market was positioned for a positive outcome. Now every call option written in the last week goes to zero, and put demand will be infinite at open. VIX will spike to 28-35 range. Dealers who sold puts are now deeply short gamma — their delta hedging (selling futures) will amplify the crash by an additional 1-2%. The options market was pricing in a +/-3% move; the actual move will be -5% to -8% because of the positioning asymmetry. I expect intraday lows near -8% before any stabilization.",
        "key_factors": [
            "VIX spike from 20.94 to 28-35 — massive vol repricing",
            "Gamma squeeze: dealers short puts must sell futures to delta hedge",
            "Call OI built on exit poll euphoria goes to zero — total call wipeout",
            "Options market priced ±3%; actual move exceeds by 2x — tail event",
            "No expiry nearby but the magnitude overwhelms any support structure"
        ],
        "risks": [
            "If VIX spikes above 30 rapidly, institutional vol sellers may step in",
            "Circuit breaker at -10% physically halts selling"
        ],
        "conviction": 5
    },

    "dii_mf": {
        "direction": "SELL",
        "confidence_pct": 70,
        "nifty_return": {
            "low_pct": -6.0,
            "base_pct": -3.5,
            "high_pct": -1.5
        },
        "sector_views": {
            "FINBK": {"direction": "SELL", "reasoning": "Short-term selloff unavoidable; but valuations will become attractive quickly."},
            "CONPFMCG": {"direction": "HOLD", "reasoning": "Defensive FMCG will fall less; consumption story unchanged by election."},
            "CONPPHARM": {"direction": "HOLD", "reasoning": "Pharma relatively insulated from political outcome."}
        },
        "thesis": "The selloff is coming, no question. But I'm less extreme than the fast-money traders. BJP at 240 in a 292-seat NDA coalition is still a functioning government — PM Modi returns, key economic ministries will be BJP-held, and the consumption/digitization story doesn't change overnight. DII flows are steady at ₹18,000+ cr/month via SIPs. We will be deploying cash aggressively if Nifty falls below 22,000. The initial panic will be 4-6% but a week-end recovery of 50% is likely. Our investment committee would see this as a buying opportunity at the right price.",
        "key_factors": [
            "PM continuity — Modi govt formation near-certain despite coalition",
            "SIP flows provide ₹18,000+ cr/month floor — DII buying on dips",
            "Nifty PE will compress from ~22x to ~20x — value zone approaching",
            "Historical: 2004 election surprise (BJP lost to UPA) — Nifty recovered fully in 3 months"
        ],
        "risks": [
            "If coalition negotiations stall, uncertainty extends beyond 1 day",
            "FII selling of ₹15,000+ cr could overwhelm DII buying capacity",
            "Policy reform deceleration could lower medium-term earnings growth"
        ],
        "conviction": 3
    },

    "macro": {
        "direction": "SELL",
        "confidence_pct": 88,
        "nifty_return": {
            "low_pct": -7.0,
            "base_pct": -4.5,
            "high_pct": -2.0
        },
        "sector_views": {
            "FINBK": {"direction": "SELL", "reasoning": "Banking reform pace slows with coalition politics — privatization, bad bank on hold."},
            "ENERGY_ONG": {"direction": "SELL", "reasoning": "PSU divestment slows; energy sector reform pipeline disrupted."},
            "INDMFG": {"direction": "SELL", "reasoning": "Infrastructure capex may face coalition bargaining — Gati Shakti pace uncertain."},
            "IT": {"direction": "HOLD", "reasoning": "IT fundamentals are global, not domestic policy-dependent."}
        },
        "thesis": "The macro implications are significant but nuanced. A coalition BJP government means: (1) slower divestment (BPCL, LIC further sales on hold), (2) fiscal discipline may slip as coalition partners demand spending, (3) labor/land reform legislation becomes harder to pass, (4) foreign investor confidence in 'Modi premium' gets a haircut. The repo rate at 6.50% is already restrictive and a weaker mandate reduces the probability of structural reforms that were supporting the India premium in EM allocations. INR likely weakens to 84+ as FII outflows accelerate. The market is repricing from 'reform certainty' to 'coalition uncertainty.'",
        "key_factors": [
            "Coalition = slower reforms (divestment, labor, land)",
            "Fiscal discipline risk — coalition partners demand spending",
            "'Modi premium' in India valuation gets a haircut",
            "INR will weaken on FII outflows — 84+ USDINR likely",
            "2004 analogy: UPA surprise → initial 11% crash, full recovery in 3 months"
        ],
        "risks": [
            "If govt formation is swift and policy continuity signaled, recovery is faster",
            "RBI may intervene to stabilize INR, calming FII nerves"
        ],
        "conviction": 4
    },

    "sector_rotation": {
        "direction": "SELL",
        "confidence_pct": 85,
        "nifty_return": {
            "low_pct": -7.0,
            "base_pct": -4.5,
            "high_pct": -2.0
        },
        "sector_views": {
            "FINBK": {"direction": "SELL", "reasoning": "Most overweight sector on exit polls; maximum unwind. BankNifty -8% or more."},
            "ENERGY_ONG": {"direction": "SELL", "reasoning": "PSU energy names (ONGC, BPCL, IOC) rallied on divestment hopes — all revert."},
            "INDMFG": {"direction": "SELL", "reasoning": "Infra/capex plays (LT +4.56%) give back all exit-poll gains and more."},
            "IT": {"direction": "HOLD", "reasoning": "Least sensitive to domestic politics; relative outperformer."},
            "CONPFMCG": {"direction": "HOLD", "reasoning": "Consumption demand doesn't change with election outcome."},
            "CONPPHARM": {"direction": "BUY", "reasoning": "Pharma is the best hiding spot — global revenues, zero political beta."}
        },
        "thesis": "The sector rotation map is clear: every 'exit poll trade' reverses. Banks (+3.5-5.7% on Jun 3) and PSUs (reform/divestment plays) get hit hardest. The rotation trade is: sell banks/infra/PSU, relative safety in IT/pharma/FMCG. BankNifty should underperform Nifty by 2-3% because banking reform (privatization, insurance, digital lending) is most directly impacted by coalition politics. At index level, Nifty's ~35% financial weight means the sectoral drag is massive.",
        "key_factors": [
            "Banks rallied most on exit polls — maximum reversion",
            "PSU divestment plays (BPCL, LIC) get de-rated immediately",
            "Infra/capex names (LT, NTPC) lose the 'reform premium'",
            "IT/pharma/FMCG have negligible domestic political sensitivity"
        ],
        "risks": [
            "In a panic selloff, even 'safe' sectors sell — correlation spikes to 1",
            "Sector rotation may take days; day-1 is indiscriminate selling"
        ],
        "conviction": 4
    },

    "corp_earnings": {
        "direction": "SELL",
        "confidence_pct": 75,
        "nifty_return": {
            "low_pct": -6.0,
            "base_pct": -3.5,
            "high_pct": -1.5
        },
        "sector_views": {
            "FINBK": {"direction": "SELL", "reasoning": "HDFCBANK +3.45%, ICICIBANK +4.12% on Jun 3 — those gains and more get wiped."},
            "IT": {"direction": "HOLD", "reasoning": "INFY/TCS earnings are dollar-driven; domestic politics doesn't change their numbers."},
            "ENERGY_ONG": {"direction": "SELL", "reasoning": "RELIANCE +2.89% gives it back; but Jio/retail earnings insulate somewhat."}
        },
        "thesis": "From a bottom-up perspective, the election result doesn't change FY25 earnings overnight. Corporate India's balance sheets are strong, bank NIMs are healthy, IT order books are solid. But the market is repricing the MULTIPLE, not earnings — the 'India premium' PE of 22-23x compresses to 20-21x as 'reform certainty' becomes 'coalition uncertainty.' The heavyweights (RELIANCE +2.89%, HDFCBANK +3.45%) that rallied hardest will fall hardest. Index impact: ~3-5% from PE compression alone, before panic overshooting.",
        "key_factors": [
            "PE multiple compression from 22-23x to 20-21x on reduced reform premium",
            "HDFCBANK + ICICIBANK (~15% of Nifty) rallied 3-4%; will fall 6-8%",
            "RELIANCE (10%+ weight) exits reform premium but Jio/retail cushion somewhat",
            "Corporate earnings fundamentals unchanged — it's a valuation repricing"
        ],
        "risks": [
            "If the selloff is mostly multiple compression (not earnings downgrade), recovery is faster",
            "Strong Q1 results in July could re-anchor sentiment"
        ],
        "conviction": 4
    },

    "event_news": {
        "direction": "SELL",
        "confidence_pct": 98,
        "nifty_return": {
            "low_pct": -10.0,
            "base_pct": -6.5,
            "high_pct": -3.5
        },
        "sector_views": {
            "FINBK": {"direction": "SELL", "reasoning": "BankNifty will crash 8-10% — every election trade in financials reverses."},
            "IT": {"direction": "SELL", "reasoning": "Day-1 panic is indiscriminate — even IT sells 2-3%."},
            "ENERGY_ONG": {"direction": "SELL", "reasoning": "PSU energy stocks were the exit-poll darlings — they fall the most."}
        },
        "thesis": "This is the single biggest event surprise in Indian markets since the 2004 election (when BJP lost to UPA, triggering an 11% single-day crash). The magnitude of the surprise is enormous: exit polls predicted 350-370 NDA seats, actual is 292 (BJP 240). That's a 60-80 seat shortfall from expectations. Historical precedent is crystal clear: 2004 BJP loss → Nifty -11.1% on May 17 (circuit breaker hit). 2024 won't be as bad because (1) NDA still forms govt, (2) PM continuity, (3) economy is stronger. But expect -5% to -8% easily, with -10% intraday possible before any recovery. This is a front-page, breaking-news, panic-selling event.",
        "key_factors": [
            "60-80 seat shortfall from exit poll consensus — maximum surprise",
            "2004 precedent: BJP surprise loss → Nifty -11.1% (circuit breaker)",
            "Exit poll premium of 3.3% (Jun 3 rally) gets FULLY reversed plus overshoot",
            "Cascading F&O liquidation, FII unwinding, retail panic selling",
            "VIX spike to 28-35 amplifies all moves via dealer hedging"
        ],
        "risks": [
            "NDA at 292 still forms government — limits tail risk vs 2004",
            "Intraday recovery possible once govt formation is confirmed",
            "Circuit breaker at -10% would halt trading and allow cooling"
        ],
        "conviction": 5
    },
}


def run_election_replay() -> ConsensusResult:
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

    consensus = _aggregate(date(2024, 6, 3), decisions)
    return consensus


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("\n" + "=" * 60)
    print("SESSION 010 — ELECTION REPLAY: June 4, 2024")
    print("Claude Code acting as all 8 agents")
    print("=" * 60 + "\n")

    consensus = run_election_replay()
    print()
    evaluate(consensus)
