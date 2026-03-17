"""
Session 007 — In-context replay of RBI May 2022 hike.

Claude Code acts as all 8 agents. Responses are hard-coded from in-context
generation, then parsed through the standard framework and evaluated.
"""

from __future__ import annotations

import json
import logging
from datetime import date

from src.agents.base import BaseAgent
from src.agents.personas import ALL_PERSONAS, PERSONA_BY_ID
from src.agents.runner import _aggregate
from src.agents.schemas import AgentDecision, ConsensusResult
from src.replay.cases.rbi_hike_may2022 import (
    ACTUAL_NIFTY_RETURN_PCT,
    EVENT,
    MARKET_STATE,
    evaluate,
)

logger = logging.getLogger(__name__)

# ─── In-context agent responses (Claude Code acting as each persona) ────────

AGENT_RESPONSES: dict[str, dict] = {

    "fii_quant": {
        "direction": "SELL",
        "confidence_pct": 85,
        "nifty_return": {
            "low_pct": -3.5,
            "base_pct": -2.0,
            "high_pct": -0.8
        },
        "sector_views": {
            "FINBK": {"direction": "SELL", "reasoning": "Banks are ground zero for rate hikes — NIM repricing takes quarters but bond MTM losses hit immediately."},
            "IT": {"direction": "BUY", "reasoning": "Defensive USD earners benefit from flight-to-quality; INR likely to weaken post-hike."},
            "ENERGY_ONG": {"direction": "SELL", "reasoning": "Risk-off plus crude already at $103 squeezes refining margins; FII positions in ONGC/RIL will see selling."}
        },
        "thesis": "An unscheduled 40bps rate hike is a tier-1 shock for Indian equities. FIIs were already net sellers (-₹2,222cr T-1) and the surprise hike + 50bps CRR increase signals RBI panic on inflation. This will trigger aggressive FII selling in cash and derivatives, with INR likely to weaken past 76.50 as the rate differential advantage shrinks vs US. Nifty should test the 16,700-16,800 zone.",
        "key_factors": [
            "Surprise off-cycle hike — not priced in by market",
            "FII already net sellers; hike accelerates outflows",
            "CRR hike of 50bps tightens liquidity materially",
            "VIX at 20.28 already elevated — will spike further",
            "Breadth abysmal (12 adv / 35 dec) — no cushion"
        ],
        "risks": [
            "DII SIP flows could provide a floor below 16,500",
            "Market may interpret proactive hike as credibility-positive for RBI medium-term",
            "Short covering rally if initial selloff is overdone"
        ],
        "conviction": 5
    },

    "retail_momentum": {
        "direction": "SELL",
        "confidence_pct": 78,
        "nifty_return": {
            "low_pct": -3.0,
            "base_pct": -1.8,
            "high_pct": -0.5
        },
        "sector_views": {
            "FINBK": {"direction": "SELL", "reasoning": "Banks were already weak (AXISBANK -1.67%, ICICI -1.45%); rate hike accelerates the downtrend."},
            "IT": {"direction": "HOLD", "reasoning": "IT was the only sector showing some green; could be a hiding spot."},
            "CONAUT": {"direction": "SELL", "reasoning": "Auto loan EMIs go up — Maruti already down 0.89%, this pushes it lower."}
        },
        "thesis": "The chart was already broken — Nifty 50 had terrible breadth (only 12 out of 50 advancing, momentum_20d at -3.47%). Now an RBI surprise hike is like throwing a rock at a falling knife. The 17,000 level, which looked like support, will be sliced through. Retail panic selling will kick in once 17,000 breaks. Targeting 16,700-16,800 on the downside with VIX spiking above 22.",
        "key_factors": [
            "Momentum already deeply negative across 1d/5d/20d",
            "Breadth ratio 0.34 — extremely bearish",
            "17,000 round number support likely to break on this shock",
            "VIX already at 20.28 — fear was building even before this"
        ],
        "risks": [
            "A mid-session recovery bounce from 16,800 could trap late shorts",
            "Heavy DII buying could create an intraday reversal"
        ],
        "conviction": 4
    },

    "dealer_hedging": {
        "direction": "SELL",
        "confidence_pct": 90,
        "nifty_return": {
            "low_pct": -3.5,
            "base_pct": -2.2,
            "high_pct": -1.0
        },
        "sector_views": {
            "FINBK": {"direction": "SELL", "reasoning": "Bank options will see massive put buying; dealers short gamma will amplify the selloff via delta hedging."},
            "IT": {"direction": "HOLD", "reasoning": "Relatively low beta to rate events; put skew will steepen less here."}
        },
        "thesis": "This is a textbook gamma squeeze scenario. VIX at 20.28 means dealers are already short gamma — the surprise hike will trigger a VIX spike to 24-26 range, forcing massive delta hedging (selling futures) that amplifies the cash market decline. The off-cycle nature of this MPC meeting means the options market had ZERO probability priced for a May hike. Implied vol repricing alone drives Nifty down 1.5-2%. Add the fundamental impact and we're looking at -2% to -3% easily. Put OI will explode at 17,000 and 16,500 strikes.",
        "key_factors": [
            "VIX spike from 20 to 24-26 forces dealer gamma hedging (selling)",
            "Zero probability of off-cycle hike was priced into options",
            "CRR hike compounds liquidity removal — less cash to deploy",
            "No weekly/monthly expiry today — no pin risk to limit downside",
            "Historical precedent: surprise RBI moves = 1.5-3% Nifty selloff"
        ],
        "risks": [
            "If VIX spikes above 26, vol selling by institutional desks could cap the move",
            "RBI may announce other supportive measures to soften the blow"
        ],
        "conviction": 5
    },

    "dii_mf": {
        "direction": "SELL",
        "confidence_pct": 55,
        "nifty_return": {
            "low_pct": -2.5,
            "base_pct": -1.2,
            "high_pct": 0.0
        },
        "sector_views": {
            "FINBK": {"direction": "SELL", "reasoning": "Short-term negative for banks as bond yields spike; but NIMs improve over 2-3 quarters."},
            "CONPFMCG": {"direction": "HOLD", "reasoning": "Defensive sector — FMCG names like HUL will see relative outperformance as a safe haven."},
            "FINNBFC": {"direction": "SELL", "reasoning": "NBFCs hit hardest by CRR and rate hikes — cost of funds rises immediately, can't reprice loans as fast."}
        },
        "thesis": "The rate hike is negative short-term, no question. But the DII floor is real — SIP flows of ₹14,000 cr/month (May 2022) plus DII net buying at +₹1,444 cr yesterday means there's a structural bid below 16,500. We'll see a 1-2% decline, but not a rout. Our investment committee would view this as an opportunity to deploy cash over the next 2-3 sessions. Nifty PE at ~20x is not cheap but not extreme either.",
        "key_factors": [
            "DII were already net buyers (+₹1,444 cr) — will step up on dips",
            "SIP flows provide monthly structural support",
            "Rate hike is negative but signals RBI credibility on inflation",
            "Nifty PE around 20x — not panic territory"
        ],
        "risks": [
            "If FII selling intensifies to -₹5,000 cr+, DII flows may not be enough",
            "Retail MF redemptions could spike if Nifty breaks 16,500 psychologically",
            "Further surprise hikes could change the medium-term narrative"
        ],
        "conviction": 3
    },

    "macro": {
        "direction": "SELL",
        "confidence_pct": 92,
        "nifty_return": {
            "low_pct": -3.5,
            "base_pct": -2.3,
            "high_pct": -1.0
        },
        "sector_views": {
            "FINBK": {"direction": "SELL", "reasoning": "Direct rate transmission — bond yields spike 15-20bps, banking index falls 3-4%."},
            "ENERGY_ONG": {"direction": "SELL", "reasoning": "Crude at $103 already painful; tighter monetary policy won't help demand destruction fears."},
            "CONAUT": {"direction": "SELL", "reasoning": "Auto loan rates will rise, dampening demand in a sector already dealing with chip shortages."},
            "IT": {"direction": "BUY", "reasoning": "Rate-insensitive; INR depreciation from rate shock is actually positive for IT earnings."}
        },
        "thesis": "This is the most significant macro event for Indian equities since the demonetization shock. An off-cycle, unanimous 40bps hike + 50bps CRR increase is the RBI telling the market: inflation is out of control and we will tighten aggressively. The combo of repo + CRR effectively removes ₹87,000 cr of liquidity from the system. Rate-sensitive sectors (banks, NBFCs, autos) will be hit hardest. The 10Y G-sec yield will spike 15-25bps, creating mark-to-market losses across bank treasuries. This is the start of a tightening cycle, not a one-off.",
        "key_factors": [
            "40bps repo hike + 50bps CRR = double tightening blow",
            "Off-cycle and unanimous — signals urgency on inflation",
            "CPI above 6% upper band for 3 straight months forced RBI's hand",
            "Crude at $103 + INR at 76.39 = imported inflation not going away",
            "First hike since Aug 2018 — marks end of accommodative era"
        ],
        "risks": [
            "If market interprets this as front-loading (done in 1-2 hikes), medium-term positive",
            "Global risk-on (Fed less hawkish than expected) could offset the domestic shock"
        ],
        "conviction": 5
    },

    "sector_rotation": {
        "direction": "SELL",
        "confidence_pct": 80,
        "nifty_return": {
            "low_pct": -3.0,
            "base_pct": -1.8,
            "high_pct": -0.5
        },
        "sector_views": {
            "FINBK": {"direction": "SELL", "reasoning": "Most rate-sensitive sector; BankNifty should underperform Nifty by 1-2% on this event."},
            "FINNBFC": {"direction": "SELL", "reasoning": "NBFCs with shorter ALM get squeezed fastest on rate hikes."},
            "IT": {"direction": "BUY", "reasoning": "Classic rate-hike rotation — move into USD earners and rate-insensitive names."},
            "CONPFMCG": {"direction": "BUY", "reasoning": "Defensive rotation target; FMCG outperforms in risk-off + rate hike scenarios."},
            "CONPPHARM": {"direction": "BUY", "reasoning": "Pharma is rate-insensitive and benefits from INR weakness (USD exports)."},
            "CONAUT": {"direction": "SELL", "reasoning": "Loan rate increase hits 2-wheeler and entry car segment demand directly."}
        },
        "thesis": "The sector sensitivity matrix is crystal clear here: rate hike → sell rate-sensitives (banks, NBFCs, autos), buy defensives (IT, pharma, FMCG). BankNifty should underperform Nifty by 1.5-2% on this event. The rotation trade is: long IT/pharma, short banks/NBFCs. At the index level, Nifty is ~35% financials-weighted, so the sectoral drag pulls the index down 1.5-2%.",
        "key_factors": [
            "Nifty ~35% weight in rate-sensitive financials = direct index drag",
            "IT/pharma defensives have low rate sensitivity per the matrix",
            "INR depreciation post-hike is a tailwind for USD earners",
            "BAJFINANCE already -2.10% T-1 — momentum compounds on NBFC selling"
        ],
        "risks": [
            "Defensive sectors could sell off too in a broad risk-off panic",
            "Rotation may take 2-3 sessions to play out, not all on day 1"
        ],
        "conviction": 4
    },

    "corp_earnings": {
        "direction": "SELL",
        "confidence_pct": 65,
        "nifty_return": {
            "low_pct": -2.5,
            "base_pct": -1.5,
            "high_pct": -0.3
        },
        "sector_views": {
            "FINBK": {"direction": "SELL", "reasoning": "Treasury losses on bond MTM; but NIM expansion over 2Q is positive — net short-term negative."},
            "IT": {"direction": "HOLD", "reasoning": "No direct earnings impact from rate hike; INFY/TCS guidance unchanged."},
            "ENERGY_ONG": {"direction": "HOLD", "reasoning": "RELIANCE earnings diversified enough; upstream benefits from crude but downstream gets squeezed."}
        },
        "thesis": "From a bottom-up perspective, the rate hike doesn't change FY23 earnings estimates dramatically — it's a 40bps move, not a regime change yet. However, the heavyweights tell the story: HDFCBANK (-0.87% T-1), ICICIBANK (-1.45%), RELIANCE (-1.23%) were already under pressure. The surprise hike will accelerate selling in these names. HDFCBANK + ICICIBANK alone are ~15% of Nifty weight — a 3-4% decline in these two drags Nifty ~50 points. I'm bearish but with lower conviction than the macro guys because earnings fundamentals don't change overnight.",
        "key_factors": [
            "HDFCBANK + ICICIBANK = ~15% Nifty weight, will sell off 3-4% on rate hike",
            "RELIANCE (-1.23% T-1) has 10%+ Nifty weight — any further weakness is material",
            "Bond MTM losses for bank treasuries reduce Q1 earnings",
            "Earnings season commentary will shift to caution on loan growth"
        ],
        "risks": [
            "If RELIANCE holds up (diversified earnings), index drop is capped",
            "Strong IT earnings could partially offset banking weakness",
            "Rate hike actually improves bank NIMs over 2-3 quarters"
        ],
        "conviction": 3
    },

    "event_news": {
        "direction": "SELL",
        "confidence_pct": 95,
        "nifty_return": {
            "low_pct": -4.0,
            "base_pct": -2.5,
            "high_pct": -1.2
        },
        "sector_views": {
            "FINBK": {"direction": "SELL", "reasoning": "BankNifty is the first domino in any rate event — expect 3-5% intraday crash."},
            "IT": {"direction": "BUY", "reasoning": "Safe haven flow + INR depreciation benefit; Nifty IT index could actually close green."},
            "ENERGY_ONG": {"direction": "SELL", "reasoning": "Risk-off selling hits all high-beta names; crude sensitivity compounds the pain."}
        },
        "thesis": "This is the biggest event trade of 2022 so far. An UNSCHEDULED off-cycle MPC meeting with a UNANIMOUS 40bps hike is a 5-sigma surprise. Historical precedent: the last off-cycle RBI action (2013 taper tantrum response) saw Nifty fall 4% over 2 sessions. The combination of repo hike + CRR hike + SDF introduction is a triple tightening. Markets HATE uncertainty, and an off-cycle action screams 'the situation is worse than we told you.' Expect panic selling in the first 30 minutes, with Nifty testing 16,600 before any bounce.",
        "key_factors": [
            "Off-cycle MPC = maximum surprise factor (not priced into any model)",
            "Triple tightening: repo + CRR + SDF introduction simultaneously",
            "Historical precedent: surprise RBI actions → 2-4% Nifty selloff",
            "Unanimous vote = all 6 MPC members alarmed by inflation",
            "First rate hike since Aug 2018 — market muscle memory for rate cuts"
        ],
        "risks": [
            "Overshooting: my base case may be too aggressive if DII buying is strong",
            "Market may rally in the last hour if the initial selloff is seen as overdone"
        ],
        "conviction": 5
    },
}


def run_incontext_replay() -> ConsensusResult:
    """
    Parse in-context agent responses and aggregate.
    """
    decisions: list[AgentDecision] = []

    for agent_id, response_data in AGENT_RESPONSES.items():
        persona = PERSONA_BY_ID[agent_id]
        agent = BaseAgent(persona)

        # Inject agent metadata (normally done by _parse_response)
        response_data["agent_id"] = agent_id
        response_data["agent_role"] = persona.role
        response_data["raw_response"] = json.dumps(response_data)

        decision = AgentDecision(**response_data)
        decisions.append(decision)
        print(f"  Parsed: {agent_id:20s} | {decision.direction.value:4s} | "
              f"conv={decision.conviction} | base={decision.nifty_return.base_pct:+.2f}%")

    consensus = _aggregate(date(2022, 5, 2), decisions)
    return consensus


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("\n" + "=" * 60)
    print("SESSION 007 — IN-CONTEXT REPLAY: RBI Hike May 2022")
    print("Claude Code acting as all 8 agents")
    print("=" * 60 + "\n")

    consensus = run_incontext_replay()

    print()
    evaluate(consensus)
