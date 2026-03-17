"""
Session 008 — Live day simulation: Mar 16 (T-1) → Mar 17 prediction.

Claude Code acts as all 8 agents against the actual Mar 16 market state.
No event — pure market regime / technical call.

Actual outcome: Nifty +1.00% (23408.80 → 23643.10).
"""

from __future__ import annotations

import json
import logging
from datetime import date

from src.agents.base import BaseAgent
from src.agents.personas import PERSONA_BY_ID
from src.agents.runner import _aggregate
from src.agents.schemas import AgentDecision, ConsensusResult, Direction, ReturnRange

logger = logging.getLogger(__name__)

ACTUAL_NIFTY_RETURN_PCT = +1.00
ACTUAL_NIFTY_CLOSE_MAR16 = 23408.80
ACTUAL_NIFTY_CLOSE_MAR17 = 23643.10

# ─── In-context agent responses ─────────────────────────────────────────────
# Context: Nifty -8.6% correction over 5 weeks (25867→23409), VIX 21.6 (elevated
# but off 23.36 peak), breadth 33% up, momentum_20d +1.7% (decaying), no event.

AGENT_RESPONSES: dict[str, dict] = {

    "fii_quant": {
        "direction": "SELL",
        "confidence_pct": 55,
        "nifty_return": {
            "low_pct": -2.0,
            "base_pct": -0.5,
            "high_pct": +0.8
        },
        "sector_views": {
            "FINBK": {"direction": "SELL", "reasoning": "Banks weak — BankNifty down 10% from peak, FII likely still net sellers."},
            "IT": {"direction": "HOLD", "reasoning": "IT has been defensive in this correction, INR weakness supports."},
            "ENERGY_ONG": {"direction": "HOLD", "reasoning": "MRPL +16% outlier, but upstream/downstream mixed."}
        },
        "thesis": "The correction is not over. Nifty has lost -8.6% in 5 weeks with VIX still at 21.6 — elevated fear. Breadth at 33% means the selling is broad-based, not just a few heavyweights. FIIs have been consistent sellers in this downdraft and without a clear catalyst to turn, expect continued choppiness. However, the pace of selling may slow — the easy shorts are done. Mildly bearish.",
        "key_factors": [
            "Nifty -8.6% in 5 weeks — sustained downtrend",
            "VIX 21.6 still elevated despite slight cooling from 23.36 peak",
            "Breadth weak at 33% — broad market participation in selloff",
            "Momentum_20d +1.7% but decaying rapidly, will turn negative soon"
        ],
        "risks": [
            "Technical bounce from oversold levels — Nifty near 200-DMA support",
            "DII buying on dips could create a sharp short-covering rally",
            "Any positive global catalyst (Fed dovish, China stimulus) reverses FII flow"
        ],
        "conviction": 2
    },

    "retail_momentum": {
        "direction": "SELL",
        "confidence_pct": 62,
        "nifty_return": {
            "low_pct": -2.5,
            "base_pct": -0.8,
            "high_pct": +0.5
        },
        "sector_views": {
            "FINBK": {"direction": "SELL", "reasoning": "BANDHANBNK -7.38% shows banking stress continuing."},
            "IT": {"direction": "HOLD", "reasoning": "No strong momentum signal either way."},
            "CONAUT": {"direction": "SELL", "reasoning": "Auto sector weak in risk-off; high beta to downside."}
        },
        "thesis": "The trend is your friend and the trend is DOWN. Five straight weeks of selling, momentum_1d at -1.0%, and breadth has been below 50% for weeks. The 23,400 level is barely holding. IDBI -16.58% and BANDHANBNK -7.38% show financial stress. I'd expect another weak session — maybe a gap-down that finds some support around 23,000. Not seeing the volume or breadth signals that precede a reversal.",
        "key_factors": [
            "Momentum negative across 1d timeframe",
            "Breadth ratio 0.50 — bearish territory",
            "23,400 holding by a thread — round number psychology",
            "Financial names showing stress (IDBI, BANDHANBNK)"
        ],
        "risks": [
            "Oversold bounce — after 5 weeks down, shorts get nervous",
            "Gap-up on positive Asian markets could trap late shorts"
        ],
        "conviction": 3
    },

    "dealer_hedging": {
        "direction": "HOLD",
        "confidence_pct": 50,
        "nifty_return": {
            "low_pct": -1.5,
            "base_pct": +0.2,
            "high_pct": +1.5
        },
        "sector_views": {
            "FINBK": {"direction": "HOLD", "reasoning": "Options positioning is neutral — no strong directional signal from put/call OI."},
            "IT": {"direction": "HOLD", "reasoning": "Low implied vol, not interesting from a derivatives perspective."}
        },
        "thesis": "VIX at 21.6 has cooled from the 23.36 peak — this means options premium is getting cheaper and dealer gamma exposure is normalizing. The sharp selloff created a lot of put OI at 23,000 and 22,500 — this acts as dealer hedging support (dealers bought puts = they go long futures to hedge near these strikes). I see a potential for a mild bounce or sideways action as the VIX compresses. No strong directional conviction without a fresh catalyst.",
        "key_factors": [
            "VIX cooling from 23.36 to 21.6 — vol sellers stepping in",
            "Put OI buildup at 23,000 creates dealer hedging floor",
            "No weekly/monthly expiry — reduces gamma-driven volatility",
            "Volatility_20d at 22.44% — historically elevated, mean-reversion due"
        ],
        "risks": [
            "Fresh negative catalyst could break through put support and trigger gamma cascade",
            "VIX could re-spike if global risk-off intensifies"
        ],
        "conviction": 2
    },

    "dii_mf": {
        "direction": "BUY",
        "confidence_pct": 60,
        "nifty_return": {
            "low_pct": -0.5,
            "base_pct": +0.5,
            "high_pct": +1.5
        },
        "sector_views": {
            "FINBK": {"direction": "BUY", "reasoning": "IDBI -16.58% is overdone; valuations compelling for SIP-driven accumulation."},
            "CONPFMCG": {"direction": "BUY", "reasoning": "Defensive allocation increases in risk-off; FMCG has relative strength."},
            "FINNBFC": {"direction": "HOLD", "reasoning": "NBFCs need credit quality data before getting constructive."}
        },
        "thesis": "This is exactly the kind of selloff where DII flows provide a floor. Nifty PE has compressed from ~22x to ~19x in 5 weeks — that's approaching the attractive zone. SIP inflows are steady at ₹18,000+ cr/month regardless of market direction. After -8.6%, the risk-reward for fresh deployment has improved significantly. Our investment committee would be incrementally adding here. Expecting a stabilization/mild bounce.",
        "key_factors": [
            "Nifty PE compression to ~19x from 22x — valuation support",
            "SIP flows provide ₹18,000+ cr/month structural bid",
            "After -8.6% correction, absolute downside is limited",
            "Historical precedent: 8-10% corrections in Nifty typically find DII floors"
        ],
        "risks": [
            "If this is the start of a bear market (not just a correction), floor is lower",
            "Retail MF redemptions could spike if Nifty breaks 22,500 psychologically",
            "Earnings downgrades in Q4 could justify lower valuations"
        ],
        "conviction": 3
    },

    "macro": {
        "direction": "HOLD",
        "confidence_pct": 45,
        "nifty_return": {
            "low_pct": -1.5,
            "base_pct": +0.0,
            "high_pct": +1.2
        },
        "sector_views": {
            "FINBK": {"direction": "HOLD", "reasoning": "No new rate signal; banks trading on technical flows, not fundamentals."},
            "ENERGY_ONG": {"direction": "HOLD", "reasoning": "Crude and INR data not available — can't make a macro call on energy."},
            "IT": {"direction": "HOLD", "reasoning": "Without USDINR data, hard to assess FX tailwind for IT."},
            "CONAUT": {"direction": "HOLD", "reasoning": "Auto demand data not fresh enough to have conviction."}
        },
        "thesis": "I lack key macro inputs — repo rate, USDINR, crude price, and FII/DII flows are all missing from this snapshot. Without these, I cannot construct a proper macro thesis. The index-level correction of -8.6% suggests something macro is driving this (possibly global risk-off, Fed/tariff fears), but without data I have to stay neutral. The VIX at 21.6 tells me uncertainty is high but not extreme.",
        "key_factors": [
            "Macro data missing: no repo rate, USDINR, crude, or flow data",
            "VIX at 21.6 suggests moderate uncertainty",
            "8.6% correction in 5 weeks is significant but not crisis-level",
            "Without FII flow data, cannot gauge foreign capital direction"
        ],
        "risks": [
            "Could be missing a major macro catalyst driving the correction",
            "Insufficient data means high model uncertainty"
        ],
        "conviction": 1
    },

    "sector_rotation": {
        "direction": "HOLD",
        "confidence_pct": 55,
        "nifty_return": {
            "low_pct": -1.0,
            "base_pct": +0.3,
            "high_pct": +1.2
        },
        "sector_views": {
            "FINBK": {"direction": "SELL", "reasoning": "IDBI -16.58% and BANDHANBNK -7.38% signal ongoing financial stress."},
            "ENERGY_ONG": {"direction": "BUY", "reasoning": "MRPL +16.07% and CHENNPETRO +7.66% show energy outperforming — rotation into crude plays."},
            "IT": {"direction": "BUY", "reasoning": "IT defensive in downtrend; classic rotation target when VIX is elevated."},
            "CONPFMCG": {"direction": "BUY", "reasoning": "Staples outperform in risk-off regimes."}
        },
        "thesis": "The sector rotation signals are mixed but interesting. Energy stocks (MRPL, CHENNPETRO) are bucking the downtrend with strong gains — possible rotation into crude-linked names. Meanwhile IDBI (-16.58%) and BANDHANBNK (-7.38%) show financials still under pressure. The classic risk-off rotation (out of banks, into IT/pharma/FMCG) appears to be playing out. At the index level this is roughly neutral — sector winners offset losers. Slight positive tilt as energy momentum is notable.",
        "key_factors": [
            "Energy outperformance: MRPL +16%, CHENNPETRO +7.7% — notable rotation",
            "Financials underperforming: IDBI -16.6%, BANDHANBNK -7.4%",
            "Classic risk-off rotation in progress (banks → defensives)",
            "Breadth at 33% but pockets of strength in energy/defensives"
        ],
        "risks": [
            "Energy outperformance could be one-day anomaly (results/news-driven)",
            "Broad selloff could overwhelm sector rotation if risk-off deepens"
        ],
        "conviction": 2
    },

    "corp_earnings": {
        "direction": "HOLD",
        "confidence_pct": 48,
        "nifty_return": {
            "low_pct": -1.0,
            "base_pct": +0.2,
            "high_pct": +1.0
        },
        "sector_views": {
            "FINBK": {"direction": "SELL", "reasoning": "IDBI -16.58% is massive; suggests specific negative news, potential contagion risk."},
            "ENERGY_ONG": {"direction": "BUY", "reasoning": "MRPL +16% is an earnings/GRM signal — refining margins may be improving."},
            "IT": {"direction": "HOLD", "reasoning": "No fresh earnings data to drive a view."}
        },
        "thesis": "The top movers are stock-specific, not index-driving. BAJEL +20% and MRPL +16% are mid/small-cap stories, while IDBI -16.58% is likely a specific event (government divestment news? asset quality issue?). None of these are Nifty 50 heavyweights with meaningful index weight. Without seeing what RELIANCE, HDFCBANK, INFY, TCS are doing, I can't make a strong bottom-up index call. The -8.6% correction has compressed earnings multiples but no Q4 data yet to tell if earnings are also declining.",
        "key_factors": [
            "Top movers are mid-cap stories — low Nifty 50 weight impact",
            "IDBI -16.58% raises banking asset quality questions",
            "Earnings multiples compressed after correction — supportive if earnings hold",
            "No fresh heavyweight earnings data to drive conviction"
        ],
        "risks": [
            "Q4 earnings guidance could disappoint in this macro environment",
            "Heavyweight stocks could gap on global cues"
        ],
        "conviction": 2
    },

    "event_news": {
        "direction": "HOLD",
        "confidence_pct": 30,
        "nifty_return": {
            "low_pct": -1.0,
            "base_pct": +0.0,
            "high_pct": +1.0
        },
        "sector_views": {
            "FINBK": {"direction": "HOLD", "reasoning": "No policy event — banking moves are technical/flow-driven."},
            "IT": {"direction": "HOLD", "reasoning": "No global tech event or FX trigger."},
            "ENERGY_ONG": {"direction": "HOLD", "reasoning": "No OPEC or crude-specific catalyst."}
        },
        "thesis": "No event = no edge for me. I'm the event/news trader and there is no event in this snapshot. The -8.6% correction could be driven by global factors (tariffs, Fed, geopolitics) but without specific event context I cannot assess the next catalyst. I'm the least useful agent in this scenario and should honestly sit this one out. Neutral by default.",
        "key_factors": [
            "No specific event or policy catalyst identified",
            "The correction itself could be the event but I need specifics",
            "IDBI -16.58% could be event-driven but not enough info"
        ],
        "risks": [
            "A surprise event overnight could move markets significantly",
            "My lack of conviction is itself a risk — missing a setup"
        ],
        "conviction": 1
    },
}


def run_live_simulation() -> ConsensusResult:
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

    consensus = _aggregate(date(2026, 3, 16), decisions)
    return consensus


def evaluate(consensus: ConsensusResult) -> dict:
    """Compare consensus to actual Mar 17 outcome."""
    predicted = consensus.avg_return_pct
    error = abs(predicted - ACTUAL_NIFTY_RETURN_PCT)
    direction_correct = (
        (consensus.consensus_direction == Direction.BUY and ACTUAL_NIFTY_RETURN_PCT > 0)
        or (consensus.consensus_direction == Direction.SELL and ACTUAL_NIFTY_RETURN_PCT < 0)
        or (consensus.consensus_direction == Direction.HOLD and abs(ACTUAL_NIFTY_RETURN_PCT) < 0.5)
    )

    print("\n" + "=" * 60)
    print("LIVE EVALUATION: Mar 16 → Mar 17, 2026")
    print("=" * 60)
    print(f"Actual Nifty return:    {ACTUAL_NIFTY_RETURN_PCT:+.2f}%")
    print(f"Predicted (consensus):  {predicted:+.2f}%")
    print(f"Prediction error:       {error:.2f}pp")
    print(f"Direction correct:      {'YES' if direction_correct else 'NO'}")
    print(f"Consensus direction:    {consensus.consensus_direction.value}")
    print(f"Votes: Bull={consensus.bull_count} Bear={consensus.bear_count} Neutral={consensus.neutral_count}")
    print(f"Return range: [{consensus.return_range.low_pct:+.2f}%, {consensus.return_range.high_pct:+.2f}%]")
    print("=" * 60)

    for dec in consensus.decisions:
        print(f"  {dec.agent_id:20s} | {dec.direction.value:4s} | "
              f"conv={dec.conviction} | base={dec.nifty_return.base_pct:+.2f}%")

    print("=" * 60 + "\n")

    return {
        "actual_return_pct": ACTUAL_NIFTY_RETURN_PCT,
        "predicted_return_pct": predicted,
        "error_pct": round(error, 4),
        "direction_correct": direction_correct,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("\n" + "=" * 60)
    print("SESSION 008 — LIVE SIMULATION: Mar 16 → Mar 17, 2026")
    print("Claude Code acting as all 8 agents (no event)")
    print("=" * 60 + "\n")

    consensus = run_live_simulation()
    print()
    evaluate(consensus)
