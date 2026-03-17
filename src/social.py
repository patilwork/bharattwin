"""
Social media content generator for BharatTwin predictions.

Generates tweet-ready text for daily predictions and scoreboard updates.

Usage:
    python -m src.social                    # generate today's content
    python -m src.social --score 2026-03-16 # generate score post
"""

from __future__ import annotations

import io
import contextlib
from datetime import date, timedelta


def generate_prediction_post(consensus_direction: str, avg_return: float,
                              conviction_avg: float, bull: int, bear: int,
                              neutral: int, nifty_close: float,
                              range_low: float, range_high: float,
                              event_headline: str | None = None) -> str:
    """Generate a tweet-ready prediction post."""

    # Direction emoji
    if consensus_direction == "BUY":
        dir_emoji = "🟢"
        dir_word = "BULLISH"
    elif consensus_direction == "SELL":
        dir_emoji = "🔴"
        dir_word = "BEARISH"
    else:
        dir_emoji = "🟡"
        dir_word = "NEUTRAL"

    # Conviction bar
    conv_filled = round(conviction_avg)
    conv_bar = "█" * conv_filled + "░" * (5 - conv_filled)

    lines = [
        f"{dir_emoji} BharatTwin Daily Prediction",
        f"",
        f"8 AI agents analyzed today's market:",
        f"",
        f"Direction: {dir_word} ({avg_return:+.1f}%)",
        f"Range: [{range_low:+.1f}%, {range_high:+.1f}%]",
        f"Conviction: [{conv_bar}] {conviction_avg:.1f}/5",
        f"Votes: {bull}🟢 {bear}🔴 {neutral}⚪",
        f"",
        f"Nifty close: {nifty_close:,.0f}",
    ]

    if event_headline:
        lines.append(f"")
        lines.append(f"Event: {event_headline[:80]}")

    lines.extend([
        f"",
        f"Track record: bharattwin.com/scoreboard",
        f"#BharatTwin #Nifty50 #IndianMarkets #AI",
    ])

    return "\n".join(lines)


def generate_score_post(prediction_date: str, actual_pct: float,
                         predicted_pct: float, direction_correct: bool,
                         running_accuracy: float, total_cases: int) -> str:
    """Generate a tweet-ready score post (day after prediction)."""

    error = abs(actual_pct - predicted_pct)
    check = "✅" if direction_correct else "❌"

    if actual_pct > 0:
        actual_emoji = "📈"
    elif actual_pct < 0:
        actual_emoji = "📉"
    else:
        actual_emoji = "➡️"

    lines = [
        f"📊 BharatTwin Score Card — {prediction_date}",
        f"",
        f"Prediction: {predicted_pct:+.2f}%",
        f"Actual:     {actual_pct:+.2f}% {actual_emoji}",
        f"Error:      {error:.2f}pp",
        f"Direction:  {check}",
        f"",
        f"Running record: {running_accuracy:.0f}% direction accuracy ({total_cases} cases)",
        f"",
        f"Full scoreboard: bharattwin.com/scoreboard",
        f"#BharatTwin #Nifty50 #AIpredictions",
    ]

    return "\n".join(lines)


def generate_agent_spotlight(agent_id: str, agent_role: str,
                              direction: str, conviction: int,
                              thesis: str, avg_error: float) -> str:
    """Generate a character-driven agent spotlight post."""

    # Agent nicknames for personality
    nicknames = {
        "fii_quant": "FII Quant Sahab",
        "retail_momentum": "Retail Bhai",
        "dealer_hedging": "Dealer Sahab",
        "dii_mf": "DII Uncle",
        "macro": "Macro Master",
        "sector_rotation": "Sector Guru",
        "corp_earnings": "Earnings Analyst",
        "event_news": "Breaking News Trader",
    }

    emojis = {
        "fii_quant": "🏦",
        "retail_momentum": "📱",
        "dealer_hedging": "🎰",
        "dii_mf": "🏛️",
        "macro": "🌍",
        "sector_rotation": "🔄",
        "corp_earnings": "📊",
        "event_news": "⚡",
    }

    nickname = nicknames.get(agent_id, agent_role)
    emoji = emojis.get(agent_id, "🤖")
    dir_emoji = "🟢" if direction == "BUY" else ("🔴" if direction == "SELL" else "🟡")

    lines = [
        f"{emoji} Agent Spotlight: {nickname}",
        f"",
        f"{dir_emoji} {direction} | Conviction: {'█' * conviction}{'░' * (5-conviction)} {conviction}/5",
        f"",
        f'"{thesis[:200]}"',
        f"",
        f"Track record: {avg_error:.2f}pp avg error",
        f"",
        f"#BharatTwin #AIagents #IndianMarkets",
    ]

    return "\n".join(lines)


def generate_weekly_recap(cases: list[dict]) -> str:
    """Generate a weekly recap post."""
    if not cases:
        return ""

    correct = sum(1 for c in cases if c.get("direction_correct"))
    avg_err = sum(c.get("error_pp", 0) for c in cases) / len(cases)

    lines = [
        f"📅 BharatTwin Weekly Recap",
        f"",
        f"Direction: {correct}/{len(cases)} correct",
        f"Avg error: {avg_err:.2f}pp",
        f"",
    ]

    for c in cases:
        check = "✅" if c.get("direction_correct") else "❌"
        lines.append(f"{check} {c['date']}: pred={c['predicted_pct']:+.1f}% actual={c['actual_pct']:+.1f}%")

    lines.extend([
        f"",
        f"Full history: bharattwin.com/scoreboard",
        f"#BharatTwin #WeeklyRecap #Nifty50",
    ])

    return "\n".join(lines)


if __name__ == "__main__":
    # Demo: generate sample posts
    print("=" * 50)
    print("SAMPLE PREDICTION POST")
    print("=" * 50)
    print(generate_prediction_post(
        consensus_direction="SELL",
        avg_return=-1.99,
        conviction_avg=4.2,
        bull=0, bear=8, neutral=0,
        nifty_close=17069,
        range_low=-3.28, range_high=-0.73,
        event_headline="RBI surprises with 40bps repo rate hike to 4.40%",
    ))

    print()
    print("=" * 50)
    print("SAMPLE SCORE POST")
    print("=" * 50)
    print(generate_score_post(
        prediction_date="2022-05-04",
        actual_pct=-2.29,
        predicted_pct=-1.99,
        direction_correct=True,
        running_accuracy=100,
        total_cases=4,
    ))

    print()
    print("=" * 50)
    print("SAMPLE AGENT SPOTLIGHT")
    print("=" * 50)
    print(generate_agent_spotlight(
        agent_id="dealer_hedging",
        agent_role="Options Dealer / Market Maker",
        direction="SELL",
        conviction=5,
        thesis="VIX at 20.28 means dealers are already short gamma — the surprise hike will trigger a VIX spike to 24-26, forcing massive delta hedging that amplifies the cash market decline.",
        avg_error=0.43,
    ))
