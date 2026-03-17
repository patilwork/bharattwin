"""
JSON output format specification injected into agent prompts.
"""

OUTPUT_JSON_SCHEMA = """\
You MUST respond with a single JSON object (no markdown fences, no commentary outside the JSON). Schema:

{
  "direction": "BUY" | "HOLD" | "SELL",
  "confidence_pct": <float 0-100>,
  "nifty_return": {
    "low_pct": <float, bear-case next-day Nifty return %>,
    "base_pct": <float, base-case next-day Nifty return %>,
    "high_pct": <float, bull-case next-day Nifty return %>
  },
  "sector_views": {
    "<SECTOR_CODE>": {
      "direction": "BUY" | "HOLD" | "SELL",
      "reasoning": "<1 sentence>"
    }
  },
  "thesis": "<2-4 sentence investment thesis>",
  "key_factors": ["<factor 1>", "<factor 2>", ...],
  "risks": ["<risk 1>", "<risk 2>", ...],
  "conviction": <int 1-5, where 5 = highest conviction>
}

Rules:
- All return percentages are next-trading-day Nifty 50 returns.
- low_pct < base_pct < high_pct.
- sector_views should cover at least 3 sectors most relevant to your analysis.
- key_factors: list the 3-5 most important inputs to your decision.
- risks: list 2-3 things that could invalidate your thesis.
- conviction 1 = very low confidence, 5 = would bet the farm.
"""
