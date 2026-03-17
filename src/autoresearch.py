"""
Autoresearch loop — Karpathy-style autonomous parameter optimization.

Modifies prediction parameters → runs 5,009-day backtest → evaluates →
keeps if improved → repeats. Runs 100+ experiments overnight.

Each experiment takes ~1 second (pure compute, no LLM calls).
100 experiments = 100 seconds. 1000 experiments = 17 minutes.

Optimizes 28 parameters:
  - 5 signal weights (momentum, mean_revert, vix, trend, volatility)
  - 7 thresholds (momentum bull/bear, VIX fear/complacency, etc.)
  - 10 IMPACT_WEIGHTs (for swarm consensus)
  - 6 noise params (for swarm amplification)

Usage:
    python -m src.autoresearch                  # 100 experiments
    python -m src.autoresearch --n 1000         # 1000 experiments
    python -m src.autoresearch --n 500 --focus weights  # only optimize weights
"""

from __future__ import annotations

import copy
import json
import logging
import math
import os
import random
import time
from datetime import datetime
from typing import Any

from src.backtest_full import PARAMS, run_full_backtest, score_results

logger = logging.getLogger(__name__)

# Best known parameters (starting point)
BEST_PARAMS = copy.deepcopy(PARAMS)
BEST_SCORE = None

# Experiment log
EXPERIMENT_LOG: list[dict] = []


def _mutate_params(params: dict, temperature: float = 0.1) -> dict:
    """
    Randomly mutate parameters. Temperature controls mutation magnitude.

    Higher temperature = larger mutations = more exploration.
    Lower temperature = smaller mutations = more exploitation.
    """
    new_params = copy.deepcopy(params)

    # Pick 1-3 parameters to mutate
    keys = list(new_params.keys())
    n_mutations = random.randint(1, 3)
    mutate_keys = random.sample(keys, min(n_mutations, len(keys)))

    for key in mutate_keys:
        val = new_params[key]
        if isinstance(val, float):
            # Gaussian mutation
            noise = random.gauss(0, abs(val) * temperature + 0.01)
            new_val = val + noise

            # Clamp weights to [0, 1]
            if key.startswith("w_"):
                new_val = max(0.01, min(1.0, new_val))
            # Clamp thresholds to reasonable ranges
            elif "vix" in key:
                new_val = max(5.0, min(40.0, new_val))
            elif "mom" in key or "mean_revert" in key:
                new_val = max(0.1, min(5.0, abs(new_val)))
                if "bear" in key:
                    new_val = -new_val
            elif "vol" in key:
                new_val = max(5.0, min(40.0, new_val))

            new_params[key] = round(new_val, 4)

    # Normalize signal weights to sum to 1
    weight_keys = [k for k in new_params if k.startswith("w_")]
    weight_sum = sum(new_params[k] for k in weight_keys)
    if weight_sum > 0:
        for k in weight_keys:
            new_params[k] = round(new_params[k] / weight_sum, 4)

    return new_params


def _composite_score(scores: dict) -> float:
    """
    Compute a single composite score from backtest results.

    Higher = better. Combines direction accuracy and error.

    Weighting:
      - Direction accuracy (60%): the primary metric
      - Inverse avg error (20%): lower error = better
      - Big move accuracy (20%): getting big days right matters most
    """
    dir_pct = scores.get("direction_pct", 0)
    avg_err = scores.get("avg_error_pp", 10)
    big_dir = scores.get("big_move_direction_pct", 0)

    # Normalize: dir% 0-100, inv_error 0-100, big_dir 0-100
    inv_error = max(0, 100 - avg_err * 50)  # 0pp → 100, 2pp → 0

    return dir_pct * 0.6 + inv_error * 0.2 + big_dir * 0.2


def run_autoresearch(
    n_experiments: int = 100,
    temperature: float = 0.15,
    cooling: bool = True,
) -> dict:
    """
    Run the autoresearch optimization loop.

    Args:
        n_experiments: Number of experiments to run.
        temperature: Initial mutation magnitude (0.05-0.3).
        cooling: If True, reduce temperature over time (simulated annealing).

    Returns:
        Best parameters found and their score.
    """
    global BEST_PARAMS, BEST_SCORE

    # Establish baseline
    print("Establishing baseline...")
    baseline_results = run_full_backtest(PARAMS)
    baseline_scores = score_results(baseline_results)
    baseline_composite = _composite_score(baseline_scores)

    BEST_PARAMS = copy.deepcopy(PARAMS)
    BEST_SCORE = baseline_composite

    print(f"Baseline: dir={baseline_scores['direction_pct']}% err={baseline_scores['avg_error_pp']}pp "
          f"composite={baseline_composite:.2f}")
    print(f"\nRunning {n_experiments} experiments...\n")

    improvements = 0
    t0 = time.time()

    for i in range(n_experiments):
        # Adaptive temperature
        if cooling:
            t = temperature * (1 - i / n_experiments * 0.7)  # Cool to 30% of initial
        else:
            t = temperature

        # Mutate
        candidate = _mutate_params(BEST_PARAMS, temperature=t)

        # Evaluate
        results = run_full_backtest(candidate)
        scores = score_results(results)
        composite = _composite_score(scores)

        # Compare
        improved = composite > BEST_SCORE
        if improved:
            BEST_PARAMS = candidate
            BEST_SCORE = composite
            improvements += 1

        # Log
        entry = {
            "experiment": i + 1,
            "composite": round(composite, 2),
            "direction_pct": scores["direction_pct"],
            "avg_error": scores["avg_error_pp"],
            "big_move_dir": scores["big_move_direction_pct"],
            "improved": improved,
            "temperature": round(t, 4),
        }
        EXPERIMENT_LOG.append(entry)

        # Progress
        if (i + 1) % 10 == 0 or improved:
            elapsed = time.time() - t0
            marker = " ★ NEW BEST" if improved else ""
            print(f"  [{i+1:>4}/{n_experiments}] "
                  f"dir={scores['direction_pct']:>5.1f}% "
                  f"err={scores['avg_error_pp']:>5.2f}pp "
                  f"big={scores['big_move_direction_pct']:>5.1f}% "
                  f"comp={composite:>6.2f} "
                  f"({elapsed:.0f}s){marker}")

    elapsed = time.time() - t0
    rate = n_experiments / elapsed

    # Final best
    best_results = run_full_backtest(BEST_PARAMS)
    best_scores = score_results(best_results)

    print(f"\n{'=' * 65}")
    print(f"AUTORESEARCH COMPLETE — {n_experiments} experiments in {elapsed:.0f}s ({rate:.1f}/s)")
    print(f"{'=' * 65}")
    print(f"  Improvements: {improvements}")
    print(f"  Baseline:  dir={baseline_scores['direction_pct']}% err={baseline_scores['avg_error_pp']}pp")
    print(f"  Best:      dir={best_scores['direction_pct']}% err={best_scores['avg_error_pp']}pp")
    print(f"  Composite: {baseline_composite:.2f} → {BEST_SCORE:.2f} "
          f"({'+' if BEST_SCORE > baseline_composite else ''}{BEST_SCORE - baseline_composite:.2f})")

    print(f"\n  Optimized parameters:")
    for k, v in sorted(BEST_PARAMS.items()):
        orig = PARAMS.get(k)
        changed = " ← CHANGED" if orig != v else ""
        print(f"    {k:<25} {v:>8.4f}  (was {orig}){changed}")

    print(f"{'=' * 65}")

    return {
        "best_params": BEST_PARAMS,
        "best_scores": best_scores,
        "baseline_scores": baseline_scores,
        "improvements": improvements,
        "experiments": n_experiments,
        "elapsed_sec": round(elapsed, 1),
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=100)
    parser.add_argument("--temp", type=float, default=0.15)
    parser.add_argument("--no-cooling", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING)
    run_autoresearch(args.n, args.temp, not args.no_cooling)
