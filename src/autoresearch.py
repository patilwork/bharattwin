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
            noise = random.gauss(0, abs(val) * temperature + 0.01)
            new_val = val + noise

            # Clamp by parameter type
            if key.startswith("w_"):
                new_val = max(0.01, min(1.0, new_val))
            elif key.startswith("dir_"):
                # Direction thresholds: small range around 0
                new_val = max(0.02, min(0.5, abs(new_val)))
                if "sell" in key:
                    new_val = -new_val
            elif "vix" in key and "w_" not in key:
                new_val = max(2.0, min(50.0, new_val))
            elif "mom" in key and "w_" not in key:
                new_val = max(0.1, min(10.0, abs(new_val)))
                if "bear" in key:
                    new_val = -new_val
            elif "vol" in key and "w_" not in key:
                new_val = max(2.0, min(40.0, new_val))
            elif "scale" in key:
                new_val = max(0.05, min(1.0, new_val))
            elif "return" in key:
                new_val = max(-2.0, min(2.0, new_val))
            elif "breadth" in key and "w_" not in key:
                new_val = max(10.0, min(90.0, new_val))
            elif "bounce" in key or "reversal" in key or "threshold" in key:
                new_val = max(0.5, min(8.0, abs(new_val)))
            elif "penalty" in key:
                new_val = max(0.1, min(1.0, new_val))

            new_params[key] = round(new_val, 4)

    # Normalize the 5 core signal weights to sum to ~0.80
    # (leaving ~0.20 for breadth, trend_20d, vix_change)
    core_weight_keys = ["w_momentum", "w_mean_revert", "w_vix_regime", "w_trend", "w_volatility"]
    core_sum = sum(new_params.get(k, 0) for k in core_weight_keys)
    if core_sum > 0:
        target = 0.80
        for k in core_weight_keys:
            if k in new_params:
                new_params[k] = round(new_params[k] / core_sum * target, 4)

    # Normalize the 3 auxiliary weights to sum to ~0.20
    aux_weight_keys = ["w_breadth", "w_trend_20d", "w_vix_change"]
    aux_sum = sum(new_params.get(k, 0) for k in aux_weight_keys)
    if aux_sum > 0:
        target = 0.20
        for k in aux_weight_keys:
            if k in new_params:
                new_params[k] = round(new_params[k] / aux_sum * target, 4)

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
    train_pct: float = 0.70,
) -> dict:
    """
    Run the autoresearch optimization loop with train/test split.

    Optimizes on train set (first 70% of days by default),
    validates on test set (last 30%) to detect overfitting.

    Args:
        n_experiments: Number of experiments to run.
        temperature: Initial mutation magnitude (0.05-0.3).
        cooling: If True, reduce temperature over time (simulated annealing).
        train_pct: Fraction of data for training (default 0.70).

    Returns:
        Best parameters found and their score.
    """
    global BEST_PARAMS, BEST_SCORE

    # Establish baseline on full dataset
    print("Establishing baseline...")
    all_results = run_full_backtest(PARAMS)

    # Train/test split (temporal — no leakage)
    split_idx = int(len(all_results) * train_pct)
    train_results = all_results[:split_idx]
    test_results = all_results[split_idx:]
    print(f"Split: {len(train_results)} train / {len(test_results)} test "
          f"({train_results[0].date}–{train_results[-1].date} | "
          f"{test_results[0].date}–{test_results[-1].date})")

    baseline_scores = score_results(train_results)
    baseline_test_scores = score_results(test_results)
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

        # Evaluate on TRAIN set only (prevents overfitting)
        all_res = run_full_backtest(candidate)
        train_res = all_res[:split_idx]
        scores = score_results(train_res)
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

    # Final evaluation on BOTH train and test
    best_all = run_full_backtest(BEST_PARAMS)
    best_train = best_all[:split_idx]
    best_test = best_all[split_idx:]
    best_train_scores = score_results(best_train)
    best_test_scores = score_results(best_test)

    print(f"\n{'=' * 70}")
    print(f"AUTORESEARCH COMPLETE — {n_experiments} experiments in {elapsed:.0f}s ({rate:.1f}/s)")
    print(f"{'=' * 70}")
    print(f"  Improvements: {improvements}")
    print(f"")
    print(f"  TRAIN ({len(best_train)} days: {best_train[0].date}–{best_train[-1].date}):")
    print(f"    Baseline: dir={baseline_scores['direction_pct']}% err={baseline_scores['avg_error_pp']}pp")
    print(f"    Best:     dir={best_train_scores['direction_pct']}% err={best_train_scores['avg_error_pp']}pp")
    print(f"    Composite: {baseline_composite:.2f} → {BEST_SCORE:.2f} "
          f"({'+' if BEST_SCORE > baseline_composite else ''}{BEST_SCORE - baseline_composite:.2f})")
    print(f"")
    print(f"  TEST ({len(best_test)} days: {best_test[0].date}–{best_test[-1].date}):")
    print(f"    Baseline: dir={baseline_test_scores['direction_pct']}% err={baseline_test_scores['avg_error_pp']}pp")
    print(f"    Best:     dir={best_test_scores['direction_pct']}% err={best_test_scores['avg_error_pp']}pp")

    # Overfit check
    train_gain = best_train_scores['direction_pct'] - baseline_scores['direction_pct']
    test_gain = best_test_scores['direction_pct'] - baseline_test_scores['direction_pct']
    if test_gain < 0 and train_gain > 1:
        print(f"    ⚠️  OVERFIT WARNING: train +{train_gain:.1f}pp but test {test_gain:+.1f}pp")
    elif test_gain >= 0:
        print(f"    ✅ Generalizes: train +{train_gain:.1f}pp, test +{test_gain:.1f}pp")

    print(f"\n  Optimized parameters:")
    for k, v in sorted(BEST_PARAMS.items()):
        orig = PARAMS.get(k)
        changed = " ← CHANGED" if orig != v else ""
        print(f"    {k:<30} {v:>8.4f}  (was {orig}){changed}")

    print(f"{'=' * 70}")

    return {
        "best_params": BEST_PARAMS,
        "best_train_scores": best_train_scores,
        "best_test_scores": best_test_scores,
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
