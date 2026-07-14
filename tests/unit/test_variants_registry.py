"""Guard the pre-registered book set: Core is the baseline primary, and Trend /
Quality must each differ from Core by EXACTLY ONE knob (clean single-variable
attribution). If someone adds a two-change variant, this fails loudly."""
from src import papertrack as pt


def test_three_variants_distinct_strategies():
    strats = [v["strategy"] for v in pt.VARIANTS]
    assert len(strats) == len(set(strats)) == 3
    assert pt.STRATEGY in strats and pt.STRATEGY_TREND in strats and pt.STRATEGY_QUALITY in strats


def test_exactly_one_primary_and_it_is_core():
    primaries = [v for v in pt.VARIANTS if v["primary"]]
    assert len(primaries) == 1
    core = primaries[0]
    assert core["strategy"] == pt.STRATEGY
    assert core["overlay"] is False and core["quality_gate"] is False  # baseline: nothing on


def test_satellites_differ_from_core_by_one_knob():
    core = next(v for v in pt.VARIANTS if v["primary"])
    for v in pt.VARIANTS:
        if v["primary"]:
            continue
        diffs = sum([v["overlay"] != core["overlay"], v["quality_gate"] != core["quality_gate"]])
        assert diffs == 1, f"{v['label']} differs from Core by {diffs} knobs, must be exactly 1"
