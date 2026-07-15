"""Guard the pre-registered book set: Core is the baseline primary, and Trend /
Quality must each differ from Core by EXACTLY ONE knob (clean single-variable
attribution). If someone adds a two-change variant, this fails loudly."""
from src import papertrack as pt


KNOBS = ["overlay", "quality_gate", "quality_tilt", "mcap_max"]


def test_variants_distinct_strategies():
    strats = [v["strategy"] for v in pt.VARIANTS]
    assert len(strats) == len(set(strats))          # all unique
    for s in (pt.STRATEGY, pt.STRATEGY_TREND, pt.STRATEGY_QUALITY,
              pt.STRATEGY_QTILT, pt.STRATEGY_SMALLCAP):
        assert s in strats


def test_exactly_one_primary_and_it_is_core():
    primaries = [v for v in pt.VARIANTS if v["primary"]]
    assert len(primaries) == 1
    core = primaries[0]
    assert core["strategy"] == pt.STRATEGY
    # baseline: every knob off
    assert not core["overlay"] and not core["quality_gate"] and not core["quality_tilt"]
    assert core["mcap_max"] is None


def test_satellites_differ_from_core_by_one_knob():
    core = next(v for v in pt.VARIANTS if v["primary"])
    for v in pt.VARIANTS:
        if v["primary"]:
            continue
        diffs = sum(bool(v[k]) != bool(core[k]) if k != "mcap_max"
                    else (v[k] is None) != (core[k] is None) for k in KNOBS)
        assert diffs == 1, f"{v['label']} differs from Core by {diffs} knobs, must be exactly 1"
