"""Tests for the NSE bhavcopy parser (pure, no network) used by the Dawn price
refresh. Covers UDiFF + legacy column aliases, the EQ/BE series filter, and junk-row
rejection."""
import io

from scripts.refresh_dawn_prices import parse_bhavcopy_csv

UDIFF = (
    "TradDt,BizDt,Sgmt,ISIN,TckrSymb,SctySrs,ClsPric,LastPric\n"
    "2026-07-14,2026-07-14,CM,INE002A01018,RELIANCE,EQ,1304.5,1304.0\n"
    "2026-07-14,2026-07-14,CM,INE669E01016,IDEA,BE,13.83,13.80\n"
    "2026-07-14,2026-07-14,CM,INE000000001,SOMEETF,EQ,0,0\n"          # zero close -> drop
    "2026-07-14,2026-07-14,CM,INE111111111,FUTX,FUT,999,999\n"        # non EQ/BE -> drop
    "2026-07-14,2026-07-14,CM,,NOISIN,EQ,50,50\n"                     # missing isin -> drop
)

LEGACY = (
    "SYMBOL,SERIES,CLOSE,ISIN\n"
    "TCS,EQ,3200.25,INE467B01029\n"
    "INFY,EQ,1500,INE009A01021\n"
)


def test_udiff_keeps_eq_be_only():
    rows = parse_bhavcopy_csv(io.StringIO(UDIFF), "2026-07-14")
    syms = {r["symbol"] for r in rows}
    assert syms == {"RELIANCE", "IDEA"}          # ETF(0), FUT, no-isin all dropped
    rel = next(r for r in rows if r["symbol"] == "RELIANCE")
    assert rel["close"] == 1304.5 and rel["isin"] == "INE002A01018"
    assert rel["date"] == "2026-07-14"


def test_legacy_format_aliases():
    rows = parse_bhavcopy_csv(io.StringIO(LEGACY), "2026-07-10")
    assert {r["symbol"] for r in rows} == {"TCS", "INFY"}
    # legacy has no TradDt -> falls back to the supplied date
    assert all(r["date"] == "2026-07-10" for r in rows)


def test_comma_thousands_and_positive_close():
    rows = parse_bhavcopy_csv(io.StringIO(
        "TckrSymb,SctySrs,ClsPric,ISIN,TradDt\nBIGCO,EQ,\"1,234.50\",INE123456789,2026-07-14\n"), "2026-07-14")
    assert len(rows) == 1 and rows[0]["close"] == 1234.5
