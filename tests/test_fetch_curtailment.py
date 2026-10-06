"""Actual curtailment: the two latest FYs the upstream rollup covers in full."""

from datetime import datetime

import pandas as pd

from src import config
from src import fetch_curtailment as fc
from fixtures import FakeResponse

DUIDS = {"AAASF1", "BBBWF1", "CCCSF1"}


def _rollup(rows):
    return pd.DataFrame(rows, columns=["duid", "fy_start", "fy_label", "curtailment_pct",
                                       "metric_version", "generation_mwh", "months_covered"])


def _early_july_rollup():
    """curtailment_by_fy.csv as published on 3 July 2026: FY25-26 has ended but
    June's data is not in yet, so it has 11 months; FY26-27 has 0-1 days."""
    rows = []
    for duid, base in [("AAASF1", 0.10), ("BBBWF1", 0.05), ("CCCSF1", 0.20)]:
        rows += [
            (duid, 2023, "FY23-24", base, "2.0-proxy", 1000.0, 12),
            (duid, 2024, "FY24-25", base + 0.01, "2.0-proxy", 1000.0, 12),
            (duid, 2025, "FY25-26", base + 0.02, "2.0-proxy", 900.0, 11),
            (duid, 2026, "FY26-27", base, "2.0-proxy", 10.0, 1),
        ]
    return _rollup(rows)


EARLY_JULY = datetime(2026, 7, 3, 9, 0, tzinfo=config.NEM_TZ)


def _fetch(monkeypatch, df, now):
    """Run the fetch as the pipeline does (no explicit date), with the clock at `now`."""
    monkeypatch.setattr(config, "nem_now", lambda: now)
    monkeypatch.setattr(fc.requests, "get",
                        lambda *a, **k: FakeResponse(200, df.to_csv(index=False).encode()))
    return fc.fetch_curtailment_by_fy(DUIDS)


def _value_columns(out):
    return [c for c in out.columns if not c.startswith("ACTUAL_MONTHS_")]


def test_early_july_keeps_the_last_two_complete_years(monkeypatch):
    out = _fetch(monkeypatch, _early_july_rollup(), EARLY_JULY)
    assert _value_columns(out) == ["DUID", "CURTAILMENT_ACTUAL_FY23-24", "CURTAILMENT_ACTUAL_FY24-25"]
    assert out["CURTAILMENT_ACTUAL_FY23-24"].notna().all()
    assert out["CURTAILMENT_ACTUAL_FY24-25"].notna().all()
    by = out.set_index("DUID")
    assert by.loc["AAASF1", "CURTAILMENT_ACTUAL_FY24-25"] == 0.11


def test_rolls_forward_once_june_lands(monkeypatch):
    df = _early_july_rollup()
    df.loc[df["fy_start"] == 2025, "months_covered"] = 12
    out = _fetch(monkeypatch, df, datetime(2026, 8, 5, tzinfo=config.NEM_TZ))
    assert _value_columns(out) == ["DUID", "CURTAILMENT_ACTUAL_FY24-25", "CURTAILMENT_ACTUAL_FY25-26"]
    assert out["CURTAILMENT_ACTUAL_FY25-26"].notna().all()


def test_a_running_fy_never_counts_as_complete():
    # A unit can have 12 months in the current FY only through bad upstream data;
    # a FY that has not ended is never shown as complete.
    df = _early_july_rollup()
    df.loc[df["fy_start"] == 2026, "months_covered"] = 12
    assert fc.select_fys(df, now=EARLY_JULY) == [2023, 2024]


def test_partial_unit_years_stay_empty():
    df = _early_july_rollup()
    df.loc[(df["duid"] == "CCCSF1") & (df["fy_start"] == 2024), "months_covered"] = 7
    out = fc.reshape(df, DUIDS, [2023, 2024]).set_index("DUID")
    assert pd.isna(out.loc["CCCSF1", "CURTAILMENT_ACTUAL_FY24-25"])
    assert out.loc["AAASF1", "CURTAILMENT_ACTUAL_FY24-25"] == 0.11


def test_months_covered_travels_with_each_year():
    df = _early_july_rollup()
    df.loc[(df["duid"] == "CCCSF1") & (df["fy_start"] == 2024), "months_covered"] = 11
    df = df[~((df["duid"] == "BBBWF1") & (df["fy_start"] == 2024))]  # no row at all
    out = fc.reshape(df, DUIDS, [2023, 2024]).set_index("DUID")
    assert out.loc["CCCSF1", "ACTUAL_MONTHS_FY24-25"] == 11
    assert pd.isna(out.loc["CCCSF1", "CURTAILMENT_ACTUAL_FY24-25"])
    assert pd.isna(out.loc["BBBWF1", "ACTUAL_MONTHS_FY24-25"])
    assert out.loc["AAASF1", "ACTUAL_MONTHS_FY24-25"] == 12
