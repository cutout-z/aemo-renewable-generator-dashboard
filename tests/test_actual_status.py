"""An upstream curtailment failure is recorded, and the validator warns or fails on it."""

import json
from datetime import datetime, timedelta, timezone

import pandas as pd

import validate_outputs as vo
from src import main as main_mod
from src import source_status
from test_validate_outputs import _run, _status, _summary

DUIDS = {"AAASF1", "BBBWF1"}


def _cached(path):
    pd.DataFrame({"DUID": sorted(DUIDS), "CURTAILMENT_ACTUAL_FY24-25": [0.1, 0.2]}).to_feather(path)


def _boom(duids):
    raise RuntimeError("HTTP 503")


def test_failure_republishing_the_cache_is_recorded(tmp_path, monkeypatch):
    cache = tmp_path / "actual_curtailment.feather"
    _cached(cache)
    monkeypatch.setattr(main_mod, "fetch_curtailment_by_fy", _boom)
    out = main_mod.refresh_actual_curtailment(DUIDS, tmp_path, cache)
    assert len(out) == 2  # the cached copy is still used
    rec = source_status.load(tmp_path)["actual_curtailment"]
    assert rec["error"] == "HTTP 503" and rec["used_cache"] is True and rec["refreshed"] is False


def test_success_records_the_years(tmp_path, monkeypatch):
    cache = tmp_path / "actual_curtailment.feather"
    monkeypatch.setattr(main_mod, "fetch_curtailment_by_fy", lambda d: pd.DataFrame(
        {"DUID": ["AAASF1"], "CURTAILMENT_ACTUAL_FY24-25": [0.1], "CURTAILMENT_ACTUAL_FY25-26": [0.2]}))
    main_mod.refresh_actual_curtailment(DUIDS, tmp_path, cache)
    rec = source_status.load(tmp_path)["actual_curtailment"]
    assert rec["refreshed"] and rec["error"] is None and rec["fys"] == ["FY24-25", "FY25-26"]
    assert cache.exists()


def test_an_empty_rollup_keeps_the_cache(tmp_path, monkeypatch):
    cache = tmp_path / "actual_curtailment.feather"
    _cached(cache)
    monkeypatch.setattr(main_mod, "fetch_curtailment_by_fy", lambda d: pd.DataFrame({"DUID": ["AAASF1"]}))
    out = main_mod.refresh_actual_curtailment(DUIDS, tmp_path, cache)
    assert "CURTAILMENT_ACTUAL_FY24-25" in out.columns
    assert source_status.load(tmp_path)["actual_curtailment"]["used_cache"] is True


def _actual(age_days, error=None, used_cache=False):
    when = (datetime.now(timezone.utc) - timedelta(days=age_days)).isoformat()
    return {"fetched_at": when, "error": error, "used_cache": used_cache, "fys": ["FY24-25", "FY25-26"]}


def test_validator_warns_on_a_fresh_fallback(tmp_path, capsys):
    status = _status() | {"actual_curtailment": _actual(1, "HTTP 503", used_cache=True)}
    assert _run(tmp_path, _summary(), status) == []
    assert "WARN: actual curtailment refresh failed" in capsys.readouterr().out


def test_validator_fails_on_a_stale_fallback(tmp_path):
    status = _status() | {"actual_curtailment": _actual(60, "HTTP 503", used_cache=True)}
    errs = _run(tmp_path, _summary(), status)
    assert any("Actual curtailment cache is 60 days old" in e for e in errs), errs


def test_validator_fails_with_no_actuals_at_all(tmp_path):
    status = _status() | {"actual_curtailment": _actual(0, "HTTP 503", used_cache=False)}
    errs = _run(tmp_path, _summary(), status)
    assert any("no cached copy" in e for e in errs), errs


def test_validator_passes_a_good_refresh(tmp_path):
    assert _run(tmp_path, _summary(), _status() | {"actual_curtailment": _actual(0)}) == []


# ── S3-4: the upstream rollup's own freshness ──────────────────────────────

def _month_ago(days):
    """'YYYY-MM' of the month that ended about `days` days ago."""
    end = datetime.now(timezone.utc) - timedelta(days=days)
    first = end.replace(day=1) - timedelta(days=1)   # a day in the month before `end`'s month
    return f"{first.year}-{first.month:02d}"


def test_success_records_the_upstream_month(tmp_path, monkeypatch):
    def fetch(duids):
        df = pd.DataFrame({"DUID": ["AAASF1"], "CURTAILMENT_ACTUAL_FY24-25": [0.1]})
        df.attrs["upstream_last_month"] = "2026-08"
        return df
    monkeypatch.setattr(main_mod, "fetch_curtailment_by_fy", fetch)
    main_mod.refresh_actual_curtailment(DUIDS, tmp_path, tmp_path / "a.feather")
    assert source_status.load(tmp_path)["actual_curtailment"]["upstream_last_month"] == "2026-08"
    # a later failure keeps the last known upstream month
    monkeypatch.setattr(main_mod, "fetch_curtailment_by_fy", _boom)
    main_mod.refresh_actual_curtailment(DUIDS, tmp_path, tmp_path / "a.feather")
    assert source_status.load(tmp_path)["actual_curtailment"]["upstream_last_month"] == "2026-08"


def test_month_end_age():
    now = datetime(2026, 10, 7, tzinfo=timezone.utc)
    assert round(vo.month_end_age_days("2026-08", now)) == 36   # August ended 1 Sep 00:00 AEST
    assert round(vo.month_end_age_days("2025-12", now)) == 279  # December rolls the year
    assert vo.month_end_age_days(None, now) is None


def test_validator_passes_a_current_upstream(tmp_path):
    rec = _actual(0) | {"upstream_last_month": _month_ago(40)}
    assert _run(tmp_path, _summary(), _status() | {"actual_curtailment": rec}) == []


def test_validator_fails_a_stalled_upstream_even_when_the_fetch_worked(tmp_path):
    month = _month_ago(100)
    rec = _actual(0) | {"upstream_last_month": month}
    errs = _run(tmp_path, _summary(), _status() | {"actual_curtailment": rec})
    assert any(f"rollup ends at {month}" in e for e in errs), errs


def test_validator_warns_without_an_upstream_month(tmp_path, capsys):
    assert _run(tmp_path, _summary(), _status() | {"actual_curtailment": _actual(0)}) == []
    assert "no upstream newest month" in capsys.readouterr().out
