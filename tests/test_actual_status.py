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
