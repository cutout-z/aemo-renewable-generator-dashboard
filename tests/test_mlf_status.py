"""An MLF fetch is recorded, and the validator fails a stale cache or a lagging MLF year (S2-4)."""

from datetime import datetime, timedelta, timezone

import pytest
import requests

import validate_outputs as vo
from src import download_mlf, source_status
from fixtures import FakeResponse
from test_validate_outputs import _run, _status, _summary

CSV = ("DUID,REGIONID,FY24-25,FY25-26,FY26-27,FY26-27 Import\n"
       "AAASF1,NSW1,0.95,0.96,0.97,0.99\n"
       "BBBWF1,VIC1,0.90,0.91,0.92,0.98\n")


def _ok(*args, **kwargs):
    return FakeResponse(200, CSV.encode())


def _down(*args, **kwargs):
    raise requests.ConnectionError("tracker down")


def test_a_good_fetch_records_the_newest_final_year(tmp_path, monkeypatch):
    monkeypatch.setattr(requests, "get", _ok)
    out = download_mlf.fetch_mlf_data(str(tmp_path), {"AAASF1"})
    assert list(out.columns) == ["DUID", "MLF_FY25-26", "MLF_FY26-27"]
    rec = source_status.load(tmp_path)["mlf"]
    assert rec["refreshed"] and rec["error"] is None and not rec["used_cache"]
    assert rec["newest_fy"] == "FY26-27" and rec["fetched_at"] == rec["attempted_at"]


def test_a_failed_fetch_republishing_the_cache_is_recorded(tmp_path, monkeypatch):
    monkeypatch.setattr(requests, "get", _ok)
    download_mlf.fetch_mlf_data(str(tmp_path))
    good = source_status.load(tmp_path)["mlf"]["fetched_at"]
    monkeypatch.setattr(requests, "get", _down)
    out = download_mlf.fetch_mlf_data(str(tmp_path))
    assert len(out) == 2  # the cached CSV is still used
    rec = source_status.load(tmp_path)["mlf"]
    assert rec["used_cache"] and "tracker down" in rec["error"] and rec["fetched_at"] == good


def test_a_failed_fetch_with_no_cache_is_recorded(tmp_path, monkeypatch):
    monkeypatch.setattr(requests, "get", _down)
    assert download_mlf.fetch_mlf_data(str(tmp_path)).empty
    rec = source_status.load(tmp_path)["mlf"]
    assert rec["error"] and not rec["used_cache"] and rec["fetched_at"] is None


def _fy(start):
    return f"FY{start % 100:02d}-{(start + 1) % 100:02d}"


def _mlf(age_days=1, newest=None, error=None, used_cache=False):
    when = (datetime.now(timezone.utc) - timedelta(days=age_days)).isoformat()
    return {"fetched_at": when, "error": error, "used_cache": used_cache,
            "newest_fy": newest or _fy(vo.current_fy_start())}


def test_validator_passes_a_current_refresh(tmp_path):
    assert _run(tmp_path, _summary(), _status() | {"mlf": _mlf()}) == []


def test_validator_warns_on_a_fresh_fallback(tmp_path, capsys):
    status = _status() | {"mlf": _mlf(10, error="tracker down", used_cache=True)}
    assert _run(tmp_path, _summary(), status) == []
    assert "WARN: MLF refresh failed" in capsys.readouterr().out


def test_validator_fails_on_a_stale_fallback(tmp_path):
    status = _status() | {"mlf": _mlf(36, error="tracker down", used_cache=True)}
    errs = _run(tmp_path, _summary(), status)
    assert any("MLF cache is 36 days old" in e for e in errs), errs


def test_validator_fails_with_no_mlfs_at_all(tmp_path):
    status = _status() | {"mlf": _mlf(error="tracker down", used_cache=False)}
    assert any("no cached copy" in e for e in _run(tmp_path, _summary(), status))


def test_validator_fails_when_the_newest_mlf_year_is_behind(tmp_path):
    behind = _fy(vo.current_fy_start() - 1)
    errs = _run(tmp_path, _summary(), _status() | {"mlf": _mlf(newest=behind)})
    assert any(f"Newest final MLF year is {behind}" in e for e in errs), errs


@pytest.mark.parametrize("when, expected", [
    (datetime(2027, 6, 30, 13, 59, tzinfo=timezone.utc), 2026),  # 23:59 AEST 30 June
    (datetime(2027, 6, 30, 14, 0, tzinfo=timezone.utc), 2027),   # 00:00 AEST 1 July
])
def test_current_fy_turns_over_in_nem_time(when, expected):
    assert vo.current_fy_start(when) == expected
