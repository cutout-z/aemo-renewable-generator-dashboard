"""A newer ELI edition is noticed: probed, logged and recorded; the validator warns
(src.post_publish_check fails the lane after publishing; see test_post_publish_check)."""

import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
import requests

from src import download_eli, source_status
from fixtures import FakeResponse
import validate_outputs as vo
from test_validate_outputs import _run, _status, _summary


class FakeSession:
    """Stands in for requests.Session; records the requests it is asked to make."""

    def __init__(self, response=None, exc=None):
        self.response, self.exc, self.calls = response, exc, []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if self.exc:
            raise self.exc
        return self.response


def _tiny_chart_data(path):
    table = pd.DataFrame([["Location", "Voltage (kV)", "Region ", "Solar Projected Curtailment",
                           "Wind Projected Curtailment"], ["Ararat", 220, "VIC", 0.46, 0.33]])
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        for sheet in ("Near Term Proj Curtailment", "Med Term Proj Curtailment"):
            pd.DataFrame([["title"]]).to_excel(xw, sheet_name=sheet, index=False, header=False)
            table.to_excel(xw, sheet_name=sheet, index=False, header=False, startrow=1)


def test_probe_asks_for_one_byte_of_next_years_file():
    session = FakeSession(FakeResponse(206, b"P"))
    rec = download_eli.probe_newer_edition(2025, session=session)
    url, kwargs = session.calls[0]
    assert url.endswith("enhanced-locational-information/2026/2026-eli-report-chart-data.xlsx")
    assert kwargs["headers"]["Range"] == "bytes=0-0" and kwargs["allow_redirects"] is False
    assert rec["newer_edition_available"] is True


def test_missing_edition_redirects_to_404():
    rec = download_eli.probe_newer_edition(
        2025, session=FakeSession(FakeResponse(302, headers={"Location": "https://aemo.com.au/404"})))
    assert rec["newer_edition_available"] is False and rec["error"] is None


def test_unknown_when_blocked_or_offline():
    rec = download_eli.probe_newer_edition(2025, session=FakeSession(FakeResponse(403)))
    assert rec["newer_edition_available"] is None and "403" in rec["error"]
    rec = download_eli.probe_newer_edition(
        2025, session=FakeSession(exc=requests.ConnectionError("down")))
    assert rec["newer_edition_available"] is None and "down" in rec["error"]


def test_fetch_records_a_newer_edition_and_warns(tmp_path, caplog):
    _tiny_chart_data(tmp_path / "eli_chart_data_2025.xlsx")
    with caplog.at_level(logging.WARNING):
        out = download_eli.fetch_eli_curtailment(str(tmp_path), session=FakeSession(FakeResponse(206)))
    assert len(out) == 1  # the 2025 data is still parsed
    assert out["ELI_EDITION"].tolist() == [2025]  # the edition travels with the cached feather
    rec = source_status.load(tmp_path)["eli"]
    assert rec["edition"] == 2025 and rec["newer_edition_available"] is True
    assert any("NEWER ELI EDITION (2026)" in r.getMessage() for r in caplog.records)


def test_validator_warns_with_the_steps_but_does_not_fail(tmp_path, capsys):
    # The owner's decision 6(b): the lane goes red after publishing (src.post_publish_check), so the
    # validator must not fail here or the run's MLF / actuals / listing updates are held back
    status = _status() | {"eli": {"edition": 2025, "newer_edition_available": True,
                                  "checked_at": source_status.now_iso(),
                                  "probed_url": "https://x/2026/2026-eli-report-chart-data.xlsx"}}
    assert _run(tmp_path, _summary(), status) == []
    out = capsys.readouterr().out
    assert "WARN: ELI 2026 has been published" in out and "still uses ELI 2025" in out
    # says exactly what to do: bump the config edition, rebuild the appendices, regenerate
    assert "ELI_CHART_DATA_URLS" in out and "ELI_REGIONAL_APPENDIX_URLS" in out
    assert "python -m src.eli_appendix" in out and "python -m src.main --full-refresh" in out
    assert "src.post_publish_check" in out


def test_newer_edition_warning_replaces_the_backstop_warning(tmp_path, capsys):
    # the probe did find the edition, so the "not found at the expected URL" backstop stays quiet
    overdue = datetime.now(timezone.utc).year - 2
    df = _summary().assign(ELI_EDITION=overdue)
    rez = {"forecasts_eli_edition": overdue, "membership_eli_edition": overdue, "isp_edition": "x"}
    eli = _eli(overdue, newer=True)
    assert _run(tmp_path, df, _status() | {"eli": eli, "rez": rez}) == []
    out = capsys.readouterr().out
    assert f"ELI {overdue + 1} has been published" in out
    assert "not found at the expected URL" not in out


def test_validator_passes_when_no_newer_edition_is_found(tmp_path, capsys):
    current = datetime.now(timezone.utc).year + 1
    df = _summary().assign(ELI_EDITION=current)
    rez = {"forecasts_eli_edition": current, "membership_eli_edition": current, "isp_edition": "x"}
    assert _run(tmp_path, df, _status() | {"eli": _eli(current, newer=False), "rez": rez}) == []
    out = capsys.readouterr().out
    assert "has been published" not in out and f"ELI edition: {current}" in out


# ── S2-1 calendar backstop ────────────────────────────────────────────────

AEST = timezone(timedelta(hours=10))


@pytest.mark.parametrize("when, expected", [
    (datetime(2026, 9, 30, 23, 59, tzinfo=AEST), 2025),
    (datetime(2026, 10, 1, 0, 0, tzinfo=AEST), 2026),
    (datetime(2026, 9, 30, 14, 0, tzinfo=timezone.utc), 2026),   # = 1 Oct 00:00 AEST
    (datetime(2027, 3, 1, tzinfo=AEST), 2026),
])
def test_expected_edition_turns_over_on_1_october(when, expected):
    assert download_eli.expected_edition(when) == expected
    assert vo.expected_eli_edition(when) == expected


def test_probe_finding_nothing_after_september_warns_with_the_eli_page(tmp_path, caplog):
    missing = FakeResponse(302, headers={"Location": "https://aemo.com.au/404"})
    with caplog.at_level(logging.WARNING):
        download_eli.check_newer_edition(tmp_path, 2025, session=FakeSession(missing),
                                         now=datetime(2026, 10, 7, tzinfo=AEST))
    msgs = " ".join(r.getMessage() for r in caplog.records)
    assert "ELI 2026 NOT FOUND at the expected URL" in msgs and "enhanced-locational-information" in msgs
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        download_eli.check_newer_edition(tmp_path, 2025, session=FakeSession(missing),
                                         now=datetime(2026, 8, 1, tzinfo=AEST))
    assert not caplog.records  # before October a missing 2026 edition is not overdue


def _eli(edition, newer=False, last_check_days=1, **extra):
    when = (datetime.now(timezone.utc) - timedelta(days=last_check_days)).isoformat()
    return {"edition": edition, "newer_edition_available": newer, "checked_at": when,
            "probed_url": f"https://x/{edition + 1}/{edition + 1}-eli-report-chart-data.xlsx"} | extra


def test_validator_backstop_warns_when_an_edition_is_overdue(tmp_path, capsys):
    overdue = datetime.now(timezone.utc).year - 2   # always older than the expected edition
    df = _summary().assign(ELI_EDITION=overdue)
    rez = {"forecasts_eli_edition": overdue, "membership_eli_edition": overdue, "isp_edition": "x"}
    assert _run(tmp_path, df, _status() | {"eli": _eli(overdue), "rez": rez}) == []
    out = capsys.readouterr().out
    assert "not found at the expected URL" in out and vo.ELI_PAGE_URL in out


def test_validator_backstop_is_quiet_for_a_current_edition(tmp_path, capsys):
    current = datetime.now(timezone.utc).year + 1   # never older than the expected edition
    df = _summary().assign(ELI_EDITION=current)
    rez = {"forecasts_eli_edition": current, "membership_eli_edition": current, "isp_edition": "x"}
    assert _run(tmp_path, df, _status() | {"eli": _eli(current), "rez": rez}) == []
    assert "not found at the expected URL" not in capsys.readouterr().out


# ── S3-3 a blind probe ────────────────────────────────────────────────────

def test_a_blocked_probe_keeps_the_last_conclusive_check(tmp_path):
    ok = FakeResponse(302, headers={"Location": "https://aemo.com.au/404"})
    first = download_eli.check_newer_edition(tmp_path, 2025, session=FakeSession(ok))
    assert first["last_conclusive_check"] == first["checked_at"]
    blocked = download_eli.check_newer_edition(tmp_path, 2025, session=FakeSession(FakeResponse(403)))
    assert blocked["newer_edition_available"] is None
    assert blocked["last_conclusive_check"] == first["checked_at"]


def test_validator_fails_when_the_probe_has_been_blind_too_long(tmp_path):
    old = (datetime.now(timezone.utc) - timedelta(days=61)).isoformat()
    eli = _eli(2025, newer=None, error="HTTP 403", last_conclusive_check=old)
    errs = _run(tmp_path, _summary(), _status() | {"eli": eli})
    assert any("last had a yes/no answer 61 days ago" in e for e in errs), errs


def test_validator_fails_when_the_probe_never_answered(tmp_path):
    eli = _eli(2025, newer=None, error="HTTP 403")
    errs = _run(tmp_path, _summary(), _status() | {"eli": eli})
    assert any("never had a yes/no answer" in e for e in errs), errs


def test_validator_passes_a_recently_blocked_probe(tmp_path, capsys):
    recent = (datetime.now(timezone.utc) - timedelta(days=20)).isoformat()
    eli = _eli(2025, newer=None, error="HTTP 403", last_conclusive_check=recent)
    assert _run(tmp_path, _summary(), _status() | {"eli": eli}) == []
    assert "could not check for a newer ELI edition" in capsys.readouterr().out
