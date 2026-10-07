"""A newer ELI edition is noticed: probed, logged and recorded, and the validator warns."""

import logging

import pandas as pd
import requests

from src import download_eli, source_status
from fixtures import FakeResponse
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


def test_validator_warns_but_does_not_fail(tmp_path, capsys):
    status = _status() | {"eli": {"edition": 2025, "newer_edition_available": True,
                                  "probed_url": "https://x/2026/2026-eli-report-chart-data.xlsx"}}
    assert _run(tmp_path, _summary(), status) == []
    assert "WARN: ELI 2026 has been published" in capsys.readouterr().out
