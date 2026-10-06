"""The Registration List is refreshed every run and cross-checked against DUDETAILSUMMARY."""

import logging
from datetime import date

import pandas as pd
import pytest
import requests

from src import download_generators as dg
from src import dudetail, source_status
from fixtures import (CHALLENGE_PAGE, DEFAULT_REG_ROWS, FakeResponse, dudetail_zip,
                      reg_row, registration_xlsx)


def _no_dudetail(monkeypatch):
    monkeypatch.setattr(dudetail, "fetch_latest_dudetailsummary",
                        lambda *a, **k: (None, None))
    monkeypatch.setattr(dg, "_load_gen_info", lambda cache_path: None)


def test_check_workbook_bytes():
    dg.check_workbook_bytes(registration_xlsx(), dg.REGISTRATION_SHEET)
    with pytest.raises(ValueError, match="Cloudflare"):
        dg.check_workbook_bytes(CHALLENGE_PAGE)
    with pytest.raises(ValueError, match="lacks sheet"):
        dg.check_workbook_bytes(registration_xlsx(sheet="Something else"), dg.REGISTRATION_SHEET)
    with pytest.raises(ValueError):
        dg.check_workbook_bytes(b"PK\x03\x04 not really a zip")


def test_refresh_replaces_a_stale_cached_list(tmp_path, monkeypatch):
    stale = registration_xlsx([r for r in DEFAULT_REG_ROWS if r["DUID"] != "WANDSF2"])
    (tmp_path / dg.REGISTRATION_FILE).write_bytes(stale)
    monkeypatch.setattr(dg, "_fetch_bytes", lambda url: registration_xlsx())
    _no_dudetail(monkeypatch)

    gens = dg.fetch_generators(str(tmp_path))

    assert "WANDSF2" in set(gens["DUID"])
    assert set(gens["CLASSIFICATION"]) == {"Semi-Scheduled"}  # kept for the page's N/A reasons
    rec = source_status.load(tmp_path)["registration_list"]
    assert rec["refreshed"] is True and rec["error"] is None


def test_refresh_keeps_last_good_copy_on_challenge_page(tmp_path, monkeypatch, caplog):
    good = registration_xlsx()
    path = tmp_path / dg.REGISTRATION_FILE
    path.write_bytes(good)
    source_status.update(tmp_path, "registration_list",
                         {"fetched_at": "2026-01-01T00:00:00+00:00"})
    monkeypatch.setattr(dg, "_fetch_bytes", lambda url: CHALLENGE_PAGE)
    _no_dudetail(monkeypatch)

    with caplog.at_level(logging.ERROR):
        gens = dg.fetch_generators(str(tmp_path))

    assert path.read_bytes() == good
    assert "WANDSF2" in set(gens["DUID"])
    assert any("REGISTRATION LIST REFRESH FAILED" in r.message for r in caplog.records)
    rec = source_status.load(tmp_path)["registration_list"]
    assert rec["refreshed"] is False
    assert rec["fetched_at"] == "2026-01-01T00:00:00+00:00"
    assert "Cloudflare" in rec["error"]


def test_refresh_without_any_copy_fails(tmp_path, monkeypatch):
    def boom(url):
        raise RuntimeError("HTTP 403")
    monkeypatch.setattr(dg, "_fetch_bytes", boom)
    with pytest.raises(RuntimeError, match="no cached copy"):
        dg.fetch_generators(str(tmp_path))


def test_parse_dudetailsummary_and_recent_window():
    dud = dudetail.parse_dudetailsummary(dudetail_zip([
        ("OLDWF1", "2015/01/01", "GENERATOR", "VIC1", "OLDWF"),
        ("WANDSF2", "2026/06/02", "GENERATOR", "QLD1", "WANDSF2"),
        ("NEWBESS1", "2026/03/01", "BIDIRECTIONAL", "NSW1", "NEWBESS"),
        ("NEWLOAD1", "2026/03/01", "LOAD", "NSW1", "NEWLOAD"),
    ]))
    recent = dudetail.recent_generators(dud, today=date(2026, 10, 5))
    assert list(recent["DUID"]) == ["WANDSF2"]

    missing = dudetail.missing_from_registration(dud, {"OLDWF1"}, today=date(2026, 10, 5))
    assert list(missing["DUID"]) == ["WANDSF2"]
    assert dudetail.missing_from_registration(
        dud, {"wandsf2"}, today=date(2026, 10, 5)).empty


def test_fetch_latest_dudetailsummary_probes_newest_first(tmp_path, monkeypatch):
    calls = []
    body = dudetail_zip([("WANDSF2", "2026/06/02", "GENERATOR", "QLD1", "WANDSF2")])

    def fake_get(url, **kwargs):
        calls.append(url)
        if "202608010000" in url:
            return FakeResponse(200, body)
        return FakeResponse(404, b"not found")

    monkeypatch.setattr(requests, "get", fake_get)
    dud, month = dudetail.fetch_latest_dudetailsummary(tmp_path, today=date(2026, 10, 5))
    assert month == "2026-08"
    assert len(calls) == 3  # Oct, Sep (404), Aug
    assert (tmp_path / dudetail.CACHE_FILE).exists()

    # Next run: nothing newer than the cached month, so no download and the cache is reused
    calls.clear()
    dud2, month2 = dudetail.fetch_latest_dudetailsummary(
        tmp_path, cached_month="2026-08", today=date(2026, 10, 5))
    assert month2 == "2026-08" and len(dud2) == len(dud)
    assert len(calls) == 2


def test_fetch_generators_warns_about_units_the_list_lacks(tmp_path, monkeypatch, caplog):
    stale = registration_xlsx([r for r in DEFAULT_REG_ROWS if r["DUID"] != "WANDSF2"])
    monkeypatch.setattr(dg, "_fetch_bytes", lambda url: stale)
    recent = (date.today() - pd.Timedelta(days=120)).strftime("%Y/%m/%d")
    dud = dudetail.parse_dudetailsummary(dudetail_zip([
        ("WANDSF1", "2020/01/01", "GENERATOR", "QLD1", "WANDSF1"),
        ("WANDSF2", recent, "GENERATOR", "QLD1", "WANDSF2"),
    ]))
    monkeypatch.setattr(dudetail, "fetch_latest_dudetailsummary",
                        lambda *a, **k: (dud, "2026-08"))
    monkeypatch.setattr(dg, "_load_gen_info", lambda cache_path: pd.DataFrame(
        {"DUID": ["WANDSF2"], "TECHNOLOGY": ["Solar PV"]}))

    with caplog.at_level(logging.WARNING):
        gens = dg.fetch_generators(str(tmp_path))

    assert "WANDSF2" not in set(gens["DUID"])  # never invented
    msgs = [r.message for r in caplog.records if "Registration List lacks WANDSF2" in r.message]
    assert msgs and "Generation Information says 'Solar PV'" in msgs[0]
    rec = source_status.load(tmp_path)["dudetailsummary"]
    assert rec["month"] == "2026-08"
    assert [m["DUID"] for m in rec["missing_from_registration"]] == ["WANDSF2"]
