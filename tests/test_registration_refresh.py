"""The Registration List is refreshed every run and cross-checked against DUDETAILSUMMARY."""

import io
import logging
import zipfile
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


# ── Editions: never replace the cached list with an older one ────────────

_CORE = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
         '<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/'
         'core-properties" xmlns:dcterms="http://purl.org/dc/terms/" '
         'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
         '<dcterms:modified xsi:type="dcterms:W3CDTF">{}</dcterms:modified></cp:coreProperties>')


def _edition(content: bytes, modified: str | None) -> bytes:
    """`content` with its last-saved stamp set to `modified` (AEMO's form), or removed (None)."""
    src, buf = zipfile.ZipFile(io.BytesIO(content)), io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as out:
        for item in src.infolist():
            if item.filename == "docProps/core.xml":
                if modified is not None:
                    out.writestr(item, _CORE.format(modified))
            else:
                out.writestr(item, src.read(item.filename))
    return buf.getvalue()


WITH_UWF1 = DEFAULT_REG_ROWS + [reg_row("UWF1", "Uungula Wind Farm", "NSW1", "Wind", "Wind - Onshore", 414)]


def _cache_and_download(tmp_path, monkeypatch, cached: bytes, download: bytes):
    path = tmp_path / dg.REGISTRATION_FILE
    path.write_bytes(cached)
    source_status.update(tmp_path, "registration_list",
                         {"fetched_at": "2026-10-14T01:00:00+00:00"})
    monkeypatch.setattr(dg, "_fetch_bytes", lambda url: download)
    _no_dudetail(monkeypatch)
    return path


def test_workbook_edition_reads_the_last_saved_stamp():
    assert dg.workbook_edition(_edition(registration_xlsx(), "2026-09-29T00:58:42Z")) \
        == "2026-09-29T00:58:42+00:00"
    assert dg.workbook_edition(_edition(registration_xlsx(), None)) is None
    assert dg.workbook_edition(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1 legacy xls") is None
    assert dg.workbook_edition(CHALLENGE_PAGE) is None


def test_older_edition_does_not_replace_the_cached_list(tmp_path, monkeypatch, caplog):
    # The UWF1 flap: the cached 10-14 edition lists UWF1; AEMO then serves the 09-29 one without it.
    newer = _edition(registration_xlsx(WITH_UWF1), "2026-10-14T03:00:00Z")
    older = _edition(registration_xlsx(), "2026-09-29T00:58:42Z")
    path = _cache_and_download(tmp_path, monkeypatch, cached=newer, download=older)

    with caplog.at_level(logging.WARNING):
        gens = dg.fetch_generators(str(tmp_path))

    assert path.read_bytes() == newer
    assert "UWF1" in set(gens["DUID"])
    assert any("download refused" in r.message and "older than the cached edition" in r.message
               for r in caplog.records if r.levelno == logging.WARNING)
    rec = source_status.load(tmp_path)["registration_list"]
    assert rec["refreshed"] is False and rec["error"] is None
    assert rec["fetched_at"] == "2026-10-14T01:00:00+00:00"    # the cache was not refreshed
    assert rec["edition"] == "2026-10-14T03:00:00+00:00"
    assert "older" in rec["refused_download"]


def test_newer_edition_replaces_the_cache_even_when_it_drops_a_unit(tmp_path, monkeypatch):
    older = _edition(registration_xlsx(WITH_UWF1), "2026-09-29T00:58:42Z")
    newer = _edition(registration_xlsx(), "2026-10-14T03:00:00Z")
    path = _cache_and_download(tmp_path, monkeypatch, cached=older, download=newer)

    gens = dg.fetch_generators(str(tmp_path))

    assert path.read_bytes() == newer
    assert "UWF1" not in set(gens["DUID"])
    rec = source_status.load(tmp_path)["registration_list"]
    assert rec["refreshed"] is True and rec["edition"] == "2026-10-14T03:00:00+00:00"
    assert "refused_download" not in rec


def test_same_edition_is_accepted(tmp_path, monkeypatch):
    same = _edition(registration_xlsx(), "2026-10-14T03:00:00Z")
    _cache_and_download(tmp_path, monkeypatch, cached=same, download=same)
    dg.fetch_generators(str(tmp_path))
    assert source_status.load(tmp_path)["registration_list"]["refreshed"] is True


def test_without_an_edition_a_download_that_drops_units_is_refused(tmp_path, monkeypatch, caplog):
    cached = _edition(registration_xlsx(WITH_UWF1), None)
    download = _edition(registration_xlsx(), "2026-10-14T03:00:00Z")
    path = _cache_and_download(tmp_path, monkeypatch, cached=cached, download=download)

    with caplog.at_level(logging.WARNING):
        gens = dg.fetch_generators(str(tmp_path))

    assert path.read_bytes() == cached and "UWF1" in set(gens["DUID"])
    assert any("drops 1 cached unit(s): UWF1" in r.message for r in caplog.records)
    assert source_status.load(tmp_path)["registration_list"]["refreshed"] is False


def test_without_an_edition_a_download_that_only_adds_units_is_accepted(tmp_path, monkeypatch):
    cached = _edition(registration_xlsx(), None)
    download = _edition(registration_xlsx(WITH_UWF1), None)
    path = _cache_and_download(tmp_path, monkeypatch, cached=cached, download=download)

    gens = dg.fetch_generators(str(tmp_path))

    assert path.read_bytes() == download and "UWF1" in set(gens["DUID"])
    rec = source_status.load(tmp_path)["registration_list"]
    assert rec["refreshed"] is True and rec["edition"] is None
