"""Generation Information: newest edition discovered, cached with its date, never pinned."""

import logging
from datetime import date

import requests

from src import gen_info, source_status
from fixtures import CHALLENGE_PAGE, FakeResponse, gen_info_xlsx

TODAY = date(2026, 10, 5)


def test_candidate_urls_newest_first_and_stop_at_cached_edition():
    cands = gen_info.candidate_urls(TODAY, newer_than="2026-07")
    editions = [e for e, _ in cands]
    assert editions == ["2026-10", "2026-10", "2026-09", "2026-09", "2026-08", "2026-08"]
    assert cands[0][1].endswith("/generation_information/2026/nem-generation-information-october-2026.xlsx")
    assert cands[1][1].endswith("/generation_information/nem-generation-information-october-2026.xlsx")
    # wraps across the year boundary
    assert gen_info.candidate_urls(date(2026, 1, 15), lookback=2)[2][0] == "2025-12"


def test_links_from_page():
    html = ('<a href="/-/media/files/electricity/nem/planning_and_forecasting/generation_information/'
            '2026/nem-generation-information-july-2026.xlsx?rev=abc&amp;sc_lang=en">Jul</a>'
            '<a href="https://www.aemo.com.au/x/nem-generation-information-april-2026.xlsx">Apr</a>')
    links = gen_info.links_from_page(html)
    assert [e for e, _ in links] == ["2026-07", "2026-04"]
    assert links[0][1].startswith("https://www.aemo.com.au/-/media/")
    assert "&amp;" not in links[0][1]


def _fake_aemo(found_url_part, body):
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        if url.endswith("generation-information"):
            return FakeResponse(403, CHALLENGE_PAGE)
        if found_url_part in url:
            return FakeResponse(200, body)
        return FakeResponse(302, b"", {"Location": "https://aemo.com.au/404"})

    return fake_get, calls


def test_refresh_finds_newest_edition_by_probing(tmp_path, monkeypatch):
    fake_get, calls = _fake_aemo("2026/nem-generation-information-july-2026", gen_info_xlsx())
    monkeypatch.setattr(requests, "get", fake_get)

    path, edition = gen_info.refresh_gen_info(tmp_path, today=TODAY)

    assert edition == "2026-07"
    assert path == tmp_path / gen_info.CACHE_FILE and path.exists()
    rec = source_status.load(tmp_path)["gen_info"]
    assert rec["edition"] == "2026-07" and rec["refreshed"] is True
    assert "july-2026" in rec["url"]

    # Next run probes only months newer than the cached edition
    calls.clear()
    path2, edition2 = gen_info.refresh_gen_info(tmp_path, today=TODAY)
    assert edition2 == "2026-07"
    assert not any("july-2026" in c for c in calls)


def test_refresh_keeps_last_good_copy_and_warns_when_old(tmp_path, monkeypatch, caplog):
    good = gen_info_xlsx()
    (tmp_path / gen_info.CACHE_FILE).write_bytes(good)
    source_status.update(tmp_path, "gen_info", {"edition": "2026-01", "fetched_at": "2026-01-20"})
    # A newer edition "exists" but the download is a challenge page
    fake_get, _ = _fake_aemo("april-2026", CHALLENGE_PAGE)
    monkeypatch.setattr(requests, "get", fake_get)

    with caplog.at_level(logging.WARNING):
        path, edition = gen_info.refresh_gen_info(tmp_path, today=TODAY)

    assert edition == "2026-01"
    assert path.read_bytes() == good
    rec = source_status.load(tmp_path)["gen_info"]
    assert rec["refreshed"] is False and "Cloudflare" in rec["error"]
    assert rec["edition_age_days"] > gen_info.STALE_AFTER_DAYS
    assert any("days old" in r.message for r in caplog.records)


def test_parse_gen_info_new_layout_has_no_rez_location_or_voltage(tmp_path):
    p = tmp_path / "gi.xlsx"
    p.write_bytes(gen_info_xlsx())
    df = gen_info.parse_gen_info(p)
    assert set(df["DUID"]) == {"WANDSF2", "ARWF1", "KIDSPHG2"}
    assert df.set_index("DUID").loc["ARWF1", "TECHNOLOGY"] == "Wind"
    assert not {"REZ_NAME", "LOCATION", "VOLTAGE_KV"} & set(df.columns)


def test_parse_gen_info_maps_rez_location_voltage_if_an_edition_adds_them(tmp_path):
    rows = [[2002, "Ararat Wind Farm", "Owner", "VIC1", "U1", "Wind", "Onshore", "ARWF1",
             "Semi-Scheduled", 240, "In Service", "Western Victoria", "Ararat", 220]]
    p = tmp_path / "gi.xlsx"
    p.write_bytes(gen_info_xlsx(rows, extra_columns=["REZ", "Location", "Voltage (kV)"]))
    row = gen_info.parse_gen_info(p).set_index("DUID").loc["ARWF1"]
    assert row["REZ_NAME"] == "Western Victoria"
    assert row["LOCATION"] == "Ararat"
    assert row["VOLTAGE_KV"] == 220
