"""Rendered-page smoke: load index.html in headless chromium, offline, with the data the repo commits.

No server and no port: page_harness routes http://aemo.test/ to this checkout, PapaParse to a vendored
copy, SheetJS to a no-op stub, nav.js to an empty script, and aborts (and records) every other request.
"""

import csv
import re

import pytest

pytest.importorskip("playwright")

from page_harness import ROOT, assert_clean, loaded_page  # noqa: E402

READY = "document.querySelectorAll('#tbody tr').length > 0"


def _committed():
    with open(ROOT / "outputs" / "summary.csv", newline="") as f:
        return list(csv.DictReader(f))


def test_page_renders_the_committed_data_with_no_errors():
    total = len(_committed())
    with loaded_page(wait_for=READY) as (page, log):
        assert_clean(log)
        # stat tiles: numbers, none a skeleton; the generator count is the committed row count
        tiles = page.locator("#stats > *")
        assert tiles.count() == 5
        for i in range(tiles.count()):
            assert re.match(r"^[\d.,]+", tiles.nth(i).inner_text().strip()), tiles.nth(i).inner_text()
        assert page.locator("#stats .skeleton").count() == 0
        assert tiles.first.inner_text().split()[0] == str(total)
        # one body row per committed generator; the badge says so
        assert page.locator("#tbody tr").count() == total
        assert page.locator("#rowCount").inner_text() == f"{total} of {total} shown"
        assert re.search(r"\d%|\d\.\d{4}", page.locator("#tbody tr").first.inner_text())
        for sel in ("#stats", "#controls", "#table", "#footer", "#sourceStatus"):
            box = page.locator(sel).bounding_box()
            assert box and box["height"] > 0 and page.locator(sel).is_visible(), sel
        # the failure states stay hidden, the footer and source line are filled from the data
        assert page.locator("#loadError").is_hidden()
        assert page.locator("#noMatch").is_hidden()
        assert page.locator("#loading").is_hidden()
        assert f"{total} generators tracked" in page.locator("#footer").inner_text()
        assert "Sources at the last publish" in page.locator("#sourceStatus").inner_text()


def test_region_filter_and_selection_export_use_the_stubbed_sheetjs():
    total = len(_committed())
    with loaded_page(wait_for=READY) as (page, log):
        page.locator("#tabs button", has_text="TAS").click()
        page.wait_for_function(f"document.querySelectorAll('#tbody tr').length < {total}")
        shown = page.locator("#tbody tr").count()
        assert page.locator("#rowCount").inner_text() == f"{shown} of {total} shown"
        page.locator("#tbody label.cbl").first.click()
        assert page.locator("#selCount").inner_text() == "1 selected"
        assert page.locator("#exportSelected").is_enabled()
        page.locator("#exportSelected").click()
        assert page.evaluate("window.__xlsxWrites") == 1  # the click reached XLSX.writeFile (the stub)
        assert_clean(log)


def test_page_renders_at_phone_width():
    with loaded_page(width=400, height=800, wait_for=READY) as (page, log):
        assert_clean(log)
        assert page.locator("#tbody tr").count() > 0
        assert page.locator("#stats").is_visible()
