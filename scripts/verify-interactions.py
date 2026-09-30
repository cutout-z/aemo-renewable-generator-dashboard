#!/usr/bin/env python3
"""Behaviour check for the AEMO Renewable Generator Dashboard — every interaction a design pass must
NOT lose: region tabs, fuel/REZ filters, search, column sorting, select-all, the XLSX export of the
selection, the sticky two-row header and the Excel/CSV downloads.

    cd ~/Design/"AEMO Renewable Generator Dashboard" && python3 -m http.server 9370 --bind 127.0.0.1 &
    /opt/anaconda3/bin/python3 scripts/verify-interactions.py

Green TODAY, on the unstyled page, and it must still be green at handback. Numbers come from
outputs/summary.csv itself. Reads only; writes nothing (the XLSX is captured, not saved).
Exit 1 on any breakage.
"""
from __future__ import annotations

import csv
import pathlib
import sys

from playwright.sync_api import sync_playwright

ROOT = pathlib.Path(__file__).resolve().parent.parent
URL = "http://127.0.0.1:9370/index.html"
CSV = ROOT / "outputs" / "summary.csv"

fails: list[str] = []


def check(ok: bool, name: str, detail: str = "") -> None:
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f"  — {detail}" if detail else ""))
    if not ok:
        fails.append(name)


with CSV.open() as fh:
    rows = list(csv.DictReader(fh))
n_all = len(rows)
region = lambda state: sum(1 for r in rows if r["STATE"] == state)
n_solar = sum(1 for r in rows if r["FUEL_TYPE"] == "Solar")
n_wind = sum(1 for r in rows if r["FUEL_TYPE"] == "Wind")
n_rez = sum(1 for r in rows if r["REZ"] == "Y")
# ELI near-term is blank for some units (wind farms with no published ELI figure) — the page pushes
# those to the end, so the "first row" assertion only applies to units that have a number.
with_eli = [r for r in rows if r["ELI_CURTAILMENT_NEAR"].strip()]
top = max(with_eli, key=lambda r: float(r["ELI_CURTAILMENT_NEAR"]))
duid_min = min(r["DUID"] for r in rows if r["DUID"].strip())
duid_max = max(r["DUID"] for r in rows if r["DUID"].strip())
print(f"csv: {n_all} generators · solar {n_solar} · wind {n_wind} · in REZ {n_rez} · "
      f"{len(with_eli)} with an ELI figure")

with sync_playwright() as pw:
    br = pw.chromium.launch()
    pg = br.new_page(viewport={"width": 1440, "height": 900})
    errors: list[str] = []
    pg.on("pageerror", lambda e: errors.append(str(e)))
    pg.on("console", lambda m: errors.append(m.text) if m.type == "error" else None)
    pg.goto(URL, wait_until="networkidle", timeout=60000)
    pg.wait_for_timeout(1500)
    nrows = lambda: pg.eval_on_selector_all("#tbody tr", "e => e.length")
    duids = lambda: pg.eval_on_selector_all("#tbody tr", "e => e.map(x => x.getAttribute('data-duid') || (x.children[1]||{}).innerText)")

    print("data")
    check(nrows() == n_all, f"all {n_all} generators render on load", f"{nrows()} rows")
    stats = " ".join(pg.eval_on_selector_all("#stats *", "e => e.map(x => x.innerText)"))
    check(str(n_solar) in stats and str(n_wind) in stats and str(n_rez) in stats,
          "the stat tiles quote the CSV counts (solar / wind / in REZ)", stats.replace("\n", " ")[:120])
    check(duids()[0] == top["DUID"], "the default sort (ELI near-term, desc) puts the top generator first",
          f"{duids()[0]} vs {top['DUID']}")

    print("tabs")
    for state in ("NSW", "VIC", "TAS"):
        pg.evaluate("""(label) => { const b = [...document.querySelectorAll('#tabs button, #tabs .seg-item')]
            .find(x => x.innerText.trim() === label); b.click(); }""", state)
        pg.wait_for_timeout(400)
        check(nrows() == region(state), f"{state} tab shows its {region(state)} generators", f"{nrows()} rows")
    pg.evaluate("""(() => { const b = [...document.querySelectorAll('#tabs button, #tabs .seg-item')]
        .find(x => x.innerText.trim() === 'All'); b.click(); })()""")
    pg.wait_for_timeout(400)

    print("filters")
    pg.select_option("#fuelFilter", "Wind")
    pg.wait_for_timeout(400)
    check(nrows() == n_wind, f"fuel=Wind filters to {n_wind}", f"{nrows()} rows")
    pg.select_option("#fuelFilter", "")
    pg.select_option("#rezFilter", "Y")
    pg.wait_for_timeout(400)
    check(nrows() == n_rez, f"REZ-only filters to {n_rez}", f"{nrows()} rows")
    pg.select_option("#rezFilter", "")
    pg.fill("#search", top["PROJECT_NAME"][:8])
    pg.wait_for_timeout(400)
    check(0 < nrows() <= 6, "search narrows the table", f"{nrows()} rows for {top['PROJECT_NAME'][:8]!r}")
    pg.fill("#search", "zzzz-no-such-project")
    pg.wait_for_timeout(400)
    check(nrows() == 0, "a search with no matches renders no rows", f"{nrows()} rows")
    pg.fill("#search", "")
    pg.wait_for_timeout(400)
    check(nrows() == n_all, "clearing the search restores every row", f"{nrows()} rows")

    print("sorting")
    click_header = """(label) => { const th = [...document.querySelectorAll('#thead th')]
        .find(x => x.innerText.trim() === label); th.click(); }"""
    pg.evaluate(click_header, "DUID")
    pg.wait_for_timeout(400)
    first_asc = duids()[0]
    pg.evaluate(click_header, "DUID")
    pg.wait_for_timeout(400)
    first_desc = duids()[0]
    check(first_asc == duid_min and first_desc == duid_max,
          "clicking a header sorts, clicking again reverses",
          f"{first_asc} (want {duid_min}) -> {first_desc} (want {duid_max})")

    print("selection and export")
    pg.evaluate("document.getElementById('selectAll').click()")
    pg.wait_for_timeout(600)
    check(pg.eval_on_selector_all("#tbody input.cb:checked, #tbody .cb:checked", "e => e.length") == n_all,
          "select-all selects every visible row")
    check(pg.get_attribute("#exportSelected", "disabled") is None,
          "the export button enables once something is selected")
    check(bool(pg.inner_text("#selCount").strip()), "the selection count is stated", pg.inner_text("#selCount"))
    with pg.expect_download(timeout=15000) as dl:
        pg.click("#exportSelected")
    check(dl.value.suggested_filename.endswith(".xlsx"),
          "the XLSX export actually downloads", dl.value.suggested_filename)
    pg.evaluate("document.getElementById('clearSelection').click()")
    pg.wait_for_timeout(400)
    check(pg.get_attribute("#exportSelected", "disabled") is not None, "clearing disables the export button")

    print("downloads and header")
    hrefs = pg.eval_on_selector_all("a[download]", "e => e.map(x => x.getAttribute('href'))")
    check(len(hrefs) == 6 and any(h.endswith("summary.csv") for h in hrefs),
          "the 5 regional workbooks + the CSV stay wired", f"{hrefs}")
    check("Source:" in pg.inner_text("#footer"), "the footer states the source",
          pg.inner_text("#footer")[-120:])
    sticky = pg.evaluate("""(() => { const w = document.querySelector('.table-wrap');
        const wt = w.getBoundingClientRect().top;
        w.scrollTop = 300;
        const th = document.querySelector('#thead th').getBoundingClientRect().top;
        return {offset: Math.round(th - wt), scrolled: w.scrollTop}; })()""")
    check(0 <= sticky["offset"] <= 40 and sticky["scrolled"] > 0,
          "the header stays pinned while the table scrolls", f"{sticky}")

    print("phone")
    phone = br.new_page(viewport={"width": 390, "height": 844}, device_scale_factor=2)
    phone.goto(URL, wait_until="networkidle", timeout=60000)
    phone.wait_for_timeout(1200)
    wrap = phone.evaluate("""(() => { const w = document.querySelector('.table-wrap');
        w.scrollLeft = 9999; return {sw: w.scrollWidth, cw: w.clientWidth, sl: w.scrollLeft}; })()""")
    check(wrap["sw"] > wrap["cw"] and wrap["sl"] > 0,
          "the dense table scrolls inside its wrapper at 390px", f"{wrap}")

    check(not errors, "no JS/console errors", "; ".join(errors[:3]))
    br.close()

print(f"\n{len(fails)} check(s) failed" if fails else "\nall interactions intact")
sys.exit(1 if fails else 0)
