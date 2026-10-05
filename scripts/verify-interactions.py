#!/usr/bin/env python3
"""Behaviour check for the AEMO Renewable Generator Dashboard — every interaction a design pass must
NOT lose: region tabs, fuel/REZ filters, search, column sorting, select-all, the XLSX export of the
selection, the sticky two-row header and the Excel/CSV downloads.

    cd ~/Design/"AEMO Renewable Generator Dashboard" && python3 -m http.server 9381 --bind 127.0.0.1 &
    /opt/anaconda3/bin/python3 scripts/verify-interactions.py

Green TODAY, on the unstyled page, and it must still be green at handback. Numbers come from
outputs/summary.csv itself. Reads only; writes nothing (the XLSX is captured, not saved).
Exit 1 on any breakage.
"""
from __future__ import annotations

import csv
import io
import pathlib
import sys
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

import openpyxl
from playwright.sync_api import sync_playwright

ROOT = pathlib.Path(__file__).resolve().parent.parent
URL = "http://127.0.0.1:9381/index.html"
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
by_duid = {r["DUID"]: r for r in rows}
duid_min = min(r["DUID"] for r in rows if r["DUID"].strip())
duid_max = max(r["DUID"] for r in rows if r["DUID"].strip())
print(f"csv: {n_all} generators · solar {n_solar} · wind {n_wind} · in REZ {n_rez} · "
      f"{len(with_eli)} with an ELI figure")

# ── The page's numbers, recomputed from the csv alone ──────────────────────────────────────────
# Mirrors detectColumns / renderTable / renderStats in index.html, but rounds on the exact decimals
# in the file (Decimal, half-up) — a binary-float `toFixed` that lands on the other side of a .5 is
# exactly the kind of wrong figure this is here to catch.
STATES = {"NSW1": "NSW", "QLD1": "QLD", "VIC1": "VIC", "SA1": "SA", "TAS1": "TAS"}
KEYS = list(rows[0].keys())
LOSS_EDGES = [Decimal(e) for e in ("0.01", "0.025", "0.05", "0.10", "0.20", "0.40", "0.70")]
MLF_EDGES = [Decimal(e) for e in ("1.00", "0.98", "0.96", "0.94", "0.92", "0.90", "0.85")]
HALF_UP = lambda d, q: d.quantize(Decimal(q), ROUND_HALF_UP)


def num(v: str | None) -> Decimal | None:
    try:
        d = Decimal((v or "").strip())
    except InvalidOperation:
        return None
    return d if d.is_finite() else None


def columns(scope: str) -> list[tuple[str, str]]:
    """(key, group) in the page's column order for a tab ('ALL' adds the State column)."""
    cols = [("DUID", "meta"), ("PROJECT_NAME", "meta"), ("FUEL_TYPE", "meta")]
    if scope == "ALL":
        cols.append(("STATE", "meta"))
    cols += [("NAMEPLATE_MW", "meta"), ("REZ", "meta"), ("REZ_NAME", "meta")]
    cols += [(k, "actual") for k in sorted(k for k in KEYS if k.startswith("CURTAILMENT_ACTUAL_"))]
    cols += [(k, "eli") for k in ("ELI_CURTAILMENT_NEAR", "ELI_CURTAILMENT_MED")]
    cols += [(k, "mlf") for k in sorted(k for k in KEYS if k.startswith("MLF_"))]
    for pre, grp in (("ISP_CURTAILMENT_", "isp-c"), ("ISP_OFFLOADING_", "isp-o")):
        cols += [(k, grp) for k in sorted(k for k in KEYS if k.startswith(pre + "FY") and not k.endswith("_LABEL"))]
        cols.append((pre + "AVG", grp))
    return [(k, g) for k, g in cols if k in KEYS]


def cell(r: dict[str, str], key: str, group: str) -> tuple[str, str | None]:
    """(text, ramp class) the page should print for one cell."""
    raw, v = r[key], num(r[key])
    if group == "meta":
        if key == "NAMEPLATE_MW" and v is not None:      # whole MW; under 1 MW to 2 dp, never "0"
            return str(HALF_UP(v, "0.01" if abs(v) < 1 else "1")), None
        if key == "REZ_NAME" and raw == "Non-REZ":      # a source states the unit is outside a zone
            return "Outside a REZ", None
        if key in ("REZ", "REZ_NAME") and r["REZ"] not in ("Y", "N"):   # blank = unknown
            return "Unknown", None
        return raw.strip(), None
    if v is None:
        return "N/A", "seq-none"
    if group == "mlf":
        step = next((i for i, e in enumerate(MLF_EDGES) if v >= e), 7)
        return str(HALF_UP(v, "0.0001")), f"seq-{step}"
    return f"{HALF_UP(v * 100, '0.1')}%", f"seq-{sum(v >= e for e in LOSS_EDGES)}"


def base_rows(scope: str) -> list[dict[str, str]]:
    return rows if scope == "ALL" else [r for r in rows if r["STATE"] == STATES[scope] or r["REGIONID"] == scope]


def expected_tiles(scope: str) -> list[list[str]]:
    base = base_rows(scope)
    n = len(base)
    solar = sum(r["FUEL_TYPE"] == "Solar" for r in base)
    wind = sum(r["FUEL_TYPE"] == "Wind" for r in base)
    rez = sum(r["REZ"] == "Y" for r in base)
    out = sum(r["REZ"] == "N" for r in base)
    mlf_key = [k for k, g in columns(scope) if g == "mlf"][-1]
    mlf = [v for r in base if (v := num(r[mlf_key])) is not None]
    near_rows = [r for r in base if num(r["ELI_CURTAILMENT_NEAR"]) is not None]
    near = [num(r["ELI_CURTAILMENT_NEAR"]) for r in near_rows]
    near_fuels = sorted({r["FUEL_TYPE"] for r in near_rows})
    near_scope = f" · {near_fuels[0].lower()} farms only" if len(near_fuels) == 1 else ""
    n_of = lambda k: f"{k} of {n} with a value" if k else "none with a value"
    where = "NEM" if scope == "ALL" else STATES[scope]
    return [
        [str(n), "Generators", where],
        [f"{solar} · {wind}", "Solar · wind farms", f"split of {n}"],
        [str(rez), "In a REZ", f"{out} outside · {n - rez - out} unknown"],
        [str(HALF_UP(sum(mlf) / len(mlf), "0.0001")) if mlf else "N/A",
         f"Avg MLF, {mlf_key.replace('MLF_', '')}", n_of(len(mlf))],
        [f"{HALF_UP(sum(near) / len(near) * 100, '0.1')}%" if near else "N/A",
         "Avg ELI near-term curtailment, 2026-28", n_of(len(near)) + (near_scope if near else "")],
    ]

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
    check(duids()[0] == top["DUID"], "the default sort (ELI near-term, desc) puts the top generator first",
          f"{duids()[0]} vs {top['DUID']}")

    print("values match the csv (every tab: the five tiles, every cell and its ramp step)")
    for scope, tab in [("ALL", "All")] + list(STATES.items()):
        pg.evaluate("""(label) => { const b = [...document.querySelectorAll('#tabs button, #tabs .seg-item')]
            .find(x => x.innerText.trim() === label); b.click(); }""", tab)
        pg.wait_for_timeout(400)
        tiles = pg.eval_on_selector_all("#stats > div", "e => e.map(t => [...t.children].map(c => c.innerText.trim()))")
        want_tiles = expected_tiles(scope)
        check(tiles == want_tiles, f"{tab}: the five stat tiles equal the figures recomputed from the csv",
              f"page {tiles} vs csv {want_tiles}")
        cols = columns(scope)
        page_cols = pg.eval_on_selector_all("#thead th[data-col]", "e => e.map(x => x.dataset.col)")
        check(page_cols == [k for k, _ in cols], f"{tab}: the columns are the csv's, in the page's order",
              f"{len(page_cols)} on page vs {len(cols)} expected")
        page = {r[0]: r[1:] for r in pg.eval_on_selector_all(
            "#tbody tr", "e => e.map(tr => [tr.dataset.duid, ...[...tr.children].slice(1).map(td => "
                         "[td.innerText.trim(), (td.className.match(/\\bseq-(\\d|none)\\b/) || [null])[0]])])")}
        want = {r["DUID"]: [list(cell(r, k, g)) for k, g in cols] for r in base_rows(scope)}
        missing = [d for d in want if d not in page]
        extra = [d for d in page if d not in want]
        bad = [(d, k, page[d][i], want[d][i]) for d in want if d in page
               for i, (k, _) in enumerate(cols) if i >= len(page[d]) or page[d][i] != want[d][i]]
        check(not missing and not extra and not bad,
              f"{tab}: every cell equals summary.csv ({len(want)} generators x {len(cols)} columns, text and step)",
              f"missing {missing[:2]}, extra {extra[:2]}, {len(bad)} wrong, e.g. {bad[:3]}")
        count = pg.inner_text("#rowCount").strip()
        check(count == f"{len(want)} of {n_all} shown", f"{tab}: the row count is stated", count)
    # REZ is Y / N / blank. A generator may be called "outside" a REZ only where the data says N; a
    # blank is unknown, and its ISP N/A tooltips must say so (all 108 wind farms are blank today).
    tips = pg.evaluate("""() => [...document.querySelectorAll('#tbody tr')].map(tr => [tr.dataset.duid,
        [...tr.querySelectorAll('td.na[title]')].map(td => td.title).filter(t => /ISP/.test(t))])""")
    wrong = [(d, t[:70]) for d, ts in tips for t in ts
             if ("outside" in t) != (by_duid[d]["REZ"] == "N")
             or (by_duid[d]["REZ"] not in ("Y", "N")) != ("no REZ is known" in t)]
    check(tips and not wrong, "ISP N/A tooltips say outside only where REZ = N, and unknown where REZ is blank",
          f"{len(wrong)} wrong, e.g. {wrong[:2]}")
    # ISP year headers come from the first row that carries a label (blank-REZ rows carry none)
    labels = {k: next((r[k] for r in rows if r[k].strip()), "") for k in KEYS if k.endswith("_LABEL")}
    heads = dict(pg.eval_on_selector_all("#thead th[data-col]", "e => e.map(x => [x.dataset.col, x.innerText.trim()])"))
    bad_heads = {k[:-6]: (heads.get(k[:-6]), v) for k, v in labels.items() if v and heads.get(k[:-6]) != v}
    check(labels and not bad_heads, "the ISP year headers read the data's FY labels", f"{bad_heads}")

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
    n_unknown = sum(1 for r in rows if r["REZ"] not in ("Y", "N"))
    pg.select_option("#rezFilter", "?")
    pg.wait_for_timeout(400)
    check(nrows() == n_unknown, f"REZ-unknown filters to {n_unknown}", f"{nrows()} rows")
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
    # Blanks last in BOTH directions, every value present in order, and the loaded data left in file
    # order (sorting allData in place reordered what the ISP labels and the export read).
    n_eli = len(with_eli)
    for direction in ("asc", "desc"):
        pg.evaluate("([c, d]) => { sortCol = c; sortDir = d; renderTable(); }", ["ELI_CURTAILMENT_NEAR", direction])
        order = duids()
        vals = [Decimal(by_duid[d]["ELI_CURTAILMENT_NEAR"]) for d in order[:n_eli] if by_duid[d]["ELI_CURTAILMENT_NEAR"].strip()]
        tail_blank = all(not by_duid[d]["ELI_CURTAILMENT_NEAR"].strip() for d in order[n_eli:])
        check(len(vals) == n_eli and vals == sorted(vals, reverse=direction == "desc") and tail_blank,
              f"sorting ELI near-term {direction}: values in order, the {n_all - n_eli} blanks last",
              f"{len(vals)} valued first; blanks last: {tail_blank}")
    file_order = pg.evaluate("allData.map(r => r.DUID)")
    check(file_order == [r["DUID"] for r in rows], "sorting leaves the loaded data in file order",
          f"first {file_order[:3]} vs csv {[r['DUID'] for r in rows[:3]]}")

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
    # Its header row: every column once, State always present, and every metric named with its group
    # (the table's short labels — "FY25-26", "FY1", "Avg" — repeat across groups).
    xl = openpyxl.load_workbook(io.BytesIO(pathlib.Path(dl.value.path()).read_bytes()), read_only=True)
    xl_heads = [c.value for c in next(xl.active.iter_rows(max_row=1))]
    groups = ("Actual curtailment", "ELI projected", "Marginal loss factor", "ISP curtailment forecast", "ISP offloading forecast")
    metric_heads = xl_heads[len([k for k, g in columns("ALL") if g == "meta"]):]
    dup = sorted({h for h in xl_heads if xl_heads.count(h) > 1})
    unnamed = [h for h in metric_heads if not str(h).startswith(groups)]
    check(not dup and "State" in xl_heads and not unnamed and len(metric_heads) == len([k for k, g in columns("ALL") if g != "meta"]),
          "the export's headers are unambiguous (each once, State kept, metrics named by group)",
          f"duplicated {dup}; without a group {unnamed[:3]}; State {'State' in xl_heads}")
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
