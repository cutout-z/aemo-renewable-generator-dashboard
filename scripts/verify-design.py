#!/usr/bin/env python3
"""Render + token check for the AEMO Renewable Generator Dashboard — the gate this pass must leave green.

    cd ~/Design/"AEMO Renewable Generator Dashboard" && python3 -m http.server 9381 --bind 127.0.0.1 &
    /opt/anaconda3/bin/python3 scripts/verify-design.py            # checks only
    /opt/anaconda3/bin/python3 scripts/verify-design.py --screens  # + design/screens/after-*.png

Why a script and not an eyeball: the failures this page actually produces are invisible in a diff —
a component class Tailwind never emitted, a heat cell still carrying an inline `rgb()`, group-header
colours hard-coded to five different hexes, a theme flip that changes nothing, a phone-width
overflow on the filter row. Exit 1 = fix it.

The DOM contract it checks is written down in AGENTS.md (section "DOM contract").
"""
from __future__ import annotations

import argparse
import csv
import pathlib
import re
import sys

from playwright.sync_api import sync_playwright

ROOT = pathlib.Path(__file__).resolve().parent.parent
URL = "http://127.0.0.1:9381/index.html"
SCREENS = ROOT / "design" / "screens"
TOKEN_SRC = ROOT / "assets" / "css" / "tailwind.src.css"
PAGE = ROOT / "index.html"
GROUPS = ["Actual Curtailment", "ELI Projected", "Marginal Loss Factor",
          "ISP Curtailment Forecast", "ISP Offloading Forecast"]
CSV = ROOT / "outputs" / "summary.csv"

# The table's shape comes from the data file, not a count frozen on the day the gate was written.
# Metric columns are every csv column the page groups (actual, ELI, MLF, ISP), labels excluded.
with CSV.open() as _fh:
    _reader = csv.DictReader(_fh)
    N_GEN = sum(1 for _ in _reader)
    METRIC_COLS = [k for k in _reader.fieldnames or []
                   if k.startswith(("CURTAILMENT_ACTUAL_", "ELI_CURTAILMENT_", "MLF_", "ISP_CURTAILMENT_", "ISP_OFFLOADING_"))
                   and not k.endswith("_LABEL")]

fails: list[str] = []


def check(ok: bool, name: str, detail: str = "") -> None:
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f"  — {detail}" if detail else ""))
    if not ok:
        fails.append(name)


def rgb(value: str):
    v = (value or "").strip()
    if v.startswith("#"):
        v = v.lstrip("#")
        if len(v) == 3:
            v = "".join(c * 2 for c in v)
        try:
            return tuple(int(v[i:i + 2], 16) for i in (0, 2, 4))
        except ValueError:
            return None
    if v.startswith("rgb"):
        parts = v[v.index("(") + 1:v.index(")")].split(",")
        try:
            return tuple(int(float(p)) for p in parts[:3])
        except ValueError:
            return None
    return None


def token_sets() -> tuple[dict[str, str], dict[str, str]]:
    """Read both theme roots out of the token source.

    Two hazards, both already paid for in this family: the source explains itself in `/* … */` comments
    and one of them names a token (the `--faint` line's "4.9:1 on --surface"), and the heat ramp lives in
    a SECOND `:root` block further down the file — so a single split on the first `[data-theme="light"]`
    leaves `seq-*` unread. Strip comments, then take every declaration block whose selector is a theme
    root (`:root` = dark, `[data-theme="light"]` = light), later blocks overriding earlier ones."""
    css = re.sub(r"/\*.*?\*/", " ", TOKEN_SRC.read_text(), flags=re.S)
    grab = lambda s: dict(re.findall(r"--([a-z0-9-]+)\s*:\s*([^;]+);", s))
    dark: dict[str, str] = {}
    light: dict[str, str] = {}
    for m in re.finditer(r"([^{}]+)\{([^{}]*)\}", css):
        sel, body = m.group(1), m.group(2)
        if "data-theme" in sel:
            light.update(grab(body))
        elif ":root" in sel:
            dark.update(grab(body))
    return dark, light


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--screens", action="store_true", help="write design/screens/after-*.png")
    args = ap.parse_args()
    dark, light = token_sets()

    print("static")
    page = PAGE.read_text()
    check('href="assets/css/app.css"' in page, "index.html links assets/css/app.css",
          "no <link> to the compiled token layer")
    hexes = re.findall(r"#[0-9a-fA-F]{3,8}\b", page)
    check(not hexes, "no raw hex colour in index.html", f"found {sorted(set(hexes))}")
    check("data-theme" in page, "index.html carries the theme attribute")

    with sync_playwright() as pw:
        br = pw.chromium.launch()
        pg = br.new_page(viewport={"width": 1440, "height": 900})
        errors: list[str] = []
        pg.on("pageerror", lambda e: errors.append(str(e)))
        pg.goto(URL, wait_until="networkidle", timeout=60000)
        pg.wait_for_timeout(1500)

        print("tokens")
        check(pg.evaluate("async () => (await fetch('assets/css/app.css')).status") == 200,
              "assets/css/app.css is served (200)")
        check(rgb(pg.evaluate("getComputedStyle(document.body).backgroundColor")) == rgb(dark["bg"]),
              "body background is the --bg token",
              pg.evaluate("getComputedStyle(document.body).backgroundColor") + f" vs {dark['bg']}")
        n_cards = pg.eval_on_selector_all(".card", "e => e.length")
        check(n_cards >= 2, "the page is composed of .card panels", f"{n_cards} found")
        if n_cards:
            check(rgb(pg.evaluate("getComputedStyle(document.querySelector('.card')).backgroundColor")) == rgb(dark["surface"]),
                  ".card background is the --surface token")

        print("shell")
        kpi = pg.eval_on_selector_all(".kpi-value", "e => e.map(x => (x.innerText||'').trim())")
        check(len(kpi) >= 4 and all(kpi), "a KPI row renders (>= 4 valued tiles)",
              f"{len(kpi)} tiles: {kpi}")
        labels = pg.eval_on_selector_all(".kpi-label", "e => e.map(x => x.innerText.trim())")
        check(len(labels) >= 4 and all(labels), "every KPI carries a label", f"{labels}")
        keep = pg.eval_on_selector_all("#fuelFilter, #rezFilter, #search", "e => e.length")
        check(keep == 3, "the three filter controls survive (#fuelFilter, #rezFilter, #search)",
              f"{keep} of 3 found")

        print("the dense table")
        rows = pg.eval_on_selector_all("#tbody tr", "e => e.length")
        check(rows == N_GEN, f"{N_GEN} generators render on load (every row of the csv)", f"{rows} rows")
        head_rows = pg.eval_on_selector_all("#thead tr", "e => e.length")
        check(head_rows == 2, "the grouped header keeps its two rows", f"{head_rows}")
        head_txt = " ".join(pg.eval_on_selector_all("#thead th", "e => e.map(x => x.innerText)")).lower()
        missing = [g for g in GROUPS if g.lower() not in head_txt]
        check(not missing, "all five metric groups are named in the header", f"missing {missing}")
        th_bgs = pg.eval_on_selector_all(
            "#thead th", "e => [...new Set(e.map(x => getComputedStyle(x).backgroundColor))]")
        check(len(th_bgs) >= 5, "the metric groups stay distinguishable by colour",
              f"{len(th_bgs)} distinct header backgrounds")
        # Every metric cell carries `.h` (percent, MLF or N/A); selecting on its text would miss the MLF
        # cells, which print a bare 4-dp number.
        heat = pg.eval_on_selector_all(
            "#tbody td.h", "e => e.map(x => ({cls: x.className, txt: x.innerText.trim(), bg: getComputedStyle(x).backgroundColor}))")
        want_heat = N_GEN * len(METRIC_COLS)
        check(len(heat) == want_heat,
              f"the heat cells render ({want_heat}: {N_GEN} generators x {len(METRIC_COLS)} metric columns)",
              f"{len(heat)} cells")
        # `.seq-none` is the token file's own step for "no value"; a stated N/A is on the ramp system, not off it.
        on_ramp = r"\bseq-(\d|none)\b"
        seq = [c for c in heat if re.search(on_ramp, c["cls"] or "")]
        check(len(seq) == len(heat), "every heat cell uses the .seq-* ramp",
              f"{len(heat) - len(seq)} cells not on the ramp")
        check(not [c for c in heat if "rgb" in (c["bg"] or "") and not re.search(on_ramp, c["cls"] or "")],
              "no heat cell carries an inline rgb() colour")
        # A class can be present and still lose the cascade (a page rule out-ranking `.seq-*`), which
        # leaves the cell unfilled while every name-based check passes. Compare the pixels to the tokens.
        ramp_rgb = {rgb(dark[f"seq-{i}"]) for i in range(8) if f"seq-{i}" in dark}
        numeric = [c for c in heat if c["txt"] != "N/A"]
        unfilled = [c for c in numeric if rgb(c["bg"]) not in ramp_rgb]
        check(not unfilled, "every numeric heat cell is actually filled from the ramp",
              f"{len(unfilled)} of {len(numeric)} unfilled, e.g. {unfilled[:2]}")
        steps = {int(re.search(r"\bseq-(\d)\b", c["cls"]).group(1)) for c in seq if re.search(r"\bseq-(\d)\b", c["cls"])}
        check(len(steps) >= 4, "the ramp is graded, not one flat step", f"steps used: {sorted(steps)}")
        na = [c for c in heat if c["txt"] == "N/A"]
        check(bool(na), "unavailable ISP values are stated as N/A, not left blank", f"{len(na)} N/A cells")
        legend = pg.evaluate("document.body.innerText")
        check("0%" in legend and "100%" in legend, "the heat scale is stated on screen (0% … 100%)")
        sticky = pg.eval_on_selector_all("#thead th", "e => e.map(x => getComputedStyle(x).position)")
        check(all(s == "sticky" for s in sticky), "header cells stay sticky", f"{set(sticky)}")

        print("open items")
        # The served shell must be the loaded page's height. Read with JS off: that is literally what the
        # browser paints first, so the comparison is deterministic rather than a race. This page used to
        # grow 526 px (58 %) the moment the CSV landed, shoving everything below the skeleton off screen.
        shapes = {}
        for w in (1440, 900, 390):
            c0 = br.new_context(viewport={"width": w, "height": 900}, java_script_enabled=False)
            s0 = c0.new_page(); s0.goto(URL, wait_until="load", timeout=60000); s0.wait_for_timeout(400)
            shell = s0.evaluate("() => ({doc: document.documentElement.scrollHeight,"
                                " bars: document.querySelectorAll('.skeleton').length})")
            c0.close()
            c1 = br.new_context(viewport={"width": w, "height": 900})
            l1 = c1.new_page(); l1.goto(URL, wait_until="networkidle", timeout=60000); l1.wait_for_timeout(1200)
            loaded = l1.evaluate("() => document.documentElement.scrollHeight")
            left = l1.evaluate("() => document.querySelectorAll('[data-shell]').length")
            c1.close()
            shapes[w] = {"shell": shell["doc"], "bars": shell["bars"], "loaded": loaded, "left": left}
        for w, s in shapes.items():
            gap = abs(s["loaded"] - s["shell"])
            check(gap <= 0.2 * max(s["loaded"], 1),
                  f"the served shell reserves the page height at {w}px (loading does not jump the layout)",
                  f"shell {s['shell']} px vs loaded {s['loaded']} px ({gap / max(s['loaded'], 1):.0%})")
        check(min(s["bars"] for s in shapes.values()) >= 15,
              "the served shell renders real skeleton content, not a blank page",
              f"{ {w: s['bars'] for w, s in shapes.items()} }")
        check(all(s["left"] == 0 for s in shapes.values()),
              "every shell placeholder is replaced once the data lands (none lingers)",
              f"{ {w: s['left'] for w, s in shapes.items()} }")

        # Sticky chrome must stay ONE row: a wrapped bar pins ~104 px of two-row chrome over the table it
        # exists to help you read. This page's bar is `#controls`; below the wrap width it stacks and is not
        # sticky, which is allowed — the invariant is only that a sticky bar is one row.
        bar = {}
        for w in (1440, 1280, 1100, 900, 768, 390):
            pg.set_viewport_size({"width": w, "height": 900})
            pg.wait_for_timeout(400)
            bar[w] = pg.evaluate("""(() => { const el = document.getElementById('controls');
                if (!el) return null; const r = el.getBoundingClientRect();
                return {h: Math.round(r.height), pos: getComputedStyle(el).position}; })()""")
        tall = {w: b for w, b in bar.items() if b and b["pos"] == "sticky" and b["h"] > 72}
        check(not tall, "wherever the control bar is sticky it is one row", f"{tall}")
        check(all(b is None or b["h"] <= 72 or b["pos"] != "sticky" for b in bar.values()),
              "no width pins a two-row control bar", f"{ {w: (b['pos'], b['h']) for w, b in bar.items()} }")
        pg.set_viewport_size({"width": 1440, "height": 900})
        pg.wait_for_timeout(300)
        check(pg.evaluate("() => { const el = document.getElementById('loading');"
                          " return !!el && getComputedStyle(el).display === 'none'; }"),
              "#loading is still the element the app removes once the table lands")

        print("themes and phone")
        flip = pg.evaluate("""(() => { const r = document.documentElement;
            const before = getComputedStyle(document.body).backgroundColor;
            r.setAttribute('data-theme','light');
            const after = getComputedStyle(document.body).backgroundColor;
            r.setAttribute('data-theme','dark'); return {before, after}; })()""")
        check(rgb(flip["after"]) == rgb(light["bg"]),
              "the light theme flips body to the light --bg token", f"{flip['after']} vs {light['bg']}")

        phone = br.new_page(viewport={"width": 390, "height": 844}, device_scale_factor=2)
        phone.goto(URL, wait_until="networkidle", timeout=60000)
        phone.wait_for_timeout(1200)
        over = phone.evaluate("document.documentElement.scrollWidth - document.documentElement.clientWidth")
        check(over <= 1, "no page-level horizontal overflow at 390px", f"{over}px over")

        state = br.new_page(viewport={"width": 1440, "height": 900})
        state.route("**/outputs/summary.csv", lambda r: r.abort())
        state.goto(URL, wait_until="domcontentloaded", timeout=60000)
        state.wait_for_timeout(1500)
        state_rows = state.eval_on_selector_all(
            ".state", "e => e.map(x => ({txt: x.innerText, h: x.getBoundingClientRect().height}))")
        check(bool(state_rows) and max(s["h"] for s in state_rows) > 0,
              "a missing data file renders a VISIBLE .state panel, not a bare error string",
              f"{len(state_rows)} .state element(s), tallest {max([s['h'] for s in state_rows], default=0):.0f}px")
        check(not errors, "no JS errors on load", "; ".join(errors[:3]))

        if args.screens:
            SCREENS.mkdir(parents=True, exist_ok=True)
            for fname, page_obj, full in (("after-top.png", pg, False), ("after-full.png", pg, True),
                                          ("after-phone.png", phone, True)):
                page_obj.screenshot(path=str(SCREENS / fname), full_page=full)
            state.screenshot(path=str(SCREENS / "after-nodata.png"))
            print(f"  wrote screenshots to {SCREENS}")

        br.close()

    print(f"\n{len(fails)} check(s) failed" if fails else "\nall checks passed")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
