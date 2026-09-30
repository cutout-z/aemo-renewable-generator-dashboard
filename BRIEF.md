# BRIEF — AEMO Renewable Generator Dashboard redesign, pass 1

**Read `CLAUDE.md` → `AGENTS.md` (the contract) before writing code.** The first line of your session
should be `git status` and `git rev-parse --short HEAD` — if git fails here, stop and say so rather
than working around it.

*Handover prompt (if the session is started fresh, this is all that needs saying):*
> Project folder `~/Design/AEMO Renewable Generator Dashboard`. Read `CLAUDE.md`, then `AGENTS.md`,
> then execute `BRIEF.md` — and stop after step 1 to report.

## The goal, stated as an outcome

The dashboard is functionally rich and visually poor: a 2019-era HTML table app whose own data is
harder to read than it should be. Make it **look like it was designed by someone who cares** and like
it belongs to the same product family as the other AEMO dashboards — modern, restrained, scannable.

**Definition of done = the real page, rendering real data, looking right when screenshotted.**
Adopting a token file, adding a stylesheet, "styling centralised", or a theme module is NOT done. A
sibling pass delivered exactly that and nothing visible changed on 37 of 39 pages.

## Hard constraints

- **It stays static.** GitHub Pages serves the repo root — one page, no server, no build step at
  deploy, no self-hosting. Everything must work as plain files a browser loads.
- **Interactivity is client-side only**, and it is the product: region switching, the fuel/REZ
  filters, search, column sorting, select-all, the client-side XLSX export of the selection, the six
  downloads, and the sticky two-row header. All of it must still work when you are done.
- **One page.** `index.html`. Do not split it, do not add a framework.
- **No new runtime dependencies.** PapaParse and SheetJS stay the only scripts. No chart library:
  the heat is HTML cells.
- **Presentation only** — `AGENTS.md` lists the forbidden trees.

## The design language — adopt it, do not invent a second one

| Where | What |
|---|---|
| `assets/css/tailwind.src.css` | **The tokens.** Surfaces, text, status, radii, shadows, both themes, and the sequential ramp `--seq-0…7` / `.seq-0…7`. The only file with literal colours. |
| `design/design-tokens.md` | The seven rules (colour = entity, contrast floor, heat needs a stated scale, …) |
| `design/tokens.html` | **The proof page** — palette, type scale, controls, a dense table with grouped columns, the ramp, the states. Open it first (`http://127.0.0.1:9370/design/tokens.html`). |
| `~/Design/aemo-credit-design` | The sibling AEMO pass (same tokens, 14 charts). Match its language; do not copy its markup. |

The rules that matter most here: **colour belongs to the metric family, not to a column index**;
**status colour is never the only signal** (a cell prints its number, a group header carries words);
**the heat scale is stated on screen**; **`--faint` is the floor for 12px text**; **`.card` is the unit
of composition and its foot carries provenance — including which publication rhythm the panel shows**.

## Baseline — measured on this clone, 2026-09-30, before the pass

`python3 scripts/verify-design.py`: **14 of 24 checks fail**; `scripts/verify-interactions.py`: 22
checks, exit 0. Page facts: **1,098 px tall at 1440×900**, **240 generator rows**, 27 header cells
over two header rows, 5 KPI-ish tiles, 2 select filters + 1 search box, a table box ~2,073 px wide
that scrolls internally, **2,477 heat cells**, **1,288 `N/A` cells**, footer reading
"240 generators tracked". Phone 390px: **the document is 469 px wide — 79 px of page-level sideways
overflow** (the controls row cannot wrap).

The failures, and the visible problems behind them:

1. **No shell.** Five unaligned horizontal bands — KPI tiles, tabs+filters, action buttons, table box,
   footer — each with its own padding and none of them a `.card`. Nothing states what the page is for.
2. **The stat tiles are ad hoc** (`.stat-label` 0.75rem uppercase + `.stat-value` 1.3rem) instead of
   the family's `.kpi-value` / `.kpi-label`, and one of them ("Avg ELI Near Curtailment") is coloured
   green or red by a 30% threshold — status colour used as decoration on a value that is not a status.
3. **Five metric groups are identified by five literal hexes** (`#5a4a8a` actual/ELI purple, `#2e7d32`
   MLF green, `#c62828` ISP-curtailment red, `#e65100` ISP-offloading orange, `#00695c` actual teal),
   applied as a **solid fill across the entire two-row header** — so the loudest thing on the page is
   the header, and "red" here means a *category*, colliding with the family's `.pill-bad`.
4. **Two hand-rolled heat scales, computed per cell as inline `rgb()`** — `curtailmentColor()` is a
   three-stop traffic-light (green `#63BE7B` → yellow `#FFEB84` → red `#F8696B`, 0 → 5% → 20%+) and
   `mlfColor()` is a diverging red→yellow→green around 0.80/0.95/1.05 — with **no legend and no stated
   stops** anywhere on the page and a `textColorForBg()` contrast guess per cell.
5. **Every numeric cell is centred**, MLF at 4 dp and percentages at 1 dp, no tabular figures: you
   cannot scan a column.
6. **2019 table affordances**: `.tab` buttons with rounded top corners, a hard-coded `th:hover`
   (`#3a62a4`), sort shown only as ▲/▼ appended to the sorted header, header text uppercase
   letter-spaced at 0.78rem.
7. **The filters are unstyled native controls** in a row with `margin-left:auto`; the action row is
   plain buttons; the selection state is the string "N assets selected".
8. **`N/A` (1,288 cells) is honest but silent** — italic muted text with a tooltip on some cells,
   a bare "-" on others. The reason ("ISP forecasts are REZ-only") must be stated where a reader meets
   it, not only in a hover.
9. **No light theme, no toggle, no `?theme=` forcing.**
10. **Phone:** 79 px of page-level overflow; the table is 2,073 px wide and scrolls inside the wrapper
    (correct for numbers) but the generator name and DUID scroll out of view.
11. **No empty/failure state**: with `outputs/summary.csv` blocked the tiles are empty, the table is
    gone and a bare sentence appears; the export button stays silently disabled.

## Order of work

One commit per step. **Stop after step 1 and report before scaling to the rest.**

1. **Wire the token layer and the shell.** Add the `assets/css/app.css` link *before* the existing
   inline `<style>` so nothing shifts by accident, then convert the page chrome: frame, background,
   typography, the title/subtitle block, the tiles (`.kpi-value` / `.kpi-label`), the tab row
   (`.seg` / `.seg-item`), the controls (`.input`, `.btn`), the footer (`.card-foot`). Once the old
   inline rules for those elements are gone, delete the inline reset — `preflight` is already ON in
   `tailwind.config.js` and must end up as the page's *only* reset — then rebuild with
   `./scripts/build-css.sh` and commit `assets/css/app.css`. Report here.
2. **Header + KPI strip.** 240 generators · solar/wind split · in-REZ count · average MLF (name the
   FY you are averaging) · average near-term curtailment — each with its label, all computed from the
   CSV, the as-of/lag in the card foot, and **no status colour on a value that is not a status**.
3. **One control bar.** Region as `.seg`, search as `.input`, the fuel and REZ filters restyled as
   `.input`/`.seg` — **keeping the ids `#fuelFilter`, `#rezFilter`, `#search` and every behaviour**,
   they are the wiring the gates use — counts as `.badge`s, and the selection count surfaced where the
   export lives. It should stay put while the table scrolls.
4. **The dense table.** `.card` with head (title + what the columns mean) → body (the table) → foot
   (`Source:`, as-of, the two publication rhythms). Two-row grouped header: the five families named,
   each with a distinguishable **token** colour (no literal hex), numerals right-aligned in tabular
   figures. Heat cells become `td.seq-0…7` with **the scale stated on screen** — for curtailment *and*
   for MLF; if one ramp cannot honestly carry both, define the second scale in the token file and say
   why in the commit message. `N/A` stays stated, with its REZ reason visible. Both header rows sticky;
   pin the generator/DUID column on phone widths.
5. **Selection and export.** Row + select-all checkboxes, `.btn` export/clear, keyboard reachable, the
   SheetJS export still writing the same columns under a recognisable filename.
6. **States.** Loading (skeleton rows), missing file (`.state` naming `outputs/summary.csv`), and a
   filter combination with no matches — stated in the table area, never a silently empty box.
7. **Themes.** Dark default; a toggle remembered per viewer; `?theme=light` / `?theme=dark` force one.
   Every surface, label and heat step must flip from the tokens alone.
8. **Phone (390px).** No page-level sideways scroll (79 px today), controls wrap, the table scrolls
   inside its card, tap targets ≥ 32px.
9. **The interactions you must not lose** — the 22 checks in `scripts/verify-interactions.py`.

## Evidence (part of done, not optional)

- `scripts/verify-design.py` exits **0** (24 checks). `--screens` writes `design/screens/after-*.png`.
- `scripts/verify-interactions.py` still exits **0**, same check count — including the downloaded XLSX.
- `/opt/anaconda3/bin/python3 tests/validate_outputs.py` exits 0 ("All validations passed").
- Screenshots for every surface you changed, desktop **and** phone, at the same positions as
  `before-*.png` (top, full, phone, no-data). Evidence lives in `design/screens/` — never next to the
  page, never a new root-level folder.
- Re-verify the export by hand once: select all → Export → open the workbook and confirm the columns
  are the ones the table shows.
- `./scripts/build-css.sh` run after the last class change, `assets/css/app.css` committed.
- `git diff --stat main` shows no `outputs/**`, `data/**`, `src/**`, `deploy/**`, `tests/**`,
  `.github/**`.

## Report back

Surfaces changed of the ones that exist; what is half-done; any decision the brief did not cover (in
particular: how you mapped curtailment and MLF onto the ramp, and what the legends say). Do not claim a
surface is done because the HTML changed — it is done when a browser renders it with real data and the
gate says so.
