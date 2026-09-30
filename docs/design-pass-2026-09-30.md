# Design pass, 2026-09-30: what changed and why

Branch `design/2026-10`. Presentation only: `outputs/`, `data/`, `src/`, `deploy/`, `tests/`, `.github/`
are untouched (`git diff --stat main` over those trees is empty).

## What changed

| File | Change |
|---|---|
| `index.html` | Rebuilt on the token layer: shell, header, KPI card, one sticky control bar, the dense table card, states, theme toggle. One inline `<style>` remains (table only, tokens only, no hex) and one inline script. |
| `assets/css/app.css` | Rebuilt with `./scripts/build-css.sh` (committed; Pages serves the repo). |
| `scripts/verify-design.py` | Two regexes now accept `seq-none` as on-ramp (see "Bent"), on top of Hermes's `58fb9d3` comment-stripping fix. |
| `design/screens/after-*.png` | Evidence, same positions as `before-*`: top, full, phone, nodata (+ `after-light-top.png`). |

Visual summary: neutral dark canvas, one card per unit (KPI strip, control bar, table). The old solid
five-colour header is now a quiet tint plus a 3px rule, a dot and the family name. Heat is one
blue ramp with the scale printed above the table. Light theme is one attribute flip.

## Decisions the brief did not cover

- **Family colours** use existing tokens, the family convention (`info, accent, good, warn, neutral`),
  and skip `--bad` so a category is never red. No token-file edit. Colour is never the only signal:
  each group header carries its name; each column header repeats the family rule.
- **Heat: one ramp, two stated scales.** On both, stronger fill = more lost.
  - Share of energy (actual, ELI, ISP curtailment, ISP offloading): steps at <1, 2.5, 5, 10, 20, 40, 70, 100%.
  - MLF as price lost (1 - MLF): steps at >=1.00, 0.98, 0.96, 0.94, 0.92, 0.90, 0.85, <0.85.
  One ramp can carry both honestly because both mean "more is lost" (README: MLF 0.90 = 90% of the
  regional price). The second scale did not need defining in the token file. Both legends are built
  from the same arrays that pick the step, so they cannot drift from the cells.
  The old per-cell green/yellow/red is gone; a low MLF no longer reads as "red = bad" against a `.pill-bad`.
- **N/A**: every missing value is now the word `N/A` (previously 1,288 `N/A` + 410 bare "-"; now 1,698 `N/A`).
  Reason is on screen in the legend, and per cell in the tooltip. All 161 non-REZ generators are N/A on every ISP
  column, all 136 missing ELI values are non-REZ, and the legend says so.
- **KPI averages** are unweighted means over generators with a value and each tile says how many that is
  (only 104 of 240 have a near-term ELI figure). They follow the region tab, not the filters.
- **No as-of date.** `summary.csv` carries none, so the foot says so and names the publication rhythms
  (monthly actuals; annual July ELI/ISP; annual July/Oct MLF, per README). Adding a stamp is a data-plane change.
- The control bar is sticky from `sm` up only; on phone a wrapped bar would cover half the screen.
- Checkbox + DUID are pinned on every width (the brief asked for phone; the table scrolls sideways on desktop too).
- Solar/Wind text colours dropped (no token for them; the word says it).

## Verified

- `tests/validate_outputs.py` exit 0. `scripts/verify-interactions.py` exit 0, 22 checks (incl. XLSX download).
  `scripts/verify-design.py` exit 0, all checks passed.
- Real browser, real data: 240 rows, 2,477 heat cells on steps 0-7, 1,698 `N/A`; dark, light (`?theme=light`),
  390px phone (0px page overflow, every interactive target >= 32px), blocked `outputs/summary.csv`
  (`.state` panel, N/A tiles, Export disabled with a title), no-match search (state panel, header kept).
- Keyboard: headers sort on Enter/Space (focus kept, `aria-sort` set); Space toggles a row checkbox.
- Export by hand: select all -> Export -> `curtailment_240_assets.xlsx`, 240 rows x 21 columns
  = the table's columns; a spot-checked generator (MANSLR1) matches the CSV.

## Bent, or worth a second look

- **Gate edit.** Two checks compared heat cells against `seq-0..7` only, so a stated `N/A` (token class
  `seq-none`) failed as "off the ramp". I widened those two regexes to `seq-(\d|none)`; the "graded steps"
  check still counts `seq-0..7` only. The alternative, `seq-0` on N/A cells, would make "no data" look like
  the lowest step. Revert `scripts/verify-design.py` if you disagree.
- **The gate cannot see** a heat cell with no fill: it once passed while every cell rendered light text on the surface
  colour (my selector out-ranked `.seq-*`). The `.state` check is also satisfied by a hidden element. Screenshots, not the
  gate, caught both; do not read "exit 0" as "looks right".
- **Commits**: steps 4-7 were written as one rewrite of the table/theme code and are one commit, not four.
- **XLSX headers** are unchanged (same columns/filename) but the ISP headers read `FY1/FY2/FY3` twice, not the resolved
  years the table shows. Pre-existing; left alone because the brief said "same columns".
- The ELI "near" column is blank for 136 generators, so its sort puts ties and blanks last.

## Not done

- Not pushed. Everything is committed on `design/2026-10` only; `main` is untouched.
