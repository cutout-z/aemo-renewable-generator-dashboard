# Design-pass contract — AEMO Renewable Generator Dashboard

Read this before touching anything. `BRIEF.md` says *what* to build; this says how work here is
allowed to happen.

## The five hard rules

1. **Work on the branch, never on `main`.** This copy is on `design/2026-10`. `main` is published:
   GitHub Pages serves this repository and the NAS lane pushes `outputs/**` + `data/*.feather` to it.
   Nothing lands on `main` until Zalen has looked at the rendered page.
2. **Presentation only.** Never touch the data or the pipeline: `outputs/**`, `data/**`, `src/**`,
   `deploy/**`, `.github/**`, `requirements.txt` and `tests/**` are off limits. This pass changes how
   the page looks, never what it says.
3. **Never invent data.** Every number on screen comes from `outputs/summary.csv` (240 rows × 31
   columns) and is traceable to the source named in the card foot. No mock arrays, no sample series,
   no "for now" values. A missing value stays `N/A` — that is an answer, not a gap to fill.
4. **Verify in a browser, not by grep.** Done = a real browser renders it with real data: element
   present, height > 0, text non-empty. An HTTP 200 on the HTML proves nothing about a visual change.
5. **Keep the three gates green.** They are committed and they are the handback evidence:

   | command | today (measured 2026-09-30, before the pass) | at handback |
   |---|---|---|
   | `/opt/anaconda3/bin/python3 tests/validate_outputs.py` | exit 0 — "All validations passed" | exit 0 (the data gate; not yours to edit) |
   | `/opt/anaconda3/bin/python3 scripts/verify-interactions.py` | exit 0 — 22 checks, "all interactions intact" | exit 0, unchanged |
   | `/opt/anaconda3/bin/python3 scripts/verify-design.py` | **exit 1** — 14 of 24 checks fail (listed in `BRIEF.md`) | **exit 0** — this is the pass's own gate |

## Facts

| | |
|---|---|
| What it is | One static page: `index.html` (508 lines — one inline `<style>`, one inline `<script>`) |
| Served by | **GitHub Pages from the repository root** (branch `main`, folder `/`; no deploy workflow — the site is configured on the repo). Public, no server, no login, no build step at deploy. |
| **Everything committed here is public** | verified 2026-09-30: `https://cutout-z.github.io/aemo-renewable-generator-dashboard/outputs/summary.csv` returns 200, so the deployed site *is* the whole tracked tree. Never commit a secret, a private snapshot, or a screenshot you would not publish. Evidence lives in `design/` **on the branch** — it is not part of the site only for as long as it never reaches `main`. |
| Stack | hand-written HTML/CSS + vanilla JS · PapaParse 5.4.1 (CSV) and SheetJS 0.18.5 (XLSX, client-side export) from CDN · the compiled Tailwind stylesheet `assets/css/app.css`, built by the standalone CLI (no Node, no npm) |
| Data | `outputs/summary.csv` (240 rows × 31 cols) + 5 regional `.xlsx`, fetched relative to the page — served because they are committed |
| Design language | `assets/css/tailwind.src.css` (the only file with literal colours) → `./scripts/build-css.sh` → `assets/css/app.css` (committed). Rules: `design/design-tokens.md`. Proof page: `design/tokens.html` |
| Data lane | `deploy/run-update.sh` on the NAS (QNAP `ai-wif-runner`): fetch → `checkout main` → `pull --ff-only`, **falling back to `git reset --hard origin/main`** → `src.main --full-refresh` → validate → commit `outputs/` + `data/*.feather` as `aemo-nas-bot` → push. It never writes `index.html`. |
| Upstream | The `CURTAILMENT_ACTUAL_*` columns are fetched from the **sibling credit dashboard's** published `curtailment_by_fy.csv`; MLF, ELI and ISP columns come from AEMO feeds with cached fallbacks. Two publication rhythms meet here (monthly actuals, annual July forecasts) — say which one a panel is showing. |
| Why this copy exists | The live checkout is `~/Documents/Zalen/AI Wif Brain Projects/AEMO Renewable Generator Dashboard`. The lane hard-resets it to `origin/main`, and macOS TCC blocks GUI agents (Claude Desktop) from running git under `~/Documents`. This clone carries its own `.git` and cannot be clobbered. |

## Local preview

```bash
cd ~/Design/"AEMO Renewable Generator Dashboard" && /opt/anaconda3/bin/python3 -m http.server 9370 --bind 127.0.0.1
# http://127.0.0.1:9370/index.html      9370 is this pass's port — do not take another
```

Cloud browsers cannot reach `127.0.0.1`; screenshots and checks go through Playwright:

```bash
/opt/anaconda3/bin/python3 scripts/verify-design.py --screens   # + design/screens/after-*.png
```

## DOM contract — the hooks the gates and the next agent depend on

| Hook | Meaning |
|---|---|
| `<html data-theme="dark\|light">` + a toggle control | theme; `?theme=light` / `?theme=dark` must force one |
| `#stats` → `.kpi-value` + `.kpi-label` | the KPI strip (≥ 4 valued, labelled tiles) |
| `#tabs` → `.seg` > `.seg-item` (active = `.seg-item-active`) | All + 5 region switches |
| `#fuelFilter`, `#rezFilter` (selects), `#search` | the filters — keep their ids, they are the wiring |
| `#table`, `#thead`, `#tbody`; two header rows; the five metric groups named in the header | the grouped table |
| rows carry `data-duid` | selection and the gates key off it |
| heat cells: `td` carrying `seq-0` … `seq-7` | the ramp — no inline `rgb()` |
| `[data-heat-legend]` (or the card foot) naming the scale(s), the stops and the direction | the stated scale |
| `N/A` cells keep a stated reason (the REZ tooltip today) | absence is explained, never blanked |
| `#selectAll`, `#exportSelected`, `#clearSelection`, `#selCount` | selection + the SheetJS export |
| `a[download]` × 6 | 5 regional workbooks + the CSV |
| `.state` > `.state-title` + `.state-body` | loading / missing-file / no-matches states |
| `#footer` (or `.card-foot`) containing `Source:` and the as-of/lag note | provenance |

Renaming or dropping any of these breaks a committed gate. If a name must change, change the gate in
the same commit and say why.

## Pitfalls

- **Two heat scales live in the page today, both hand-rolled per cell** (`mlfColor()`, a diverging
  red→yellow→green around 0.80/0.95/1.05; `curtailmentColor()`, a three-stop green→yellow→red for
  0→5%→20%+). Moving them to the token ramp means the JS picks a *step*, never a colour. If one ramp
  cannot honestly carry both meanings, define the second scale **in the token file** (a documented
  family decision) rather than adding a second inline colour function.
- **The group header colours are five literal hexes** (`#5a4a8a`, `#2e7d32`, `#c62828`, `#e65100`,
  `#00695c`) plus a hard-coded hover (`#3a62a4`). Those five families must keep five *distinguishable*
  token colours and their words — colour is never the only thing telling the reader which family a
  column belongs to.
- **`th`/`td` are centred today**; the family right-aligns numeric cells in tabular figures, and MLF
  (4 dp) and percentages (1 dp) need to line up down the column.
- **Tailwind purges what its scanner cannot see.** `index.html` is a content source (so class strings
  built in the inline script are found), but a class added only to the CSS does nothing until
  `./scripts/build-css.sh` runs and `assets/css/app.css` is committed. `.seq-*` sit outside `@layer`,
  so purging never removes them.
- **Preflight is ON** in `tailwind.config.js`: it becomes the page's only reset the moment the inline
  `* { margin:0; padding:0 }` and body rules go (BRIEF step 1). Until then the inline block still wins
  — `app.css` is linked *before* it.
- **The XLSX export is generated client-side** from the current selection by SheetJS; it is used. Keep
  it working (same columns, same filename shape) and re-verify it after the restyle.
- The table's own scroll box (`max-height: 75vh` today) is deliberate — 240 rows × 27 columns. Keep a
  contained scroll box, not a 6,000-pixel page.
- Nothing in this repo feeds the Brain dashboard or any lane. Do not add a side channel.

## Finishing (the handback)

- [ ] Working tree clean; everything committed **on `design/2026-10`**.
- [ ] Branch pushed: `git push -u origin design/2026-10` — a branch that exists only in this folder
      dies with the folder. (Do not push `main`, do not merge, do not open a PR.)
- [ ] All three gates run and their exact results stated.
- [ ] After-screenshots in `design/screens/` (desktop + phone, same scroll positions as `before-*.png`).
- [ ] `./scripts/build-css.sh` run after the last class change; `assets/css/app.css` committed.
- [ ] `git diff --stat main` shows no `outputs/**`, `data/**`, `src/**`, `deploy/**`, `tests/**`,
      `.github/**`.
- [ ] `docs/design-pass-2026-09-30.md` — what changed (files + visual summary), how it was verified,
      what you deliberately left alone, anything you suspect you bent. This is how the next agent
      reconstructs intent without the transcript.
- [ ] A short report: surfaces changed of the ones that exist; what is half-done; decisions the brief
      did not cover.

If time runs out mid-change: commit what works, leave the branch pushed, and say plainly what is
half-done. Never leave a half-finished change uncommitted — an end-session sweep would commit it as
one opaque blob.
