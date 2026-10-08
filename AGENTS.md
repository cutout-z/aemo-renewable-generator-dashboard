# Repo contract — AEMO Renewable Generator Dashboard

Read this before touching anything. It says how work in this repo is allowed to happen, for design
passes and logic/integrity passes alike; `README.md` says what the dashboard shows and how the pipeline
builds it. The family conventions and shared AEMO facts below are generated from the `agent-contracts`
repo; where this file's repo-specific rules are stricter, they win.

<!-- BEGIN agent-contracts:family -->
<!-- source: family/AGENTS.family.md sha256:f43f0fc253a7 — edit in cutout-z/agent-contracts, not here -->
## Family conventions (every repo, every agent)

*Generated from `agent-contracts/family/AGENTS.family.md`. Edit it there, never here: a drift check
reports any local edit.* These apply to every agent (Hermes, Claude Code, Codex or any other),
whatever app drives it. Where this repo's own rules (above or below this block) are stricter, they
win. Machine-specific conventions (which checkout is which, the lane runtime, where memory lives)
are in the owner's private contract, which each harness loads separately.

### Branches, concurrency, cleanup

- Work in a working clone, on a branch, never in a live or serving checkout. Merge to `main` only if
  this repo's contract says the agent may; otherwise push the branch and hand back for review.
- Other agents may be working in this repo right now. Fetch before you act. If a branch moved
  unexpectedly, or files you didn't touch changed, stop and report rather than reconcile.
- Use a worktree for parallel work, not a second clone. At session end, remove the worktrees you
  created and leave each checkout on the branch it was on when you arrived.
- Push every branch you want kept. An unpushed branch is one disk failure from gone.

### What needs the owner's yes

An explicit instruction from the owner in the current session covers that action only, not similar
later ones. Without one, declare these and wait for a yes:

- **Publishing**: anything that changes what other people can see (`main` on a published repo,
  Pages, public data files).
- **Data and ETL**: data files, pipeline code, data contracts (columns, keys, paths, schemas).
- **The instruments**: `check.sh`, guard tests, audit and verify scripts. Never weaken one to make
  something pass. If one is wrong, say so and leave it.
- **Infra**: ports, scheduled jobs, servers, publish pipelines.
- **Another agent's state**: another agent's memory, config or notes.

Never read, quote or commit secrets: `.env`, auth files, keys, tokens.

### Verification standard

- A change is done when you have seen the evidence yourself: tests run (exact counts, failures
  named, pre-existing failures shown to exist on `main`), and for UI, a real browser render.
- For a fix, show its test fails with the fix reverted and passes with it applied.
- Don't relay another agent's or subagent's numbers. Re-run or read the evidence yourself.

### Attribution and handback

- **Every commit names its agent** in a trailer: `Co-Authored-By: <Agent> <model> <email>`, e.g.
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`, `Co-Authored-By: Codex …`,
  `Co-Authored-By: Hermes …`. Audits match on the `Co-Authored-By: <Agent>` prefix.
- **End every piece of work with a handback note on the branch**, using the repo's own path
  convention, or `docs/handback-<YYYY-MM-DD>-<topic>.md` if it has none. On a repo whose `main` is
  published, keep notes and evidence on the branch; they don't merge. Start the note with:

  ```yaml
  ---
  project: <project name as on the owner's board>
  agent: claude | codex | hermes
  branch: <branch>
  merged: false
  status: <one line>
  outstanding:
    - "to-do [mac-local] <item>"   # [mac-local] = only actionable on the owner's Mac
  ---
  ```

  Then cover what changed, the evidence, what's left, and any decision the brief didn't cover.
<!-- END agent-contracts:family -->

<!-- BEGIN agent-contracts:aemo-facts -->
<!-- source: facts/AEMO-FACTS.md sha256:c6c3cdf689bd — edit in cutout-z/agent-contracts, not here -->
## AEMO shared facts — every agent, every repo

One canonical home for cross-repo AEMO domain facts, so a fact established in one repo is never
invisible to an agent working in another (the battery-MLF lesson, 2026-10-05: the TLF orientation
was established inside one repo's rollout doc while other agents recomputed with the wrong factor).

**Who reads this:** any agent (Hermes, Claude Code, Codex) working in an AEMO repo. It is stamped
into each AEMO repo's `AGENTS.md` between `agent-contracts:aemo-facts` markers, and the `aemo-audit`
and `aemo-logic-pass` skills point here instead of duplicating facts. A fact here overrides anything
remembered or re-derived.

**Maintenance:** append with date + source; supersede in place (`~~SUPERSEDED~~ <date> <reason>`).
Location: `agent-contracts/facts/AEMO-FACTS.md` (git, `cutout-z/agent-contracts`). Edit here only; `scripts/stamp.py` copies it into each AEMO repo's AGENTS.md.

| Fact | Detail | Verified | Source |
|---|---|---|---|
| TLF orientation | `TRANSMISSIONLOSSFACTOR` = **Import** MLF; `SECONDARY_TLF` = **Export** MLF for BIDIRECTIONAL (battery) units. A battery's export MLF comes from `SECONDARY_TLF`, never `TRANSMISSIONLOSSFACTOR`. Confirmed 51/51 differing batteries against AEMO's 2026-27 workbook. The MLF Tracker used the import factor for FY24-25/FY25-26 battery export values until fix `dbaf6f7` (2026-10-05); downstream battery revenue was restated (~−$13.1M across 26 batteries' months) after the fix. | 2026-10-05 | AEMO 2026-27 MLF workbook; `aemo-credit-design/audits/Logic Pass Rollout 2026-10-05.md` |
| FCAS regime | FCAS causer-pays contribution factors are DEAD — replaced by the Frequency Performance Payment (FPP) on 8 June 2025 (5-minute contribution factors on NEMWEB). Never recommend or resurrect causer-pays factor tracking; FPP cost-allocation factors per DUID are the future extension. | 2026-09-01 | AEMO/NEMWEB; `aemo-audit` skill; credit repo `docs/FUTURE_DATA_SOURCES.md` |
<!-- END agent-contracts:aemo-facts -->

## Hard rules

1. **`main` is the live site, and so is everything on it.** GitHub Pages builds `main` at `/` and
   serves the whole tracked tree (`gh api repos/cutout-z/aemo-renewable-generator-dashboard/pages`:
   `legacy` build, `main`, `/`). Anything committed to `main` is public, this file included. Never
   commit a secret, a private snapshot or pass evidence there; nothing reaches `main` without the owner's yes.
2. **Lane-owned paths are not hand-edited.** `outputs/**` and `data/*.feather` are written by the
   pipeline and committed by the NAS lane (`deploy/run-update.sh`: `git add outputs/ data/*.feather`).
   Change them only by running the pipeline, with the owner's yes. A design pass touches `index.html` and
   `assets/css/**` (plus `tailwind.config.js`) only.
3. **Hand-built reference files change only through their builders.** `rez_forecasts.feather`,
   `rez_membership.feather` (`python -m src.eli_appendix`), `isp_rez_curtailment.feather`
   (`python -m src.isp_rez_appendix`), `isp_rez_crosswalk.csv` (hand-maintained), and
   `rez_forecast_history.csv`, which is **append-only**: never rewrite or delete a row (README,
   "Edition history"). The lane never fetches these (deploy/README.md).
4. **Never invent data.** Every value on the page comes from `outputs/summary.csv` or
   `outputs/source_status.json` and carries its edition (`ELI_EDITION`, `ISP_EDITION`, `ISPA3_EDITION`).
   A missing value stays `N/A` with its stated reason; a REZ is `N` only when a source says so (README).
5. **Data contracts change only with the owner's yes** (next section), in the same commit as the page,
   the validator and the tests that read them.
6. **Keep the tests green.** Baseline below; a new failure is a regression until you say otherwise.

## Facts

| | |
|---|---|
| What it is | One static page, `index.html` (914 lines, inline `<style>` + `<script>`), reading `outputs/summary.csv` (241 rows x 52 cols) and `outputs/source_status.json` (`index.html`) |
| Served by | GitHub Pages, `main` at `/`, no deploy workflow, no build at deploy; live at `cutout-z.github.io/aemo-renewable-generator-dashboard` (`gh api .../pages`, README) |
| Stack | Python 3 pipeline (`src/`, pandas, pyarrow, openpyxl, requests: `requirements.txt`); page: vanilla JS, PapaParse 5.4.1 + SheetJS 0.18.5 from jsDelivr, compiled Tailwind `assets/css/app.css` (committed) from `assets/css/tailwind.src.css` (`index.html`) |
| Data lane | NAS runner (QNAP `ai-wif-runner`), `nas-job aemo-renewable-generator-dashboard` → `deploy/run-update.sh --full-refresh`: fetch, `checkout main`, `pull --ff-only` else **`reset --hard origin/main`**, `src.main`, `tests/validate_outputs.py`, commit `outputs/` + `data/*.feather` as `aemo-nas-bot` **only if `summary.csv` changed**, push, then `src.post_publish_check` (deploy/README.md, run-update.sh) |
| Lane schedule | Daily at 09:15 AWST (NAS crontab `15 9 * * *`, checked 2026-10-08), matching README and `deploy/README.md`. Daily since 2026-10-07; before that, monthly on the 1st. The lane registry (`tools/nas-runner/configs/brain-ops.nas.toml`) is not in this repo |
| Fallback runner | `.github/workflows/update.yml`, manual `workflow_dispatch` only; `full_refresh` defaults `true`; commits as `github-actions[bot]` |
| Upstream | `aemo-generator-credit-dashboard` (`curtailment_by_fy.csv`) and `aemo-mlf-tracker` (`outputs/summary.csv`), both read over Pages (`src/config.py`); AEMO Registration List, MMSDM DUDETAILSUMMARY, Generation Information, ELI chart data + appendices, 2026 ISP A3 |
| Lane goes red | when `data/source_status.json` records a confirmed newer ELI edition; the run's other updates still publish (decision 6(b), `src/post_publish_check.py`) |
| Design tooling | `scripts/build-css.sh` (Tailwind v3.4.17 standalone), `scripts/verify-design.py`, `scripts/verify-interactions.py`, `design/`, the design `AGENTS.md`/`BRIEF.md` live **only on `origin/design/2026-10`** (21 commits not on `main`); `main` got `index.html` + `assets/css/**` + `tailwind.config.js` by path (private design-pass notes) |

## Data contracts (the owner's yes before any change)

- **`outputs/summary.csv` columns** are read by name by `index.html`, the workbooks and
  `tests/validate_outputs.py`: identity (`DUID`, `PROJECT_NAME`, `REGIONID`, `FUEL_TYPE` non-null),
  `MLF_FY*`, `CURTAILMENT_ACTUAL_FY*` + `ACTUAL_MONTHS_FY*`, `ELI_CURTAILMENT_NEAR/MED` + `ELI_SOURCE`,
  `ISP_CURTAILMENT_*` / `ISP_OFFLOADING_*` (`FY1..3`, `_LABEL`, `AVG`), `ISPA3_*`, `*_EDITION`, `REZ*`.
- **Enumerations the validator enforces:** `REZ` in {`Y`,`N`,empty}, `Non-REZ` only with `N`, every
  `Y`/`N` has a `REZ_SOURCE`; `ELI_SOURCE` in {`location`,`location-name`,empty} (`per-DUID` fails);
  `FUEL_TYPE` Solar or Wind; MLF in [0.5, 1.5]; curtailment and `ISPA3_*` in [0, 1] (README, "Output Validation").
- **Published file names** the page links: `outputs/summary.csv`, `outputs/source_status.json`,
  `outputs/{NSW,QLD,VIC,SA,TAS}_curtailment.xlsx` (`EXCEL_FILES` in `index.html`).
- **Upstream columns** this repo depends on: credit rollup `duid`, `fy_start`, `months_covered`,
  `curtailment_pct`, `last_month` (`src/fetch_curtailment.py`); MLF tracker `DUID` + `FY*` columns
  (`src/download_mlf.py`). A rename upstream breaks this lane.

## Verify

```bash
cd <worktree> && /opt/anaconda3/bin/python3 -m pytest -q tests      # offline; conftest blocks requests
/opt/anaconda3/bin/python3 tests/validate_outputs.py                 # the publish gate the lane runs
# a pipeline run on a copy of data/ that leaves data/ and outputs/ alone (still fetches upstream):
/opt/anaconda3/bin/python3 -m src.main --cache-dir /tmp/ren-cache --output-dir /tmp/ren-out
/opt/anaconda3/bin/python3 tests/validate_outputs.py --outputs-dir /tmp/ren-out --cache-dir /tmp/ren-cache
```

**Baseline (2026-10-08, `ebc6cd6`, fresh worktree): `pytest` 182 passed, 0 failed (23 test files).**
`validate_outputs.py` exits 1 on a fresh checkout: `data/source_status.json missing`. That file is
gitignored and written only by a pipeline run, so this is environmental, not a regression.

**Local preview:** port **9370** (private design-pass notes; design-branch `AGENTS.md`):
`cd <worktree> && /opt/anaconda3/bin/python3 -m http.server 9370 --bind 127.0.0.1`, then open
`http://127.0.0.1:9370/index.html` (`?theme=light|dark` forces a theme). The design-branch gates
hard-code **9381** (`scripts/verify-*.py` on `origin/design/2026-10`); serve there to run them. Verify a
page change in a real browser (rows render, heat cells filled, no JS errors), not by grep.

## Pitfalls that have bitten

- **The lane hard-resets `main`** to `origin/main` when it cannot fast-forward (`1bfe02b`). Work left
  uncommitted or unpushed in a checkout the lane uses is gone.
- **A branch merge of `design/2026-10` would publish its evidence** (`design/`, `docs/`, `scripts/`),
  because Pages serves the whole tree; design work reaches `main` by path. The first by-path merge used
  an anchored filter that missed `assets/css/app.css` and published the new page on the old stylesheet
  (`0bdf704`, fixed in `2e4b9b2`; private design-pass notes).
- **Tests pin wording** in `index.html`, `README.md` and `deploy/README.md` (`test_cadence_wording`,
  `test_eli_horizons`, `test_isp_labels`, `test_measure_definitions`, `test_isp_rez_appendix`). A copy
  edit can turn the suite red; change the test in the same commit and say why.
- **A seed with no edition went stale silently:** the hand-seeded `data/eli_per_duid.feather` would
  have kept 104 units on ELI 2025. It is retired (still committed, never read) and `per-DUID` now fails
  validation (README). Any new source must carry its edition.
- **`src.seed_from_workbook` overwrites caches only with `--force`** and never the ELI-appendix files
  (`a944616`). Don't reach for `--force` to fix a data question.
- **The ELI chart workbook is downloaded once per edition**; a corrected re-issue under the same URL is
  not picked up (README). A new edition needs `src/config.py` + `src.eli_appendix` + crosswalk rows.
- **AEMO pages sit behind Cloudflare**; cached-source fallback is the intended path, not a bug to
  route around (private ops log, closed 2026-06-08).
- **The GitHub Actions fallback with `full_refresh=false`** would republish feather caches the lane
  has since replaced (`update.yml` comment, `940b8a5`).
- **FY rollover is read in NEM time** (`config.NEM_TZ`, AEST), not the host clock (`827709f`).

## Working alongside other agents

- `aemo-nas-bot` commits to `main` whenever `summary.csv` changes, touching only `outputs/**` and
  `data/*.feather` (run-update.sh); a branch that holds neither merges past those commits cleanly.
- Hermes triggers and inspects the lane (`brain-ops-nas workflow aemo-renewable-generator-dashboard`,
  skill `aemo-audit`). `deploy/run-update.sh` and the Actions workflow both push to `main`: run them
  only with the owner's yes.
- `origin/design/2026-10` carries an older, design-pass-only `AGENTS.md`/`CLAUDE.md`; on `main` this file is the contract.

## Handback checklist

- [ ] `pytest -q tests` run; exact counts stated, every failure named and classed (yours / environmental).
- [ ] `git diff --stat origin/main` shows no `outputs/**` or `data/**` unless the owner approved a data change.
- [ ] No data-contract change (columns, enumerations, file names) without the owner's yes, stated in the report.
- [ ] Page changes verified in a real browser on 9370 (or the gates on 9381), dark and light, desktop and phone.
- [ ] After any class change, `assets/css/app.css` rebuilt (`build-css.sh` from `design/2026-10`) and committed.
- [ ] Nothing in the diff you would not publish: `main` serves every tracked file.
- [ ] A short report: what changed, what was verified and how, what is half-done, decisions taken.
