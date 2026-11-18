# Frequency-Driven Updates — NAS runner (production)

> **VPS retired 2026-05-29.** The NAS runner runs every lane; any VPS or systemd setup described in this repo is historical. The current estate (hosts, lanes, schedules) is in ARCHITECTURE.md in the agent-contracts repo.

The production model:

- The **NAS runner** (QNAP `ai-wif-runner` container) runs the scheduled
  renewable-generator source monitor after the upstream Credit Dashboard and
  MLF Tracker outputs are available.
- GitHub stores code, small source snapshots in `data/*.feather`, and
  publishable `outputs/`.
- GitHub Pages deploys after the NAS lane pushes updated files.
- GitHub Actions remains available for manual verification, but is not the
  primary scheduled data runner.

This project is downstream of:

- `aemo-generator-credit-dashboard` for actual FY curtailment.
- `aemo-mlf-tracker` for MLF history.

The source footprint is small, so the lane uses `--full-refresh` to avoid
stale persistent feather caches.

## Lane

QNAP scheduled tasks invoke `nas-job aemo-renewable-generator-dashboard`, which
runs this repo's `deploy/run-update.sh` (renamed from the retired VPS-era
`run-vps-update.sh` in the 2026-09 cleanup) with the lane's `PIPELINE_ARGS`:

| Lane | `PIPELINE_ARGS` | Purpose |
| --- | --- | --- |
| Renewable generator source monitor | `--full-refresh` | Check for updated generator listing, MLF feed, a newer ELI edition, or actual curtailment rollup, and publish only when canonical summary data changes. |

The lane registry, cadence windows and report paths live in
`tools/nas-runner/configs/brain-ops.nas.toml` (the NAS runner tooling).
`deploy/run-update.sh` runs `tests/validate_outputs.py` (a failure stops the run
before its commit) and commits/pushes only when `outputs/summary.csv` changed, and the script self-heals a rewritten `main`: if
`git pull --ff-only` is impossible it resets onto the fetched remote instead
of exiting 128. Each run then restores `outputs/` and `data/*.feather` to `HEAD`
and removes stray files there, so a failed or manual run's leftovers are neither
read nor published, and "changed" means changed versus the committed
`outputs/summary.csv`. The ELI chart-data workbook is downloaded once per edition and
then read from the cache (if parsing or the first download fails, the last
`eli_curtailment.feather` is reused); the REZ forecasts and membership are not
fetched by the lane at all, but come from the committed `rez_*.feather` files
built by hand with `python -m src.eli_appendix`. Likewise the 2026 ISP REZ figures come from the
committed `isp_rez_curtailment.feather`, `isp_rez_crosswalk.csv` and `rez_forecast_history.csv`,
built by hand with `python -m src.isp_rez_appendix`.

Every normal exit of `deploy/run-update.sh` (after the push, and on the no-change exits)
runs `python -m src.post_publish_check` last. It exits non-zero while the ELI probe has
confirmed a newer edition than the one wired in, so the lane is red but the run's other
updates are still published; its message says how to wire the edition in.

## Lane data checks

With `RUN_TESTS=1`, after the validator and before `git add`, the lane runs the
offline test suite (`python -m pytest -q --ignore=tests/test_page_smoke.py tests`;
the network is blocked in `tests/conftest.py`; the page smoke test needs Playwright,
which the lane venv does not have), which checks the committed reference files such as
`isp_rez_curtailment.feather` against hand-checked values. It also flags the
run when `data/isp_rez_curtailment.feather` differs in content from `HEAD`, and
restores it from `HEAD` before `git add`, so a changed copy is never published:
that file is a fixed-edition reference (2026 ISP A3) built by hand, and a lane
run must never change it. A failure turns the lane red. By default the run
still commits and pushes (warn-only); `LANE_TESTS_BLOCK_PUSH=1` holds it back
instead: nothing is committed or pushed, `outputs/` and `data/*.feather` go back
to `HEAD`, and the next run starts again, so the held run's data is published
only when a later run passes. pytest and hypothesis are dev dependencies, not in
`requirements.txt`; they were installed by hand in the NAS lane venv (2026-11-18).
A venv without pytest is a failed check (`pytest missing`): under the default it
still publishes, and with `LANE_TESTS_BLOCK_PUSH=1` it holds every run back until
pytest is reinstalled.

## Env

`deploy/env.example` documents the settings the lane injects (`APP_DIR`,
`PIPELINE_ARGS`, test/push toggles).
