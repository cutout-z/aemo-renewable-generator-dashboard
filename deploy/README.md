# Frequency-Driven Updates — NAS runner (production)

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
| Renewable generator source monitor | `--full-refresh` | Check for updated generator listing, MLF feed, ELI/REZ data, or actual curtailment rollup, and publish only when canonical summary data changes. |

The lane registry, cadence windows and report paths live in
`tools/nas-runner/configs/brain-ops.nas.toml` (the NAS runner tooling).
`deploy/run-update.sh` runs the full test suite and commits/pushes only when
`outputs/` changed, and the script self-heals a rewritten `main`: if
`git pull --ff-only` is impossible it resets onto the fetched remote instead
of exiting 128. If AEMO ELI/REZ workbook URLs fail, the pipeline falls back
to cached source snapshots rather than dropping columns.

## Env

`deploy/env.example` documents the settings the lane injects (`APP_DIR`,
`PIPELINE_ARGS`, test/push toggles).
