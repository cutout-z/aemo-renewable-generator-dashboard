# VPS Monthly Updates

Production model:

- Hetzner VPS runs the monthly renewable generator dashboard refresh after the upstream Credit Dashboard and MLF Tracker outputs are available.
- GitHub stores code, small source snapshots in `data/*.feather`, and publishable `outputs/`.
- GitHub Pages deploys after the VPS pushes updated files.
- GitHub Actions remains available for manual verification, but should not be the primary scheduled data runner.

This project is downstream of:

- `aemo-generator-credit-dashboard` for actual FY curtailment.
- `aemo-mlf-tracker` for MLF history.

The source footprint is small, so the VPS lane uses `--full-refresh` to avoid stale persistent feather caches.

## Lane

| Lane | Timer | Pipeline args | Purpose |
| --- | --- | --- | --- |
| Monthly renewable generator dashboard | `aemo-renewable-generator-dashboard.timer` | `--full-refresh` | Refresh generator listing, MLF feed, ELI/REZ data, actual curtailment rollup, and workbooks. |

Recommended layout:

```text
/opt/aemo-renewable-generator-dashboard      git checkout + virtualenv
/etc/aemo-renewable-generator-dashboard/env  service settings
```

Create `/etc/aemo-renewable-generator-dashboard/env` from `env.example`. The service user needs a repo-scoped deploy key that can push to `cutout-z/aemo-renewable-generator-dashboard`.

## Install Timer

```bash
sudo cp deploy/aemo-renewable-generator-dashboard.service /etc/systemd/system/
sudo cp deploy/aemo-renewable-generator-dashboard.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now aemo-renewable-generator-dashboard.timer
```

Run once manually:

```bash
sudo systemctl start aemo-renewable-generator-dashboard.service
journalctl -u aemo-renewable-generator-dashboard.service -f
```
