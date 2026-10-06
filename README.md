# AEMO Renewable Generator Dashboard

Interactive dashboard tracking curtailment, marginal loss factors (MLFs), and ISP forecasts for NEM solar and wind generators.

**Live dashboard:** [cutout-z.github.io/aemo-renewable-generator-dashboard](https://cutout-z.github.io/aemo-renewable-generator-dashboard/)

## What it shows

For every utility-scale solar and wind farm in the NEM:

| Category | Columns | Update frequency |
|----------|---------|-----------------|
| **Actual curtailment** | Last 2 completed FYs, sourced from the credit dashboard | Monthly |
| **ELI projected curtailment** | Near-term (2026-28) and medium-term (2030-35) projections | Annual (July) |
| **Marginal loss factors** | Last 2 actual FYs + current year draft | Annual (July/Oct) |
| **ISP curtailment forecast** | Next 3 FY forecasts + average, by REZ | Annual (July) |
| **ISP economic offloading** | Next 3 FY forecasts + average, by REZ | Annual (July) |

## Data sources

| Data | Source | URL |
|------|--------|-----|
| Generator listing | AEMO NEM Registration and Exemption List (re-downloaded every run), cross-checked against MMSDM DUDETAILSUMMARY | [NEM-Registration-and-Exemption-List.xls](https://www.aemo.com.au/-/media/Files/Electricity/NEM/Participant_Information/NEM-Registration-and-Exemption-List.xls), [nemweb MMSDM](https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/) |
| Projected curtailment | AEMO Enhanced Locational Information (ELI) Report | [aemo.com.au/.../inputs-assumptions-methodologies](https://aemo.com.au/energy-systems/electricity/national-electricity-market-nem/nem-forecasting-and-planning/forecasting-and-planning-data/inputs-assumptions-and-methodologies) |
| REZ forecasts | Appendices to AEMO ELI Report | Same as above |
| MLFs | AEMO MLF Tracker (via [cutout-z/aemo-mlf-tracker](https://github.com/cutout-z/aemo-mlf-tracker)) | [cutout-z.github.io/aemo-mlf-tracker](https://cutout-z.github.io/aemo-mlf-tracker/) |
| Actual curtailment | AEMO Generator Credit Dashboard ([cutout-z/aemo-generator-credit-dashboard](https://github.com/cutout-z/aemo-generator-credit-dashboard)) | [cutout-z.github.io/aemo-generator-credit-dashboard/data/curtailment_by_fy.csv](https://cutout-z.github.io/aemo-generator-credit-dashboard/data/curtailment_by_fy.csv) |

## Methodology

### Actual curtailment

Sourced from the [Generator Credit Dashboard](https://github.com/cutout-z/aemo-generator-credit-dashboard), which computes monthly per-DUID curtailment from AEMO's `INTERMITTENT_GEN_SCADA` table (quality flags separate grid curtailment from mechanical outages from Dec 2024 onwards). Its pipeline re-pulls only the last two months of dispatch each run, so the shared history is never rebuilt from scratch.

This dashboard fetches the credit dashboard's published FY rollup (`curtailment_by_fy.csv`) and surfaces the two latest financial years it covers in full: a FY qualifies once it has ended (in NEM time, AEST) and the rollup has a unit with all 12 months in it. Just after 1 July the year that has ended still has 11 months upstream, so the table keeps the previous two years until June's data lands instead of showing an empty column. The rollup is a generation-weighted average across the 12 months of each FY:

```
curtailment_FY = Σ(monthly_curtailment × monthly_generation) / Σ(monthly_generation)
```

Values are in [0, 1]. Partial FYs (`months_covered < 12`) are excluded from the cross-sectional table.

### ELI projected curtailment

Per the AEMO ELI report, curtailment projections are based on the introduction of a hypothetical 100 MW generator at each connection point. They represent the proportion of energy that would be curtailed due to network constraints.

- **Near term**: Based on current system operating conditions (2026-28 horizon)
- **Medium term**: Based on projected future system conditions including committed network augmentations (2030-35 horizon)

These are projections, not actuals. They indicate the *risk* of curtailment at each connection point.

Each unit takes its seeded per-DUID value where there is one (`ELI_SOURCE` =
`per-DUID`). Otherwise it is filled from the location table (`ELI_SOURCE` =
`location`): same `LOCATION` in the unit's own region, the row at the unit's
connection voltage if there is one, else the location's only row (several voltages
and none matching = left empty); wind farms take the Wind columns, solar farms the
Solar columns. `ELI_SOURCE` is empty where there is no value. On the seeded solar
farms this rule reproduces the per-DUID values for 101/104 (near) and 102/104
(medium). Units with no `LOCATION` (today: every wind farm and the unseeded solar
farms, since no AEMO table available to the pipeline maps a DUID to an ELI
location) cannot be filled.

### ISP curtailment & economic offloading forecasts

From the ISP appendices, published with the ELI report:

- **Curtailment**: Proportion of energy curtailed due to network thermal limits, voltage stability, or system strength constraints
- **Economic offloading**: Proportion of energy where the generator would choose not to dispatch due to negative prices (economic decision, not physical constraint)

These are forecast at the REZ level and mapped to individual farms by REZ membership
(joined on `REZ_NAME`). Units outside a REZ, or whose REZ is unknown, show N/A.

### REZ membership

`summary.csv` carries three REZ columns:

| Column | Values |
|--------|--------|
| `REZ` | `Y` in a REZ · `N` a source says it is outside every REZ · empty = unknown |
| `REZ_NAME` | the zone name · `Non-REZ` only when `REZ` is `N` · empty = unknown |
| `REZ_SOURCE` | `geninfo` · `seed` · empty — where the `Y`/`N` came from |

Precedence: NEM Generation Information where it states a REZ (no edition has a REZ
column today), then the seeded workbook (`generator_enrichment.feather`, from the
databook's Summary tab: `REZ (Y/N)` and `REZ`), otherwise unknown. **`N` is
established only by the seed's explicit `REZ (Y/N)` = `N`** (or a Generation
Information cell reading "Non-REZ"); a blank or missing value is never read as
"outside". The seed covers 104 solar DUIDs and no wind farms, so every wind farm is
unknown until a source covers it.

### Generator listing

The Registration and Exemption List is downloaded on every run. A download only
replaces the cached copy if it is a real workbook with the `PU and Scheduled Loads`
sheet; a failed fetch or a Cloudflare challenge page keeps the last good copy and
logs `REGISTRATION LIST REFRESH FAILED` with that copy's age. The newest MMSDM
DUDETAILSUMMARY on nemweb is then checked: every GENERATOR DUID whose registration
took effect in the last 24 months but is absent from the list is logged as a warning
(DUDETAILSUMMARY has no fuel type, so such units are reported, never added).
Each run records what it fetched in `data/source_status.json` (not committed).

NEM Generation Information is republished about quarterly under a new file name, so
it is not pinned: each run reads the edition links off AEMO's Generation Information
page when it can, otherwise probes `nem-generation-information-<month>-<year>.xlsx`
newest month first down to the cached edition, keeps the last good copy on failure,
and warns when the cached edition is more than ~4 months old. The July 2026 edition
has site, owner, region, technology, DUID, capacity and commitment status but **no
REZ, location or connection-voltage column**, so it currently enriches nothing; it is
used to name the technology of unlisted units in the DUDETAILSUMMARY warning, and
its REZ/location/voltage columns are picked up automatically if an edition adds them.

### MLFs

Marginal Loss Factors represent the electrical losses between a generator's connection point and the regional reference node. An MLF of 0.90 means the generator receives 90% of the regional reference price.

Data sourced from the [AEMO MLF Tracker](https://github.com/cutout-z/aemo-mlf-tracker) project, which extracts MLFs from the DUDETAILSUMMARY table in AEMO's MMSDM archive.

## Construction

### Pipeline

```
Registration List     → download_generators.py → generator listing (spine)
DUDETAILSUMMARY       → dudetail.py            → warns about registered units the list lacks
MLF Tracker CSV       → download_mlf.py        → MLF columns
ELI Chart Data        → download_eli.py        → projected curtailment
ELI Appendices        → download_rez.py        → REZ forecasts
Credit Dashboard CSV  → fetch_curtailment.py   → actual curtailment
                    ↓
                merge.py → summary.csv → index.html (dashboard)
                         → *.xlsx      (per-state workbooks)
```

### Running locally

```bash
pip install -r requirements.txt

# Fetch + merge + write outputs
python -m src.main

# Ignore feather caches and re-fetch everything
python -m src.main --full-refresh

# Run against a copy of the caches, writing outputs elsewhere (data/ and outputs/ untouched)
python -m src.main --cache-dir /tmp/ren-cache --output-dir /tmp/ren-out
python tests/validate_outputs.py --outputs-dir /tmp/ren-out

# Offline unit tests
python -m pytest -q tests

# Then open index.html in a browser
```

### Automation

Production updates run on the **NAS runner** (QNAP `ai-wif-runner` container) via the `nas-job aemo-renewable-generator-dashboard` lane documented in [`deploy/README.md`](deploy/README.md):

- A QNAP scheduled task fires the lane daily after the upstream Credit Dashboard and MLF Tracker lanes have had time to publish.
- The lane refreshes generator listing, MLF feed, ELI/REZ source data, and the Credit Dashboard curtailment rollup.
- If AEMO ELI/REZ workbook URLs fail, the pipeline falls back to cached source snapshots rather than dropping projected-curtailment or REZ forecast columns.
- The lane commits as `aemo-nas-bot` and publishes only when canonical `outputs/summary.csv` changes, so daily workbook/cache regeneration does not create noisy commits.
- GitHub Actions is kept as a manual verification/fallback runner with optional `full_refresh`.
- GitHub Pages deploys on those pushes.

*Historical:* this lane ran on a Hetzner VPS under the `aemo-renewable-generator-dashboard.timer` systemd timer before the 2026-09 NAS migration. That setup is retired and its unit files were deleted in the same cleanup.

## Output Validation

After the pipeline runs and before committing, an automated validation step (`tests/validate_outputs.py`) checks:

- `summary.csv` exists and has 100+ generators
- No null values in identity columns (DUID, PROJECT_NAME, REGIONID, FUEL_TYPE)
- Fuel types are strictly Solar or Wind
- All 5 NEM regions are present
- Nameplate capacity > 0 MW for all generators
- MLF values in [0.5, 1.5]
- Curtailment values in [0, 1]
- All 5 regional Excel workbooks exist
- REZ contract: `REZ` in {`Y`, `N`, empty}; `Non-REZ` only with `REZ` = `N`; every `Y`/`N` has a `REZ_SOURCE`; `Y` has a zone name
- `ELI_SOURCE` in {`per-DUID`, `location`, empty}, empty exactly when there is no ELI value
- `TECHNOLOGY` is not "Renewable" for every row
- Source freshness (`data/source_status.json`, written by the run): the Registration List's last good fetch is at most 30 days old, and at most 3 GENERATOR DUIDs registered in the last 24 months (per DUDETAILSUMMARY) are missing from it; a stale Generation Information edition is reported as a warning
- Actual curtailment (`actual_curtailment` in `data/source_status.json`): a run whose upstream fetch failed and republished the cached rollup prints a warning; it fails when that cache's last good fetch is more than 35 days old, or when there was no cache and the summary has no actual columns

If any check fails, the NAS lane or manual fallback workflow exits before committing — preventing bad data from reaching the dashboard.

## Outputs

| File | Description |
|------|-------------|
| `outputs/summary.csv` | All generators, all columns — loaded by the dashboard |
| `outputs/NSW_curtailment.xlsx` | NSW generators — summary table + heatmap sheets |
| `outputs/QLD_curtailment.xlsx` | QLD generators |
| `outputs/VIC_curtailment.xlsx` | VIC generators |
| `outputs/SA_curtailment.xlsx` | SA generators |
| `outputs/TAS_curtailment.xlsx` | TAS generators |
