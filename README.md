# AEMO Renewable Generator Dashboard

Interactive dashboard tracking curtailment, marginal loss factors (MLFs), and ISP forecasts for NEM solar and wind generators.

**Live dashboard:** [cutout-z.github.io/aemo-renewable-generator-dashboard](https://cutout-z.github.io/aemo-renewable-generator-dashboard/)

## What it shows

For every utility-scale solar and wind farm in the NEM:

| Category | Columns | Update frequency |
|----------|---------|-----------------|
| **Actual curtailment** | Last 2 completed FYs, sourced from the credit dashboard | Monthly |
| **ELI projected curtailment** | Near-term (2026-28) and medium-term (2030-35) projections | 2025 edition; updated by hand (config plus `python -m src.eli_appendix`) when AEMO publishes a new one |
| **Marginal loss factors** | Final MLFs for the latest two FYs the MLF tracker publishes (it has no draft column; today FY25-26 and FY26-27) | AEMO publishes final MLFs by about 1 April for the next FY, with occasional mid-year revisions; read via the MLF tracker |
| **ISP curtailment forecast** | The Final 2024 ISP's forecast (Step Change) for 2025-26 to 2027-28 + average, by REZ. Superseded by AEMO's 2026 ISP (25 June 2026); see below | 2024 ISP via the 2025 ELI appendices; changes only when a new ELI edition republishes ISP figures (the ISP itself is every two years) |
| **ISP economic offloading** | Same source and years | As above |

## Data sources

| Data | Source | URL |
|------|--------|-----|
| Generator listing | AEMO NEM Registration and Exemption List (re-downloaded every run), cross-checked against MMSDM DUDETAILSUMMARY | [NEM-Registration-and-Exemption-List.xls](https://www.aemo.com.au/-/media/Files/Electricity/NEM/Participant_Information/NEM-Registration-and-Exemption-List.xls), [nemweb MMSDM](https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/) |
| Projected curtailment | AEMO Enhanced Locational Information (ELI) Report | [aemo.com.au/.../enhanced-locational-information](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/nem-forecasting-and-planning/forecasting-and-planning-data/enhanced-locational-information) |
| REZ forecasts | Regional appendices to AEMO's ELI Report (forecasts from the ISP named in them) | Same as above |
| MLFs | AEMO MLF Tracker (via [cutout-z/aemo-mlf-tracker](https://github.com/cutout-z/aemo-mlf-tracker)) | [cutout-z.github.io/aemo-mlf-tracker](https://cutout-z.github.io/aemo-mlf-tracker/) |
| Actual curtailment | AEMO Generator Credit Dashboard ([cutout-z/aemo-generator-credit-dashboard](https://github.com/cutout-z/aemo-generator-credit-dashboard)) | [cutout-z.github.io/aemo-generator-credit-dashboard/data/curtailment_by_fy.csv](https://cutout-z.github.io/aemo-generator-credit-dashboard/data/curtailment_by_fy.csv) |

## Methodology

### Three different "curtailment" figures

The table puts three curtailment families side by side. They measure different
things and are not comparable with each other:

| Family | What it is | Includes | Excludes |
|--------|-----------|----------|----------|
| **Actual** | Measured, this unit, a full FY: 1 − energy generated ÷ the availability the unit offered into dispatch | every reason output fell short of offered availability: network and system-security constraints **and economic curtailment** (output withheld at negative prices) | outages already taken out of the offered availability |
| **ELI** | Modelled by AEMO: share of a hypothetical **new 300 MW** solar or wind project's energy at the unit's ELI location that would not be dispatched | transmission (thermal or stability) limits, minimum synchronous units for system security (near term only), limited regional demand or export | negative-price (economic) bidding, outages, maintenance |
| **ISP** | Forecast by the 2024 ISP (Step Change) for the **whole REZ**, as two figures | curtailment: output reduced by transmission congestion; economic offloading: output reduced on market price | anything unit-specific |

So an actual figure above the ELI or ISP figure is not by itself a sign the
projection was wrong: the actual includes economic curtailment, which ELI leaves
out and ISP reports separately as offloading.

### Actual curtailment

Sourced from the [Generator Credit Dashboard](https://github.com/cutout-z/aemo-generator-credit-dashboard), which computes a monthly curtailment proxy per DUID from AEMO dispatch data: SCADA output against the unit's bid-in `AVAILABILITY` in `DISPATCHLOAD` (not the UIGF forecast). It is a shortfall against offered availability, not a split by cause: network and system-security constraints and economic curtailment (bidding off at negative prices) are all in it. Its pipeline re-pulls only the last two months of dispatch each run, so the shared history is never rebuilt from scratch.

This dashboard fetches the credit dashboard's published FY rollup (`curtailment_by_fy.csv`) and surfaces the two latest financial years it covers in full: a FY qualifies once it has ended (in NEM time, AEST) and the rollup has a unit with all 12 months in it. Just after 1 July the year that has ended still has 11 months upstream, so the table keeps the previous two years until June's data lands instead of showing an empty column. Each FY value in the rollup is a ratio of summed energy over the FY's months (not an average of monthly percentages):

```
curtailment_FY = 1 − Σ(eligible actual MWh) / Σ(potential MWh)     potential = offered AVAILABILITY
```

Values are in [0, 1]. Partial FYs (`months_covered < 12`) are excluded from the cross-sectional table. `ACTUAL_MONTHS_<FY>` carries the rollup's `months_covered` for each unit and year (empty = no row), and `CLASSIFICATION` the Registration List's Semi-Scheduled / Non-Scheduled, so the page can say why a value is missing: a partial year (and, when the earlier year is complete, that a month has no data rather than a late start), a non-scheduled unit absent from the rollup, or no row at all.

### ELI projected curtailment

Per AEMO's 2025 ELI report, a hypothetical **300 MW** wind or solar project was tested in turn at representative locations across the NEM and dispatched in market modelling consistent with the 2024 ESOO, subject to network limits, system security limits, regional demand and market conditions:

```
projected curtailment = 1 − additional wind and solar energy dispatched / additional wind and solar energy available
```

Energy goes undispatched in that model because of intra- or inter-regional transmission limits (thermal or stability), the need to run coal and gas units for system security (near term only), or limited export and local demand (including distributed PV). It is **not** only network constraints, and it leaves out what the report says happens in practice but is not modelled: negative-price periods driven by economic bidding, transmission outages, generator maintenance. It is the curtailment of an additional project at the location, not a forecast for the existing unit.

- **Near term (2026-28)**: operating conditions before the key ISP transmission is built: committed augmentations only, minimum synchronous-unit requirements for system security in force
- **Medium term (2030-35)**: once that transmission is built (committed, anticipated and actionable projects advised to commission before 2031) and system security limits are resolved

These are the horizons the 2025 ELI report's executive summary gives; its Table 2 calls the conditions representative of 2026-2029 and 2031-2035, depending on the speed of development. The page labels the columns Near (26-28) and Med (30-35), the workbooks ELI Near Term (2026-28) and ELI Med Term (2030-35).

These are projections, not actuals. They indicate the *risk* of curtailment for a new connection at each location.

Every unit is filled from AEMO's location table (`ELI_SOURCE` = `location`):
same `LOCATION` in the unit's own region, the row at the unit's connection voltage
if there is one, else the location's only row (several voltages and none matching =
left empty); wind farms take the Wind columns, solar farms the Solar columns.
`ELI_SOURCE` is empty where there is no value. No AEMO table maps a DUID to an ELI
location, so a unit with no `LOCATION` (every wind farm, and the unseeded solar
farms) is matched by name instead (`ELI_SOURCE` = `location-name`): the one ELI
location in its region named as a whole word in its project name (Ararat Wind Farm
→ Ararat), same voltage rules. On the 21 seeded solar farms the name rule fires
for, it picks the seeded location for 20 (the 21st, Stubbo, now has its own ELI
location). Today 104 solar farms are filled by location and 10 farms (6 wind, 4
solar) by name; the other wind farms stay empty.

Until 2026-10 the 104 seeded solar farms took hand-seeded per-DUID values
(`data/eli_per_duid.feather`, `ELI_SOURCE` = `per-DUID`) ahead of the location
table. That file carries no edition and nothing rebuilds it, so a new ELI edition
would have left those units on the 2025 seed. The pipeline no longer reads it; the
switch moved five published cells by under 0.2 percentage points (Metz and White
Rock near term 23.10% → 22.91%, Wollar near term 15.87% → 15.85%, New England 1
and 2 medium term 5.08% → 5.15%), where the seed differed from AEMO's table.

### ISP curtailment & economic offloading forecasts

From the "VRE curtailment and economic offloading – ISP forecast" table in each REZ section of the ELI regional appendices. The 2024 ISP (Appendix 3) defines both as a percentage of the REZ's wind and solar (VRE) energy:

- **Curtailment** (the ISP's "transmission curtailment"): generation reduces output because of transmission network congestion (constrained down or off by operational limits)
- **Economic offloading** (the ISP's "economic spill"): generation reduces output because of market price

The forecasts are the Final 2024 ISP's (Step Change scenario), as the 2025 ELI appendices state; they were made in 2024 for 2025-26, 2026-27 and 2027-28. 2025-26 has since ended: its value is still the 2024 forecast, not an outcome, the page's ISP group headers say "(2024 ISP)" and an ended year's column header has a tooltip saying so (the Actual columns carry outcomes). The values are not relabelled or replaced; they change when a new ELI edition republishes newer ISP forecasts.

**The 2024 ISP has been superseded.** AEMO published the 2026 ISP on 25 June 2026. These figures come from the 2025 ELI regional appendices and change only when a new ELI edition republishes them; the dashboard does not read the ISP itself (ingesting the 2026 ISP directly would be a new source). The page says so in a note above the table and marks the ISP group headers "(2024 ISP, superseded)". The note is driven by `ISP_SUPERSEDED` in `index.html` and the data's `ISP_EDITION`, so it disappears once the data carries a newer ISP edition.

These are forecast at the REZ level and mapped to individual farms by REZ membership
(joined on `REZ_NAME`). Units outside a REZ, or whose REZ is unknown, show N/A.

### REZ membership

`summary.csv` carries three REZ columns:

| Column | Values |
|--------|--------|
| `REZ` | `Y` in a REZ · `N` a source says it is outside every REZ · empty = unknown |
| `REZ_NAME` | the zone name · `Non-REZ` only when `REZ` is `N` · empty = unknown |
| `REZ_SOURCE` | `geninfo` · `eli` · `eli-station` · `seed` · empty — where the `Y`/`N` came from |

Precedence: NEM Generation Information where it states a REZ (no edition has a REZ
column today); then the ELI regional appendices (`eli`), which list each existing
unit by DUID under its REZ or under "Non-REZ" (`data/rez_membership.feather`, built
by `python -m src.eli_appendix`); then a unit the appendices don't list at the same
DUDETAILSUMMARY station as units they list in one section (`eli-station`, e.g.
WANDSF2 from WANDSF1); then the seeded workbook (`generator_enrichment.feather`:
`REZ (Y/N)` and `REZ`); otherwise unknown. **`N` is only ever written when a source
says so explicitly**; a blank or missing value is never read as "outside". The
appendices use the same REZ boundaries as the ISP forecasts above, and agree with
the seed on all 97 units both cover. Today: wind 66 in a REZ, 17 outside, 25 unknown
(units the 2025 appendices don't list, mostly small non-scheduled or newer farms).

When a new ELI edition is published, rebuild both reference files:

```
python -m src.eli_appendix          # downloads the five appendix PDFs; needs pdftotext (poppler)
python -m src.eli_appendix --isp-edition "2026 ISP"   # when the text cites more than one ISP
```

Both files record the editions they hold (`ELI_EDITION`, the appendix year;
`ISP_EDITION`, the ISP the forecasts come from, read from the appendix text), and so
does the ELI chart data. `summary.csv` carries them as `ELI_EDITION` and
`ISP_EDITION`, and the page's "2025 ELI" / "(2024 ISP)" labels are read from those
columns. Validation fails when the appendix files' ELI edition differs from the
chart-data edition, so bumping `config.ELI_*` without this rebuild cannot publish a
mix of editions under one label. (The files committed before 2026-10-07 were stamped
ELI 2025 / 2024 ISP by hand, from what they already held, not re-parsed.)

### Generator listing

The Registration and Exemption List is downloaded on every run. A download only
replaces the cached copy if it is a real workbook with the `PU and Scheduled Loads`
sheet; a failed fetch or a Cloudflare challenge page keeps the last good copy and
logs `REGISTRATION LIST REFRESH FAILED` with that copy's age. The newest MMSDM
DUDETAILSUMMARY on nemweb is then checked: every GENERATOR DUID whose registration
took effect in the last 24 months but is absent from the list is logged as a warning
(DUDETAILSUMMARY has no fuel type, so such units are reported, never added).
Each run records what it fetched in `data/source_status.json` (not committed), and publishes a trimmed copy, `outputs/source_status.json` (per source: edition, fetch dates, whether this run refreshed it, any error), which the page footer shows as "Sources at the last publish". The lane commits it with `outputs/` only when `summary.csv` changes, so its dates are those of the last run that changed values, not of the last run.

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
- The ELI chart-data workbook is downloaded once per edition and then read from `data/` (a corrected re-issue under the same URL is not picked up); if parsing or the first download fails, the last `eli_curtailment.feather` is reused. REZ forecasts and membership are not fetched in the lane at all: they come from the committed `rez_*.feather` files that `python -m src.eli_appendix` builds by hand.
- The lane commits as `aemo-nas-bot` and publishes only when canonical `outputs/summary.csv` changes, so daily workbook/cache regeneration does not create noisy commits.
- GitHub Actions is kept as a manual verification/fallback runner. Its `full_refresh` input defaults to `true`, matching the NAS lane (`PIPELINE_ARGS` defaults to `--full-refresh`); set it to `false` only to rebuild outputs from the committed caches.
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
- `ELI_SOURCE` in {`location`, `location-name`, empty}, empty exactly when there is no ELI value (`per-DUID`, the retired hand seed, fails)
- Editions: ELI values carry one `ELI_EDITION` and ISP values one `ISP_EDITION`; the REZ appendix files (`rez` in `data/source_status.json`) record an ELI edition, and it equals the chart-data edition (configured and published)
- `TECHNOLOGY` is not "Renewable" for every row
- Source freshness (`data/source_status.json`, written by the run): the Registration List's last good fetch is at most 30 days old, and at most 3 GENERATOR DUIDs registered in the last 24 months (per DUDETAILSUMMARY) are missing from it; a stale Generation Information edition is reported as a warning
- ELI edition (`eli` in `data/source_status.json`): each refresh probes next year's chart-data file on AEMO (one-byte Range request; a missing file redirects to /404) and a published newer edition is printed as a warning, not a failure. The probe guesses one file name, so from 1 October of year Y (each edition so far was out by July) an edition older than Y with nothing found prints a warning to check [AEMO's ELI page](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/nem-forecasting-and-planning/forecasting-and-planning-data/enhanced-locational-information). Validation fails when the probe's last yes/no answer (`last_conclusive_check`; a 403 or network error is no answer) is more than 60 days old
- MLFs (`mlf` in `data/source_status.json`: last good fetch, this run's error, whether the cached tracker CSV was republished, newest final FY column): a republished cache prints a warning and fails when its last good fetch is more than 35 days old; it also fails when the newest final MLF year is older than the current financial year (NEM time), e.g. still FY26-27 on 1 July 2027
- Actual curtailment (`actual_curtailment` in `data/source_status.json`): a run whose upstream fetch failed and republished the cached rollup prints a warning; it fails when that cache's last good fetch is more than 35 days old, or when there was no cache and the summary has no actual columns. It also records the upstream rollup's newest month (`upstream_last_month`) and fails when that month ended more than 75 days ago: the credit pipeline has stalled even if the fetch itself works

If any check fails, the NAS lane or manual fallback workflow exits before committing — preventing bad data from reaching the dashboard.

## Outputs

| File | Description |
|------|-------------|
| `outputs/summary.csv` | All generators, all columns — loaded by the dashboard |
| `outputs/source_status.json` | Each source's edition and fetch dates at the last publish — the page footer |
| `outputs/NSW_curtailment.xlsx` | NSW generators — summary table + heatmap sheets |
| `outputs/QLD_curtailment.xlsx` | QLD generators |
| `outputs/VIC_curtailment.xlsx` | VIC generators |
| `outputs/SA_curtailment.xlsx` | SA generators |
| `outputs/TAS_curtailment.xlsx` | TAS generators |
