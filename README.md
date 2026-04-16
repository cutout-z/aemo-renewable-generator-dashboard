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
| Generator listing | AEMO NEM Generation Information | [aemo.com.au/energy-systems/electricity/.../generation-information](https://aemo.com.au/energy-systems/electricity/national-electricity-market-nem/nem-forecasting-and-planning/forecasting-and-planning-data/generation-information) |
| Projected curtailment | AEMO Enhanced Locational Information (ELI) Report | [aemo.com.au/.../inputs-assumptions-methodologies](https://aemo.com.au/energy-systems/electricity/national-electricity-market-nem/nem-forecasting-and-planning/forecasting-and-planning-data/inputs-assumptions-and-methodologies) |
| REZ forecasts | Appendices to AEMO ELI Report | Same as above |
| MLFs | AEMO MLF Tracker (via [cutout-z/aemo-mlf-tracker](https://github.com/cutout-z/aemo-mlf-tracker)) | [cutout-z.github.io/aemo-mlf-tracker](https://cutout-z.github.io/aemo-mlf-tracker/) |
| Actual curtailment | AEMO Generator Credit Dashboard ([cutout-z/aemo-generator-credit-dashboard](https://github.com/cutout-z/aemo-generator-credit-dashboard)) | [cutout-z.github.io/aemo-generator-credit-dashboard/data/curtailment_by_fy.csv](https://cutout-z.github.io/aemo-generator-credit-dashboard/data/curtailment_by_fy.csv) |

## Methodology

### Actual curtailment

Sourced from the [Generator Credit Dashboard](https://github.com/cutout-z/aemo-generator-credit-dashboard), which computes monthly per-DUID curtailment from AEMO's `INTERMITTENT_GEN_SCADA` table (quality flags separate grid curtailment from mechanical outages from Dec 2024 onwards). Its pipeline re-pulls only the last two months of dispatch each run, so the shared history is never rebuilt from scratch.

This dashboard fetches the credit dashboard's published FY rollup (`curtailment_by_fy.csv`) and surfaces the last two completed financial years. The rollup is a generation-weighted average across the 12 months of each FY:

```
curtailment_FY = Σ(monthly_curtailment × monthly_generation) / Σ(monthly_generation)
```

Values are in [0, 1]. Partial FYs (`months_covered < 12`) are excluded from the cross-sectional table.

### ELI projected curtailment

Per the AEMO ELI report, curtailment projections are based on the introduction of a hypothetical 100 MW generator at each connection point. They represent the proportion of energy that would be curtailed due to network constraints.

- **Near term**: Based on current system operating conditions (2026-28 horizon)
- **Medium term**: Based on projected future system conditions including committed network augmentations (2030-35 horizon)

These are projections, not actuals. They indicate the *risk* of curtailment at each connection point.

### ISP curtailment & economic offloading forecasts

From the ISP appendices, published with the ELI report:

- **Curtailment**: Proportion of energy curtailed due to network thermal limits, voltage stability, or system strength constraints
- **Economic offloading**: Proportion of energy where the generator would choose not to dispatch due to negative prices (economic decision, not physical constraint)

These are forecast at the REZ level and mapped to individual farms by REZ membership. Non-REZ farms show N/A.

### MLFs

Marginal Loss Factors represent the electrical losses between a generator's connection point and the regional reference node. An MLF of 0.90 means the generator receives 90% of the regional reference price.

Data sourced from the [AEMO MLF Tracker](https://github.com/cutout-z/aemo-mlf-tracker) project, which extracts MLFs from the DUDETAILSUMMARY table in AEMO's MMSDM archive.

## Construction

### Pipeline

```
NEM Generation Info   → download_generators.py → generator listing (spine)
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

# Then open index.html in a browser
```

### Automation

GitHub Actions runs on:
- **20th of each month**: refresh generator listing, MLFs, ELI, and fetch the credit dashboard's latest FY curtailment rollup (credit dashboard itself runs on the 18th).
- **Manual trigger**: `workflow_dispatch` with optional `full_refresh` flag.

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

If any check fails, the workflow exits before committing — preventing bad data from reaching the dashboard.

## Outputs

| File | Description |
|------|-------------|
| `outputs/summary.csv` | All generators, all columns — loaded by the dashboard |
| `outputs/NSW_curtailment.xlsx` | NSW generators — summary table + heatmap sheets |
| `outputs/QLD_curtailment.xlsx` | QLD generators |
| `outputs/VIC_curtailment.xlsx` | VIC generators |
| `outputs/SA_curtailment.xlsx` | SA generators |
| `outputs/TAS_curtailment.xlsx` | TAS generators |
