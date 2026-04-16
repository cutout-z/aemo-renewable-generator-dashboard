# AEMO Renewable Generator Dashboard

Interactive dashboard tracking curtailment, marginal loss factors (MLFs), and ISP forecasts for NEM solar and wind generators.

**Live dashboard:** [cutout-z.github.io/aemo-renewable-generator-dashboard](https://cutout-z.github.io/aemo-renewable-generator-dashboard/)

## What it shows

For every utility-scale solar and wind farm in the NEM:

| Category | Columns | Update frequency |
|----------|---------|-----------------|
| **Actual curtailment** | Last 2 FYs of metered curtailment (SCADA vs UIGF) | Monthly |
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
| Actual curtailment | AEMO NEMWEB MMSDM Archive (DISPATCH_UNIT_SCADA + INTERMITTENT_GEN_FCST_DATA) | [nemweb.com.au/Data_Archive](https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/) |

## Methodology

### Actual curtailment

Calculated from AEMO's 5-minute dispatch data:

```
curtailment % = 1 - (actual_MW / uigf_MW)
```

- **actual_MW**: `DISPATCH_UNIT_SCADA` — actual dispatched MW per 5-minute interval
- **uigf_MW**: `INTERMITTENT_GEN_FCST_DATA` — AEMO's unconstrained intermittent generation forecast (what the generator *would have* produced without network constraints or curtailment)

Aggregated monthly per DUID, then averaged to financial year. This is the standard methodology used by AEMO in their Quarterly Energy Dynamics reports.

UIGF is available for all semi-scheduled generators (i.e., all utility-scale solar and wind farms). Values are clipped to [0, 1] — negative curtailment (actual > UIGF) is set to zero.

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
NEM Generation Info → download_generators.py → generator listing (spine)
MLF Tracker CSV     → download_mlf.py        → MLF columns
ELI Chart Data      → download_eli.py         → projected curtailment
ELI Appendices      → download_rez.py         → REZ forecasts
SCADA + UIGF        → download_curtailment.py → actual curtailment
                    ↓
                merge.py → summary.csv → index.html (dashboard)
                         → *.xlsx      (per-state workbooks)
```

### Running locally

```bash
pip install -r requirements.txt

# Full run (includes SCADA download — slow first time, ~50-150MB per month)
python -m src.main --full-refresh

# Quick run (skip SCADA, use cached data)
python -m src.main --skip-scada

# Then open index.html in a browser
```

### Automation

GitHub Actions runs on:
- **3rd of each month**: Refresh SCADA curtailment data and MLFs
- **August 1**: Annual refresh after ELI report and new FY MLFs are published (~July)
- **Manual trigger**: `workflow_dispatch` with optional `full_refresh` and `skip_scada` flags

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
