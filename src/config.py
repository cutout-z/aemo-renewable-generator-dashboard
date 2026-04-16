"""Configuration for AEMO Solar & Wind Curtailment Dashboard."""

from datetime import datetime

# ─── Regions ────────────────────────────────────────────────────────────────

REGIONS = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]

REGION_NAMES = {
    "NSW1": "NSW",
    "QLD1": "QLD",
    "VIC1": "VIC",
    "SA1": "SA",
    "TAS1": "TAS",
}

# Reverse map for matching state names to region IDs
STATE_TO_REGION = {v: k for k, v in REGION_NAMES.items()}

# ─── Financial Year Logic ───────────────────────────────────────────────────

def current_fy_start() -> int:
    """Return the start calendar year of the current financial year.
    FY runs July 1 to June 30. E.g. in March 2026 → FY25-26 → returns 2025.
    """
    now = datetime.now()
    return now.year if now.month >= 7 else now.year - 1


def fy_label(start_year: int) -> str:
    """E.g. 2024 → 'FY24-25'."""
    return f"FY{start_year % 100:02d}-{(start_year + 1) % 100:02d}"


def fy_short(start_year: int) -> str:
    """E.g. 2024 → '2024-25'."""
    return f"{start_year}-{(start_year + 1) % 100:02d}"


# ─── Fuel Types ─────────────────────────────────────────────────────────────

# Technology types that map to Solar or Wind in NEM Generation Information
SOLAR_TECH_TYPES = ["Solar - Photovoltaic", "Photovoltaic", "Solar"]
WIND_TECH_TYPES = ["Wind - Onshore", "Wind", "Wind - Offshore"]

FUEL_TYPE_MAP = {
    "Solar": "Solar",
    "Wind": "Wind",
}

# ─── Data Sources ───────────────────────────────────────────────────────────

# MLF Tracker (reuse existing project)
MLF_TRACKER_SUMMARY_URL = (
    "https://cutout-z.github.io/aemo-mlf-tracker/outputs/summary.csv"
)

# NEM Generation Information workbook
NEM_GEN_INFO_URL = (
    "https://aemo.com.au/-/media/files/electricity/nem/"
    "planning_and_forecasting/generation_information/"
    "nem-generation-information-april-2025.xlsx"
)

# ELI report chart data — explicit URLs per publication year
# AEMO changes URL patterns each year, so we maintain an explicit mapping
ELI_CHART_DATA_URLS = {
    2025: (
        "https://aemo.com.au/-/media/files/electricity/nem/"
        "planning_and_forecasting/inputs-assumptions-methodologies/2025/"
        "2025-eli-report-chart-data.xlsx"
    ),
}

# ELI appendix (REZ forecasts) — explicit URLs
ELI_APPENDIX_URLS = {
    2025: (
        "https://aemo.com.au/-/media/files/electricity/nem/"
        "planning_and_forecasting/inputs-assumptions-methodologies/2025/"
        "appendices-to-2025-eli-report.xlsx"
    ),
}

# Actual curtailment: consolidated FY rollup from the credit dashboard pipeline.
# The credit dashboard computes monthly curtailment per DUID from
# INTERMITTENT_GEN_SCADA and publishes the FY rollup via GitHub Pages.
CREDIT_CURTAILMENT_URL = (
    "https://cutout-z.github.io/aemo-generator-credit-dashboard/"
    "data/curtailment_by_fy.csv"
)

# ─── Paths (relative to project root) ──────────────────────────────────────

DATA_DIR = "data"
OUTPUT_DIR = "outputs"
SUMMARY_CSV = "outputs/summary.csv"

# Cache files
GENERATOR_CACHE = "data/generators.feather"
MLF_CACHE = "data/mlf_tracker.feather"
ELI_CURTAILMENT_CACHE = "data/eli_curtailment.feather"
REZ_FORECAST_CACHE = "data/rez_forecasts.feather"
CURTAILMENT_CACHE = "data/actual_curtailment.feather"

# ─── Network ────────────────────────────────────────────────────────────────

MAX_RETRIES = 3
RETRY_BACKOFF = 5  # seconds
REQUEST_TIMEOUT = 60
USER_AGENT = "Mozilla/5.0 AEMO-Solar-Curtailment-Dashboard"
