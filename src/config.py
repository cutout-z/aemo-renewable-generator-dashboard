"""Configuration for AEMO Solar & Wind Curtailment Dashboard."""

from datetime import datetime, timedelta, timezone

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

# NEM market time is AEST (UTC+10, no daylight saving, as Australia/Brisbane).
# The FY rollover is read in it, not in the host's local time: the GitHub runner
# is UTC (10 hours late), the NAS container's zone is whatever it was built with.
NEM_TZ = timezone(timedelta(hours=10), "AEST")


def nem_now() -> datetime:
    """The current time in NEM market time (AEST)."""
    return datetime.now(NEM_TZ)


def current_fy_start(now: datetime | None = None) -> int:
    """Return the start calendar year of the current financial year, in NEM time.
    FY runs July 1 to June 30. E.g. in March 2026 → FY25-26 → returns 2025.
    A naive `now` is taken to be NEM time already.
    """
    now = now or nem_now()
    if now.tzinfo is not None:
        now = now.astimezone(NEM_TZ)
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

# NEM Generation Information workbook: republished about quarterly under a new
# name (nem-generation-information-<month>-<year>.xlsx), so src/gen_info.py
# discovers the newest edition instead of pinning one.
NEM_GEN_INFO_PAGE_URL = (
    "https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/"
    "nem-forecasting-and-planning/forecasting-and-planning-data/generation-information"
)
NEM_GEN_INFO_BASE_URL = (
    "https://www.aemo.com.au/-/media/files/electricity/nem/"
    "planning_and_forecasting/generation_information/"
)

# ELI report chart data (location-based projected curtailment). AEMO moved the
# ELI files under planning_and_forecasting/enhanced-locational-information/<year>/
# in 2025; src/download_eli.py logs a warning when next year's edition appears.
ELI_BASE_URL = (
    "https://www.aemo.com.au/-/media/files/electricity/nem/"
    "planning_and_forecasting/enhanced-locational-information/"
)
# AEMO's ELI page, where a new edition is announced; scripts get 403 from it, so the
# pipeline probes a guessed file name instead and points people here when it finds nothing
ELI_PAGE_URL = (
    "https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/"
    "nem-forecasting-and-planning/forecasting-and-planning-data/enhanced-locational-information"
)
# ELI projected-curtailment horizons, as AEMO's 2025 ELI report states them (executive
# summary: "near-term (2026 to 2028), and medium-term (2030 to 2035) horizons"; Table 2
# calls the conditions representative of 2026-2029 and 2031-2035, depending on the speed
# of development). The page, README and workbooks label the columns with these.
ELI_HORIZONS = {"NEAR": (2026, 2028), "MED": (2030, 2035)}

ELI_CHART_DATA_URLS = {
    2025: ELI_BASE_URL + "2025/2025-eli-report-chart-data.xlsx",
}

# ELI regional appendices (PDF): REZ membership by DUID and the ISP REZ
# forecasts. Read by `python -m src.eli_appendix` once per edition; the
# pipeline uses the data/rez_membership.feather and data/rez_forecasts.feather
# it writes.
ELI_REGIONAL_APPENDIX_URLS = {
    2025: {
        state: ELI_BASE_URL + f"2025/2025-eli-report-appendix-{part}.pdf"
        for state, part in {
            "NSW": "a3-new-south-wales", "QLD": "a4-queensland", "SA": "a5-south-australia",
            "TAS": "a6-tasmania", "VIC": "a7-victoria",
        }.items()
    },
}

# The REZ curtailment / economic-offloading forecasts in the 2025 ELI appendices are
# the Final 2024 ISP's (Step Change scenario), as the appendices state; their three
# years (2025-26 to 2027-28) were forecast in 2024, so the first has since ended.
ISP_FORECAST_EDITION = "2024 ISP"
ISP_FORECAST_SCENARIO = "Step Change"

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
