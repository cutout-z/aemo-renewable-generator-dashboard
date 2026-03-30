"""Download and parse solar + wind farm listing from AEMO NEM Generation Information."""

import logging
import time
from pathlib import Path

import pandas as pd
import requests

from . import config

logger = logging.getLogger(__name__)

# Sheet in the NEM Generation Information workbook
EXISTING_SHEET = "ExistingGeneration&NewDevs"


def fetch_generators(cache_dir: str) -> pd.DataFrame:
    """Download NEM Generation Information and extract solar + wind farms.

    Returns DataFrame with columns:
        DUID, PROJECT_NAME, LOCATION, REZ, REZ_NAME, STATE, REGIONID,
        NAMEPLATE_MW, VOLTAGE_KV, FUEL_TYPE, TECHNOLOGY, UNIT_STATUS
    """
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)
    xlsx_path = cache_path / "nem-generation-information.xlsx"

    if not xlsx_path.exists():
        logger.info("Downloading NEM Generation Information from AEMO...")
        _download_with_retry(config.NEM_GEN_INFO_URL, xlsx_path)

    logger.info("Parsing NEM Generation Information...")
    df = pd.read_excel(xlsx_path, engine="openpyxl", sheet_name=EXISTING_SHEET)

    # Standardise column names — AEMO changes these slightly between releases
    col_map = _detect_columns(df)
    df = df.rename(columns=col_map)

    # Filter to solar and wind only
    df = _filter_solar_wind(df)

    # Filter to existing/committed generators
    if "UNIT_STATUS" in df.columns:
        valid_statuses = [
            "Existing", "Committed", "In Commissioning",
            "In Service", "Existing and Committed",
        ]
        status_mask = df["UNIT_STATUS"].str.strip().isin(valid_statuses)
        # Also keep rows where status contains "Existing" or "Committed"
        contains_mask = (
            df["UNIT_STATUS"].str.contains("Existing", case=False, na=False) |
            df["UNIT_STATUS"].str.contains("Committed", case=False, na=False) |
            df["UNIT_STATUS"].str.contains("Commissioning", case=False, na=False)
        )
        df = df[status_mask | contains_mask].copy()

    # Build REGIONID from State
    if "STATE" in df.columns:
        df["REGIONID"] = df["STATE"].map(config.STATE_TO_REGION)

    # Clean up
    df = df.dropna(subset=["DUID"])
    df["DUID"] = df["DUID"].astype(str).str.strip()
    df["NAMEPLATE_MW"] = pd.to_numeric(df.get("NAMEPLATE_MW"), errors="coerce")
    df["VOLTAGE_KV"] = pd.to_numeric(df.get("VOLTAGE_KV"), errors="coerce")

    # Determine fuel type from technology
    df["FUEL_TYPE"] = df.apply(_classify_fuel, axis=1)

    # Determine REZ membership
    if "REZ_NAME" in df.columns:
        df["REZ"] = df["REZ_NAME"].apply(
            lambda x: "N" if pd.isna(x) or str(x).strip() in ("", "Non-REZ", "-", "N/A") else "Y"
        )
        df["REZ_NAME"] = df["REZ_NAME"].fillna("Non-REZ")
    else:
        df["REZ"] = "N"
        df["REZ_NAME"] = "Non-REZ"

    # Select final columns
    keep_cols = [
        "DUID", "PROJECT_NAME", "LOCATION", "REZ", "REZ_NAME",
        "STATE", "REGIONID", "NAMEPLATE_MW", "VOLTAGE_KV",
        "FUEL_TYPE", "TECHNOLOGY", "UNIT_STATUS",
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    df = df[keep_cols].copy()

    # Deduplicate by DUID
    df = df.drop_duplicates(subset="DUID", keep="first")

    logger.info(f"Loaded {len(df)} solar/wind generators "
                f"({len(df[df['FUEL_TYPE'] == 'Solar'])} solar, "
                f"{len(df[df['FUEL_TYPE'] == 'Wind'])} wind)")
    return df


def _detect_columns(df: pd.DataFrame) -> dict:
    """Map AEMO's column headers to our standard names.

    AEMO changes column names slightly between quarterly releases,
    so we do fuzzy matching.
    """
    col_map = {}
    columns_lower = {c: c.lower().strip() for c in df.columns}

    mappings = {
        "DUID": ["duid"],
        "PROJECT_NAME": ["site name", "station name", "project name"],
        "LOCATION": ["location", "connection point", "connection point name"],
        "STATE": ["region", "state"],
        "TECHNOLOGY": [
            "technology type", "technology type - descriptor",
            "technology type - primary",
        ],
        "FUEL_TYPE_RAW": [
            "fuel type", "fuel source - descriptor", "fuel source - primary",
            "fuel type - primary", "fuel bucket summary",
        ],
        "NAMEPLATE_MW": [
            "nameplate capacity (mw)", "reg cap generation (mw)",
            "upper nameplate capacity (mw)", "nameplate capacity",
        ],
        "VOLTAGE_KV": ["voltage (kv)", "voltage level (kv)", "voltage"],
        "UNIT_STATUS": [
            "unit status", "status", "status bucket summary",
        ],
        "REZ_NAME": ["rez", "rez name", "renewable energy zone"],
    }

    for target, candidates in mappings.items():
        for orig_col, lower_col in columns_lower.items():
            if any(c in lower_col for c in candidates):
                if target not in col_map:
                    col_map[orig_col] = target
                break

    return col_map


def _filter_solar_wind(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to solar and wind generators using technology and fuel type columns."""
    masks = []

    if "TECHNOLOGY" in df.columns:
        tech_lower = df["TECHNOLOGY"].astype(str).str.lower()
        masks.append(
            tech_lower.str.contains("solar|photovoltaic", na=False) |
            tech_lower.str.contains("wind", na=False)
        )

    if "FUEL_TYPE_RAW" in df.columns:
        fuel_lower = df["FUEL_TYPE_RAW"].astype(str).str.lower()
        masks.append(
            fuel_lower.str.contains("solar", na=False) |
            fuel_lower.str.contains("wind", na=False)
        )

    if masks:
        combined = masks[0]
        for m in masks[1:]:
            combined = combined | m
        return df[combined].copy()

    logger.warning("Could not identify technology/fuel columns for filtering")
    return df


def _classify_fuel(row) -> str:
    """Classify a generator as Solar or Wind from its technology/fuel columns."""
    for col in ["TECHNOLOGY", "FUEL_TYPE_RAW"]:
        val = str(row.get(col, "")).lower()
        if "solar" in val or "photovoltaic" in val:
            return "Solar"
        if "wind" in val:
            return "Wind"
    return "Unknown"


def _download_with_retry(url: str, dest: Path):
    """Download a file with retry logic."""
    for attempt in range(config.MAX_RETRIES):
        try:
            resp = requests.get(
                url,
                timeout=config.REQUEST_TIMEOUT,
                headers={"User-Agent": config.USER_AGENT},
            )
            resp.raise_for_status()
            dest.write_bytes(resp.content)
            logger.info(f"Downloaded {len(resp.content) / 1024:.0f} KB → {dest.name}")
            return
        except requests.RequestException as e:
            if attempt < config.MAX_RETRIES - 1:
                wait = config.RETRY_BACKOFF * (attempt + 1)
                logger.warning(f"Download failed (attempt {attempt + 1}): {e}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise RuntimeError(f"Failed to download {url}: {e}")
