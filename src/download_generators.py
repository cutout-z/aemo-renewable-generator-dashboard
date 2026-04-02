"""Download and parse solar + wind farm listing from AEMO.

Primary source: NEM Registration and Exemption List (always available)
Enrichment: NEM Generation Information workbook (when downloadable) for REZ/location/voltage
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import pandas as pd
import requests

from . import config

logger = logging.getLogger(__name__)

# NEM Registration and Exemption List — reliable, always available
REGISTRATION_URL = (
    "https://www.aemo.com.au/-/media/Files/Electricity/NEM/"
    "Participant_Information/NEM-Registration-and-Exemption-List.xls"
)
REGISTRATION_SHEET = "PU and Scheduled Loads"

# NEM Generation Information — has REZ/location/voltage but URL changes quarterly
NEM_GEN_INFO_SHEETS = [
    "ExistingGeneration&NewDevs",
    "Existing Generation & New Devs",
    "ExistingGeneration-Registered",
]


def fetch_generators(cache_dir: str) -> pd.DataFrame:
    """Download generator data and extract solar + wind farms.

    Returns DataFrame with columns:
        DUID, PROJECT_NAME, LOCATION, REZ, REZ_NAME, STATE, REGIONID,
        NAMEPLATE_MW, VOLTAGE_KV, FUEL_TYPE, TECHNOLOGY, UNIT_STATUS
    """
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)

    # ── Primary: NEM Registration List (always works) ───────────────
    reg_path = cache_path / "NEM-Registration-and-Exemption-List.xls"
    if not reg_path.exists():
        logger.info("Downloading NEM Registration List from AEMO...")
        _download_with_retry(REGISTRATION_URL, reg_path)

    generators = _parse_registration_list(reg_path)

    # ── Enrichment: NEM Generation Information (may fail) ───────────
    gen_info = _try_download_gen_info(cache_path)
    if gen_info is not None and not gen_info.empty:
        generators = _enrich_with_gen_info(generators, gen_info)
    else:
        logger.info("NEM Generation Information not available")

    # ── Enrichment: Seeded data from workbook (if available) ────────
    enrich_path = Path(cache_dir) / "generator_enrichment.feather"
    if enrich_path.exists():
        logger.info("Enriching from seeded workbook data...")
        enrich = pd.read_feather(enrich_path)
        generators = _enrich_with_gen_info(generators, enrich)

    # Build REGIONID from STATE
    if "STATE" in generators.columns:
        generators["REGIONID"] = generators["STATE"].map(config.STATE_TO_REGION)
    elif "REGIONID" in generators.columns:
        reverse_map = {v: k for k, v in config.STATE_TO_REGION.items()}
        generators["STATE"] = generators["REGIONID"].map(config.REGION_NAMES)

    # Determine REZ membership
    if "REZ_NAME" in generators.columns:
        generators["REZ"] = generators["REZ_NAME"].apply(
            lambda x: "N" if pd.isna(x) or str(x).strip() in ("", "Non-REZ", "-", "N/A") else "Y"
        )
        generators["REZ_NAME"] = generators["REZ_NAME"].fillna("Non-REZ")
    else:
        generators["REZ"] = "N"
        generators["REZ_NAME"] = "Non-REZ"

    # Select final columns
    keep_cols = [
        "DUID", "PROJECT_NAME", "LOCATION", "REZ", "REZ_NAME",
        "STATE", "REGIONID", "NAMEPLATE_MW", "VOLTAGE_KV",
        "FUEL_TYPE", "TECHNOLOGY", "UNIT_STATUS",
    ]
    keep_cols = [c for c in keep_cols if c in generators.columns]
    generators = generators[keep_cols].copy()

    # Deduplicate by DUID
    generators = generators.drop_duplicates(subset="DUID", keep="first")

    solar_count = len(generators[generators["FUEL_TYPE"] == "Solar"])
    wind_count = len(generators[generators["FUEL_TYPE"] == "Wind"])
    logger.info(f"Loaded {len(generators)} solar/wind generators "
                f"({solar_count} solar, {wind_count} wind)")
    return generators


def _parse_registration_list(xls_path: Path) -> pd.DataFrame:
    """Parse the NEM Registration and Exemption List for solar/wind generators."""
    logger.info("Parsing NEM Registration List...")

    # Try openpyxl first, fall back to xlrd for .xls
    try:
        df = pd.read_excel(xls_path, engine="openpyxl", sheet_name=REGISTRATION_SHEET)
    except Exception:
        try:
            df = pd.read_excel(xls_path, sheet_name=REGISTRATION_SHEET)
        except Exception as e:
            logger.error(f"Failed to parse Registration List: {e}")
            return pd.DataFrame()

    # Map columns
    col_map = {}
    columns_lower = {c: c.lower().strip() for c in df.columns}
    mappings = {
        "DUID": ["duid"],
        "PROJECT_NAME": ["station name", "station"],
        "REGIONID": ["region"],
        "TECHNOLOGY": ["technology type - descriptor", "technology type"],
        "FUEL_SOURCE": ["fuel source - descriptor", "fuel source - primary"],
        "NAMEPLATE_MW": ["reg cap generation (mw)", "reg cap (mw)", "nameplate capacity"],
        "DISPATCH_TYPE": ["dispatch type"],
        "CLASSIFICATION": ["classification"],
    }
    for target, candidates in mappings.items():
        for orig_col, lower_col in columns_lower.items():
            if any(c in lower_col for c in candidates):
                if target not in col_map:
                    col_map[orig_col] = target
                break

    df = df.rename(columns=col_map)
    df = df.dropna(subset=["DUID"])
    df["DUID"] = df["DUID"].astype(str).str.strip()
    df = df[df["DUID"] != "-"]  # Exclude placeholder DUIDs (e.g. Portland Wind Farm, Callide)

    # Filter to solar and wind
    mask = pd.Series(False, index=df.index)
    for col in ["TECHNOLOGY", "FUEL_SOURCE"]:
        if col in df.columns:
            col_lower = df[col].astype(str).str.lower()
            mask = mask | col_lower.str.contains("solar|photovoltaic", na=False)
            mask = mask | col_lower.str.contains("wind", na=False)
    df = df[mask].copy()

    # Classify fuel type
    df["FUEL_TYPE"] = df.apply(_classify_fuel, axis=1)

    # Convert capacity
    if "NAMEPLATE_MW" in df.columns:
        df["NAMEPLATE_MW"] = pd.to_numeric(df["NAMEPLATE_MW"], errors="coerce")

    # Map REGIONID to STATE
    if "REGIONID" in df.columns:
        df["STATE"] = df["REGIONID"].map(config.REGION_NAMES)

    df = df.drop_duplicates(subset="DUID", keep="first")
    logger.info(f"Parsed {len(df)} solar/wind generators from Registration List")
    return df


def _try_download_gen_info(cache_path: Path) -> pd.DataFrame | None:
    """Try to download and parse NEM Generation Information for enrichment."""
    xlsx_path = cache_path / "nem-generation-information.xlsx"

    if not xlsx_path.exists():
        try:
            logger.info("Trying to download NEM Generation Information...")
            _download_with_retry(config.NEM_GEN_INFO_URL, xlsx_path)
        except Exception as e:
            logger.info(f"NEM Generation Information download failed: {e}")
            return None

    try:
        xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
        sheet = None
        for candidate in NEM_GEN_INFO_SHEETS:
            if candidate in xls.sheet_names:
                sheet = candidate
                break
        if sheet is None:
            logger.warning(f"No matching sheet in Gen Info. Available: {xls.sheet_names}")
            return None

        df = pd.read_excel(xls, sheet_name=sheet)

        # Map columns
        col_map = _detect_gen_info_columns(df)
        df = df.rename(columns=col_map)

        # Filter to solar/wind with DUID
        df = df.dropna(subset=["DUID"])
        df["DUID"] = df["DUID"].astype(str).str.strip()

        mask = pd.Series(False, index=df.index)
        for col in ["TECHNOLOGY", "FUEL_TYPE_RAW"]:
            if col in df.columns:
                col_lower = df[col].astype(str).str.lower()
                mask = mask | col_lower.str.contains("solar|photovoltaic", na=False)
                mask = mask | col_lower.str.contains("wind", na=False)
        df = df[mask].copy()

        logger.info(f"Parsed {len(df)} generators from NEM Gen Info for enrichment")
        return df

    except Exception as e:
        logger.warning(f"Failed to parse NEM Gen Info: {e}")
        return None


def _enrich_with_gen_info(generators: pd.DataFrame, gen_info: pd.DataFrame) -> pd.DataFrame:
    """Enrich registration list data with location/REZ/voltage from Gen Info."""
    enrichment_cols = []
    # NAMEPLATE_MW: prefer Gen Info nameplate over Registration List reg cap
    prefer_enriched = set()
    for col in ["LOCATION", "REZ_NAME", "VOLTAGE_KV", "UNIT_STATUS", "NAMEPLATE_MW"]:
        if col in gen_info.columns:
            enrichment_cols.append(col)
            if col == "NAMEPLATE_MW":
                prefer_enriched.add(col)

    if not enrichment_cols:
        return generators

    enrich = gen_info[["DUID"] + enrichment_cols].drop_duplicates(subset="DUID", keep="first")
    result = generators.merge(enrich, on="DUID", how="left", suffixes=("", "_enriched"))

    for col in enrichment_cols:
        enriched_col = f"{col}_enriched"
        if enriched_col in result.columns:
            if col in result.columns:
                if col in prefer_enriched:
                    # Prefer enriched value, fall back to original
                    result[col] = result[enriched_col].fillna(result[col])
                else:
                    result[col] = result[col].fillna(result[enriched_col])
            else:
                result[col] = result[enriched_col]
            result = result.drop(columns=[enriched_col])

    logger.info(f"Enriched generators with {enrichment_cols} from NEM Gen Info")
    return result


def _detect_gen_info_columns(df: pd.DataFrame) -> dict:
    """Map NEM Gen Info column headers to standard names."""
    col_map = {}
    columns_lower = {c: c.lower().strip() for c in df.columns}

    mappings = {
        "DUID": ["duid"],
        "PROJECT_NAME": ["site name", "station name", "project name"],
        "LOCATION": ["location", "connection point"],
        "STATE": ["region", "state"],
        "TECHNOLOGY": ["technology type", "technology type - descriptor"],
        "FUEL_TYPE_RAW": ["fuel type", "fuel source - descriptor", "fuel bucket summary"],
        "NAMEPLATE_MW": ["nameplate capacity (mw)", "upper nameplate capacity (mw)"],
        "VOLTAGE_KV": ["voltage (kv)", "voltage"],
        "UNIT_STATUS": ["unit status", "status bucket summary"],
        "REZ_NAME": ["rez", "rez name", "renewable energy zone"],
    }

    for target, candidates in mappings.items():
        for orig_col, lower_col in columns_lower.items():
            if any(c in lower_col for c in candidates):
                if target not in col_map:
                    col_map[orig_col] = target
                break

    return col_map


def _classify_fuel(row) -> str:
    """Classify a generator as Solar or Wind from its technology/fuel columns."""
    for col in ["TECHNOLOGY", "FUEL_SOURCE", "FUEL_TYPE_RAW"]:
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
