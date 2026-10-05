"""Download and parse solar + wind farm listing from AEMO.

Primary source: NEM Registration and Exemption List (always available)
Enrichment: NEM Generation Information workbook (when downloadable) for REZ/location/voltage
"""

from __future__ import annotations

import io
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from . import config, dudetail, source_status

logger = logging.getLogger(__name__)

# NEM Registration and Exemption List — reliable, always available
REGISTRATION_URL = (
    "https://www.aemo.com.au/-/media/Files/Electricity/NEM/"
    "Participant_Information/NEM-Registration-and-Exemption-List.xls"
)
REGISTRATION_SHEET = "PU and Scheduled Loads"
REGISTRATION_FILE = "NEM-Registration-and-Exemption-List.xls"

# Leading bytes of a real workbook: xlsx is a zip, legacy xls an OLE2 compound file
_XLSX_MAGIC = b"PK\x03\x04"
_XLS_MAGIC = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"

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

    # ── Primary: NEM Registration List (refreshed every run) ────────
    reg_path = refresh_registration_list(cache_path)

    generators = _parse_registration_list(reg_path)
    if generators.empty:
        raise RuntimeError(f"No solar/wind generators parsed from {reg_path}")

    # ── Cross-check: units AEMO has registered that the list lacks ──
    _cross_check_dudetailsummary(cache_path, _registered_duids(reg_path))

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


def refresh_registration_list(cache_path: Path) -> Path:
    """Download the Registration List on every run, keeping the last good copy.

    The download only replaces the cached file if it is a real workbook with
    the expected sheet (AEMO's Cloudflare front sometimes answers scripted
    requests with an HTML challenge page). On failure the previous copy is used
    and the failure is logged at ERROR with its age; with no previous copy the
    run cannot continue.
    """
    reg_path = cache_path / REGISTRATION_FILE
    previous = source_status.load(cache_path).get("registration_list", {})
    last_good = previous.get("fetched_at")
    if last_good is None and reg_path.exists():
        last_good = datetime.fromtimestamp(reg_path.stat().st_mtime, timezone.utc).isoformat()

    record = {"url": REGISTRATION_URL, "file": REGISTRATION_FILE,
              "fetched_at": last_good, "refreshed": False, "error": None,
              "checked_at": source_status.now_iso()}
    try:
        logger.info("Downloading NEM Registration List from AEMO...")
        content = _fetch_bytes(REGISTRATION_URL)
        check_workbook_bytes(content, REGISTRATION_SHEET)
        tmp = reg_path.with_name(reg_path.name + ".part")
        tmp.write_bytes(content)
        tmp.replace(reg_path)
        record.update(fetched_at=source_status.now_iso(), refreshed=True)
        logger.info(f"Registration List refreshed ({len(content) / 1024:.0f} KB)")
    except Exception as e:
        record["error"] = str(e)
        if not reg_path.exists():
            source_status.update(cache_path, "registration_list", record)
            raise RuntimeError(f"Registration List download failed and no cached copy: {e}")
        age = source_status.age_days(last_good)
        age_txt = f"{age:.0f} days old" if age is not None else "age unknown"
        logger.error("!" * 72)
        logger.error(f"REGISTRATION LIST REFRESH FAILED: {e}")
        logger.error(f"Using the last good copy (fetched {last_good}, {age_txt}); "
                     "units registered since then are missing from the dashboard.")
        logger.error("!" * 72)

    source_status.update(cache_path, "registration_list", record)
    return reg_path


def check_workbook_bytes(content: bytes, required_sheet: str | None = None) -> None:
    """Raise ValueError unless `content` is a workbook (with `required_sheet`)."""
    if not (content.startswith(_XLSX_MAGIC) or content.startswith(_XLS_MAGIC)):
        head = content[:2048].lower()
        if b"<html" in head or b"<!doctype" in head:
            kind = "Cloudflare challenge page" if b"just a moment" in head else "HTML page"
            raise ValueError(f"got an {kind}, not a workbook")
        raise ValueError("response is not an Excel workbook")
    try:
        sheets = pd.ExcelFile(io.BytesIO(content)).sheet_names
    except Exception as e:
        raise ValueError(f"workbook does not open: {e}")
    if required_sheet and required_sheet not in sheets:
        raise ValueError(f"workbook lacks sheet {required_sheet!r} (has {sheets})")


def _registered_duids(xls_path: Path) -> set[str]:
    """Every DUID on the Registration List's generator sheet, any fuel."""
    try:
        df = pd.read_excel(xls_path, sheet_name=REGISTRATION_SHEET)
    except Exception as e:
        logger.warning(f"Could not read DUIDs from Registration List: {e}")
        return set()
    duid_col = next((c for c in df.columns if str(c).strip().lower() == "duid"), None)
    if duid_col is None:
        return set()
    duids = df[duid_col].dropna().astype(str).str.strip()
    return set(duids[duids != "-"])


def _cross_check_dudetailsummary(cache_path: Path, registered: set[str]) -> None:
    """Warn about recently registered GENERATOR DUIDs the Registration List lacks."""
    previous = source_status.load(cache_path).get("dudetailsummary", {})
    record = {"months_window": dudetail.RECENT_MONTHS, "checked_at": source_status.now_iso(),
              "month": previous.get("month"), "error": None}
    try:
        dud, month = dudetail.fetch_latest_dudetailsummary(
            cache_path, cached_month=previous.get("month"))
        if dud is None or not registered:
            record["error"] = "DUDETAILSUMMARY or Registration List DUIDs unavailable"
            logger.warning(f"Skipping DUDETAILSUMMARY cross-check: {record['error']}")
        else:
            recent = dudetail.recent_generators(dud)
            missing = dudetail.missing_from_registration(dud, registered)
            record.update(month=month, recent_generators=len(recent),
                          missing_from_registration=[
                              {"DUID": r.DUID, "REGIONID": r.REGIONID,
                               "STATIONID": r.STATIONID,
                               "first_start": r.FIRST_START.date().isoformat()}
                              for r in missing.itertuples()])
            logger.info(f"DUDETAILSUMMARY {month}: {len(recent)} GENERATOR DUIDs registered "
                        f"in the last {dudetail.RECENT_MONTHS} months, "
                        f"{len(missing)} missing from the Registration List")
            for r in missing.itertuples():
                logger.warning(f"Registration List lacks {r.DUID} ({r.STATIONID}, {r.REGIONID}), "
                               f"in DUDETAILSUMMARY since {r.FIRST_START.date()}; "
                               "fuel unknown, not added")
    except Exception as e:
        record["error"] = str(e)
        logger.warning(f"DUDETAILSUMMARY cross-check failed: {e}")
    source_status.update(cache_path, "dudetailsummary", record)


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


def _fetch_bytes(url: str) -> bytes:
    """GET a URL with retry logic and return the body."""
    for attempt in range(config.MAX_RETRIES):
        try:
            resp = requests.get(
                url,
                timeout=config.REQUEST_TIMEOUT,
                headers={"User-Agent": config.USER_AGENT},
            )
            resp.raise_for_status()
            return resp.content
        except requests.RequestException as e:
            if attempt < config.MAX_RETRIES - 1:
                wait = config.RETRY_BACKOFF * (attempt + 1)
                logger.warning(f"Download failed (attempt {attempt + 1}): {e}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise RuntimeError(f"Failed to download {url}: {e}")


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
