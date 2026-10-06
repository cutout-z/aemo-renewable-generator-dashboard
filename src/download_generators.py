"""Download and parse solar + wind farm listing from AEMO.

Primary source: NEM Registration and Exemption List (refreshed every run)
Cross-check: MMSDM DUDETAILSUMMARY (src/dudetail.py)
Enrichment: newest NEM Generation Information edition (src/gen_info.py) for any
REZ/location/voltage columns it carries, then the seeded workbook data
"""

from __future__ import annotations

import io
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from . import config, dudetail, eli_appendix, gen_info as gen_info_mod, source_status

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

# Columns taken from Generation Information when an edition carries them.
# NAMEPLATE_MW is deliberately not taken: its capacities are per survey row
# (stages, DC/AC) and would add a third MW source to the table. REZ is handled
# separately by assign_rez().
GEN_INFO_ENRICH_COLS = ["LOCATION", "VOLTAGE_KV"]
SEED_ENRICH_COLS = ["LOCATION", "VOLTAGE_KV", "UNIT_STATUS", "NAMEPLATE_MW"]

# REZ output contract (summary.csv):
#   REZ        "Y" in a REZ | "N" a source says outside every REZ | "" unknown
#   REZ_NAME   zone name | "Non-REZ" (only with REZ "N") | "" unknown
#   REZ_SOURCE "geninfo" | "eli" | "eli-station" | "seed" | "" — where the Y/N came from
NON_REZ_NAME = "Non-REZ"
_NON_REZ_VALUES = {"non-rez", "non rez", "nonrez", "not in a rez", "outside rez",
                   "outside a rez", "no rez"}
# Blank-ish cells say nothing: they are never read as "outside a REZ"
_UNSTATED_VALUES = {"", "-", "n/a", "na", "nan", "none", "tbc", "tbd", "unknown"}


def fetch_generators(cache_dir: str) -> pd.DataFrame:
    """Download generator data and extract solar + wind farms.

    Returns DataFrame with columns:
        DUID, PROJECT_NAME, LOCATION, REZ, REZ_NAME, REZ_SOURCE, STATE, REGIONID,
        NAMEPLATE_MW, VOLTAGE_KV, FUEL_TYPE, TECHNOLOGY, UNIT_STATUS
    (REZ contract: see NON_REZ_NAME above / assign_rez)
    """
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)

    # ── Primary: NEM Registration List (refreshed every run) ────────
    reg_path = refresh_registration_list(cache_path)

    generators = _parse_registration_list(reg_path)
    if generators.empty:
        raise RuntimeError(f"No solar/wind generators parsed from {reg_path}")

    # ── NEM Generation Information: newest edition, cached ──────────
    gen_info = _load_gen_info(cache_path)

    # ── Cross-check: units AEMO has registered that the list lacks ──
    _cross_check_dudetailsummary(cache_path, _registered_duids(reg_path), gen_info)

    # ── Enrichment: NEM Generation Information ──────────────────────
    if gen_info is not None and not gen_info.empty:
        generators = _enrich_with_gen_info(generators, gen_info, columns=GEN_INFO_ENRICH_COLS)

    # ── Enrichment: Seeded data from workbook (if available) ────────
    enrich_path = Path(cache_dir) / "generator_enrichment.feather"
    seed = None
    if enrich_path.exists():
        logger.info("Enriching from seeded workbook data...")
        seed = pd.read_feather(enrich_path)
        generators = _enrich_with_gen_info(generators, seed, columns=SEED_ENRICH_COLS)

    # Build REGIONID from STATE
    if "STATE" in generators.columns:
        generators["REGIONID"] = generators["STATE"].map(config.STATE_TO_REGION)
    elif "REGIONID" in generators.columns:
        reverse_map = {v: k for k, v in config.STATE_TO_REGION.items()}
        generators["STATE"] = generators["REGIONID"].map(config.REGION_NAMES)

    # Determine REZ membership: Y / N / unknown, with its source
    membership = eli_appendix.load_membership(cache_path)
    dud = dudetail.load_cached(cache_path)
    stations = dudetail.station_ids(dud) if dud is not None else {}
    generators = assign_rez(generators, gen_info=gen_info, seed=seed,
                            membership=membership, stations=stations)

    # Select final columns
    keep_cols = [
        "DUID", "PROJECT_NAME", "LOCATION", "REZ", "REZ_NAME", "REZ_SOURCE",
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


def classify_rez_value(value) -> tuple[str, str] | None:
    """Read one REZ cell: ("Y", zone) | ("N", "Non-REZ") | None if it says nothing."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    low = text.lower()
    if low in _UNSTATED_VALUES:
        return None
    if low in _NON_REZ_VALUES:
        return "N", NON_REZ_NAME
    return "Y", text


def assign_rez(generators: pd.DataFrame, gen_info: pd.DataFrame | None = None,
               seed: pd.DataFrame | None = None, membership: pd.DataFrame | None = None,
               stations: dict[str, str] | None = None) -> pd.DataFrame:
    """Set REZ / REZ_NAME / REZ_SOURCE for every unit, wind included.

    Precedence:
      1. Generation Information where it states a REZ (no edition has a REZ
         column today) — "geninfo";
      2. the ELI regional appendices, which list each unit under its REZ or
         under "Non-REZ" (src/eli_appendix.py) — "eli";
      3. a unit the appendices don't list, at the same DUDETAILSUMMARY station
         as units they do list, all in one section — "eli-station"
         (WANDSF2 from WANDSF1, CLRKCWF2 from CLRKCWF1);
      4. the seeded workbook's "REZ (Y/N)" + "REZ" columns — "seed";
      otherwise unknown (""). "N"/"Non-REZ" is only ever written when a source
    says so explicitly. A blank or missing value is unknown, never "outside".
    """
    out = generators.copy()
    gi = {}
    if gen_info is not None and "REZ_NAME" in gen_info.columns:
        gi = dict(zip(gen_info["DUID"].astype(str), gen_info["REZ_NAME"]))
    sd = {}
    if seed is not None and "DUID" in seed.columns:
        for r in seed.drop_duplicates(subset="DUID").to_dict("records"):
            sd[str(r["DUID"])] = r
    eli, by_station = {}, {}
    if membership is not None and not membership.empty:
        eli = dict(zip(membership["DUID"].astype(str), membership["REZ_NAME"]))
        stations = stations or {}
        sections: dict[str, set[str]] = {}
        for duid, name in eli.items():
            if duid in stations:
                sections.setdefault(stations[duid], set()).add(name)
        by_station = {st: names.pop() for st, names in sections.items() if len(names) == 1}

    rez, names, sources = [], [], []
    for duid in out["DUID"].astype(str):
        result, source = classify_rez_value(gi.get(duid)), "geninfo"
        if result is None and duid in eli:
            result, source = classify_rez_value(eli[duid]), "eli"
        if result is None and (stations or {}).get(duid) in by_station:
            result, source = classify_rez_value(by_station[stations[duid]]), "eli-station"
        if result is None and duid in sd:
            source = "seed"
            row = sd[duid]
            flag = str(row.get("REZ") or "").strip().upper()
            named = classify_rez_value(row.get("REZ_NAME"))
            if flag == "N":
                result = ("N", NON_REZ_NAME)
            elif flag == "Y" and named and named[0] == "Y":
                result = named
            elif flag not in ("Y", "N"):
                result = named
        if result is None:
            rez.append(""); names.append(""); sources.append("")
        else:
            rez.append(result[0]); names.append(result[1]); sources.append(source)

    out["REZ"] = rez
    out["REZ_NAME"] = names
    out["REZ_SOURCE"] = sources
    if "FUEL_TYPE" in out.columns:
        counts = out.groupby(["FUEL_TYPE", "REZ"]).size().to_dict()
        logger.info("REZ membership (fuel, Y/N/''=unknown): "
                    + ", ".join(f"{f} {r or 'unknown'}: {n}" for (f, r), n in sorted(counts.items())))
    return out


def _load_gen_info(cache_path: Path) -> pd.DataFrame | None:
    """Refresh (if a newer edition exists) and parse Generation Information."""
    try:
        path, edition = gen_info_mod.refresh_gen_info(cache_path)
        if path is None:
            return None
        df = gen_info_mod.parse_gen_info(path)
        df["GEN_INFO_EDITION"] = edition
        return df
    except Exception as e:
        logger.warning(f"NEM Generation Information unavailable: {e}")
        return None


def _cross_check_dudetailsummary(cache_path: Path, registered: set[str],
                                 gen_info: pd.DataFrame | None = None) -> None:
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
            tech = {}
            if gen_info is not None and "TECHNOLOGY" in gen_info.columns:
                tech = dict(zip(gen_info["DUID"], gen_info["TECHNOLOGY"].astype(str)))
            record.update(month=month, recent_generators=len(recent),
                          missing_from_registration=[
                              {"DUID": r.DUID, "REGIONID": r.REGIONID,
                               "STATIONID": r.STATIONID,
                               "first_start": r.FIRST_START.date().isoformat(),
                               "gen_info_technology": tech.get(r.DUID)}
                              for r in missing.itertuples()])
            logger.info(f"DUDETAILSUMMARY {month}: {len(recent)} GENERATOR DUIDs registered "
                        f"in the last {dudetail.RECENT_MONTHS} months, "
                        f"{len(missing)} missing from the Registration List")
            for r in missing.itertuples():
                logger.warning(f"Registration List lacks {r.DUID} ({r.STATIONID}, {r.REGIONID}), "
                               f"in DUDETAILSUMMARY since {r.FIRST_START.date()}; "
                               + (f"Generation Information says {tech[r.DUID]!r}; "
                                  if r.DUID in tech else "fuel unknown; ")
                               + "not added")
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

    # Map columns (candidates in order of preference: the descriptor columns,
    # e.g. "Wind - Onshore", win over the "- Primary" ones, e.g. "Renewable")
    mappings = {
        "DUID": ["duid"],
        "PROJECT_NAME": ["station name", "station"],
        "REGIONID": ["region"],
        "TECHNOLOGY": ["technology type - descriptor", "technology type"],
        # Primary ("Solar", "Wind", "Battery Storage"): the descriptor names the
        # co-located fuel for hybrid batteries (HPR1 "Wind"), which must stay out
        "FUEL_SOURCE": ["fuel source - primary", "fuel source"],
        "NAMEPLATE_MW": ["reg cap generation (mw)", "reg cap (mw)", "nameplate capacity"],
        "DISPATCH_TYPE": ["dispatch type"],
        "CLASSIFICATION": ["classification"],
    }
    df = df.rename(columns=_map_columns(df.columns, mappings))
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


def _enrich_with_gen_info(generators: pd.DataFrame, gen_info: pd.DataFrame,
                          columns: list[str] | None = None) -> pd.DataFrame:
    """Enrich registration list data with location/REZ/voltage from another source."""
    enrichment_cols = []
    # NAMEPLATE_MW: prefer the enriching source's nameplate over Registration List reg cap
    prefer_enriched = set()
    for col in columns or ["LOCATION", "REZ_NAME", "VOLTAGE_KV", "UNIT_STATUS", "NAMEPLATE_MW"]:
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

    logger.info(f"Enriched generators with {enrichment_cols}")
    return result


def _map_columns(columns, mappings: dict[str, list[str]]) -> dict:
    """Map source headers to standard names.

    For each target, candidates are tried in order: first as an exact
    (case-insensitive) header, then as a substring. A header is used once.
    """
    lower = {c: str(c).lower().strip() for c in columns}
    col_map: dict = {}
    for target, candidates in mappings.items():
        found = None
        for cand in candidates:
            found = next((c for c, l in lower.items() if l == cand and c not in col_map), None)
            if found is not None:
                break
        if found is None:
            for cand in candidates:
                found = next((c for c, l in lower.items() if cand in l and c not in col_map), None)
                if found is not None:
                    break
        if found is not None:
            col_map[found] = target
    return col_map


def _detect_gen_info_columns(df: pd.DataFrame) -> dict:
    """Map NEM Gen Info column headers to standard names."""
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
    return _map_columns(df.columns, mappings)


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
