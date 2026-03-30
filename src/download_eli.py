"""Download and parse AEMO Enhanced Locational Information (ELI) report chart data.

The ELI report contains projected curtailment by connection point for:
- Near term (next 2-3 years): curtailment % at each connection point
- Medium term (5-10 years): curtailment % at each connection point
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import pandas as pd
import requests

from . import config

logger = logging.getLogger(__name__)

# Sheet names in the ELI chart data workbook
NEAR_TERM_SHEETS = [
    "Near Term Proj Curtailment",
    "Near Term Projected Curtailment",
    "Near-Term Projected Curtailment",
]
MEDIUM_TERM_SHEETS = [
    "Med Term Proj Curtailment",
    "Medium Term Proj Curtailment",
    "Medium Term Projected Curtailment",
    "Medium-Term Projected Curtailment",
]


def fetch_eli_curtailment(cache_dir: str, eli_year: int | None = None) -> pd.DataFrame:
    """Download and parse ELI report chart data for projected curtailment.

    Returns DataFrame with columns:
        LOCATION, VOLTAGE_KV, REGION, SOLAR_CURTAILMENT_NEAR, WIND_CURTAILMENT_NEAR,
        SOLAR_CURTAILMENT_MED, WIND_CURTAILMENT_MED
    """
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)

    # Determine which ELI report year to use
    if eli_year is None:
        eli_year = max(config.ELI_CHART_DATA_URLS.keys())

    url = config.ELI_CHART_DATA_URLS.get(eli_year)
    if not url:
        logger.error(f"No ELI chart data URL configured for year {eli_year}")
        return pd.DataFrame()

    xlsx_path = cache_path / f"eli_chart_data_{eli_year}.xlsx"

    if not xlsx_path.exists():
        logger.info(f"Downloading ELI chart data for {eli_year}...")
        _download_with_retry(url, xlsx_path)

    # Parse near-term and medium-term curtailment
    near_term = _parse_curtailment_sheet(xlsx_path, NEAR_TERM_SHEETS, "near")
    medium_term = _parse_curtailment_sheet(xlsx_path, MEDIUM_TERM_SHEETS, "med")

    if near_term.empty and medium_term.empty:
        logger.warning("No curtailment data parsed from ELI report")
        return pd.DataFrame()

    # Merge near and medium term on location + voltage + region
    if not near_term.empty and not medium_term.empty:
        result = pd.merge(
            near_term, medium_term,
            on=["LOCATION", "VOLTAGE_KV", "REGION"],
            how="outer",
        )
    elif not near_term.empty:
        result = near_term
    else:
        result = medium_term

    logger.info(f"Parsed ELI curtailment for {len(result)} connection points")
    return result


def _parse_curtailment_sheet(
    xlsx_path: Path,
    sheet_candidates: list[str],
    term: str,
) -> pd.DataFrame:
    """Parse a curtailment sheet from the ELI workbook.

    Args:
        xlsx_path: Path to the ELI chart data Excel file
        sheet_candidates: List of possible sheet names to try
        term: 'near' or 'med' — used for column naming

    Returns DataFrame with LOCATION, VOLTAGE_KV, REGION, and curtailment columns.
    """
    try:
        xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
    except Exception as e:
        logger.error(f"Failed to open ELI workbook: {e}")
        return pd.DataFrame()

    # Find matching sheet
    sheet_name = None
    for candidate in sheet_candidates:
        if candidate in xls.sheet_names:
            sheet_name = candidate
            break

    if sheet_name is None:
        # Try fuzzy match
        for name in xls.sheet_names:
            name_lower = name.lower()
            if "curtailment" in name_lower and term[:3].lower() in name_lower:
                sheet_name = name
                break

    if sheet_name is None:
        logger.warning(f"No {term}-term curtailment sheet found. Available: {xls.sheet_names}")
        return pd.DataFrame()

    logger.info(f"Parsing {term}-term curtailment from sheet '{sheet_name}'...")
    df = pd.read_excel(xls, sheet_name=sheet_name, header=None)

    # Find the header row containing "Location"
    header_idx = None
    for i in range(min(20, len(df))):
        row_vals = [str(v).strip().lower() for v in df.iloc[i].tolist()]
        if "location" in row_vals:
            header_idx = i
            break

    if header_idx is None:
        logger.warning(f"No 'Location' header found in {sheet_name}")
        return pd.DataFrame()

    headers = [str(v).strip() for v in df.iloc[header_idx].tolist()]
    data = df.iloc[header_idx + 1:].copy()
    data.columns = headers

    # Identify columns
    loc_col = _find_col(headers, ["Location"])
    volt_col = _find_col(headers, ["Voltage (kV)", "Voltage", "kV"])
    region_col = _find_col(headers, ["Region"])
    solar_col = _find_col(headers, ["Solar Projected Curtailment", "Solar Curtailment", "Solar"])
    wind_col = _find_col(headers, ["Wind Projected Curtailment", "Wind Curtailment", "Wind"])

    if loc_col is None:
        logger.warning(f"Cannot find Location column in {sheet_name}")
        return pd.DataFrame()

    # Build result
    rows = []
    for _, row in data.iterrows():
        loc = row.get(loc_col) if loc_col else None
        if pd.isna(loc) or str(loc).strip() == "":
            continue

        entry = {
            "LOCATION": str(loc).strip(),
            "VOLTAGE_KV": pd.to_numeric(row.get(volt_col), errors="coerce") if volt_col else None,
            "REGION": str(row.get(region_col, "")).strip() if region_col else "",
        }

        solar_suffix = f"SOLAR_CURTAILMENT_{'NEAR' if term == 'near' else 'MED'}"
        wind_suffix = f"WIND_CURTAILMENT_{'NEAR' if term == 'near' else 'MED'}"

        entry[solar_suffix] = pd.to_numeric(row.get(solar_col), errors="coerce") if solar_col else None
        entry[wind_suffix] = pd.to_numeric(row.get(wind_col), errors="coerce") if wind_col else None

        rows.append(entry)

    result = pd.DataFrame(rows)

    # Clean up region names (strip whitespace)
    if "REGION" in result.columns:
        result["REGION"] = result["REGION"].str.strip()

    logger.info(f"Parsed {len(result)} {term}-term curtailment entries")
    return result


def _find_col(headers: list[str], candidates: list[str]) -> str | None:
    """Find the first matching column header."""
    headers_lower = {h: h.lower().strip() for h in headers}
    for candidate in candidates:
        candidate_lower = candidate.lower().strip()
        for orig, lower in headers_lower.items():
            if candidate_lower == lower or candidate_lower in lower:
                return orig
    return None


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
