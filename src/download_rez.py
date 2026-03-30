"""Download and parse REZ-level curtailment and economic offloading forecasts.

Source: Appendices to AEMO's Enhanced Locational Information (ELI) report.
These contain ISP curtailment and economic offloading forecasts by REZ for upcoming FYs.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import pandas as pd
import requests

from . import config

logger = logging.getLogger(__name__)

# Sheet name candidates in the ELI appendix workbook
REZ_SHEET_CANDIDATES = [
    "REZ forecast",
    "REZ Forecast",
    "Curtailment by REZ",
    "REZ curtailment",
]


def fetch_rez_forecasts(cache_dir: str, eli_year: int | None = None) -> pd.DataFrame:
    """Download and parse REZ-level curtailment + offloading forecasts.

    Returns DataFrame with columns:
        STATE, REZ_NAME,
        CURTAILMENT_FY1, CURTAILMENT_FY2, CURTAILMENT_FY3, CURTAILMENT_AVG,
        OFFLOADING_FY1, OFFLOADING_FY2, OFFLOADING_FY3, OFFLOADING_AVG,
        CURTAILMENT_FY1_LABEL, ... (the actual FY labels like '25-26')
    """
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)

    if eli_year is None:
        eli_year = max(config.ELI_APPENDIX_URLS.keys())

    url = config.ELI_APPENDIX_URLS.get(eli_year)
    if not url:
        # Fall back to the ELI chart data URL (some years bundle everything)
        url = config.ELI_CHART_DATA_URLS.get(eli_year)
    if not url:
        logger.error(f"No ELI appendix URL configured for year {eli_year}")
        return pd.DataFrame()

    xlsx_path = cache_path / f"eli_appendix_{eli_year}.xlsx"

    if not xlsx_path.exists():
        logger.info(f"Downloading ELI appendix for {eli_year}...")
        _download_with_retry(url, xlsx_path)

    return _parse_rez_sheet(xlsx_path)


def _parse_rez_sheet(xlsx_path: Path) -> pd.DataFrame:
    """Parse REZ forecast sheet from the ELI appendix workbook."""
    try:
        xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
    except Exception as e:
        logger.error(f"Failed to open ELI appendix: {e}")
        return pd.DataFrame()

    # Find the REZ forecast sheet
    sheet_name = None
    for candidate in REZ_SHEET_CANDIDATES:
        if candidate in xls.sheet_names:
            sheet_name = candidate
            break
    if sheet_name is None:
        for name in xls.sheet_names:
            if "rez" in name.lower() and ("forecast" in name.lower() or "curtail" in name.lower()):
                sheet_name = name
                break
    if sheet_name is None:
        logger.warning(f"No REZ forecast sheet found. Available: {xls.sheet_names}")
        return pd.DataFrame()

    logger.info(f"Parsing REZ forecasts from sheet '{sheet_name}'...")
    df = pd.read_excel(xls, sheet_name=sheet_name, header=None)

    # Find the header row containing "State" or "REZ"
    header_idx = None
    for i in range(min(20, len(df))):
        row_vals = [str(v).strip().lower() for v in df.iloc[i].tolist()]
        if "state" in row_vals and "rez" in row_vals:
            header_idx = i
            break
        if "state" in row_vals or ("rez" in row_vals and "no" in row_vals):
            header_idx = i
            break

    if header_idx is None:
        logger.warning("No header row found in REZ forecast sheet")
        return pd.DataFrame()

    headers = [str(v).strip() for v in df.iloc[header_idx].tolist()]
    data = df.iloc[header_idx + 1:].copy()
    data.columns = headers

    # Find the divider row between curtailment and economic offloading sections
    # The sheet has: State, No, REZ, Solar farms, Wind farms, then FY columns for curtailment, then FY columns for offloading
    # We need to detect which columns are curtailment and which are offloading

    # Find State and REZ columns
    state_col = _find_col(headers, ["State"])
    rez_col = _find_col(headers, ["REZ"])

    if state_col is None or rez_col is None:
        logger.warning(f"Cannot find State/REZ columns. Headers: {headers}")
        return pd.DataFrame()

    # Find FY-like columns (patterns like '25-26', '26-27', etc.)
    fy_cols = []
    for h in headers:
        # Match patterns like '25-26', '2025-26', '2025-2026'
        h_clean = h.strip()
        if len(h_clean) >= 5 and "-" in h_clean:
            parts = h_clean.split("-")
            if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                fy_cols.append(h)

    # Also look for "Average" columns
    avg_cols = [h for h in headers if "average" in h.lower() or "avg" in h.lower()]

    # Detect curtailment vs offloading sections from the row above headers
    # In the workbook, row 6 has "Curtailment" and "Economic offloading" as section headers
    curtailment_fy_cols = []
    offloading_fy_cols = []

    if header_idx > 0:
        section_row = [str(v).strip().lower() for v in df.iloc[header_idx - 1].tolist()]
        curtailment_start = None
        offloading_start = None
        for idx, val in enumerate(section_row):
            if "curtailment" in val and curtailment_start is None:
                curtailment_start = idx
            if "offloading" in val or "economic" in val:
                offloading_start = idx

        if curtailment_start is not None and offloading_start is not None:
            # FY cols before offloading_start are curtailment, after are offloading
            for col_idx, h in enumerate(headers):
                if h in fy_cols or h in avg_cols:
                    if col_idx < offloading_start:
                        curtailment_fy_cols.append((col_idx, h))
                    else:
                        offloading_fy_cols.append((col_idx, h))

    # Fallback: split FY columns in half if section detection failed
    if not curtailment_fy_cols and not offloading_fy_cols and fy_cols:
        all_fy = fy_cols + avg_cols
        mid = len(all_fy) // 2
        curtailment_fy_cols = [(headers.index(h), h) for h in all_fy[:mid]]
        offloading_fy_cols = [(headers.index(h), h) for h in all_fy[mid:]]

    # Parse data rows
    rows = []
    current_state = ""
    for _, row in data.iterrows():
        state_val = row.get(state_col, "")
        rez_val = row.get(rez_col, "")

        if pd.notna(state_val) and str(state_val).strip():
            state_str = str(state_val).strip()
            # Skip subtotal and total rows
            if any(x in state_str.lower() for x in ["subtotal", "total"]):
                continue
            current_state = state_str

        if pd.isna(rez_val) or str(rez_val).strip() == "":
            continue

        rez_str = str(rez_val).strip()
        if any(x in rez_str.lower() for x in ["subtotal", "total"]):
            continue

        entry = {
            "STATE": current_state,
            "REZ_NAME": rez_str,
        }

        # Extract curtailment FY values
        for i, (col_idx, col_name) in enumerate(curtailment_fy_cols):
            val = pd.to_numeric(row.iloc[col_idx] if col_idx < len(row) else None, errors="coerce")
            if "average" in col_name.lower() or "avg" in col_name.lower():
                entry["CURTAILMENT_AVG"] = val
            else:
                entry[f"CURTAILMENT_FY{i+1}"] = val
                entry[f"CURTAILMENT_FY{i+1}_LABEL"] = col_name

        # Extract offloading FY values
        off_idx = 0
        for col_idx, col_name in offloading_fy_cols:
            val = pd.to_numeric(row.iloc[col_idx] if col_idx < len(row) else None, errors="coerce")
            if "average" in col_name.lower() or "avg" in col_name.lower():
                entry["OFFLOADING_AVG"] = val
            else:
                off_idx += 1
                entry[f"OFFLOADING_FY{off_idx}"] = val
                entry[f"OFFLOADING_FY{off_idx}_LABEL"] = col_name

        # Compute averages if not present
        if "CURTAILMENT_AVG" not in entry:
            c_vals = [entry.get(f"CURTAILMENT_FY{j+1}") for j in range(3)]
            c_vals = [v for v in c_vals if pd.notna(v)]
            entry["CURTAILMENT_AVG"] = sum(c_vals) / len(c_vals) if c_vals else None

        if "OFFLOADING_AVG" not in entry:
            o_vals = [entry.get(f"OFFLOADING_FY{j+1}") for j in range(3)]
            o_vals = [v for v in o_vals if pd.notna(v)]
            entry["OFFLOADING_AVG"] = sum(o_vals) / len(o_vals) if o_vals else None

        rows.append(entry)

    result = pd.DataFrame(rows)

    if result.empty:
        logger.warning("No REZ forecast data parsed")
    else:
        logger.info(f"Parsed REZ forecasts for {len(result)} zones across "
                    f"{result['STATE'].nunique()} states")

    return result


def _find_col(headers: list[str], candidates: list[str]) -> str | None:
    """Find the first matching column header."""
    headers_lower = {h: h.lower().strip() for h in headers}
    for candidate in candidates:
        candidate_lower = candidate.lower().strip()
        for orig, lower in headers_lower.items():
            if candidate_lower == lower:
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
