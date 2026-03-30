"""Calculate actual curtailment from AEMO NEMWEB SCADA and UIGF data.

Methodology:
    curtailment_pct = 1 - (actual_MW / uigf_MW)

    - actual_MW: DISPATCH_UNIT_SCADA — actual dispatched MW per 5-min interval
    - uigf_MW: INTERMITTENT_GEN_FCST_DATA — AEMO's unconstrained intermittent
      generation forecast (what the generator would have produced without curtailment)

    Aggregated to monthly, then annual averages per DUID.
    Last 2 financial years (rolling).
"""

from __future__ import annotations

import csv
import io
import logging
import time
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

from . import config

logger = logging.getLogger(__name__)


def calculate_actual_curtailment(
    cache_dir: str,
    generator_duids: set[str],
    full_refresh: bool = False,
) -> pd.DataFrame:
    """Calculate actual curtailment for solar/wind generators over last 2 FYs.

    Returns DataFrame with columns:
        DUID, CURTAILMENT_ACTUAL_FY1, CURTAILMENT_ACTUAL_FY2,
        CURTAILMENT_ACTUAL_FY1_LABEL, CURTAILMENT_ACTUAL_FY2_LABEL
    """
    # Determine date range: last 2 complete financial years
    fy_current_start = config.current_fy_start()
    # Last 2 complete FYs: e.g. if current is FY25-26, we want FY23-24 and FY24-25
    fy2_start = fy_current_start - 1  # most recent complete FY
    fy1_start = fy_current_start - 2  # second most recent

    fy1_label = config.fy_label(fy1_start)
    fy2_label = config.fy_label(fy2_start)

    logger.info(f"Calculating actual curtailment for {fy1_label} and {fy2_label}")
    logger.info(f"Tracking {len(generator_duids)} DUIDs")

    # Date range: July of fy1_start to June of fy_current_start
    start_year, start_month = fy1_start, 7
    end_year = fy_current_start
    end_month = 6

    # Generate month list
    months = _month_range(start_year, start_month, end_year, end_month)
    logger.info(f"Processing {len(months)} months of SCADA + UIGF data...")

    scada_dir = Path(cache_dir) / "scada"
    uigf_dir = Path(cache_dir) / "uigf"
    scada_dir.mkdir(parents=True, exist_ok=True)
    uigf_dir.mkdir(parents=True, exist_ok=True)

    # Process month by month to manage memory
    monthly_results = []
    for year, month in months:
        try:
            monthly = _process_month(
                year, month, generator_duids, scada_dir, uigf_dir, full_refresh
            )
            if monthly is not None and not monthly.empty:
                monthly_results.append(monthly)
        except Exception as e:
            logger.warning(f"Failed to process {year}-{month:02d}: {e}")
            continue

    if not monthly_results:
        logger.warning("No curtailment data could be calculated")
        return pd.DataFrame()

    all_monthly = pd.concat(monthly_results, ignore_index=True)

    # Assign financial year
    all_monthly["FY_START"] = all_monthly.apply(
        lambda r: r["YEAR"] if r["MONTH"] >= 7 else r["YEAR"] - 1, axis=1
    )

    # Aggregate to annual curtailment per DUID per FY
    annual = (
        all_monthly.groupby(["DUID", "FY_START"])
        .agg(
            total_scada=("SCADA_SUM", "sum"),
            total_uigf=("UIGF_SUM", "sum"),
        )
        .reset_index()
    )
    annual["CURTAILMENT_PCT"] = (
        1 - annual["total_scada"] / annual["total_uigf"]
    ).clip(lower=0)

    # Handle division by zero (UIGF = 0 means no expected generation)
    annual.loc[annual["total_uigf"] == 0, "CURTAILMENT_PCT"] = 0

    # Pivot to wide format: one row per DUID with FY columns
    result_rows = []
    for duid in generator_duids:
        duid_data = annual[annual["DUID"] == duid]
        entry = {"DUID": duid}

        fy1_row = duid_data[duid_data["FY_START"] == fy1_start]
        fy2_row = duid_data[duid_data["FY_START"] == fy2_start]

        entry[f"CURTAILMENT_ACTUAL_{fy1_label}"] = (
            fy1_row["CURTAILMENT_PCT"].values[0] if len(fy1_row) > 0 else None
        )
        entry[f"CURTAILMENT_ACTUAL_{fy2_label}"] = (
            fy2_row["CURTAILMENT_PCT"].values[0] if len(fy2_row) > 0 else None
        )

        result_rows.append(entry)

    result = pd.DataFrame(result_rows)
    valid = result.dropna(subset=[f"CURTAILMENT_ACTUAL_{fy1_label}", f"CURTAILMENT_ACTUAL_{fy2_label}"], how="all")
    logger.info(f"Calculated curtailment for {len(valid)} generators across 2 FYs")
    return result


def _process_month(
    year: int,
    month: int,
    duids: set[str],
    scada_dir: Path,
    uigf_dir: Path,
    full_refresh: bool,
) -> pd.DataFrame | None:
    """Process one month: download SCADA + UIGF, calculate per-DUID curtailment.

    Returns DataFrame with columns: DUID, YEAR, MONTH, SCADA_SUM, UIGF_SUM
    """
    # Load SCADA
    scada = _load_scada_month(year, month, duids, scada_dir, full_refresh)
    if scada is None or scada.empty:
        return None

    # Load UIGF
    uigf = _load_uigf_month(year, month, duids, uigf_dir, full_refresh)
    if uigf is None or uigf.empty:
        logger.warning(f"No UIGF data for {year}-{month:02d}, skipping")
        return None

    # Aggregate per DUID for this month
    scada_monthly = scada.groupby("DUID")["SCADAVALUE"].sum().reset_index()
    scada_monthly.columns = ["DUID", "SCADA_SUM"]

    uigf_monthly = uigf.groupby("DUID")["UIGF_VALUE"].sum().reset_index()
    uigf_monthly.columns = ["DUID", "UIGF_SUM"]

    result = pd.merge(scada_monthly, uigf_monthly, on="DUID", how="outer")
    result["YEAR"] = year
    result["MONTH"] = month
    result["SCADA_SUM"] = result["SCADA_SUM"].fillna(0)
    result["UIGF_SUM"] = result["UIGF_SUM"].fillna(0)

    return result


def _load_scada_month(
    year: int, month: int, duids: set[str], cache_dir: Path, full_refresh: bool
) -> pd.DataFrame | None:
    """Load SCADA data for one month, filtered to relevant DUIDs."""
    feather_path = cache_dir / f"scada_{year}_{month:02d}.feather"

    if feather_path.exists() and not full_refresh:
        df = pd.read_feather(feather_path)
        return df[df["DUID"].isin(duids)].copy()

    url = config.NEMWEB_SCADA_URL_TEMPLATE.format(year=year, month=month)
    logger.info(f"Downloading SCADA {year}-{month:02d}...")

    try:
        resp = _download_with_retry_resp(url)
    except Exception as e:
        logger.warning(f"SCADA download failed for {year}-{month:02d}: {e}")
        return None

    # Parse AEMO CSV format
    df = _parse_aemo_csv(resp.content, ["SETTLEMENTDATE", "DUID", "SCADAVALUE"])
    if df is None or df.empty:
        return None

    df["SCADAVALUE"] = pd.to_numeric(df["SCADAVALUE"], errors="coerce")
    df = df.dropna(subset=["SCADAVALUE"])

    # Cache full month (all DUIDs) for reuse
    df.reset_index(drop=True).to_feather(feather_path)
    logger.info(f"Cached SCADA {year}-{month:02d} ({len(df):,} rows)")

    return df[df["DUID"].isin(duids)].copy()


def _load_uigf_month(
    year: int, month: int, duids: set[str], cache_dir: Path, full_refresh: bool
) -> pd.DataFrame | None:
    """Load UIGF data for one month, filtered to relevant DUIDs."""
    feather_path = cache_dir / f"uigf_{year}_{month:02d}.feather"

    if feather_path.exists() and not full_refresh:
        df = pd.read_feather(feather_path)
        return df[df["DUID"].isin(duids)].copy()

    url = config.NEMWEB_UIGF_URL_TEMPLATE.format(year=year, month=month)
    logger.info(f"Downloading UIGF {year}-{month:02d}...")

    try:
        resp = _download_with_retry_resp(url)
    except Exception as e:
        logger.warning(f"UIGF download failed for {year}-{month:02d}: {e}")
        return None

    # UIGF comes as a ZIP file
    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            csv_names = [n for n in zf.namelist() if n.upper().endswith(".CSV")]
            if not csv_names:
                logger.warning(f"No CSV in UIGF ZIP for {year}-{month:02d}")
                return None
            csv_content = zf.read(csv_names[0])
    except zipfile.BadZipFile:
        # Sometimes AEMO serves CSVs directly (not zipped)
        csv_content = resp.content

    df = _parse_aemo_csv(csv_content, ["RUN_DATETIME", "DUID", "UIGF"])
    if df is None or df.empty:
        # Try alternative column name
        df = _parse_aemo_csv(csv_content, ["INTERVAL_DATETIME", "DUID", "UIGF"])

    if df is None or df.empty:
        logger.warning(f"No UIGF data parsed for {year}-{month:02d}")
        return None

    # Rename UIGF column
    if "UIGF" in df.columns:
        df = df.rename(columns={"UIGF": "UIGF_VALUE"})
    df["UIGF_VALUE"] = pd.to_numeric(df["UIGF_VALUE"], errors="coerce")
    df = df.dropna(subset=["UIGF_VALUE"])

    # The UIGF table has multiple forecast horizons per interval.
    # We want the closest-to-dispatch forecast per DUID per interval.
    # Group by datetime + DUID and take the max UIGF (latest forecast)
    datetime_col = "RUN_DATETIME" if "RUN_DATETIME" in df.columns else "INTERVAL_DATETIME"
    if datetime_col in df.columns:
        df = df.groupby([datetime_col, "DUID"])["UIGF_VALUE"].max().reset_index()

    # Cache
    df_cache = df[["DUID", "UIGF_VALUE"]].copy()
    if datetime_col in df.columns:
        df_cache[datetime_col] = df[datetime_col]
    df_cache.reset_index(drop=True).to_feather(feather_path)
    logger.info(f"Cached UIGF {year}-{month:02d} ({len(df_cache):,} rows)")

    return df_cache[df_cache["DUID"].isin(duids)].copy()


def _parse_aemo_csv(content: bytes, required_cols: list[str]) -> pd.DataFrame | None:
    """Parse AEMO's non-standard CSV format (C/I/D/C rows).

    AEMO CSVs have:
    - Row 1: C header (metadata)
    - Row 2: I header (column names)
    - Data rows start with 'D'
    - Last row starts with 'C' (footer)
    """
    try:
        text = content.decode("utf-8", errors="replace")
    except Exception:
        return None

    lines = text.splitlines()

    # Find the I (header) row
    header_line = None
    data_start = 0
    for i, line in enumerate(lines):
        if line.startswith("I,"):
            header_line = line
            data_start = i + 1
            break

    if header_line is None:
        # Try parsing as regular CSV
        try:
            df = pd.read_csv(io.BytesIO(content), header=1, dtype=str, low_memory=False)
            first_col = df.columns[0]
            df = df[df[first_col] == "D"].copy()
            available = [c for c in required_cols if c in df.columns]
            if len(available) >= 2:
                return df[available].copy()
        except Exception:
            pass
        return None

    # Parse header
    reader = csv.reader(io.StringIO(header_line))
    headers = next(reader)

    # Parse data rows (D rows only)
    data_lines = [line for line in lines[data_start:] if line.startswith("D,")]

    if not data_lines:
        return None

    rows = []
    reader = csv.reader(io.StringIO("\n".join(data_lines)))
    for fields in reader:
        if len(fields) >= len(headers):
            row = dict(zip(headers, fields))
            rows.append(row)

    if not rows:
        return None

    df = pd.DataFrame(rows)

    # Select required columns
    available = [c for c in required_cols if c in df.columns]
    if len(available) < 2:
        logger.warning(f"Missing columns. Need {required_cols}, have {list(df.columns)[:20]}")
        return None

    return df[available].copy()


def _month_range(
    start_year: int, start_month: int,
    end_year: int, end_month: int,
) -> list[tuple[int, int]]:
    """Generate list of (year, month) tuples inclusive."""
    months = []
    y, m = start_year, start_month
    while (y, m) <= (end_year, end_month):
        months.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


def _download_with_retry_resp(url: str) -> requests.Response:
    """Download and return the response object."""
    for attempt in range(config.MAX_RETRIES):
        try:
            resp = requests.get(
                url,
                timeout=config.REQUEST_TIMEOUT,
                headers={"User-Agent": config.USER_AGENT},
            )
            if resp.status_code == 404:
                raise RuntimeError(f"404 Not Found: {url}")
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt < config.MAX_RETRIES - 1:
                wait = config.RETRY_BACKOFF * (attempt + 1)
                logger.warning(f"Download failed (attempt {attempt + 1}): {e}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise RuntimeError(f"Failed after {config.MAX_RETRIES} attempts: {e}")
