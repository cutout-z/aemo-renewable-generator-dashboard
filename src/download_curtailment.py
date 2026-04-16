"""Calculate actual curtailment from AEMO dispatch data via NEMOSIS.

Methodology:
    curtailment_pct = 1 - (actual_MW / available_MW)

    - actual_MW: DISPATCH_UNIT_SCADA.SCADAVALUE — actual dispatched MW per 5-min interval
    - available_MW: DISPATCHLOAD.AVAILABILITY — unconstrained available capacity per 5-min
      interval. For semi-scheduled generators (all utility-scale solar/wind), this equals
      the UIGF (unconstrained intermittent generation forecast).

    Both tables sourced via NEMOSIS from AEMO's MMSDM archive.
    Aggregated per DUID per financial year. Last 2 complete FYs (rolling).

Memory strategy:
    Loading 2 years of SCADA + DISPATCHLOAD simultaneously exceeds the ~7 GB runner
    RAM limit. We process one FY at a time: download SCADA for FY, download DISPATCHLOAD
    for FY, merge and aggregate to ~239 rows, then free everything before the next FY.
"""

from __future__ import annotations

import gc
import logging
from pathlib import Path

import pandas as pd
from nemosis import dynamic_data_compiler

from . import config

logger = logging.getLogger(__name__)


def _fetch_and_aggregate_fy(
    fy_start: int,
    nemosis_cache: str,
    duid_list: list[str],
    full_refresh: bool,
) -> pd.DataFrame | None:
    """Download SCADA + DISPATCHLOAD for one financial year and return per-DUID aggregates.

    Returns a DataFrame with columns [DUID, FY_START, total_scada, total_avail],
    or None if data is unavailable.
    """
    fy_label = config.fy_label(fy_start)
    start_time = f"{fy_start}/07/01 00:00:00"
    end_time = f"{fy_start + 1}/07/01 00:00:00"

    # ── SCADA ─────────────────────────────────────────────────────────────────
    logger.info(f"Fetching DISPATCH_UNIT_SCADA for {fy_label}...")
    scada = dynamic_data_compiler(
        start_time=start_time,
        end_time=end_time,
        table_name="DISPATCH_UNIT_SCADA",
        raw_data_location=nemosis_cache,
        select_columns=["SETTLEMENTDATE", "DUID", "SCADAVALUE"],
        filter_cols=["DUID"],
        filter_values=[duid_list],
        fformat="parquet",
        rebuild=full_refresh,
    )

    if scada is None or scada.empty:
        logger.warning(f"No SCADA data for {fy_label}")
        return None

    scada["SCADAVALUE"] = pd.to_numeric(scada["SCADAVALUE"], errors="coerce")
    scada = scada.dropna(subset=["SCADAVALUE"])
    scada["SETTLEMENTDATE"] = pd.to_datetime(scada["SETTLEMENTDATE"])
    logger.info(f"SCADA {fy_label}: {len(scada):,} rows for {scada['DUID'].nunique()} DUIDs")

    # Delete raw NEMOSIS downloads before loading DISPATCHLOAD (large ZIPs)
    cache_path = Path(nemosis_cache)
    removed = 0
    for pattern in ("*.zip", "*.csv"):
        for f in cache_path.glob(pattern):
            f.unlink(missing_ok=True)
            removed += 1
    if removed:
        logger.info(f"Freed {removed} raw NEMOSIS files before DISPATCHLOAD")

    # ── DISPATCHLOAD ──────────────────────────────────────────────────────────
    logger.info(f"Fetching DISPATCHLOAD for {fy_label}...")
    dispatch = dynamic_data_compiler(
        start_time=start_time,
        end_time=end_time,
        table_name="DISPATCHLOAD",
        raw_data_location=nemosis_cache,
        select_columns=["SETTLEMENTDATE", "DUID", "AVAILABILITY", "INTERVENTION"],
        filter_cols=["DUID"],
        filter_values=[duid_list],
        fformat="parquet",
        rebuild=full_refresh,
    )

    if dispatch is None or dispatch.empty:
        logger.warning(f"No DISPATCHLOAD data for {fy_label}")
        del scada
        gc.collect()
        return None

    dispatch["INTERVENTION"] = pd.to_numeric(dispatch["INTERVENTION"], errors="coerce")
    dispatch = dispatch[dispatch["INTERVENTION"] == 0]
    dispatch["AVAILABILITY"] = pd.to_numeric(dispatch["AVAILABILITY"], errors="coerce")
    dispatch = dispatch.dropna(subset=["AVAILABILITY"])
    dispatch["SETTLEMENTDATE"] = pd.to_datetime(dispatch["SETTLEMENTDATE"])
    logger.info(f"DISPATCHLOAD {fy_label}: {len(dispatch):,} rows for {dispatch['DUID'].nunique()} DUIDs")

    # ── Merge and aggregate ───────────────────────────────────────────────────
    merged = pd.merge(
        scada[["SETTLEMENTDATE", "DUID", "SCADAVALUE"]],
        dispatch[["SETTLEMENTDATE", "DUID", "AVAILABILITY"]],
        on=["SETTLEMENTDATE", "DUID"],
        how="inner",
    )
    logger.info(f"Merged {fy_label}: {len(merged):,} rows")

    del scada, dispatch
    gc.collect()

    merged["FY_START"] = fy_start
    annual = (
        merged.groupby(["DUID", "FY_START"])
        .agg(total_scada=("SCADAVALUE", "sum"), total_avail=("AVAILABILITY", "sum"))
        .reset_index()
    )

    del merged
    gc.collect()

    return annual


def calculate_actual_curtailment(
    cache_dir: str,
    generator_duids: set[str],
    full_refresh: bool = False,
) -> pd.DataFrame:
    """Calculate actual curtailment for solar/wind generators over last 2 FYs.

    Returns DataFrame with columns:
        DUID, CURTAILMENT_ACTUAL_{fy1_label}, CURTAILMENT_ACTUAL_{fy2_label}
    """
    fy_current_start = config.current_fy_start()
    fy2_start = fy_current_start - 1
    fy1_start = fy_current_start - 2

    fy1_label = config.fy_label(fy1_start)
    fy2_label = config.fy_label(fy2_start)

    logger.info(f"Calculating actual curtailment for {fy1_label} and {fy2_label}")
    logger.info(f"Tracking {len(generator_duids)} DUIDs")

    nemosis_cache = str(Path(cache_dir) / "nemosis_cache")
    Path(nemosis_cache).mkdir(parents=True, exist_ok=True)

    duid_list = list(generator_duids)

    # Process one FY at a time to stay within runner memory limits
    fy_results = []
    for fy_start in [fy1_start, fy2_start]:
        result = _fetch_and_aggregate_fy(fy_start, nemosis_cache, duid_list, full_refresh)
        if result is not None:
            fy_results.append(result)

    if not fy_results:
        logger.warning("No curtailment data returned for any FY")
        return pd.DataFrame()

    annual = pd.concat(fy_results, ignore_index=True)

    # ── Curtailment rate ──────────────────────────────────────────────────────
    annual["CURTAILMENT_PCT"] = (1 - annual["total_scada"] / annual["total_avail"]).clip(lower=0)
    annual.loc[annual["total_avail"] == 0, "CURTAILMENT_PCT"] = 0

    # ── Pivot to wide format ──────────────────────────────────────────────────
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
    valid = result.dropna(
        subset=[f"CURTAILMENT_ACTUAL_{fy1_label}", f"CURTAILMENT_ACTUAL_{fy2_label}"],
        how="all",
    )
    logger.info(f"Calculated curtailment for {len(valid)} generators across 2 FYs")
    return result
