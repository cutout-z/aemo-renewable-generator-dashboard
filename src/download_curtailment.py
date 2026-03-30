"""Calculate actual curtailment from AEMO dispatch data via NEMOSIS.

Methodology:
    curtailment_pct = 1 - (actual_MW / available_MW)

    - actual_MW: DISPATCH_UNIT_SCADA.SCADAVALUE — actual dispatched MW per 5-min interval
    - available_MW: DISPATCHLOAD.AVAILABILITY — unconstrained available capacity per 5-min
      interval. For semi-scheduled generators (all utility-scale solar/wind), this equals
      the UIGF (unconstrained intermittent generation forecast).

    Both tables sourced via NEMOSIS from AEMO's MMSDM archive.
    Aggregated per DUID per financial year. Last 2 complete FYs (rolling).
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from nemosis import dynamic_data_compiler

from . import config

logger = logging.getLogger(__name__)


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

    start_time = f"{fy1_start}/07/01 00:00:00"
    end_time = f"{fy_current_start}/07/01 00:00:00"

    nemosis_cache = str(Path(cache_dir) / "nemosis_cache")
    Path(nemosis_cache).mkdir(parents=True, exist_ok=True)

    duid_list = list(generator_duids)

    # ── SCADA (actual output) via NEMOSIS ─────────────────────────────────
    logger.info("Fetching DISPATCH_UNIT_SCADA via NEMOSIS...")
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
        logger.warning("No SCADA data returned from NEMOSIS")
        return pd.DataFrame()

    scada["SCADAVALUE"] = pd.to_numeric(scada["SCADAVALUE"], errors="coerce")
    scada = scada.dropna(subset=["SCADAVALUE"])
    logger.info(f"SCADA: {len(scada):,} rows for {scada['DUID'].nunique()} DUIDs")

    # ── AVAILABILITY (unconstrained capacity) via NEMOSIS ─────────────────
    logger.info("Fetching DISPATCHLOAD (AVAILABILITY) via NEMOSIS...")
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
        logger.warning("No DISPATCHLOAD data returned from NEMOSIS")
        return pd.DataFrame()

    # Filter to non-intervention intervals (INTERVENTION is int64 in NEMOSIS)
    dispatch["INTERVENTION"] = pd.to_numeric(dispatch["INTERVENTION"], errors="coerce")
    dispatch = dispatch[dispatch["INTERVENTION"] == 0]
    dispatch["AVAILABILITY"] = pd.to_numeric(dispatch["AVAILABILITY"], errors="coerce")
    dispatch = dispatch.dropna(subset=["AVAILABILITY"])
    logger.info(
        f"DISPATCHLOAD: {len(dispatch):,} rows for {dispatch['DUID'].nunique()} DUIDs"
    )

    # ── Merge on SETTLEMENTDATE + DUID ────────────────────────────────────
    scada["SETTLEMENTDATE"] = pd.to_datetime(scada["SETTLEMENTDATE"])
    dispatch["SETTLEMENTDATE"] = pd.to_datetime(dispatch["SETTLEMENTDATE"])

    merged = pd.merge(
        scada[["SETTLEMENTDATE", "DUID", "SCADAVALUE"]],
        dispatch[["SETTLEMENTDATE", "DUID", "AVAILABILITY"]],
        on=["SETTLEMENTDATE", "DUID"],
        how="inner",
    )
    logger.info(f"Merged: {len(merged):,} rows")

    # ── Assign financial year ─────────────────────────────────────────────
    merged["MONTH"] = merged["SETTLEMENTDATE"].dt.month
    merged["FY_START"] = merged["SETTLEMENTDATE"].dt.year
    merged.loc[merged["MONTH"] < 7, "FY_START"] -= 1

    # ── Aggregate per DUID per FY ─────────────────────────────────────────
    annual = (
        merged.groupby(["DUID", "FY_START"])
        .agg(total_scada=("SCADAVALUE", "sum"), total_avail=("AVAILABILITY", "sum"))
        .reset_index()
    )
    annual["CURTAILMENT_PCT"] = (1 - annual["total_scada"] / annual["total_avail"]).clip(
        lower=0
    )
    annual.loc[annual["total_avail"] == 0, "CURTAILMENT_PCT"] = 0

    # ── Pivot to wide format ──────────────────────────────────────────────
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
