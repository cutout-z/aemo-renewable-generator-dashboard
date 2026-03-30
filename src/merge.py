"""Merge all data sources into a single per-farm summary DataFrame."""

import logging

import pandas as pd

from . import config

logger = logging.getLogger(__name__)


def build_summary(
    generators: pd.DataFrame,
    mlf_data: pd.DataFrame,
    eli_curtailment: pd.DataFrame,
    rez_forecasts: pd.DataFrame,
    actual_curtailment: pd.DataFrame,
) -> pd.DataFrame:
    """Join all data sources into the master summary.

    Merge strategy:
    1. Generators (spine) LEFT JOIN MLF on DUID
    2. LEFT JOIN actual curtailment on DUID
    3. LEFT JOIN ELI projected curtailment on LOCATION + VOLTAGE_KV + REGION
    4. LEFT JOIN REZ forecasts on REZ_NAME

    Returns wide-format DataFrame sorted by FUEL_TYPE → STATE → PROJECT_NAME.
    """
    summary = generators.copy()

    # 1. Merge MLF data on DUID
    if not mlf_data.empty:
        summary = summary.merge(mlf_data, on="DUID", how="left")
        mlf_cols = [c for c in mlf_data.columns if c.startswith("MLF_")]
        logger.info(f"Merged MLF data ({len(mlf_cols)} columns)")
    else:
        logger.warning("No MLF data to merge")

    # 2. Merge actual curtailment on DUID
    if not actual_curtailment.empty:
        summary = summary.merge(actual_curtailment, on="DUID", how="left")
        curt_cols = [c for c in actual_curtailment.columns if c.startswith("CURTAILMENT_ACTUAL_")]
        logger.info(f"Merged actual curtailment ({len(curt_cols)} columns)")
    else:
        logger.warning("No actual curtailment data to merge")

    # 3. Merge ELI projected curtailment on LOCATION + VOLTAGE_KV + REGION
    if not eli_curtailment.empty:
        summary = _merge_eli(summary, eli_curtailment)
    else:
        logger.warning("No ELI curtailment data to merge")

    # 4. Merge REZ forecasts on REZ_NAME
    if not rez_forecasts.empty:
        summary = _merge_rez(summary, rez_forecasts)
    else:
        logger.warning("No REZ forecast data to merge")

    # Sort: Fuel Type → State → Project Name
    sort_cols = []
    if "FUEL_TYPE" in summary.columns:
        sort_cols.append("FUEL_TYPE")
    if "STATE" in summary.columns:
        sort_cols.append("STATE")
    if "PROJECT_NAME" in summary.columns:
        sort_cols.append("PROJECT_NAME")
    if sort_cols:
        summary = summary.sort_values(sort_cols).reset_index(drop=True)

    logger.info(f"Built summary: {len(summary)} generators × {len(summary.columns)} columns")
    return summary


def _merge_eli(summary: pd.DataFrame, eli: pd.DataFrame) -> pd.DataFrame:
    """Merge ELI projected curtailment into summary.

    ELI data is keyed by (LOCATION, VOLTAGE_KV, REGION).
    Generators have LOCATION and VOLTAGE_KV. We need to fuzzy-match.
    """
    if "LOCATION" not in summary.columns or "LOCATION" not in eli.columns:
        logger.warning("Cannot merge ELI data — no LOCATION column")
        return summary

    # Normalise location names for matching
    summary["_loc_key"] = summary["LOCATION"].astype(str).str.strip().str.lower()
    eli["_loc_key"] = eli["LOCATION"].astype(str).str.strip().str.lower()

    # Build join key: location + voltage (where available)
    if "VOLTAGE_KV" in summary.columns and "VOLTAGE_KV" in eli.columns:
        summary["_volt_key"] = summary["VOLTAGE_KV"].fillna(0).astype(int)
        eli["_volt_key"] = eli["VOLTAGE_KV"].fillna(0).astype(int)
        merge_on = ["_loc_key", "_volt_key"]
    else:
        merge_on = ["_loc_key"]

    # Also match on region if available
    if "REGION" in eli.columns and "STATE" in summary.columns:
        eli["_region_key"] = eli["REGION"].astype(str).str.strip().str.upper()
        summary["_region_key"] = summary["STATE"].astype(str).str.strip().str.upper()
        merge_on.append("_region_key")

    # Identify ELI value columns to bring in
    eli_value_cols = [c for c in eli.columns if c.startswith(("SOLAR_CURTAILMENT_", "WIND_CURTAILMENT_"))]

    if not eli_value_cols:
        logger.warning("No curtailment value columns in ELI data")
        return summary

    eli_merge = eli[merge_on + eli_value_cols].drop_duplicates(subset=merge_on, keep="first")

    result = summary.merge(eli_merge, on=merge_on, how="left")

    # For each generator, pick the right curtailment column based on fuel type
    # Solar farms get SOLAR_CURTAILMENT_*, Wind farms get WIND_CURTAILMENT_*
    for term in ["NEAR", "MED"]:
        solar_col = f"SOLAR_CURTAILMENT_{term}"
        wind_col = f"WIND_CURTAILMENT_{term}"
        target_col = f"ELI_CURTAILMENT_{term}"

        if solar_col in result.columns or wind_col in result.columns:
            result[target_col] = result.apply(
                lambda row: (
                    row.get(solar_col)
                    if row.get("FUEL_TYPE") == "Solar"
                    else row.get(wind_col)
                ),
                axis=1,
            )

    # Clean up temp columns
    drop_cols = [c for c in result.columns if c.startswith("_")]
    drop_cols += [c for c in result.columns if c.startswith(("SOLAR_CURTAILMENT_", "WIND_CURTAILMENT_"))]
    result = result.drop(columns=drop_cols, errors="ignore")

    matched = result["ELI_CURTAILMENT_NEAR"].notna().sum() if "ELI_CURTAILMENT_NEAR" in result.columns else 0
    logger.info(f"Merged ELI curtailment ({matched}/{len(result)} matched)")

    return result


def _merge_rez(summary: pd.DataFrame, rez: pd.DataFrame) -> pd.DataFrame:
    """Merge REZ forecasts into summary based on REZ_NAME."""
    if "REZ_NAME" not in summary.columns or "REZ_NAME" not in rez.columns:
        logger.warning("Cannot merge REZ data — no REZ_NAME column")
        return summary

    # Normalise REZ names
    summary["_rez_key"] = summary["REZ_NAME"].astype(str).str.strip().str.lower()
    rez["_rez_key"] = rez["REZ_NAME"].astype(str).str.strip().str.lower()

    # REZ value columns
    rez_value_cols = [
        c for c in rez.columns
        if c.startswith(("CURTAILMENT_FY", "CURTAILMENT_AVG", "OFFLOADING_FY", "OFFLOADING_AVG"))
    ]

    if not rez_value_cols:
        logger.warning("No forecast value columns in REZ data")
        return summary

    rez_merge = rez[["_rez_key"] + rez_value_cols].drop_duplicates(subset="_rez_key", keep="first")

    # Rename to avoid collision with ELI curtailment columns
    rename_map = {}
    for col in rez_value_cols:
        if col.startswith("CURTAILMENT_"):
            rename_map[col] = f"ISP_{col}"
        elif col.startswith("OFFLOADING_"):
            rename_map[col] = f"ISP_{col}"
    rez_merge = rez_merge.rename(columns=rename_map)

    result = summary.merge(rez_merge, on="_rez_key", how="left")

    # Non-REZ farms: set ISP columns to N/A (not null — these are genuinely not applicable)
    non_rez_mask = result["REZ"] == "N"
    isp_cols = [c for c in result.columns if c.startswith("ISP_")]
    for col in isp_cols:
        # Keep NaN for non-REZ (will display as N/A in dashboard)
        pass

    # Clean up
    result = result.drop(columns=["_rez_key"], errors="ignore")

    matched = result[isp_cols[0]].notna().sum() if isp_cols else 0
    logger.info(f"Merged REZ forecasts ({matched}/{len(result)} matched)")

    return result
