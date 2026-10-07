"""Merge all data sources into a single per-farm summary DataFrame."""

from __future__ import annotations

import logging
import re

import pandas as pd

logger = logging.getLogger(__name__)


def build_summary(
    generators: pd.DataFrame,
    mlf_data: pd.DataFrame,
    eli_curtailment: pd.DataFrame,
    rez_forecasts: pd.DataFrame,
    actual_curtailment: pd.DataFrame,
    cache_dir: str | None = None,
) -> pd.DataFrame:
    """Join all data sources into the master summary.

    Merge strategy:
    1. Generators (spine) LEFT JOIN MLF on DUID
    2. LEFT JOIN actual curtailment on DUID
    3. ELI projected curtailment from AEMO's location table for every unit
       (LOCATION + region, voltage when it picks one row; by project name when
       there is no LOCATION), fuel-matched. ELI_SOURCE records which rule.
       The hand-seeded eli_per_duid.feather is not read: it carries no edition
       and nothing rebuilds it, so it would pin 2025 values over a new edition.
    4. LEFT JOIN REZ forecasts on REZ_NAME
    5. ELI_EDITION (chart-data edition) and ISP_EDITION (the ISP the REZ forecasts
       are from), one value for every row, read from the tables themselves so the
       page and the validator see what was actually merged

    `cache_dir` is accepted for callers' compatibility and no longer read.

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

    # 3. ELI projected curtailment, all from the location table
    summary = _init_eli(summary)
    if not eli_curtailment.empty:
        summary = _fill_eli_from_location(summary, eli_curtailment)
    else:
        logger.warning("No location-based ELI curtailment data")
    _log_eli_coverage(summary)

    # 4. Merge REZ forecasts on REZ_NAME
    if not rez_forecasts.empty:
        summary = _merge_rez(summary, rez_forecasts)
    else:
        logger.warning("No REZ forecast data to merge")

    # 5. Editions the ELI and ISP columns come from
    summary = _add_editions(summary, eli_curtailment, rez_forecasts)

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


ELI_TERMS = ("NEAR", "MED")
ELI_COLS = [f"ELI_CURTAILMENT_{t}" for t in ELI_TERMS]


def _init_eli(summary: pd.DataFrame) -> pd.DataFrame:
    """Empty ELI columns and ELI_SOURCE = "" for every unit; the location fill sets them."""
    result = summary.copy()
    for col in ELI_COLS:
        result[col] = float("nan")
    result["ELI_SOURCE"] = ""
    return result


def _one_value(df: pd.DataFrame, col: str):
    """The single value of `col` in `df`, or None (absent, empty or mixed)."""
    if df is None or df.empty or col not in df.columns:
        return None
    values = df[col].dropna().unique()
    return values[0] if len(values) == 1 else None


def _add_editions(summary: pd.DataFrame, eli: pd.DataFrame, rez: pd.DataFrame) -> pd.DataFrame:
    """ELI_EDITION / ISP_EDITION columns; empty when the table carries no edition."""
    result = summary.copy()
    eli_ed, isp_ed = _one_value(eli, "ELI_EDITION"), _one_value(rez, "ISP_EDITION")
    result["ELI_EDITION"] = pd.array([int(eli_ed)] * len(result) if eli_ed is not None
                                     else [pd.NA] * len(result), dtype="Int64")
    result["ISP_EDITION"] = "" if isp_ed is None else str(isp_ed)
    if eli_ed is None and not eli.empty:
        logger.warning("ELI chart data carries no ELI_EDITION; rerun with --full-refresh")
    if isp_ed is None and not rez.empty:
        logger.warning("REZ forecasts carry no ISP_EDITION; rerun python -m src.eli_appendix")
    return result


def _norm_region(value) -> str:
    """'NSW', 'NSW1', ' nsw ' → 'NSW' (ELI tables use state names, the spine REGIONID)."""
    text = str(value or "").strip().upper()
    return text[:-1] if text.endswith("1") else text


def _norm_volt(value):
    """Voltage as a 1-dp float key (3.3 kV stays 3.3; the old int cast made it 3)."""
    v = pd.to_numeric(value, errors="coerce")
    return None if pd.isna(v) else round(float(v), 1)


def _fill_eli_from_location(summary: pd.DataFrame, eli: pd.DataFrame) -> pd.DataFrame:
    """Fill ELI for every unit from the location-based table.

    A unit matches ELI rows with the same LOCATION (case-insensitive) in its own
    region. If its connection voltage equals one of those rows' voltage, that row
    is used; otherwise the location must have a single row (several voltages and
    no exact voltage = ambiguous, left empty). Wind farms take the WIND columns,
    solar farms the SOLAR columns. ELI_SOURCE = "location" where filled.

    A unit with no LOCATION (every wind farm: the seed covers solar only) is
    matched by name instead: if exactly one ELI location in its region appears
    as a whole word in its PROJECT_NAME (Ararat Wind Farm → Ararat, Bulgana
    Green Power Hub → Bulgana), that location is used, same voltage rules.
    ELI_SOURCE = "location-name". On the 21 seeded solar farms this rule
    fires for, it picks the seeded location for 20; the 21st (Stubbo) now has
    its own ELI location where the seed used neighbouring Beryl.
    """
    result = summary.copy()
    if "LOCATION" not in result.columns or "LOCATION" not in eli.columns:
        logger.info("Location-based ELI fill skipped: no LOCATION column")
        return result

    table = eli.copy()
    table["_loc"] = table["LOCATION"].astype(str).str.strip().str.lower()
    table["_reg"] = table["REGION"].map(_norm_region) if "REGION" in table.columns else ""
    table["_volt"] = table["VOLTAGE_KV"].map(_norm_volt) if "VOLTAGE_KV" in table.columns else None
    table = table.drop_duplicates(subset=["_loc", "_reg", "_volt"], keep="first")
    by_place = {k: g for k, g in table.groupby(["_loc", "_reg"])}

    region_col = "STATE" if "STATE" in result.columns else "REGIONID"
    filled = ambiguous = 0
    for idx, row in result.iterrows():
        if row["ELI_SOURCE"]:
            continue
        fuel = str(row.get("FUEL_TYPE", "")).upper()
        if fuel not in ("SOLAR", "WIND"):
            continue
        region = _norm_region(row.get(region_col))
        location, source = row.get("LOCATION"), "location"
        if pd.isna(location) or not str(location).strip():
            location, source = _location_from_name(row.get("PROJECT_NAME"), region, by_place), "location-name"
            if location is None:
                continue
        rows = by_place.get((str(location).strip().lower(), region))
        if rows is None:
            continue
        volt = _norm_volt(row.get("VOLTAGE_KV"))
        exact = rows[rows["_volt"] == volt] if volt is not None else rows.iloc[0:0]
        if len(exact) >= 1:
            match = exact.iloc[0]
        elif len(rows) == 1:
            match = rows.iloc[0]
        else:
            ambiguous += 1
            continue
        values = {f"ELI_CURTAILMENT_{t}": match.get(f"{fuel}_CURTAILMENT_{t}") for t in ELI_TERMS}
        if all(pd.isna(v) for v in values.values()):
            continue
        for col, v in values.items():
            result.at[idx, col] = v
        result.at[idx, "ELI_SOURCE"] = source
        filled += 1

    logger.info(f"Location-based ELI filled {filled} unit(s)"
                + (f"; {ambiguous} ambiguous (several voltages, none matching)" if ambiguous else ""))
    return result


def _location_from_name(name, region: str, by_place: dict) -> str | None:
    """The one ELI location in `region` named as a whole word in `name`, else None."""
    text = str(name or "").lower()
    if not text.strip():
        return None
    hits = {loc for loc, reg in by_place if reg == region
            and re.search(r"\b" + re.escape(loc) + r"\b", text)}
    return hits.pop() if len(hits) == 1 else None


def _log_eli_coverage(summary: pd.DataFrame) -> None:
    if "FUEL_TYPE" not in summary.columns:
        return
    counts = summary.groupby(["FUEL_TYPE", "ELI_SOURCE"]).size().to_dict()
    logger.info("ELI coverage (fuel, source): "
                + ", ".join(f"{f} {src or 'none'}: {n}" for (f, src), n in sorted(counts.items())))


def _merge_rez(summary: pd.DataFrame, rez: pd.DataFrame) -> pd.DataFrame:
    """Merge REZ forecasts into summary based on REZ_NAME."""
    if "REZ_NAME" not in summary.columns or "REZ_NAME" not in rez.columns:
        logger.warning("Cannot merge REZ data — no REZ_NAME column")
        return summary

    # Normalise REZ names
    summary["_rez_key"] = summary["REZ_NAME"].fillna("").astype(str).str.strip().str.lower()
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

    # Units outside a REZ ("Non-REZ") or with REZ unknown ("") match no forecast
    # row, so their ISP columns stay empty.
    isp_cols = [c for c in result.columns if c.startswith("ISP_")]

    # Clean up
    result = result.drop(columns=["_rez_key"], errors="ignore")

    matched = result[isp_cols[0]].notna().sum() if isp_cols else 0
    logger.info(f"Merged REZ forecasts ({matched}/{len(result)} matched)")

    return result
