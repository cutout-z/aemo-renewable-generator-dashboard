"""Seed cache files from the local Excel workbook.

Usage:
    python -m src.seed_from_workbook /path/to/solar-databook.xlsx

This parses the workbook tabs and writes feather cache files so the
main pipeline can run without downloading from AEMO.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

from . import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def seed(workbook_path: str):
    """Parse the workbook and create cache files."""
    wb_path = Path(workbook_path)
    if not wb_path.exists():
        logger.error(f"Workbook not found: {wb_path}")
        sys.exit(1)

    cache_dir = PROJECT_ROOT / config.DATA_DIR
    cache_dir.mkdir(parents=True, exist_ok=True)

    xls = pd.ExcelFile(wb_path, engine="openpyxl")
    logger.info(f"Sheets: {xls.sheet_names}")

    # ── ELI Curtailment (Near Term + Medium Term) ────────────────────
    eli_data = _parse_eli_sheets(xls)
    if not eli_data.empty:
        eli_path = PROJECT_ROOT / config.ELI_CURTAILMENT_CACHE
        eli_data.reset_index(drop=True).to_feather(eli_path)
        logger.info(f"Seeded ELI curtailment: {len(eli_data)} entries → {eli_path}")

    # ── REZ Forecasts ────────────────────────────────────────────────
    rez_data = _parse_rez_sheet(xls)
    if not rez_data.empty:
        rez_path = PROJECT_ROOT / config.REZ_FORECAST_CACHE
        rez_data.reset_index(drop=True).to_feather(rez_path)
        logger.info(f"Seeded REZ forecasts: {len(rez_data)} entries → {rez_path}")

    # ── Generator enrichment + pre-matched curtailment from Summary tab ─
    gen_enrich = _parse_summary_generators(xls)
    if not gen_enrich.empty:
        enrich_path = PROJECT_ROOT / config.DATA_DIR / "generator_enrichment.feather"
        gen_enrich.reset_index(drop=True).to_feather(enrich_path)
        logger.info(f"Seeded generator enrichment: {len(gen_enrich)} entries → {enrich_path}")

    # Pre-matched ELI curtailment per DUID from Summary tab
    eli_per_duid = _parse_summary_eli(xls)
    if not eli_per_duid.empty:
        eli_duid_path = PROJECT_ROOT / config.DATA_DIR / "eli_per_duid.feather"
        eli_per_duid.reset_index(drop=True).to_feather(eli_duid_path)
        logger.info(f"Seeded per-DUID ELI curtailment: {len(eli_per_duid)} entries → {eli_duid_path}")

    logger.info("Seed complete.")


def _parse_eli_sheets(xls: pd.ExcelFile) -> pd.DataFrame:
    """Parse Near Term and Medium Term curtailment sheets."""
    near = _parse_curtailment_tab(xls, "Near Term Proj Curtailment", "NEAR")
    med = _parse_curtailment_tab(xls, "Med Term Proj Curtailment", "MED")

    if near.empty and med.empty:
        return pd.DataFrame()

    if not near.empty and not med.empty:
        return pd.merge(near, med, on=["LOCATION", "VOLTAGE_KV", "REGION"], how="outer")
    return near if not near.empty else med


def _parse_curtailment_tab(xls: pd.ExcelFile, sheet_name: str, term: str) -> pd.DataFrame:
    """Parse a curtailment tab from the workbook."""
    if sheet_name not in xls.sheet_names:
        logger.warning(f"Sheet '{sheet_name}' not found")
        return pd.DataFrame()

    df = pd.read_excel(xls, sheet_name=sheet_name, header=None)

    # Find header row with "Location"
    header_idx = None
    for i in range(min(20, len(df))):
        row_vals = [str(v).strip().lower() for v in df.iloc[i].tolist()]
        if "location" in row_vals:
            header_idx = i
            break

    if header_idx is None:
        logger.warning(f"No header row found in '{sheet_name}'")
        return pd.DataFrame()

    headers = [str(v).strip() for v in df.iloc[header_idx].tolist()]
    data = df.iloc[header_idx + 1:].copy()
    data.columns = headers

    rows = []
    for _, row in data.iterrows():
        loc = row.get("Location")
        if pd.isna(loc) or str(loc).strip() == "":
            continue

        voltage_col = None
        for h in headers:
            if "voltage" in h.lower() or "kv" in h.lower():
                voltage_col = h
                break

        region_col = None
        for h in headers:
            if "region" in h.lower():
                region_col = h
                break

        solar_col = None
        for h in headers:
            if "solar" in h.lower() and "curtailment" in h.lower():
                solar_col = h
                break

        wind_col = None
        for h in headers:
            if "wind" in h.lower() and "curtailment" in h.lower():
                wind_col = h
                break

        entry = {
            "LOCATION": str(loc).strip(),
            "VOLTAGE_KV": pd.to_numeric(row.get(voltage_col), errors="coerce") if voltage_col else None,
            "REGION": str(row.get(region_col, "")).strip() if region_col else "",
            f"SOLAR_CURTAILMENT_{term}": pd.to_numeric(row.get(solar_col), errors="coerce") if solar_col else None,
            f"WIND_CURTAILMENT_{term}": pd.to_numeric(row.get(wind_col), errors="coerce") if wind_col else None,
        }
        rows.append(entry)

    result = pd.DataFrame(rows)
    logger.info(f"Parsed {len(result)} entries from '{sheet_name}'")
    return result


def _parse_rez_sheet(xls: pd.ExcelFile) -> pd.DataFrame:
    """Parse REZ forecast sheet from the workbook."""
    sheet_name = "REZ forecast"
    if sheet_name not in xls.sheet_names:
        logger.warning(f"Sheet '{sheet_name}' not found")
        return pd.DataFrame()

    df = pd.read_excel(xls, sheet_name=sheet_name, header=None)

    # Find header row with "State" and "REZ"
    header_idx = None
    for i in range(min(20, len(df))):
        row_vals = [str(v).strip().lower() for v in df.iloc[i].tolist()]
        if "state" in row_vals and "rez" in row_vals:
            header_idx = i
            break

    if header_idx is None:
        logger.warning("No header row found in REZ forecast sheet")
        return pd.DataFrame()

    headers = [str(v).strip() for v in df.iloc[header_idx].tolist()]
    data = df.iloc[header_idx + 1:].copy()
    data.columns = headers

    # Detect section boundaries from the row above headers
    section_row = [str(v).strip().lower() for v in df.iloc[header_idx - 1].tolist()] if header_idx > 0 else []
    curtailment_start = None
    offloading_start = None
    for idx, val in enumerate(section_row):
        if "curtailment" in val and curtailment_start is None:
            curtailment_start = idx
        if "offloading" in val or "economic" in val:
            offloading_start = idx

    # Find FY columns and Average columns
    fy_cols = []
    avg_cols = []
    for i, h in enumerate(headers):
        h_clean = h.strip()
        if "average" in h_clean.lower() or "avg" in h_clean.lower():
            avg_cols.append((i, h))
        elif len(h_clean) >= 5 and "-" in h_clean:
            parts = h_clean.split("-")
            if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                fy_cols.append((i, h))

    # Split into curtailment and offloading based on section headers
    curtailment_cols = []
    offloading_cols = []
    all_data_cols = fy_cols + avg_cols
    all_data_cols.sort(key=lambda x: x[0])

    if offloading_start is not None:
        for idx, h in all_data_cols:
            if idx < offloading_start:
                curtailment_cols.append((idx, h))
            else:
                offloading_cols.append((idx, h))
    else:
        mid = len(all_data_cols) // 2
        curtailment_cols = all_data_cols[:mid]
        offloading_cols = all_data_cols[mid:]

    # Parse rows
    rows = []
    current_state = ""
    for _, row in data.iterrows():
        state_val = row.get("State", "")
        rez_val = row.get("REZ", "")

        if pd.notna(state_val) and str(state_val).strip():
            state_str = str(state_val).strip()
            if any(x in state_str.lower() for x in ["subtotal", "total"]):
                continue
            current_state = state_str

        if pd.isna(rez_val) or str(rez_val).strip() == "":
            continue
        rez_str = str(rez_val).strip()
        if any(x in rez_str.lower() for x in ["subtotal", "total"]):
            continue

        entry = {"STATE": current_state, "REZ_NAME": rez_str}

        # Curtailment values
        for i, (col_idx, col_name) in enumerate(curtailment_cols):
            val = pd.to_numeric(row.iloc[col_idx] if col_idx < len(row) else None, errors="coerce")
            if "average" in col_name.lower():
                entry["CURTAILMENT_AVG"] = val
            else:
                entry[f"CURTAILMENT_FY{i+1}"] = val
                entry[f"CURTAILMENT_FY{i+1}_LABEL"] = col_name

        # Offloading values
        off_idx = 0
        for col_idx, col_name in offloading_cols:
            val = pd.to_numeric(row.iloc[col_idx] if col_idx < len(row) else None, errors="coerce")
            if "average" in col_name.lower():
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
    logger.info(f"Parsed {len(result)} REZ forecast entries")
    return result


def _parse_summary_generators(xls: pd.ExcelFile) -> pd.DataFrame:
    """Extract generator enrichment data (DUID → location, REZ, voltage) from Summary tab."""
    if "Summary" not in xls.sheet_names:
        return pd.DataFrame()

    df = pd.read_excel(xls, sheet_name="Summary", header=None)

    # Find the header row with "DUID"
    header_idx = None
    for i in range(min(20, len(df))):
        row_vals = [str(v).strip().lower() for v in df.iloc[i].tolist()]
        if "duid" in row_vals:
            header_idx = i
            break

    if header_idx is None:
        logger.warning("No DUID header in Summary tab")
        return pd.DataFrame()

    headers = [str(v).strip() for v in df.iloc[header_idx].tolist()]
    data = df.iloc[header_idx + 1:].copy()
    data.columns = headers

    # Find relevant columns
    # Map Summary tab columns. The tab has:
    # Project name, DUID, Location, REZ (Y/N), REZ, State, Nameplate capacity (MW), Voltage (kV)
    col_map = {}
    assigned = set()
    for h in headers:
        hl = h.lower().strip()
        if "duid" in hl and "DUID" not in assigned:
            col_map[h] = "DUID"
            assigned.add("DUID")
        elif "project" in hl and "PROJECT_NAME" not in assigned:
            col_map[h] = "PROJECT_NAME"
            assigned.add("PROJECT_NAME")
        elif "location" in hl and "LOCATION" not in assigned:
            col_map[h] = "LOCATION"
            assigned.add("LOCATION")
        elif "rez (y/n)" in hl and "REZ" not in assigned:
            col_map[h] = "REZ"
            assigned.add("REZ")
        elif hl == "rez" and "REZ_NAME" not in assigned:
            col_map[h] = "REZ_NAME"
            assigned.add("REZ_NAME")
        elif "state" in hl and "STATE" not in assigned:
            col_map[h] = "STATE"
            assigned.add("STATE")
        elif ("nameplate" in hl or ("capacity" in hl and "mw" in hl)) and "NAMEPLATE_MW" not in assigned:
            col_map[h] = "NAMEPLATE_MW"
            assigned.add("NAMEPLATE_MW")
        elif ("voltage" in hl or "kv" in hl) and "VOLTAGE_KV" not in assigned:
            col_map[h] = "VOLTAGE_KV"
            assigned.add("VOLTAGE_KV")

    data = data.rename(columns=col_map)
    data = data.dropna(subset=["DUID"])
    data["DUID"] = data["DUID"].astype(str).str.strip()

    # Select enrichment columns
    keep = ["DUID"]
    for col in ["PROJECT_NAME", "LOCATION", "REZ", "REZ_NAME", "STATE", "NAMEPLATE_MW", "VOLTAGE_KV"]:
        if col in data.columns:
            keep.append(col)
    data = data[keep].drop_duplicates(subset="DUID", keep="first")

    # Clean up types
    if "NAMEPLATE_MW" in data.columns:
        data["NAMEPLATE_MW"] = pd.to_numeric(data["NAMEPLATE_MW"], errors="coerce")
    if "VOLTAGE_KV" in data.columns:
        data["VOLTAGE_KV"] = pd.to_numeric(data["VOLTAGE_KV"], errors="coerce")

    logger.info(f"Parsed {len(data)} generator enrichment entries from Summary tab")
    return data


def _parse_summary_eli(xls: pd.ExcelFile) -> pd.DataFrame:
    """Extract pre-matched ELI curtailment per DUID from the Summary tab.

    The Summary tab already has the correct VLOOKUP'd near-term and
    medium-term curtailment values mapped to each DUID.
    """
    if "Summary" not in xls.sheet_names:
        return pd.DataFrame()

    df = pd.read_excel(xls, sheet_name="Summary", header=None)

    # Find header row
    header_idx = None
    for i in range(min(20, len(df))):
        row_vals = [str(v).strip().lower() for v in df.iloc[i].tolist()]
        if "duid" in row_vals:
            header_idx = i
            break

    if header_idx is None:
        return pd.DataFrame()

    headers = [str(v).strip() for v in df.iloc[header_idx].tolist()]
    data = df.iloc[header_idx + 1:].copy()
    data.columns = headers

    # Find DUID column
    duid_col = None
    for h in headers:
        if "duid" in h.lower():
            duid_col = h
            break
    if duid_col is None:
        return pd.DataFrame()

    # Find near-term and medium-term curtailment columns
    # Row 11 has "1. Projected curtailment" spanning the near/med columns
    near_col = None
    med_col = None
    for h in headers:
        hl = h.lower()
        if "near" in hl and "term" in hl:
            near_col = h
        elif "medium" in hl and "term" in hl:
            med_col = h

    if near_col is None and med_col is None:
        return pd.DataFrame()

    rows = []
    for _, row in data.iterrows():
        duid = row.get(duid_col)
        if pd.isna(duid) or str(duid).strip() == "":
            continue
        entry = {"DUID": str(duid).strip()}
        if near_col:
            entry["ELI_CURTAILMENT_NEAR"] = pd.to_numeric(row.get(near_col), errors="coerce")
        if med_col:
            entry["ELI_CURTAILMENT_MED"] = pd.to_numeric(row.get(med_col), errors="coerce")
        rows.append(entry)

    result = pd.DataFrame(rows)
    result = result.dropna(subset=["DUID"])
    logger.info(f"Parsed {len(result)} per-DUID ELI curtailment entries from Summary")
    return result


def main():
    parser = argparse.ArgumentParser(description="Seed cache from Excel workbook")
    parser.add_argument("workbook", help="Path to the solar databook Excel file")
    args = parser.parse_args()
    seed(args.workbook)


if __name__ == "__main__":
    main()
