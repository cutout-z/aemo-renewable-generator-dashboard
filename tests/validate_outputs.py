"""Post-pipeline validation for AEMO Renewable Generator Dashboard.

Checks summary.csv and regional Excel workbooks for data integrity
before committing to the repository. Exits non-zero on any failure.
"""

import sys
from pathlib import Path

import pandas as pd

OUTPUTS_DIR = Path(__file__).parent.parent / "outputs"
REGIONS = {"NSW1", "QLD1", "VIC1", "SA1", "TAS1"}
FUEL_TYPES = {"Solar", "Wind"}
REGION_NAMES = {"NSW1": "NSW", "QLD1": "QLD", "VIC1": "VIC", "SA1": "SA", "TAS1": "TAS"}

errors = []


def check(condition, msg):
    if not condition:
        errors.append(msg)
        print(f"  FAIL: {msg}")
    return condition


def validate():
    summary_path = OUTPUTS_DIR / "summary.csv"
    check(summary_path.exists(), "summary.csv does not exist")
    if not summary_path.exists():
        return

    df = pd.read_csv(summary_path)
    print(f"summary.csv: {len(df)} rows, {len(df.columns)} columns")

    # --- Structure ---
    check(len(df) >= 100, f"Unexpectedly few generators: {len(df)} (expected 100+)")
    required_cols = ["DUID", "PROJECT_NAME", "REGIONID", "FUEL_TYPE", "NAMEPLATE_MW"]
    for col in required_cols:
        check(col in df.columns, f"Missing column: {col}")

    # --- No null identity columns ---
    for col in ["DUID", "PROJECT_NAME", "REGIONID", "FUEL_TYPE"]:
        if col in df.columns:
            nulls = df[col].isna().sum()
            check(nulls == 0, f"{col} has {nulls} null values")

    # --- Fuel types are Solar/Wind only ---
    if "FUEL_TYPE" in df.columns:
        unexpected = set(df["FUEL_TYPE"].unique()) - FUEL_TYPES
        check(len(unexpected) == 0, f"Unexpected fuel types: {unexpected}")

    # --- All 5 regions present ---
    if "REGIONID" in df.columns:
        regions_present = set(df["REGIONID"].unique())
        for r in REGIONS:
            check(r in regions_present, f"Region {r} missing")

    # --- Nameplate capacity > 0 ---
    if "NAMEPLATE_MW" in df.columns:
        bad_cap = df[df["NAMEPLATE_MW"] <= 0]
        check(len(bad_cap) == 0, f"{len(bad_cap)} generators have capacity <= 0 MW")

    # --- MLF values in [0.5, 1.5] ---
    mlf_cols = [c for c in df.columns if c.startswith("MLF_")]
    for col in mlf_cols:
        vals = df[col].dropna()
        if len(vals) > 0:
            check(vals.min() >= 0.5, f"{col} has value below 0.5 (min={vals.min():.4f})")
            check(vals.max() <= 1.5, f"{col} has value above 1.5 (max={vals.max():.4f})")

    # --- Curtailment values in [0, 1] ---
    curt_cols = [c for c in df.columns if "CURTAILMENT" in c and df[c].dtype in ["float64", "float32"]]
    for col in curt_cols:
        vals = df[col].dropna()
        if len(vals) > 0:
            check(vals.min() >= 0, f"{col} has negative value (min={vals.min():.4f})")
            check(vals.max() <= 1, f"{col} exceeds 1.0 (max={vals.max():.4f})")

    # --- Regional Excel workbooks exist ---
    for region_id, name in REGION_NAMES.items():
        xlsx_path = OUTPUTS_DIR / f"{name}_curtailment.xlsx"
        check(xlsx_path.exists(), f"{xlsx_path.name} does not exist")


if __name__ == "__main__":
    print("Validating AEMO Renewable Generator Dashboard outputs...")
    validate()
    if errors:
        print(f"\n{len(errors)} validation error(s) found — aborting.")
        sys.exit(1)
    else:
        print("\nAll validations passed.")
