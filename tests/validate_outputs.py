"""Post-pipeline validation for AEMO Renewable Generator Dashboard.

Checks summary.csv and regional Excel workbooks for data integrity
before committing to the repository. Exits non-zero on any failure.
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

OUTPUTS_DIR = Path(__file__).parent.parent / "outputs"
CACHE_DIR = Path(__file__).parent.parent / "data"
STATUS_FILE = "source_status.json"
REGIONS = {"NSW1", "QLD1", "VIC1", "SA1", "TAS1"}
FUEL_TYPES = {"Solar", "Wind"}
REGION_NAMES = {"NSW1": "NSW", "QLD1": "QLD", "VIC1": "VIC", "SA1": "SA", "TAS1": "TAS"}

# REZ / ELI output contract
REZ_VALUES = {"Y", "N", ""}
REZ_SOURCES = {"geninfo", "seed", ""}
ELI_SOURCES = {"per-DUID", "location", ""}
NON_REZ = "Non-REZ"

# Source freshness: the lane runs daily, so a list this old means refreshes keep failing
MAX_REGISTRATION_AGE_DAYS = 30
# Recently registered GENERATOR DUIDs (DUDETAILSUMMARY) the Registration List may lack
MAX_UNLISTED_RECENT_GENERATORS = 3
GEN_INFO_STALE_DAYS = 122

errors = []


def check(condition, msg):
    if not condition:
        errors.append(msg)
        print(f"  FAIL: {msg}")
    return condition


def validate(outputs_dir: Path = OUTPUTS_DIR):
    outputs_dir = Path(outputs_dir)
    summary_path = outputs_dir / "summary.csv"
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

    # --- DUIDs are unique (guard against merge-side duplication) ---
    if "DUID" in df.columns:
        dup_count = len(df) - df["DUID"].nunique()
        check(dup_count == 0, f"{dup_count} duplicate DUID row(s) in summary.csv")

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

    check_rez(df)
    check_eli_source(df)

    # --- TECHNOLOGY is the descriptor, not the Registration List's "Renewable" ---
    if "TECHNOLOGY" in df.columns and len(df):
        check(not (df["TECHNOLOGY"].astype(str) == "Renewable").all(),
              'TECHNOLOGY is "Renewable" for every row (primary column mapped, not the descriptor)')

    # --- Regional Excel workbooks exist ---
    for region_id, name in REGION_NAMES.items():
        xlsx_path = outputs_dir / f"{name}_curtailment.xlsx"
        check(xlsx_path.exists(), f"{xlsx_path.name} does not exist")


def _text(df, col):
    return df[col].fillna("").astype(str).str.strip() if col in df.columns else None


def check_rez(df):
    """REZ is Y/N/unknown; "Non-REZ" only with REZ=N and a named source."""
    for col in ("REZ", "REZ_NAME", "REZ_SOURCE"):
        check(col in df.columns, f"Missing column: {col}")
    if not {"REZ", "REZ_NAME", "REZ_SOURCE"} <= set(df.columns):
        return
    rez, name, src = _text(df, "REZ"), _text(df, "REZ_NAME"), _text(df, "REZ_SOURCE")
    bad = sorted(set(rez) - REZ_VALUES)
    check(not bad, f"REZ has values outside {{Y, N, ''}}: {bad}")
    bad = sorted(set(src) - REZ_SOURCES)
    check(not bad, f"REZ_SOURCE has unexpected values: {bad}")
    n = ((name == NON_REZ) & (rez != "N")).sum()
    check(n == 0, f'{n} row(s) say REZ_NAME "{NON_REZ}" without REZ = N')
    n = ((rez == "N") & (name != NON_REZ)).sum()
    check(n == 0, f'{n} row(s) have REZ = N but REZ_NAME is not "{NON_REZ}"')
    n = ((rez == "Y") & ((name == "") | (name == NON_REZ))).sum()
    check(n == 0, f"{n} row(s) have REZ = Y without a zone name")
    n = ((rez == "") & (name != "")).sum()
    check(n == 0, f"{n} row(s) have a REZ_NAME but REZ unknown")
    n = ((rez != "") & (src == "")).sum()
    check(n == 0, f"{n} row(s) state REZ Y/N with no REZ_SOURCE (e.g. Non-REZ for lack of data)")
    n = ((rez == "") & (src != "")).sum()
    check(n == 0, f"{n} row(s) have a REZ_SOURCE but REZ unknown")


def check_eli_source(df):
    """ELI_SOURCE names where each ELI value came from, and is empty only with no value."""
    check("ELI_SOURCE" in df.columns, "Missing column: ELI_SOURCE")
    eli_cols = [c for c in ("ELI_CURTAILMENT_NEAR", "ELI_CURTAILMENT_MED") if c in df.columns]
    if "ELI_SOURCE" not in df.columns or not eli_cols:
        return
    src = _text(df, "ELI_SOURCE")
    bad = sorted(set(src) - ELI_SOURCES)
    check(not bad, f"ELI_SOURCE has unexpected values: {bad}")
    has_value = df[eli_cols].notna().any(axis=1)
    n = (has_value & (src == "")).sum()
    check(n == 0, f"{n} row(s) have an ELI value but no ELI_SOURCE")
    n = (~has_value & (src != "")).sum()
    check(n == 0, f"{n} row(s) have an ELI_SOURCE but no ELI value")


def _age_days(iso):
    if not iso:
        return None
    try:
        then = datetime.fromisoformat(str(iso))
    except ValueError:
        return None
    if then.tzinfo is None:
        then = then.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - then).total_seconds() / 86400


def validate_sources(cache_dir: Path):
    """Fail when the generator spine has stopped refreshing or is visibly missing units."""
    path = Path(cache_dir) / STATUS_FILE
    check(path.exists(), f"{path} missing: run the pipeline (it records source freshness)")
    if not path.exists():
        return
    status = json.loads(path.read_text(encoding="utf-8"))

    reg = status.get("registration_list", {})
    age = _age_days(reg.get("fetched_at"))
    check(age is not None, "Registration List has never been fetched successfully")
    if age is not None:
        print(f"Registration List: last good fetch {age:.1f} days ago"
              + (f" (this run failed: {reg.get('error')})" if reg.get("error") else ""))
        check(age <= MAX_REGISTRATION_AGE_DAYS,
              f"Registration List is {age:.0f} days old (> {MAX_REGISTRATION_AGE_DAYS}); "
              "refreshes keep failing, new units are missing")

    dud = status.get("dudetailsummary", {})
    missing = dud.get("missing_from_registration")
    if missing is None:
        print(f"  WARN: DUDETAILSUMMARY cross-check did not run ({dud.get('error')})")
    else:
        ids = ", ".join(m["DUID"] for m in missing) or "none"
        print(f"DUDETAILSUMMARY {dud.get('month')}: recently registered generators "
              f"missing from the Registration List: {ids}")
        check(len(missing) <= MAX_UNLISTED_RECENT_GENERATORS,
              f"{len(missing)} GENERATOR DUIDs registered in the last 24 months are missing "
              f"from the Registration List (> {MAX_UNLISTED_RECENT_GENERATORS}): {ids}")

    gi = status.get("gen_info", {})
    edition, gi_age = gi.get("edition"), gi.get("edition_age_days")
    if not edition:
        print("  WARN: no NEM Generation Information edition cached")
    elif gi_age is not None and gi_age > GEN_INFO_STALE_DAYS:
        print(f"  WARN: NEM Generation Information edition {edition} is {gi_age} days old")
    else:
        print(f"NEM Generation Information edition: {edition}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--outputs-dir", default=str(OUTPUTS_DIR),
                        help="Directory holding summary.csv and the workbooks (default: outputs/)")
    parser.add_argument("--cache-dir", default=str(CACHE_DIR),
                        help="Pipeline cache directory holding source_status.json (default: data/)")
    args = parser.parse_args(argv)
    print("Validating AEMO Renewable Generator Dashboard outputs...")
    validate(Path(args.outputs_dir))
    validate_sources(Path(args.cache_dir))
    if errors:
        print(f"\n{len(errors)} validation error(s) found — aborting.")
        sys.exit(1)
    else:
        print("\nAll validations passed.")


if __name__ == "__main__":
    main()
