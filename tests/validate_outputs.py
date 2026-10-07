"""Post-pipeline validation for AEMO Renewable Generator Dashboard.

Checks summary.csv and regional Excel workbooks for data integrity
before committing to the repository. Exits non-zero on any failure.
"""

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
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
REZ_SOURCES = {"geninfo", "eli", "eli-station", "seed", ""}
# "per-DUID" (the hand-seeded 2025 values) is gone: every ELI value comes from AEMO's location table
ELI_SOURCES = {"location", "location-name", ""}
NON_REZ = "Non-REZ"

# Source freshness: the lane runs daily, so a list this old means refreshes keep failing
MAX_REGISTRATION_AGE_DAYS = 30
# Recently registered GENERATOR DUIDs (DUDETAILSUMMARY) the Registration List may lack
MAX_UNLISTED_RECENT_GENERATORS = 3
GEN_INFO_STALE_DAYS = 122
# ELI edition probe: it must have had a yes/no answer (not a 403 or network error)
# within this many days, or the newer-edition check is blind
MAX_ELI_PROBE_AGE_DAYS = 60
ELI_PAGE_URL = ("https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/"
                "nem-forecasting-and-planning/forecasting-and-planning-data/enhanced-locational-information")
# NEM market time (AEST, no daylight saving), as src/config.py
NEM_TZ = timezone(timedelta(hours=10))
# Actual curtailment (the credit dashboard's FY rollup, fetched daily): a run that fell
# back to the cached copy warns; a last good fetch older than this fails
MAX_ACTUAL_AGE_DAYS = 35

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
        return None

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
    check_editions(df)

    # --- TECHNOLOGY is the descriptor, not the Registration List's "Renewable" ---
    if "TECHNOLOGY" in df.columns and len(df):
        check(not (df["TECHNOLOGY"].astype(str) == "Renewable").all(),
              'TECHNOLOGY is "Renewable" for every row (primary column mapped, not the descriptor)')

    # --- Regional Excel workbooks exist ---
    for region_id, name in REGION_NAMES.items():
        xlsx_path = outputs_dir / f"{name}_curtailment.xlsx"
        check(xlsx_path.exists(), f"{xlsx_path.name} does not exist")
    return df


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


def _single(df, col):
    """The one non-blank value of `col`, or None; a mixed column fails."""
    if df is None or col not in df.columns:
        return None
    values = sorted(set(df[col].dropna().astype(str).str.strip()) - {""})
    check(len(values) <= 1, f"{col} has more than one value: {values}")
    return values[0] if len(values) == 1 else None


def check_editions(df):
    """ELI and ISP values say which edition they come from (S2-3)."""
    eli_cols = [c for c in ("ELI_CURTAILMENT_NEAR", "ELI_CURTAILMENT_MED") if c in df.columns]
    isp_cols = [c for c in df.columns if c.startswith("ISP_") and not c.endswith(("_LABEL", "_EDITION"))]
    if eli_cols and df[eli_cols].notna().any().any():
        check("ELI_EDITION" in df.columns and _single(df, "ELI_EDITION") is not None,
              "ELI values carry no ELI_EDITION (chart data parsed before editions were recorded: "
              "rerun with --full-refresh)")
    if isp_cols and df[isp_cols].notna().any().any():
        check("ISP_EDITION" in df.columns and _single(df, "ISP_EDITION") is not None,
              "ISP values carry no ISP_EDITION (REZ feathers predate editions: rerun "
              "python -m src.eli_appendix)")


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


def validate_sources(cache_dir: Path, df=None):
    """Fail when a source has stopped refreshing, is visibly missing units, or the
    published editions disagree. `df` is the summary validate() read (or None)."""
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

    check_actual_curtailment(status)

    check_eli(status.get("eli", {}))

    check_edition_match(status, df)


def expected_eli_edition(now=None):
    """The newest ELI edition that should be out by `now`: Y from 1 October of year Y (NEM time)."""
    now = (now or datetime.now(timezone.utc)).astimezone(NEM_TZ)
    return now.year if now.month >= 10 else now.year - 1


def check_eli(eli, now=None):
    """Newer ELI edition (warn), calendar backstop (warn), blind probe (fail)."""
    if not eli:
        print("  WARN: no ELI record in source_status.json (the run did not refresh ELI)")
        return
    edition, newer = eli.get("edition"), eli.get("newer_edition_available")
    if newer:
        print(f"  WARN: ELI {(edition or 0) + 1} has been published "
              f"({eli.get('probed_url')}); the dashboard still uses ELI {edition}")
    elif newer is None:
        print(f"  WARN: could not check for a newer ELI edition ({eli.get('error')})")
    else:
        print(f"ELI edition: {edition} (no newer chart data found; checked {eli.get('checked_at')})")

    # S2-1 calendar backstop: the probe guesses one file name; after the usual
    # publication month, "not found" is a reason to look, not an all-clear
    expected = expected_eli_edition(now)
    if edition is not None and edition < expected and not newer:
        print(f"  WARN: ELI {expected} not found at the expected URL ({eli.get('probed_url')}), "
              f"although each edition so far was out by July. It may be published under another "
              f"file name: check AEMO's ELI page {ELI_PAGE_URL}")

    # S3-3: a probe that keeps getting 403 / network errors is blind; fail after a while.
    # Records written before last_conclusive_check existed count their own conclusive check.
    last = eli.get("last_conclusive_check") or (eli.get("checked_at") if newer is not None else None)
    age = _age_days(last)
    if check(age is not None, "The ELI newer-edition probe has never had a yes/no answer "
                              f"({eli.get('error')}); check AEMO's ELI page {ELI_PAGE_URL}"):
        check(age <= MAX_ELI_PROBE_AGE_DAYS,
              f"The ELI newer-edition probe last had a yes/no answer {age:.0f} days ago "
              f"(> {MAX_ELI_PROBE_AGE_DAYS}; latest: {eli.get('error')}); a new edition would go "
              f"unnoticed: check AEMO's ELI page {ELI_PAGE_URL}")


def check_edition_match(status, df=None):
    """The REZ/ISP appendix files must be the same ELI edition as the chart data (S2-3).

    Bumping config.ELI_* to a new year without rerunning `python -m src.eli_appendix`
    (or republishing an older cached chart-data feather) would otherwise publish a mix
    of editions under one label.
    """
    rez = status.get("rez")
    if not rez:
        print("  WARN: no rez record in source_status.json (run predates it)")
        return
    published = _single(df, "ELI_EDITION") if df is not None else None
    chart = {str(v) for v in (status.get("eli", {}).get("edition"), published) if v is not None}
    for key, what in (("forecasts_eli_edition", "rez_forecasts.feather"),
                      ("membership_eli_edition", "rez_membership.feather")):
        appendix = rez.get(key)
        if not check(appendix is not None,
                     f"{what} records no ELI edition: rerun python -m src.eli_appendix"):
            continue
        check(not chart or chart == {str(appendix)},
              f"{what} is from the ELI {appendix} appendices but the ELI chart data is "
              f"{', '.join(sorted(chart))}: rerun python -m src.eli_appendix for the new edition")
    check(len(chart) <= 1, f"ELI chart-data editions disagree: configured/probed "
                           f"{status.get('eli', {}).get('edition')}, published {published}")
    print(f"ELI appendices {rez.get('forecasts_eli_edition')}, ISP forecasts "
          f"{rez.get('isp_edition')}, chart data {', '.join(sorted(chart)) or 'unknown'}")


def check_actual_curtailment(status):
    """Warn when this run republished cached actuals; fail when they are stale or absent."""
    rec = status.get("actual_curtailment")
    if not rec:
        print("  WARN: no actual-curtailment record in source_status.json (run predates it, "
              "or the run used the cache without refreshing)")
        return
    age = _age_days(rec.get("fetched_at"))
    fys = ", ".join(rec.get("fys") or []) or "none"
    if not rec.get("error"):
        print(f"Actual curtailment: refreshed {age:.1f} days ago ({fys})" if age is not None
              else f"Actual curtailment: {fys}")
        return
    if not rec.get("used_cache"):
        check(False, f"Actual curtailment refresh failed and there is no cached copy, so the "
                     f"summary has no actual columns: {rec.get('error')}")
        return
    age_txt = f"{age:.0f} days old" if age is not None else "of unknown age"
    print(f"  WARN: actual curtailment refresh failed ({rec.get('error')}); this run "
          f"republished the cached copy, {age_txt} ({fys})")
    check(age is not None and age <= MAX_ACTUAL_AGE_DAYS,
          f"Actual curtailment cache is {age_txt} (> {MAX_ACTUAL_AGE_DAYS} days) and the "
          f"upstream refresh keeps failing: {rec.get('error')}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--outputs-dir", default=str(OUTPUTS_DIR),
                        help="Directory holding summary.csv and the workbooks (default: outputs/)")
    parser.add_argument("--cache-dir", default=str(CACHE_DIR),
                        help="Pipeline cache directory holding source_status.json (default: data/)")
    args = parser.parse_args(argv)
    print("Validating AEMO Renewable Generator Dashboard outputs...")
    df = validate(Path(args.outputs_dir))
    validate_sources(Path(args.cache_dir), df)
    if errors:
        print(f"\n{len(errors)} validation error(s) found — aborting.")
        sys.exit(1)
    else:
        print("\nAll validations passed.")


if __name__ == "__main__":
    main()
