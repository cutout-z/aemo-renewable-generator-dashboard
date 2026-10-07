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

# Run as a script from the repo root (python tests/validate_outputs.py): make `src` importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.post_publish_check import newer_edition_message  # noqa: E402
from src import config, eli_appendix, isp_rez_appendix, rez_history  # noqa: E402

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
# MLFs (the aemo-mlf-tracker summary.csv): a run that republished the cached copy fails
# when its last good fetch is older than this
MAX_MLF_AGE_DAYS = 35
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
# The credit rollup's newest month (its own content, not our fetch): it must have ended
# within this many days, allowing for nemweb lag plus a monthly lane (S3-4)
MAX_UPSTREAM_MONTH_AGE_DAYS = 75

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
    check_mlf(status)

    check_eli(status.get("eli", {}))

    check_edition_match(status, df)

    check_isp_a3(df, cache_dir)


def expected_eli_edition(now=None):
    """The newest ELI edition that should be out by `now`: Y from 1 October of year Y (NEM time)."""
    now = (now or datetime.now(timezone.utc)).astimezone(NEM_TZ)
    return now.year if now.month >= 10 else now.year - 1


def check_eli(eli, now=None):
    """Newer ELI edition (warn here; src.post_publish_check fails the lane after publishing),
    calendar backstop (warn), blind probe (fail)."""
    if not eli:
        print("  WARN: no ELI record in source_status.json (the run did not refresh ELI)")
        return
    edition, newer = eli.get("edition"), eli.get("newer_edition_available")
    if newer:
        # Decision 6(b): the lane goes red, but only after this run has published its other
        # updates (MLF, actuals, generator list); failing here would hold them back
        print(f"  WARN: {newer_edition_message(eli)}. The lane fails after publishing "
              "(python -m src.post_publish_check) until this is done")
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


def check_isp_a3(df, cache_dir):
    """The ISP A3 REZ figures (ISPA3_* columns) against the reference files they come from.

    Every REZ in rez_forecasts.feather has A3 values or is on the explicit not-in-A3 list (the
    crosswalk's "no one-to-one match" rows, and A3 zones with no table); the summary's editions
    are the data files' editions; the history file holds every edition the page shows.
    """
    cache_dir = Path(cache_dir)
    path = cache_dir / config.ISP_A3_FILE
    has_cols = df is not None and "ISPA3_EDITION" in df.columns
    if not path.exists():
        check(not has_cols, f"summary.csv has ISPA3_* columns but {path} is missing")
        print(f"  WARN: no ISP A3 REZ table ({path}); run python -m src.isp_rez_appendix")
        return
    table = pd.read_feather(path)
    edition = isp_rez_appendix.edition(table)
    if not check(edition is not None, f"{config.ISP_A3_FILE} carries no single ISP_EDITION"):
        return
    rez_path = cache_dir / Path(config.REZ_FORECAST_CACHE).name
    eli_forecasts = pd.read_feather(rez_path) if rez_path.exists() else None
    crosswalk = isp_rez_appendix.load_crosswalk(cache_dir)
    for problem in isp_rez_appendix.check_crosswalk(crosswalk, table, eli_forecasts):
        check(False, f"{config.ISP_REZ_CROSSWALK_FILE}: {problem}")
    if eli_forecasts is not None:
        gaps = isp_rez_appendix.not_in_a3(crosswalk, table, eli_forecasts)
        print(f"{edition} A3: {len(set(eli_forecasts['REZ_NAME']))} ELI-appendix REZs, "
              f"{len(gaps)} without A3 values (listed in the crosswalk): " + "; ".join(gaps))

    if not check(has_cols, f"summary.csv has no ISPA3_* columns although {config.ISP_A3_FILE} "
                           "exists: rerun the pipeline"):
        return
    check(_single(df, "ISPA3_EDITION") == edition,
          f"summary.csv ISPA3_EDITION {_single(df, 'ISPA3_EDITION')} is not {config.ISP_A3_FILE}'s {edition}")
    check(_single(df, "ISPA3_SCENARIO") == config.ISP_A3_SCENARIO,
          f"summary.csv ISPA3_SCENARIO is {_single(df, 'ISPA3_SCENARIO')}, not {config.ISP_A3_SCENARIO}")
    for i in (1, 2, 3):
        years = set(table[f"Y{i}_LABEL"])
        check({_single(df, f"ISPA3_Y{i}_LABEL")} == years,
              f"summary.csv ISPA3_Y{i}_LABEL is {_single(df, f'ISPA3_Y{i}_LABEL')}, the A3 table has {sorted(years)}")
    if eli_forecasts is not None and "ISP_EDITION" in df.columns:
        ed = eli_appendix.editions(eli_forecasts)
        check(_single(df, "ISP_EDITION") in (None, ed["isp_edition"]),
              f"summary.csv ISP_EDITION {_single(df, 'ISP_EDITION')} is not rez_forecasts.feather's "
              f"{ed['isp_edition']}")
    for col in [c for c in df.columns if c.startswith(("ISPA3_TRANSMISSION_", "ISPA3_SPILL_"))]:
        vals = pd.to_numeric(df[col], errors="coerce").dropna()
        check(vals.empty or (vals.min() >= 0 and vals.max() <= 1), f"{col} has values outside [0, 1]")
    if "REZ" in df.columns and "ISPA3_MATCH" in df.columns:
        match = _text(df, "ISPA3_MATCH")
        lost = sorted(set(_text(df, "REZ_NAME")[(_text(df, "REZ") == "Y") & (match == "not in the crosswalk")]))
        check(not lost, f"REZ(s) on the page not in {config.ISP_REZ_CROSSWALK_FILE}: {lost}")

    history = rez_history.load(cache_dir / config.REZ_HISTORY_FILE)
    held = rez_history.editions(history)
    shown = {(f"{edition} {config.ISP_A3_SOURCE}", "", edition)}
    eli_ed, isp_ed = _single(df, "ELI_EDITION"), _single(df, "ISP_EDITION")
    if eli_ed and isp_ed:
        shown.add((rez_history.eli_source(eli_ed), str(eli_ed), isp_ed))
    for key in sorted(shown - held):
        check(False, f"{config.REZ_HISTORY_FILE} lacks the edition the page shows: {key} "
                     "(python -m src.rez_history eli, or python -m src.isp_rez_appendix)")


def current_fy_start(now=None):
    """Start year of the current financial year in NEM time (FY26-27 → 2026)."""
    now = (now or datetime.now(timezone.utc)).astimezone(NEM_TZ)
    return now.year if now.month >= 7 else now.year - 1


def _fy_start(label):
    """'FY26-27' → 2026; None if not a FY label."""
    text = str(label or "")
    if len(text) >= 4 and text.startswith("FY") and text[2:4].isdigit():
        return 2000 + int(text[2:4])
    return None


def check_mlf(status, now=None):
    """Fail when MLFs come from a stale cache or the newest final MLF year is behind (S2-4)."""
    rec = status.get("mlf")
    if not rec:
        print("  WARN: no MLF record in source_status.json (run predates it, or the run used "
              "the MLF cache without refreshing)")
        return
    if rec.get("error") and not rec.get("used_cache"):
        check(False, f"MLF fetch failed and there is no cached copy, so the summary has no MLF "
                     f"columns: {rec.get('error')}")
        return
    age = _age_days(rec.get("fetched_at"))
    age_txt = f"{age:.0f} days old" if age is not None else "of unknown age"
    if rec.get("used_cache"):
        print(f"  WARN: MLF refresh failed ({rec.get('error')}); this run republished the cached "
              f"tracker CSV, {age_txt}")
        check(age is not None and age <= MAX_MLF_AGE_DAYS,
              f"MLF cache is {age_txt} (> {MAX_MLF_AGE_DAYS} days) and the MLF tracker fetch keeps "
              f"failing: {rec.get('error')}")
    newest, current = rec.get("newest_fy"), current_fy_start(now)
    start = _fy_start(newest)
    current_label = f"FY{current % 100:02d}-{(current + 1) % 100:02d}"
    if check(start is not None, "The MLF tracker CSV has no final FY column"):
        check(start >= current,
              f"Newest final MLF year is {newest}, older than the current financial year "
              f"{current_label}: the MLF tracker has not published this year's MLFs")
        print(f"MLF: newest final year {newest} (tracker fetched {age_txt})")


def month_end_age_days(month, now=None):
    """Days since the end of 'YYYY-MM' (None if unparseable)."""
    try:
        year, mon = (int(x) for x in str(month).split("-"))
        end = datetime(year + mon // 12, mon % 12 + 1, 1, tzinfo=NEM_TZ)
    except (ValueError, TypeError):
        return None
    return ((now or datetime.now(timezone.utc)) - end).total_seconds() / 86400


def check_upstream_month(rec, now=None):
    """Fail when the credit rollup's newest month ended too long ago, even if our fetch worked."""
    month = rec.get("upstream_last_month")
    age = month_end_age_days(month, now)
    if age is None:
        print("  WARN: the actual-curtailment record has no upstream newest month (run predates it)")
        return
    print(f"Actual curtailment upstream: data to {month} (ended {age:.0f} days ago)")
    check(age <= MAX_UPSTREAM_MONTH_AGE_DAYS,
          f"The credit dashboard's curtailment rollup ends at {month}, {age:.0f} days ago "
          f"(> {MAX_UPSTREAM_MONTH_AGE_DAYS}): its pipeline has stalled, so the actuals are not current")


def check_actual_curtailment(status):
    """Warn when this run republished cached actuals; fail when they are stale or absent."""
    rec = status.get("actual_curtailment")
    if not rec:
        print("  WARN: no actual-curtailment record in source_status.json (run predates it, "
              "or the run used the cache without refreshing)")
        return
    check_upstream_month(rec)
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
