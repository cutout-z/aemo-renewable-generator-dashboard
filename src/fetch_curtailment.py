"""Fetch per-DUID FY curtailment from the credit dashboard.

The credit dashboard (aemo-generator-credit-dashboard) already computes monthly
per-DUID curtailment from AEMO dispatch data: DISPATCH_UNIT_SCADA output against
the unit's bid-in AVAILABILITY in DISPATCHLOAD, with INTERMITTENT_GEN_SCADA
quality flags splitting grid vs mechanical from Dec 2024+. It publishes a generation-weighted
FY rollup to its GitHub Pages site. We fetch that here rather than re-run our own
NEMOSIS pipeline — one source of truth, incremental on the credit side.
"""

from __future__ import annotations

import logging
from io import StringIO

import pandas as pd
import requests

from . import config

logger = logging.getLogger(__name__)


# How many completed FYs the table shows
N_FYS = 2
# A FY counts as complete in the rollup when a unit has all 12 months in it
FULL_YEAR_MONTHS = 12


def fetch_curtailment_by_fy(generator_duids: set[str], now=None) -> pd.DataFrame:
    """Fetch and reshape FY curtailment for the renewable dashboard table.

    Returns DataFrame with columns:
        DUID, CURTAILMENT_ACTUAL_{fy1_label}, CURTAILMENT_ACTUAL_{fy2_label}
    where fy1/fy2 are the two latest FYs the rollup covers in full (see
    `select_fys`).
    """
    logger.info(f"Fetching FY curtailment from {config.CREDIT_CURTAILMENT_URL}")
    resp = requests.get(
        config.CREDIT_CURTAILMENT_URL,
        timeout=config.REQUEST_TIMEOUT,
        headers={"User-Agent": config.USER_AGENT},
    )
    resp.raise_for_status()
    df = pd.read_csv(StringIO(resp.text))
    logger.info(f"Fetched {len(df)} DUID×FY rows from credit dashboard")
    result = reshape(df, generator_duids, select_fys(df, now=now))
    # How current the upstream content is (a fetch can succeed while the credit pipeline has
    # stalled and Pages keeps serving an old CSV); src/main.py records it in source_status.json
    result.attrs["upstream_last_month"] = upstream_last_month(df)
    return result


def upstream_last_month(df: pd.DataFrame) -> str | None:
    """The newest month the rollup has data for, as 'YYYY-MM' (None if it cannot tell).

    Read from the rollup's last_month column; failing that, from the latest FY's
    largest months_covered (a FY starts in July).
    """
    if "last_month" in df.columns:
        months = df["last_month"].dropna().astype(str).str.strip()
        months = months[months.str.fullmatch(r"\d{4}-\d{2}")]
        if len(months):
            return months.max()
    if {"fy_start", "months_covered"} <= set(df.columns) and len(df):
        fy = int(df["fy_start"].max())
        covered = int(df.loc[df["fy_start"] == fy, "months_covered"].max())
        if covered >= 1:
            index = 6 + covered - 1          # July = month index 6 (0-based)
            return f"{fy + index // 12}-{index % 12 + 1:02d}"
    return None


def select_fys(df: pd.DataFrame, now=None, n: int = N_FYS) -> list[int]:
    """The `n` latest FYs (fy_start, oldest first) that the rollup covers in full.

    A FY qualifies once it has ended (in NEM time) and the rollup has at least
    one unit with all 12 months in it. Picking "the last two FYs by calendar"
    instead blanked a whole column for about a month after each 1 July: the
    FY just ended has 11 months in the rollup until June's data lands upstream,
    so every unit was N/A for it, and the older full year had been dropped.
    """
    current = config.current_fy_start(now)
    full = df.loc[(df["months_covered"] >= FULL_YEAR_MONTHS) & (df["fy_start"] < current),
                  "fy_start"]
    fys = sorted({int(y) for y in full})[-n:]
    if fys != list(range(current - n, current)):
        logger.warning(f"Latest complete FYs in the curtailment rollup: "
                       f"{[config.fy_label(y) for y in fys]} (the FY just ended is not complete "
                       f"upstream yet)")
    return fys


def reshape(df: pd.DataFrame, generator_duids: set[str], fys: list[int]) -> pd.DataFrame:
    """One row per DUID, one CURTAILMENT_ACTUAL_<FY> column per selected FY.

    A unit with fewer than 12 months in a FY gets no value for it: partial FYs
    would mislead the cross-sectional comparison the dashboard is built for.
    ACTUAL_MONTHS_<FY> carries the rollup's months_covered for each unit and FY
    (empty when the rollup has no row for it), so the page can say why a value
    is missing: a partial year, or no row at all.
    """
    labels = {y: config.fy_label(y) for y in fys}
    rows_fy = df[df["fy_start"].isin(fys)].drop_duplicates(subset=["duid", "fy_start"], keep="first")
    values, months = {}, {}
    for r in rows_fy.itertuples():
        key = (r.duid, int(r.fy_start))
        months[key] = r.months_covered
        if r.months_covered >= FULL_YEAR_MONTHS:
            values[key] = r.curtailment_pct

    rows = []
    for duid in sorted(generator_duids):
        row = {"DUID": duid}
        for y, label in labels.items():
            row[f"CURTAILMENT_ACTUAL_{label}"] = values.get((duid, y))
        for y, label in labels.items():
            row[f"ACTUAL_MONTHS_{label}"] = months.get((duid, y))
        rows.append(row)

    value_cols = [f"CURTAILMENT_ACTUAL_{labels[y]}" for y in fys]
    month_cols = [f"ACTUAL_MONTHS_{labels[y]}" for y in fys]
    result = pd.DataFrame(rows, columns=["DUID"] + value_cols + month_cols)
    for col in value_cols + month_cols:
        result[col] = pd.to_numeric(result[col], errors="coerce").astype("float64")
    matched = result.dropna(subset=value_cols, how="all") if value_cols else result.iloc[0:0]
    logger.info(f"Matched curtailment for {len(matched)} of {len(result)} generators "
                f"({', '.join(labels.values()) or 'no complete FY'})")
    return result
