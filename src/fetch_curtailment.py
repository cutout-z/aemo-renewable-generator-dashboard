"""Fetch per-DUID FY curtailment from the credit dashboard.

The credit dashboard (aemo-generator-credit-dashboard) already computes monthly
per-DUID curtailment from AEMO's INTERMITTENT_GEN_SCADA (with quality flags
splitting grid vs mechanical from Dec 2024+). It publishes a generation-weighted
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
    return reshape(df, generator_duids, select_fys(df, now=now))


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
    """
    labels = {y: config.fy_label(y) for y in fys}
    full = df[df["fy_start"].isin(fys) & (df["months_covered"] >= FULL_YEAR_MONTHS)]
    full = full.drop_duplicates(subset=["duid", "fy_start"], keep="first")
    values = {(r.duid, int(r.fy_start)): float(r.curtailment_pct) for r in full.itertuples()}

    rows = []
    for duid in sorted(generator_duids):
        row = {"DUID": duid}
        for y, label in labels.items():
            row[f"CURTAILMENT_ACTUAL_{label}"] = values.get((duid, y))
        rows.append(row)

    columns = ["DUID"] + [f"CURTAILMENT_ACTUAL_{labels[y]}" for y in fys]
    result = pd.DataFrame(rows, columns=columns)
    value_cols = columns[1:]
    for col in value_cols:
        result[col] = pd.to_numeric(result[col], errors="coerce").astype("float64")
    matched = result.dropna(subset=value_cols, how="all") if value_cols else result.iloc[0:0]
    logger.info(f"Matched curtailment for {len(matched)} of {len(result)} generators "
                f"({', '.join(labels.values()) or 'no complete FY'})")
    return result
