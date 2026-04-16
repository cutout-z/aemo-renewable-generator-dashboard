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


def fetch_curtailment_by_fy(generator_duids: set[str]) -> pd.DataFrame:
    """Fetch and reshape FY curtailment for the renewable dashboard table.

    Returns DataFrame with columns:
        DUID, CURTAILMENT_ACTUAL_{fy1_label}, CURTAILMENT_ACTUAL_{fy2_label}
    where fy1/fy2 are the two most recently completed financial years.
    """
    fy_current_start = config.current_fy_start()
    fy1_start = fy_current_start - 2
    fy2_start = fy_current_start - 1
    fy1_label = config.fy_label(fy1_start)
    fy2_label = config.fy_label(fy2_start)

    logger.info(f"Fetching FY curtailment from {config.CREDIT_CURTAILMENT_URL}")
    resp = requests.get(
        config.CREDIT_CURTAILMENT_URL,
        timeout=config.REQUEST_TIMEOUT,
        headers={"User-Agent": config.USER_AGENT},
    )
    resp.raise_for_status()
    df = pd.read_csv(StringIO(resp.text))
    logger.info(f"Fetched {len(df)} DUID×FY rows from credit dashboard")

    needed = df[df["fy_start"].isin([fy1_start, fy2_start])].copy()

    # Require a full 12-month FY to report a value — partial FYs would mislead
    # the cross-sectional comparison the dashboard is built for.
    needed = needed[needed["months_covered"] >= 12]

    rows = []
    for duid in generator_duids:
        d = needed[needed["duid"] == duid]
        fy1 = d[d["fy_start"] == fy1_start]["curtailment_pct"]
        fy2 = d[d["fy_start"] == fy2_start]["curtailment_pct"]
        rows.append({
            "DUID": duid,
            f"CURTAILMENT_ACTUAL_{fy1_label}": float(fy1.iloc[0]) if len(fy1) else None,
            f"CURTAILMENT_ACTUAL_{fy2_label}": float(fy2.iloc[0]) if len(fy2) else None,
        })

    result = pd.DataFrame(rows)
    matched = result.dropna(
        subset=[f"CURTAILMENT_ACTUAL_{fy1_label}", f"CURTAILMENT_ACTUAL_{fy2_label}"],
        how="all",
    )
    logger.info(f"Matched curtailment for {len(matched)} of {len(result)} generators")
    return result
