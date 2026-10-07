"""REZ-level ISP curtailment and economic offloading forecasts.

Source: the "VRE curtailment and economic offloading – ISP forecast" table in
each REZ section of AEMO's ELI regional appendices (PDF only). The table is
extracted once per ELI edition by `python -m src.eli_appendix`, which writes
data/rez_forecasts.feather; this module only reads it.

Columns: STATE, REZ_NAME, CURTAILMENT_FY1..3 (+ _LABEL like '25-26'),
CURTAILMENT_AVG, OFFLOADING_FY1..3 (+ _LABEL), OFFLOADING_AVG. Values are
fractions (0.10 = 10%). A year AEMO shows as "-" (no VRE projected in the REZ)
is empty, not 0, and the averages are over the years that have a value.
ELI_EDITION / ISP_EDITION say which appendix and ISP edition the file holds.

Each run records both reference files' editions in source_status.json ("rez"),
so tests/validate_outputs.py can fail when they differ from the ELI chart-data
edition (config bumped, `python -m src.eli_appendix` not rerun).
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from . import config, eli_appendix, source_status

logger = logging.getLogger(__name__)


STATUS_KEY = "rez"


def fetch_rez_forecasts(cache_dir: str) -> pd.DataFrame:
    """Load the REZ forecasts extracted from the ELI regional appendices."""
    path = Path(cache_dir) / Path(config.REZ_FORECAST_CACHE).name
    forecasts = pd.read_feather(path) if path.exists() else pd.DataFrame()
    record_editions(cache_dir, forecasts)
    if forecasts.empty:
        logger.warning(f"No REZ forecasts at {path}; run `python -m src.eli_appendix`")
        return forecasts
    ed = eli_appendix.editions(forecasts)
    # The file's own edition, not the config's newest year: a config bump without a
    # rebuild would otherwise log an edition the file does not hold
    logger.info(f"Loaded REZ forecasts for {len(forecasts)} zones (ELI "
                f"{ed['eli_edition'] or 'edition not recorded'} regional appendices, "
                f"{ed['isp_edition'] or 'ISP edition not recorded'})")
    return forecasts


def record_editions(cache_dir: str, forecasts: pd.DataFrame) -> dict:
    """Write both appendix reference files' editions to source_status.json."""
    fc = eli_appendix.editions(forecasts)
    mpath = Path(cache_dir) / eli_appendix.MEMBERSHIP_FILE
    mb = eli_appendix.editions(pd.read_feather(mpath) if mpath.exists() else None)
    record = {"checked_at": source_status.now_iso(),
              "forecasts_eli_edition": fc["eli_edition"], "isp_edition": fc["isp_edition"],
              "membership_eli_edition": mb["eli_edition"]}
    source_status.update(cache_dir, STATUS_KEY, record)
    return record
