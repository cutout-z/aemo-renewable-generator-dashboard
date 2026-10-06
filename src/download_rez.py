"""REZ-level ISP curtailment and economic offloading forecasts.

Source: the "VRE curtailment and economic offloading – ISP forecast" table in
each REZ section of AEMO's ELI regional appendices (PDF only). The table is
extracted once per ELI edition by `python -m src.eli_appendix`, which writes
data/rez_forecasts.feather; this module only reads it.

Columns: STATE, REZ_NAME, CURTAILMENT_FY1..3 (+ _LABEL like '25-26'),
CURTAILMENT_AVG, OFFLOADING_FY1..3 (+ _LABEL), OFFLOADING_AVG. Values are
fractions (0.10 = 10%). A year AEMO shows as "-" (no VRE projected in the REZ)
is empty, not 0, and the averages are over the years that have a value.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from . import config

logger = logging.getLogger(__name__)


def fetch_rez_forecasts(cache_dir: str) -> pd.DataFrame:
    """Load the REZ forecasts extracted from the ELI regional appendices."""
    path = Path(cache_dir) / Path(config.REZ_FORECAST_CACHE).name
    if not path.exists():
        logger.warning(f"No REZ forecasts at {path}; run `python -m src.eli_appendix`")
        return pd.DataFrame()
    forecasts = pd.read_feather(path)
    logger.info(f"Loaded REZ forecasts for {len(forecasts)} zones "
                f"(ELI {max(config.ELI_REGIONAL_APPENDIX_URLS)} regional appendices)")
    return forecasts
