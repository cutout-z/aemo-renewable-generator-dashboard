"""Fetch MLF data from the existing aemo-mlf-tracker dashboard.

Each fetch is recorded in source_status.json ("mlf": fetched_at, refreshed,
error, used_cache, newest_fy), so tests/validate_outputs.py can fail when a
cached copy keeps being republished or the newest final MLF year falls behind
the current financial year.
"""

from __future__ import annotations

import logging
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

from datetime import datetime, timezone

from . import config, source_status

logger = logging.getLogger(__name__)

STATUS_KEY = "mlf"


def fetch_mlf_data(cache_dir: str, generator_duids: set[str] | None = None) -> pd.DataFrame:
    """Download MLF summary from aemo-mlf-tracker and extract relevant columns.

    Returns DataFrame with columns: DUID, plus FY MLF columns (e.g. FY23-24, FY24-25, FY25-26).
    Only returns the last 2 actual FYs + draft if available.
    """
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)
    csv_path = cache_path / "mlf_tracker_summary.csv"

    previous = source_status.load(cache_path).get(STATUS_KEY, {})
    record = {"attempted_at": source_status.now_iso(), "url": config.MLF_TRACKER_SUMMARY_URL,
              "refreshed": False, "error": None, "used_cache": False,
              "fetched_at": previous.get("fetched_at") or _mtime_iso(csv_path),
              "newest_fy": previous.get("newest_fy"), "fys": previous.get("fys", [])}

    # Try to download fresh copy
    try:
        logger.info("Fetching MLF data from aemo-mlf-tracker...")
        resp = requests.get(
            config.MLF_TRACKER_SUMMARY_URL,
            timeout=config.REQUEST_TIMEOUT,
            headers={"User-Agent": config.USER_AGENT},
        )
        resp.raise_for_status()
        csv_path.write_text(resp.text, encoding="utf-8")
        logger.info(f"Downloaded MLF summary ({len(resp.text) / 1024:.0f} KB)")
        record.update(refreshed=True, fetched_at=record["attempted_at"])
    except requests.RequestException as e:
        record["error"] = str(e)
        if csv_path.exists():
            record["used_cache"] = True
            logger.error(f"MLF REFRESH FAILED ({e}); using the cached copy (last good fetch "
                         f"{record['fetched_at'] or 'unknown'})")
        else:
            logger.error(f"Failed to fetch MLF data and no cache available: {e}")
            source_status.update(cache_path, STATUS_KEY, record)
            return pd.DataFrame()

    df = pd.read_csv(csv_path)

    # Find all FY columns (both final and draft)
    fy_cols = sorted([c for c in df.columns if c.startswith("FY") and "Import" not in c])
    final_cols = [c for c in fy_cols if "Draft" not in c]
    draft_cols = [c for c in fy_cols if "Draft" in c]

    # Keep last 2 actual FYs + any draft columns
    keep_fy = final_cols[-2:] if len(final_cols) >= 2 else final_cols
    keep_fy += draft_cols

    record["newest_fy"] = final_cols[-1] if final_cols else None
    record["fys"] = list(keep_fy)
    source_status.update(cache_path, STATUS_KEY, record)

    # Select columns
    id_cols = ["DUID"]
    available = [c for c in id_cols + keep_fy if c in df.columns]
    df = df[available].copy()

    # Filter to solar/wind DUIDs if provided
    if generator_duids:
        df = df[df["DUID"].isin(generator_duids)].copy()

    # Convert FY columns to numeric
    for col in keep_fy:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Rename columns for clarity: FY24-25 → MLF_FY24-25
    rename_map = {c: f"MLF_{c}" for c in keep_fy if c in df.columns}
    df = df.rename(columns=rename_map)

    df = df.dropna(subset=["DUID"])
    df = df.drop_duplicates(subset="DUID", keep="first")

    logger.info(f"Loaded MLF data for {len(df)} generators ({len(keep_fy)} FY columns)")
    return df


def _mtime_iso(path: Path) -> str | None:
    """When a cached file was last written: the best guess at a last good fetch
    for a cache that predates the status record."""
    if not path.exists():
        return None
    when = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).replace(microsecond=0)
    return when.isoformat()
