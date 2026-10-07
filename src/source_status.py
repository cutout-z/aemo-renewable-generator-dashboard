"""Freshness record for the upstream files the pipeline depends on.

Each run writes `source_status.json` into the cache directory: when each source
was last fetched successfully, whether this run's refresh worked, and what the
cross-checks found. tests/validate_outputs.py reads it, so a source that has
silently stopped refreshing fails validation instead of being published.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

STATUS_FILE = "source_status.json"


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load(cache_dir: str | Path) -> dict:
    path = Path(cache_dir) / STATUS_FILE
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        logger.warning(f"Ignoring unreadable {STATUS_FILE}: {e}")
        return {}


def update(cache_dir: str | Path, key: str, record: dict) -> dict:
    """Replace one source's record and write the file back."""
    status = load(cache_dir)
    status[key] = record
    path = Path(cache_dir) / STATUS_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(status, indent=2, sort_keys=True, default=str) + "\n",
                    encoding="utf-8")
    return status


# What the page footer shows, per source (outputs/source_status.json): editions and
# fetch dates, not URLs or cross-check detail. Errors are cut short.
PUBLISHED_FIELDS = {
    "registration_list": ("fetched_at", "refreshed", "error"),
    "eli": ("edition", "checked_at", "last_conclusive_check", "newer_edition_available", "error"),
    "rez": ("forecasts_eli_edition", "membership_eli_edition", "isp_edition"),
    "mlf": ("newest_fy", "fetched_at", "refreshed", "used_cache", "error"),
    "actual_curtailment": ("fys", "upstream_last_month", "fetched_at", "refreshed",
                           "used_cache", "error"),
    "gen_info": ("edition", "fetched_at"),
}
MAX_ERROR_CHARS = 160


def trimmed(status: dict) -> dict:
    """The published subset of a status dict."""
    out = {}
    for key, fields in PUBLISHED_FIELDS.items():
        rec = status.get(key)
        if not rec:
            continue
        out[key] = {f: rec.get(f) for f in fields if f in rec}
        err = out[key].get("error")
        if isinstance(err, str) and len(err) > MAX_ERROR_CHARS:
            out[key]["error"] = err[:MAX_ERROR_CHARS - 1] + "…"
    return out


def publish(cache_dir: str | Path, output_dir: str | Path) -> Path:
    """Write the trimmed status next to summary.csv, for the page footer."""
    path = Path(output_dir) / STATUS_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(trimmed(load(cache_dir)), indent=2, sort_keys=True, default=str)
                    + "\n", encoding="utf-8")
    return path


def age_days(iso: str | None, now: datetime | None = None) -> float | None:
    """Age of an ISO timestamp in days (None if missing or unparseable)."""
    if not iso:
        return None
    try:
        then = datetime.fromisoformat(iso)
    except ValueError:
        return None
    if then.tzinfo is None:
        then = then.replace(tzinfo=timezone.utc)
    now = now or datetime.now(timezone.utc)
    return (now - then).total_seconds() / 86400
