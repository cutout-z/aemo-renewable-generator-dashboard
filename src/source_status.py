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
