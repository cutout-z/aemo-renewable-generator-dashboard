"""Find, cache and parse the newest NEM Generation Information workbook.

AEMO republishes Generation Information roughly quarterly under a new file
name, so a pinned URL goes dead (the April 2025 link now redirects to the 404
page). Each run:

1. tries to read the edition links off the Generation Information page
   (often blocked by Cloudflare for scripted requests);
2. probes the predictable file names newest-first, month by month, down to the
   edition already cached;
3. keeps the cached edition when nothing newer is found or a download is not a
   real workbook, and warns when that edition is more than ~4 months old.

The edition (YYYY-MM) and fetch time are recorded in data/source_status.json.

What the July 2026 edition actually contains (sheet "Generator Information"):
site name, owner, region, technology type/detail, DUID, unit capacity and
commitment status — but no REZ, location or connection voltage column. The
parser still maps those columns if a future edition adds them.
"""

from __future__ import annotations

import logging
import re
from datetime import date
from pathlib import Path

import pandas as pd
import requests

from . import config, source_status

logger = logging.getLogger(__name__)

CACHE_FILE = "nem-generation-information.xlsx"
STATUS_KEY = "gen_info"
STALE_AFTER_DAYS = 122  # ~4 months: AEMO publishes about quarterly
LOOKBACK_MONTHS = 15

MONTHS = ["january", "february", "march", "april", "may", "june", "july",
          "august", "september", "october", "november", "december"]

SHEETS = [
    "Generator Information",
    "ExistingGeneration&NewDevs",
    "Existing Generation & New Devs",
    "ExistingGeneration-Registered",
]

_LINK_RE = re.compile(
    r"""href=["']([^"']*nem-generation-information-([a-z]+)-(\d{4})[^"']*\.xlsx[^"']*)["']""",
    re.IGNORECASE,
)


def edition_label(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def edition_age_days(edition: str, today: date | None = None) -> int | None:
    try:
        y, m = (int(p) for p in edition.split("-"))
    except (AttributeError, ValueError):
        return None
    return ((today or date.today()) - date(y, m, 1)).days


def candidate_urls(today: date | None = None, newer_than: str | None = None,
                   lookback: int = LOOKBACK_MONTHS) -> list[tuple[str, str]]:
    """Predictable (edition, url) pairs, newest month first."""
    today = today or date.today()
    out = []
    y, m = today.year, today.month
    for _ in range(lookback):
        label = edition_label(y, m)
        if newer_than and label <= newer_than:
            break
        name = f"nem-generation-information-{MONTHS[m - 1]}-{y}.xlsx"
        out.append((label, f"{config.NEM_GEN_INFO_BASE_URL}{y}/{name}"))
        out.append((label, f"{config.NEM_GEN_INFO_BASE_URL}{name}"))
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return out


def links_from_page(html: str, base: str = "https://www.aemo.com.au") -> list[tuple[str, str]]:
    """(edition, url) pairs linked from the Generation Information page, newest first."""
    found = {}
    for href, month, year in _LINK_RE.findall(html):
        month = month.lower()
        if month not in MONTHS:
            continue
        label = edition_label(int(year), MONTHS.index(month) + 1)
        url = href if href.startswith("http") else base + ("" if href.startswith("/") else "/") + href
        found.setdefault(label, url.replace("&amp;", "&"))
    return sorted(found.items(), reverse=True)


def _get(url: str, **kwargs):
    return requests.get(url, timeout=config.REQUEST_TIMEOUT,
                        headers={"User-Agent": config.USER_AGENT}, **kwargs)


def refresh_gen_info(cache_dir: str | Path, today: date | None = None) -> tuple[Path | None, str | None]:
    """Make sure the newest reachable edition is cached; return (path, edition)."""
    from .download_generators import check_workbook_bytes

    cache_path = Path(cache_dir)
    xlsx_path = cache_path / CACHE_FILE
    previous = source_status.load(cache_path).get(STATUS_KEY, {})
    cached_edition = previous.get("edition") if xlsx_path.exists() else None
    record = dict(previous) if cached_edition else {}
    record.update(checked_at=source_status.now_iso(), refreshed=False, error=None)

    candidates: list[tuple[str, str]] = []
    try:
        resp = _get(config.NEM_GEN_INFO_PAGE_URL)
        if resp.status_code == 200:
            candidates = [c for c in links_from_page(resp.text)
                          if not cached_edition or c[0] > cached_edition]
            logger.info(f"Generation Information page lists {len(candidates)} newer edition link(s)")
        else:
            logger.info(f"Generation Information page returned HTTP {resp.status_code}; "
                        "probing file names instead")
    except requests.RequestException as e:
        logger.info(f"Generation Information page unreachable ({e}); probing file names instead")

    candidates += candidate_urls(today, newer_than=cached_edition)
    tried = set()
    for edition, url in candidates:
        if url in tried:
            continue
        tried.add(url)
        try:
            resp = _get(url, allow_redirects=False)
        except requests.RequestException as e:
            record["error"] = f"{edition}: {e}"
            continue
        if resp.status_code != 200:
            continue  # AEMO answers a missing file with a 302 to its 404 page
        try:
            check_workbook_bytes(resp.content)
        except ValueError as e:
            record["error"] = f"{edition}: {e}"
            logger.warning(f"Generation Information {edition} at {url}: {e}")
            continue
        tmp = xlsx_path.with_name(xlsx_path.name + ".part")
        cache_path.mkdir(parents=True, exist_ok=True)
        tmp.write_bytes(resp.content)
        tmp.replace(xlsx_path)
        record.update(edition=edition, url=url, fetched_at=source_status.now_iso(),
                      refreshed=True, error=None)
        logger.info(f"Downloaded Generation Information {edition} "
                    f"({len(resp.content) / 1024:.0f} KB) from {url}")
        cached_edition = edition
        break

    if cached_edition is None:
        logger.warning("No Generation Information edition found or cached")
        record["edition"] = None
    else:
        age = edition_age_days(cached_edition, today)
        record["edition_age_days"] = age
        if age is not None and age > STALE_AFTER_DAYS:
            logger.warning(f"Generation Information edition {cached_edition} is {age} days old "
                           f"(> {STALE_AFTER_DAYS}); no newer edition could be found")
        elif not record["refreshed"]:
            logger.info(f"Generation Information: keeping cached edition {cached_edition}")

    source_status.update(cache_path, STATUS_KEY, record)
    return (xlsx_path if cached_edition else None), cached_edition


def parse_gen_info(xlsx_path: str | Path) -> pd.DataFrame:
    """Parse the generator sheet into standard column names, one row per DUID."""
    from .download_generators import _detect_gen_info_columns

    xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
    sheet = next((s for s in SHEETS if s in xls.sheet_names), None)
    if sheet is None:
        raise ValueError(f"No generator sheet in Generation Information: {xls.sheet_names}")

    raw = pd.read_excel(xls, sheet_name=sheet, header=None)
    header_idx = next(
        (i for i in range(min(20, len(raw)))
         if "duid" in [str(v).strip().lower() for v in raw.iloc[i].tolist()]),
        None,
    )
    if header_idx is None:
        raise ValueError(f"No DUID header row in sheet {sheet!r}")
    df = raw.iloc[header_idx + 1:].copy()
    df.columns = [str(v).strip() for v in raw.iloc[header_idx].tolist()]
    df = df.rename(columns=_detect_gen_info_columns(df))

    df = df.dropna(subset=["DUID"])
    df["DUID"] = df["DUID"].astype(str).str.strip()
    df = df[~df["DUID"].isin(["", "-", "nan"])]
    df = df.drop_duplicates(subset="DUID", keep="first").reset_index(drop=True)

    present = [c for c in ("REZ_NAME", "LOCATION", "VOLTAGE_KV") if c in df.columns]
    logger.info(f"Generation Information sheet {sheet!r}: {len(df)} DUIDs; "
                f"REZ/location/voltage columns present: {present or 'none'}")
    return df
