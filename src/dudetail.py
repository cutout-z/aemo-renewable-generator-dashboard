"""Cross-check the Registration List against MMSDM DUDETAILSUMMARY (nemweb).

The NEM Registration and Exemption List is the generator spine, but it is a
spreadsheet on aemo.com.au that can go stale (or be served a Cloudflare page).
DUDETAILSUMMARY in the monthly MMSDM archive on nemweb lists every registered
DUID with the date its registration took effect. Any GENERATOR DUID that first
appears there within the last 24 months but is missing from the Registration
List is logged as a warning: the list we parsed is probably out of date.

DUDETAILSUMMARY carries no fuel type, so missing DUIDs are only reported —
never added to the dashboard with a guessed fuel.
"""

from __future__ import annotations

import csv
import io
import logging
import zipfile
from datetime import date
from pathlib import Path

import pandas as pd
import requests

from . import config

logger = logging.getLogger(__name__)

MMSDM_BASE_URL = "https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/"
DUDETAIL_URL_TEMPLATE = (
    MMSDM_BASE_URL
    + "{year:04d}/MMSDM_{year:04d}_{month:02d}/"
    "MMSDM_Historical_Data_SQLLoader/DATA/"
    "PUBLIC_ARCHIVE%23DUDETAILSUMMARY%23FILE01%23{year:04d}{month:02d}010000.zip"
)
CACHE_FILE = "dudetailsummary_latest.zip"
RECENT_MONTHS = 24


def _months_newest_first(today: date, count: int):
    y, m = today.year, today.month
    for _ in range(count):
        yield y, m
        m -= 1
        if m == 0:
            y, m = y - 1, 12


def fetch_latest_dudetailsummary(
    cache_dir: str | Path,
    cached_month: str | None = None,
    today: date | None = None,
    months_back: int = 4,
) -> tuple[pd.DataFrame | None, str | None]:
    """Return (DUDETAILSUMMARY, "YYYY-MM") for the newest archive month.

    Probes newest-first down to the month already cached; MMSDM months are
    immutable once published, so the cached zip is reused when nothing newer
    exists (or when nemweb cannot be reached).
    """
    cache_path = Path(cache_dir) / CACHE_FILE
    today = today or date.today()

    for year, month in _months_newest_first(today, months_back):
        label = f"{year:04d}-{month:02d}"
        if cached_month and label <= cached_month:
            break
        url = DUDETAIL_URL_TEMPLATE.format(year=year, month=month)
        try:
            resp = requests.get(url, timeout=config.REQUEST_TIMEOUT,
                                headers={"User-Agent": config.USER_AGENT})
        except requests.RequestException as e:
            logger.warning(f"DUDETAILSUMMARY probe failed for {label}: {e}")
            break
        if resp.status_code == 404:
            continue
        if resp.status_code != 200 or not resp.content.startswith(b"PK"):
            logger.warning(f"DUDETAILSUMMARY {label}: unexpected response "
                           f"(HTTP {resp.status_code}); keeping cached copy")
            break
        df = parse_dudetailsummary(resp.content)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(resp.content)
        logger.info(f"Downloaded DUDETAILSUMMARY {label} ({df['DUID'].nunique()} DUIDs)")
        return df, label

    if cache_path.exists() and cached_month:
        return parse_dudetailsummary(cache_path.read_bytes()), cached_month
    return None, None


def parse_dudetailsummary(zip_bytes: bytes) -> pd.DataFrame:
    """Parse an MMSDM DUDETAILSUMMARY zip (column names from its I row)."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not names:
            raise ValueError("No CSV in DUDETAILSUMMARY zip")
        text = zf.read(names[0]).decode("utf-8", errors="replace")

    header = None
    rows = []
    for fields in csv.reader(io.StringIO(text)):
        if not fields:
            continue
        if fields[0] == "I" and len(fields) > 4 and fields[2] == "DUDETAILSUMMARY":
            header = fields[4:]
        elif fields[0] == "D" and header is not None:
            rows.append(fields[4:4 + len(header)])
    if header is None:
        raise ValueError("No DUDETAILSUMMARY header row in archive")

    df = pd.DataFrame(rows, columns=header)
    df["START_DATE"] = pd.to_datetime(df["START_DATE"], errors="coerce")
    return df


def recent_generators(dud: pd.DataFrame, today: date | None = None,
                      months: int = RECENT_MONTHS) -> pd.DataFrame:
    """GENERATOR DUIDs whose first DUDETAILSUMMARY record is within `months`."""
    today = pd.Timestamp(today or date.today())
    cutoff = today - pd.DateOffset(months=months)
    gens = dud[dud["DISPATCHTYPE"].str.upper() == "GENERATOR"]
    first = (
        gens.sort_values("START_DATE")
        .groupby("DUID", as_index=False)
        .agg(FIRST_START=("START_DATE", "min"),
             REGIONID=("REGIONID", "last"),
             STATIONID=("STATIONID", "last"))
    )
    return first[first["FIRST_START"] >= cutoff].reset_index(drop=True)


def missing_from_registration(dud: pd.DataFrame, registered_duids: set[str],
                              today: date | None = None,
                              months: int = RECENT_MONTHS) -> pd.DataFrame:
    """Recently registered GENERATOR DUIDs absent from the Registration List."""
    recent = recent_generators(dud, today=today, months=months)
    registered = {str(d).strip().upper() for d in registered_duids}
    missing = recent[~recent["DUID"].str.strip().str.upper().isin(registered)]
    return missing.reset_index(drop=True)
