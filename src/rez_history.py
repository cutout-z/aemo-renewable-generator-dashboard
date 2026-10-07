"""Append-only history of every REZ forecast edition the dashboard has held.

data/rez_forecast_history.csv keeps each edition's REZ figures in long format, one row per
zone, scenario, year and measure, so movements between editions can be analysed. The page
never reads it. Each edition keeps its own REZ ids and names; `crosswalk_key` is the REZ name
the page joins on (the ELI appendices' name) and `crosswalk_match` how the zone maps onto it
(data/isp_rez_crosswalk.csv), so rows from different editions can be lined up.

An edition is (source, eli_edition, isp_edition). Appending never rewrites earlier rows: the
file is opened for append, an edition already present with the same rows is skipped, and one
already present with different rows is refused (a corrected re-issue needs its own source label).

    python -m src.rez_history eli         # after `python -m src.eli_appendix` builds a new ELI edition

The ISP A3 rows are appended by `python -m src.isp_rez_appendix`.
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from . import config, eli_appendix

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent

COLUMNS = ["source", "eli_edition", "isp_edition", "rez_id", "rez_name", "state", "scenario", "fy",
           "measure", "value", "crosswalk_key", "crosswalk_match", "retrieved_at"]
EDITION_KEY = ["source", "eli_edition", "isp_edition"]
# measure names: each source's own terms (A3's glossary defines transmission/network curtailment and
# economic spill as the ELI appendices' curtailment and economic offloading; see the README)
ELI_MEASURES = {"CURTAILMENT": "curtailment", "OFFLOADING": "offloading"}
A3_MEASURES = {"TRANSMISSION": "transmission_curtailment", "SPILL": "economic_spill"}


class HistoryConflict(ValueError):
    """An edition already in the history file with different figures."""


def _fy(label: str) -> str:
    """'25-26' → '2025-26'; '2029-30' stays."""
    text = str(label)
    return f"20{text}" if len(text) == 5 and text[2] == "-" else text


def _value(v) -> str:
    return "" if v is None or pd.isna(v) else repr(round(float(v), 6))


def eli_source(eli_edition) -> str:
    return f"ELI {eli_edition} regional appendices"


def rows_from_eli(forecasts: pd.DataFrame, crosswalk: pd.DataFrame, retrieved_at: str) -> pd.DataFrame:
    """History rows for the ELI appendix forecasts (data/rez_forecasts.feather)."""
    ed = eli_appendix.editions(forecasts)
    if ed["eli_edition"] is None or ed["isp_edition"] is None:
        raise ValueError("rez_forecasts carries no ELI_EDITION / ISP_EDITION; rerun python -m src.eli_appendix")
    cw = crosswalk[crosswalk["eli_edition"] == str(ed["eli_edition"])]
    ids = dict(zip(cw["eli_rez_name"].str.lower(), cw["eli_rez_id"]))
    rows = []
    for r in forecasts.itertuples(index=False):
        r = r._asdict()
        for prefix, measure in ELI_MEASURES.items():
            for i in (1, 2, 3):
                rows.append({
                    "source": eli_source(ed["eli_edition"]), "eli_edition": str(ed["eli_edition"]),
                    "isp_edition": ed["isp_edition"], "rez_id": ids.get(r["REZ_NAME"].lower(), ""),
                    "rez_name": r["REZ_NAME"], "state": r["STATE"],
                    "scenario": config.ISP_FORECAST_SCENARIO, "fy": _fy(r[f"{prefix}_FY{i}_LABEL"]),
                    "measure": measure, "value": _value(r[f"{prefix}_FY{i}"]),
                    "crosswalk_key": r["REZ_NAME"], "crosswalk_match": "exact",
                    "retrieved_at": retrieved_at,
                })
    return pd.DataFrame(rows, columns=COLUMNS)


def rows_from_isp_a3(table: pd.DataFrame, crosswalk: pd.DataFrame, source: str,
                     retrieved_at: str) -> pd.DataFrame:
    """History rows for an ISP A3 table (src.isp_rez_appendix); zones with no table add none."""
    eds = table["ISP_EDITION"].dropna().unique()
    if len(eds) != 1:
        raise ValueError(f"the A3 table carries {len(eds)} ISP editions")
    isp_ed = str(eds[0])
    cw = crosswalk[crosswalk["isp_edition"] == isp_ed]
    rows = []
    for r in table[table["HAS_TABLE"]].itertuples(index=False):
        links = cw[(cw["isp_rez_id"] == r.REZ_ID) & (cw["isp_rez_name"] == r.REZ_NAME)]
        key = "; ".join(n for n in links["eli_rez_name"] if n)
        match = "; ".join(sorted(set(links["match"])))
        for prefix, measure in A3_MEASURES.items():
            for i in (1, 2, 3):
                rows.append({
                    "source": f"{isp_ed} {source}", "eli_edition": "", "isp_edition": isp_ed,
                    "rez_id": r.REZ_ID, "rez_name": r.REZ_NAME, "state": r.STATE,
                    "scenario": r.SCENARIO, "fy": _fy(getattr(r, f"Y{i}_LABEL")),
                    "measure": measure, "value": _value(getattr(r, f"{prefix}_Y{i}")),
                    "crosswalk_key": key, "crosswalk_match": match, "retrieved_at": retrieved_at,
                })
    return pd.DataFrame(rows, columns=COLUMNS)


def load(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        return pd.DataFrame(columns=COLUMNS)
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def editions(history: pd.DataFrame) -> set[tuple[str, str, str]]:
    """{(source, eli_edition, isp_edition)} held in a history table."""
    return {tuple(k) for k in history[EDITION_KEY].drop_duplicates().itertuples(index=False)}


def _as_written(rows: pd.DataFrame) -> pd.DataFrame:
    """The rows as they read back from the CSV (all text), for comparing with what is on file."""
    return pd.read_csv(io.StringIO(rows.to_csv(index=False)), dtype=str, keep_default_na=False)


def _sorted_records(df: pd.DataFrame) -> list[tuple]:
    cols = [c for c in COLUMNS if c != "retrieved_at"]
    return sorted(df[cols].itertuples(index=False, name=None))


def append(path: str | Path, rows: pd.DataFrame) -> int:
    """Append each edition in `rows` that the file does not hold yet; return rows added.

    Earlier rows are never rewritten (the file is opened for append). An edition already on
    file with identical figures is skipped; with different figures it raises HistoryConflict.
    """
    path = Path(path)
    rows = _as_written(rows.reindex(columns=COLUMNS))
    existing = load(path)
    if list(existing.columns) != COLUMNS:
        raise HistoryConflict(f"{path} has columns {list(existing.columns)}, expected {COLUMNS}")
    new = []
    for key, group in rows.groupby(EDITION_KEY, sort=False):
        held = existing[(existing[EDITION_KEY] == pd.Series(key, index=EDITION_KEY)).all(axis=1)]
        if held.empty:
            new.append(group)
        elif _sorted_records(held) == _sorted_records(group):
            logger.info(f"History already holds {key}; nothing appended for it")
        else:
            raise HistoryConflict(f"{path} already holds {key} with different figures; the history "
                                  "is append-only (give a corrected re-issue its own source label)")
    if not new:
        return 0
    out = pd.concat(new, ignore_index=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists() or path.stat().st_size == 0
    if not write_header and not path.read_bytes().endswith(b"\n"):
        raise HistoryConflict(f"{path} does not end in a newline; refusing to append to it")
    with path.open("a", encoding="utf-8", newline="") as fh:
        out.to_csv(fh, index=False, header=write_header, lineterminator="\n")
    return len(out)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("what", choices=["eli"],
                        help="eli: append data/rez_forecasts.feather's edition")
    parser.add_argument("--cache-dir", default=str(PROJECT_ROOT / config.DATA_DIR))
    parser.add_argument("--retrieved-at", default=datetime.now(timezone.utc).date().isoformat())
    args = parser.parse_args(argv)
    cache = Path(args.cache_dir)
    from .isp_rez_appendix import load_crosswalk
    forecasts = pd.read_feather(cache / Path(config.REZ_FORECAST_CACHE).name)
    rows = rows_from_eli(forecasts, load_crosswalk(cache), args.retrieved_at)
    try:
        added = append(cache / config.REZ_HISTORY_FILE, rows)
    except HistoryConflict as e:
        logger.error(str(e))
        return 1
    logger.info(f"{added} row(s) appended to {config.REZ_HISTORY_FILE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
