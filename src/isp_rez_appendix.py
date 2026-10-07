"""REZ VRE curtailment forecasts read directly from the ISP's REZ appendix (Appendix A3).

AEMO's 2026 ISP (25 June 2026), Appendix A3 "Renewable Energy Zones", has one section
per REZ (plus three NSW distribution-project zones) with a "VRE curtailment" table:

                         2029-30                    2039-40                    2049-50
    Scenario     Transmission  Economic    Transmission  Economic    Transmission  Economic
                 curtailment   spill       curtailment   spill       curtailment   spill
    Slower Growth      0%        11%           0%          10%           6%          34%
    Step Change        6%        18%           1%          36%           5%          52%
    Accelerated Transition ...

All three scenarios are kept. A "-" (no VRE projected in the zone that year) is stored
as empty, never 0, as src/eli_appendix.py does for the ELI tables.

The PDF changes once per ISP (every two years), so it is read by hand, not on every run:

    python -m src.isp_rez_appendix                 # downloads config.ISP_A3_URLS' newest edition
    python -m src.isp_rez_appendix a3.txt          # or parse pdftotext -layout output (or the PDF)

This writes data/isp_rez_curtailment.feather (one row per zone and scenario, with
ISP_EDITION) and appends the edition to data/rez_forecast_history.csv. It does NOT touch
the ELI appendix files (rez_forecasts.feather, rez_membership.feather).

The A3 zones do not all match the REZ names the page joins on (the ELI appendices' 2024 ISP
zones): data/isp_rez_crosswalk.csv maps them, and only rows marked exact or renamed are
joined (src/merge.py). A section that has no table, or a table that does not parse, fails.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from . import config, eli_appendix, rez_history

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SCENARIOS = ("Slower Growth", "Step Change", "Accelerated Transition")
MEASURES = ("TRANSMISSION", "SPILL")          # transmission curtailment, economic spill
VALUE_COLS = [f"{m}_Y{i}" for m in MEASURES for i in (1, 2, 3)]
LABEL_COLS = [f"Y{i}_LABEL" for i in (1, 2, 3)]
COLUMNS = (["STATE", "REZ_ID", "REZ_NAME", "SCENARIO", "HAS_TABLE"] + VALUE_COLS + LABEL_COLS
           + ["ISP_EDITION"])

# Crosswalk match types; only these two carry A3 values onto a page REZ
JOINED_MATCHES = ("exact", "renamed")
MATCH_TYPES = JOINED_MATCHES + ("no one-to-one match",)
CROSSWALK_COLUMNS = ["isp_edition", "isp_rez_id", "isp_rez_name", "eli_edition", "eli_rez_id",
                     "eli_rez_name", "state", "match", "note"]

_STATE_BY_PREFIX = {"N": "NSW", "Q": "QLD", "S": "SA", "T": "TAS", "V": "VIC"}
# "A3.3.2 New South Wales": the regional section a distribution zone (no REZ id) sits in
_REGION = re.compile(r"^A3\.\d+\.\d+\s+(New South Wales|Queensland|South Australia|Tasmania|Victoria)\s*$")
_REGION_STATE = {"New South Wales": "NSW", "Queensland": "QLD", "South Australia": "SA",
                 "Tasmania": "TAS", "Victoria": "VIC"}
# "N2 – New England", "V3,V4 – Western Victoria REZ"
_REZ_HEAD = re.compile(r"^([NQSTV]\d+(?:,[NQSTV]\d+)*)\s+[–-]\s+(\S.*?)\s*$")
_YEARS = re.compile(r"\b(20\d{2})-(\d{2})\b")
_CELL = re.compile(r"^(?:\d+(?:\.\d+)?%|[-–])$")


class ParseError(ValueError):
    """A REZ section's VRE curtailment table could not be read."""


def _sections(lines: list[str]):
    """Yield (state, rez_id, rez_name, start, end) per zone: a heading line followed by 'Summary'."""
    heads, state = [], None
    for i, line in enumerate(lines):
        r = _REGION.match(line.strip())
        if r:
            state = _REGION_STATE[r.group(1)]
        if line.strip() != "Summary":
            continue
        j = i - 1
        while j >= 0 and not lines[j].strip():
            j -= 1
        head = lines[j].strip()
        m = _REZ_HEAD.match(head)
        if m:
            rez_id, name = m.group(1), re.sub(r"\s+REZ$", "", m.group(2))
            zone_state = _STATE_BY_PREFIX[rez_id[0]]
        elif head.lower().endswith(" distribution") and state:
            rez_id, name, zone_state = "", head, state
        else:
            raise ParseError(f"line {j + 1}: a 'Summary' under an unrecognised heading {head!r}")
        heads.append((zone_state, rez_id, name, j))
    for k, (zone_state, rez_id, name, start) in enumerate(heads):
        end = heads[k + 1][3] if k + 1 < len(heads) else len(lines)
        yield zone_state, rez_id, name, start, end


def _cells(tokens: list[str]) -> list[float | None]:
    return [None if t in ("-", "–") else float(t.rstrip("%")) / 100 for t in tokens]


def _scenario(label: str) -> str:
    """'Accelerated' (the label wraps over two lines) → 'Accelerated Transition'."""
    for s in SCENARIOS:
        if label and s.startswith(label):
            return s
    return label


def _parse_table(lines: list[str], where: str) -> tuple[list[str], dict[str, list]]:
    """The year labels and {scenario: [tc1, es1, tc2, es2, tc3, es3]} of one VRE curtailment table.

    Rows look like "Step Change   6%  18%  1%  36%  5%  52%"; A3's Hunter-Central Coast wraps
    "Accelerated" / values / "Transition" over three lines, and Gippsland Onshore has footnote
    lines after the table, which end it once all three scenarios are read.
    """
    labels, rows, pending = None, {}, ""
    for line in lines:
        text = line.strip()
        if "\u00a9" in text or len(rows) == len(SCENARIOS):   # the page footer, or done
            break
        if not text:
            continue
        if labels is None:
            years = _YEARS.findall(text)
            if len(years) == 3:
                labels = [f"{a}-{b}" for a, b in years]
            continue
        tokens = text.split()
        k = next((n for n, t in enumerate(tokens) if _CELL.match(t)), None)
        if k is None:
            if _scenario(text) in SCENARIOS:
                pending = text                 # a label whose values are on the next line
            continue
        label, values = _scenario(" ".join(tokens[:k]) or pending), tokens[k:]
        pending = ""
        if label not in SCENARIOS or len(values) != 6 or not all(_CELL.match(v) for v in values):
            raise ParseError(f"{where}: unreadable table row {text!r}")
        if label in rows:
            raise ParseError(f"{where}: scenario {label!r} appears twice")
        rows[label] = _cells(values)
    if labels is None:
        raise ParseError(f"{where}: no 'yyyy-yy' year header after 'VRE curtailment'")
    missing = [s for s in SCENARIOS if s not in rows]
    if missing:
        raise ParseError(f"{where}: no row for {', '.join(missing)}")
    return labels, rows


def parse_a3_text(text: str, edition: str) -> pd.DataFrame:
    """Every zone's VRE curtailment table, all scenarios (COLUMNS; values are fractions).

    A zone whose section has no "VRE curtailment" table at all is kept with HAS_TABLE
    False and empty values only when the section says why (A3's T4 North Tasmania Coast:
    "no VRE curtailment ... occurs in this REZ"); any other gap raises ParseError.
    """
    lines = text.split("\n")      # not splitlines(): pdftotext's form feeds are page breaks, not lines
    out, years_seen = [], set()
    sections = list(_sections(lines))
    if not sections:
        raise ParseError("no REZ sections found (is this the ISP's Appendix A3?)")
    for state, rez_id, name, start, end in sections:
        where = f"{rez_id or '-'} {name}"
        body = lines[start:end]
        at = [i for i, l in enumerate(body) if l.strip() == "VRE curtailment"]
        base = {"STATE": state, "REZ_ID": rez_id, "REZ_NAME": name, "ISP_EDITION": edition}
        if not at:
            prose = " ".join(" ".join(body).split())
            if re.search(r"no VRE curtailment(?: or [a-z ]+)? occurs in this REZ", prose):
                logger.info(f"{where}: no VRE curtailment table (A3: no VRE projected)")
                for scenario in SCENARIOS:
                    out.append({**base, "SCENARIO": scenario, "HAS_TABLE": False})
                continue
            raise ParseError(f"{where}: no 'VRE curtailment' table in its section")
        if len(at) > 1:
            raise ParseError(f"{where}: {len(at)} 'VRE curtailment' tables in one section")
        labels, rows = _parse_table(body[at[0] + 1:at[0] + 40], where)
        years_seen.add(tuple(labels))
        for scenario in SCENARIOS:
            v = rows[scenario]
            row = {**base, "SCENARIO": scenario, "HAS_TABLE": True}
            for i in range(3):
                row[f"TRANSMISSION_Y{i + 1}"] = v[2 * i]
                row[f"SPILL_Y{i + 1}"] = v[2 * i + 1]
                row[f"Y{i + 1}_LABEL"] = labels[i]
            out.append(row)
    if len(years_seen) != 1:
        raise ParseError(f"the tables use different years: {sorted(years_seen)}")
    labels = next(iter(years_seen))
    df = pd.DataFrame(out).reindex(columns=COLUMNS)
    for i, label in enumerate(labels, 1):          # sections with no table share the years
        df[f"Y{i}_LABEL"] = label
    df["HAS_TABLE"] = df["HAS_TABLE"].astype(bool)
    for col in VALUE_COLS:
        df[col] = pd.to_numeric(df[col]).astype(float)
    return df


def edition(df: pd.DataFrame | None) -> str | None:
    """The one ISP_EDITION a table carries, or None."""
    if df is None or df.empty or "ISP_EDITION" not in df.columns:
        return None
    values = df["ISP_EDITION"].dropna().unique()
    return str(values[0]) if len(values) == 1 else None


# ── Crosswalk ────────────────────────────────────────────────────────────

def load_crosswalk(cache_dir: str | Path) -> pd.DataFrame:
    path = Path(cache_dir) / config.ISP_REZ_CROSSWALK_FILE
    if not path.exists():
        return pd.DataFrame(columns=CROSSWALK_COLUMNS)
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def page_mapping(crosswalk: pd.DataFrame, isp_edition: str, eli_edition) -> dict[str, tuple[str, str, str]]:
    """{page REZ_NAME (lower-case): (isp_rez_id, isp_rez_name, match)} for one pair of editions.

    A page REZ is joined only when it has exactly one crosswalk row and that row is exact or
    renamed; anything else (split, merged, moved, no counterpart) maps to ("", "", match) so its
    2026 columns stay blank rather than borrowing a neighbour's figures.
    """
    cw = crosswalk[(crosswalk["isp_edition"] == str(isp_edition))
                   & (crosswalk["eli_edition"] == str(eli_edition))
                   & (crosswalk["eli_rez_name"] != "")]
    out = {}
    for name, g in cw.groupby(cw["eli_rez_name"].str.strip().str.lower()):
        row = g.iloc[0]
        if len(g) == 1 and row["match"] in JOINED_MATCHES:
            out[name] = (row["isp_rez_id"], row["isp_rez_name"], row["match"])
        else:
            out[name] = ("", "", "no one-to-one match")
    return out


def check_crosswalk(crosswalk: pd.DataFrame, isp: pd.DataFrame, eli_forecasts: pd.DataFrame) -> list[str]:
    """Problems that would publish wrong or silently missing figures (empty list = fine).

    Every A3 zone and every ELI-appendix REZ must appear in the crosswalk for their editions;
    match types must be known; an A3 zone may feed at most one page REZ; and every page REZ
    the crosswalk joins must have A3 values or be a zone A3 gives no table for.
    """
    problems = []
    ed, eli_ed = edition(isp), (eli_appendix.editions(eli_forecasts)["eli_edition"]
                                if eli_forecasts is not None else None)
    if ed is None:
        return ["the ISP A3 table carries no single ISP_EDITION"]
    cw = crosswalk[crosswalk["isp_edition"] == ed]
    if cw.empty:
        return [f"{config.ISP_REZ_CROSSWALK_FILE} has no rows for the {ed}"]
    bad = sorted(set(cw["match"]) - set(MATCH_TYPES))
    if bad:
        problems.append(f"crosswalk match types not in {MATCH_TYPES}: {bad}")
    zones = {(r.REZ_ID, r.REZ_NAME) for r in isp.itertuples()}
    listed = {(r.isp_rez_id, r.isp_rez_name) for r in cw.itertuples() if r.isp_rez_name}
    for rez_id, name in sorted(zones - listed):
        problems.append(f"{ed} zone {rez_id or '-'} {name} is not in the crosswalk")
    for rez_id, name in sorted(listed - zones):
        problems.append(f"crosswalk names {ed} zone {rez_id or '-'} {name}, which A3 does not have")
    joined = cw[cw["match"].isin(JOINED_MATCHES)]
    twice = joined[joined.duplicated(["isp_rez_id", "isp_rez_name"], keep=False)]
    for name in sorted(set(twice["isp_rez_name"])):
        problems.append(f"{ed} zone {name} is joined to more than one page REZ")
    if eli_forecasts is not None and not eli_forecasts.empty and eli_ed is not None:
        cw_eli = cw[cw["eli_edition"] == str(eli_ed)]
        page = set(cw_eli["eli_rez_name"].str.strip().str.lower()) - {""}
        for name in sorted(set(eli_forecasts["REZ_NAME"].str.strip()) - {""}):
            if name.lower() not in page:
                problems.append(f"ELI {eli_ed} REZ {name} is not in the crosswalk for the {ed}")
        mapping = page_mapping(crosswalk, ed, eli_ed)
        for name, (rez_id, isp_name, match) in sorted(mapping.items()):
            if match in JOINED_MATCHES and (rez_id, isp_name) not in zones:
                problems.append(f"crosswalk joins {name} to {rez_id} {isp_name}, which has no A3 row")
    return problems


def not_in_a3(crosswalk: pd.DataFrame, isp: pd.DataFrame, eli_forecasts: pd.DataFrame) -> list[str]:
    """Page REZs (ELI appendix names) that get no A3 values, with the reason, for logs and the handback."""
    ed = edition(isp)
    eli_ed = eli_appendix.editions(eli_forecasts)["eli_edition"]
    mapping = page_mapping(crosswalk, ed, eli_ed)
    no_table = {(r.REZ_ID, r.REZ_NAME) for r in isp.itertuples() if not r.HAS_TABLE}
    out = []
    for name in sorted(set(eli_forecasts["REZ_NAME"].str.strip()) - {""}):
        rez_id, isp_name, match = mapping.get(name.lower(), ("", "", "not in the crosswalk"))
        if match not in JOINED_MATCHES:
            out.append(f"{name}: {match}")
        elif (rez_id, isp_name) in no_table:
            out.append(f"{name}: {rez_id} {isp_name} has no VRE curtailment table in A3")
    return out


# ── Build ────────────────────────────────────────────────────────────────

def _text_from(path: Path) -> str:
    if path.suffix.lower() == ".pdf":
        return eli_appendix._pdf_to_text(path)
    return path.read_text(encoding="utf-8")


def _download_text(url: str) -> str:
    resp = requests.get(url, timeout=config.REQUEST_TIMEOUT,
                        headers={"User-Agent": config.BROWSER_USER_AGENT})
    resp.raise_for_status()
    if not resp.content.startswith(b"%PDF"):
        raise RuntimeError(f"{url} did not return a PDF")
    with tempfile.TemporaryDirectory() as tmp:
        pdf = Path(tmp) / "a3.pdf"
        pdf.write_bytes(resp.content)
        return eli_appendix._pdf_to_text(pdf)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("source", nargs="?",
                        help="A3 PDF or its pdftotext -layout output (default: download it)")
    parser.add_argument("--edition", default=max(config.ISP_A3_URLS),
                        help='ISP edition, e.g. "2026 ISP" (default: the newest in config.ISP_A3_URLS)')
    parser.add_argument("--cache-dir", default=str(PROJECT_ROOT / config.DATA_DIR))
    parser.add_argument("--retrieved-at", default=datetime.now(timezone.utc).date().isoformat(),
                        help="Date the PDF was retrieved, for the history file (default: today)")
    args = parser.parse_args(argv)

    text = _text_from(Path(args.source)) if args.source else _download_text(config.ISP_A3_URLS[args.edition])
    try:
        table = parse_a3_text(text, args.edition)
    except ParseError as e:
        logger.error(f"ISP A3 not parsed, nothing written: {e}")
        return 1
    cache = Path(args.cache_dir)
    crosswalk = load_crosswalk(cache)
    eli_path = cache / Path(config.REZ_FORECAST_CACHE).name
    eli_forecasts = pd.read_feather(eli_path) if eli_path.exists() else None
    problems = check_crosswalk(crosswalk, table, eli_forecasts)
    if problems:
        for p in problems:
            logger.error(p)
        logger.error(f"Update {config.ISP_REZ_CROSSWALK_FILE} first; nothing written")
        return 1

    table.to_feather(cache / config.ISP_A3_FILE)
    rows = rez_history.rows_from_isp_a3(table, crosswalk, config.ISP_A3_SOURCE, args.retrieved_at)
    added = rez_history.append(cache / config.REZ_HISTORY_FILE, rows)
    zones = table.drop_duplicates(["REZ_ID", "REZ_NAME"])
    logger.info(f"Wrote {len(zones)} zones x {len(SCENARIOS)} scenarios ({args.edition}) to "
                f"{config.ISP_A3_FILE}; {added} history row(s) appended")
    if eli_forecasts is not None:
        for line in not_in_a3(crosswalk, table, eli_forecasts):
            logger.info(f"No {args.edition} values on the page for {line}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
