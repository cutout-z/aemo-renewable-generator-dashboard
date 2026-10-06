"""REZ membership and REZ forecasts from AEMO's ELI regional appendices.

AEMO's Enhanced Locational Information (ELI) report has one appendix per region
(A3 NSW, A4 QLD, A5 SA, A6 TAS, A7 VIC), published as PDF only. Each has one
section per REZ plus a "Non-REZ" section. A section lists the existing
generators in it by DUID (curtailment and hosting-capacity tables) and gives
the ISP forecast of curtailment and economic offloading for three years.

That makes the appendices the one AEMO source that places existing units,
wind farms included, in or outside a REZ by DUID, on the same REZ boundaries
as the forecasts the dashboard shows.

The PDFs change once a year, so they are not read on every run. Rebuild the
reference files when a new ELI edition comes out:

    python -m src.eli_appendix            # downloads the PDFs (needs pdftotext)
    python -m src.eli_appendix a3.txt ... # or parse text already extracted

This writes data/rez_membership.feather and data/rez_forecasts.feather, which
the pipeline reads (src/download_generators.py, src/main.py).
"""

from __future__ import annotations

import argparse
import logging
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd
import requests

from . import config

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent

MEMBERSHIP_FILE = "rez_membership.feather"
NON_REZ = "Non-REZ"

# "A3.6 N2 – New England", "A1.14 Non-REZ" (the VIC appendix numbers its
# body sections A1.x); contents-page lines end in a page number and are skipped
_SECTION = re.compile(r"^A\d+\.\d+\s+(?:([A-Z]{1,2}\d+)\s+[–-]\s+(.+?)|(Non-REZ))\s*$")
# A generator table row: DUID, two or more spaces, the generator name
_ROW = re.compile(r"^\s{0,12}([A-Z][A-Z0-9_\-]{2,11})\s{2,}(\S.*?)(?:\s{2,}|$)")
_NAME_HINT = re.compile(r"farm|park|plant|station|project|hub|power|energy|hybrid|solar|wind|"
                        r"battery|bess|range|ridge|hill|creek|rocks", re.IGNORECASE)
_SCENARIO = re.compile(r"^\s*Step Change\s+(.*)$")
_FY = re.compile(r"(20\d{2})-(20\d{2})")


def parse_appendix_text(text: str, state: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse one appendix's `pdftotext -layout` text.

    Returns (membership, forecasts):
      membership: STATE, REZ_ID, REZ_NAME, DUID, GENERATOR  (REZ_ID "" for Non-REZ)
      forecasts:  STATE, REZ_NAME, CURTAILMENT_FY1..3 (+ _LABEL), CURTAILMENT_AVG,
                  OFFLOADING_FY1..3 (+ _LABEL), OFFLOADING_AVG  (fractions, 0.10 = 10%)
    """
    members, forecasts = [], {}
    section = None
    fy_labels: list[str] = []
    wrapped = False  # "Step" and "Change" on two lines, the values on the line between
    for line in text.splitlines():
        m = _SECTION.match(line.rstrip())
        if m:
            section = ("", NON_REZ) if m.group(3) else (m.group(1), m.group(2).strip())
            fy_labels, wrapped = [], False
            continue
        if section is None:
            continue
        years = _FY.findall(line)
        if len(years) >= 3:
            fy_labels = [f"{a[2:]}-{b[2:]}" for a, b in years[:3]]
            continue
        s = _SCENARIO.match(line)
        if line.strip() == "Step" and fy_labels:
            wrapped = True
            continue
        if (s or wrapped) and line.strip() and fy_labels and section[1] not in forecasts:
            values = _cells(s.group(1) if s else line)
            wrapped = False
            if values is not None and len(values) == 6:
                forecasts[section[1]] = _forecast_row(state, section[1], fy_labels, values)
            continue
        r = _ROW.match(line)
        if r and _is_duid(r.group(1), r.group(2)):
            members.append({"STATE": state, "REZ_ID": section[0], "REZ_NAME": section[1],
                            "DUID": r.group(1), "GENERATOR": r.group(2).strip()})

    membership = pd.DataFrame(members, columns=["STATE", "REZ_ID", "REZ_NAME", "DUID", "GENERATOR"])
    membership = resolve_duplicates(membership.drop_duplicates(subset=["DUID", "REZ_NAME"]))
    return membership, pd.DataFrame(list(forecasts.values()))


def _is_duid(token: str, name: str) -> bool:
    """DUIDs carry a digit (AVLSF1) or are an all-caps code beside a plant name (CATHROCK)."""
    if token.lower() in ("step", "scenario", "duid", "constraint"):
        return False
    if any(c.isdigit() for c in token):
        return bool(re.search(r"[A-Za-z]{3}", name)) and not name.lower().startswith(("hours", "value"))
    return token.isupper() and len(token) >= 4 and bool(_NAME_HINT.search(name))


def _cells(text: str) -> list[float | None] | None:
    """'10  16  15  26' → fractions; AEMO's "-" (no VRE projected there) → None, not 0."""
    out = []
    for token in text.split():
        if token in ("-", "–"):
            out.append(None)
        else:
            try:
                out.append(float(token.rstrip("%")) / 100)
            except ValueError:
                return None
    return out


def _mean(values: list[float | None]) -> float | None:
    known = [v for v in values if v is not None]
    return sum(known) / len(known) if known else None


def _forecast_row(state: str, rez: str, labels: list[str], values: list[float | None]) -> dict:
    curtail, offload = values[0::2], values[1::2]
    row = {"STATE": state, "REZ_NAME": rez}
    for i in range(3):
        row[f"CURTAILMENT_FY{i + 1}"] = curtail[i]
        row[f"CURTAILMENT_FY{i + 1}_LABEL"] = labels[i]
    row["CURTAILMENT_AVG"] = _mean(curtail)
    for i in range(3):
        row[f"OFFLOADING_FY{i + 1}"] = offload[i]
        row[f"OFFLOADING_FY{i + 1}_LABEL"] = labels[i]
    row["OFFLOADING_AVG"] = _mean(offload)
    return row


def resolve_duplicates(membership: pd.DataFrame) -> pd.DataFrame:
    """One row per DUID. A unit listed under a REZ and under Non-REZ keeps the REZ:
    the REZ section carries its curtailment record; the Non-REZ listing is the
    hosting-capacity table's reference set (NEWENSF2, BRYB2WF2, DUNDWF2/3 in 2025)."""
    if membership.empty:
        return membership
    ranked = membership.assign(_non=membership["REZ_NAME"].eq(NON_REZ))
    dupes = ranked[ranked["DUID"].duplicated(keep=False)]
    for duid, g in dupes.groupby("DUID"):
        logger.info(f"{duid} listed under {', '.join(g['REZ_NAME'])}; using "
                    f"{g.sort_values('_non').iloc[0]['REZ_NAME']}")
    return (ranked.sort_values(["DUID", "_non"]).drop_duplicates(subset="DUID", keep="first")
            .drop(columns="_non").sort_values(["STATE", "REZ_ID", "DUID"]).reset_index(drop=True))


def load_membership(cache_dir: str | Path) -> pd.DataFrame | None:
    path = Path(cache_dir) / MEMBERSHIP_FILE
    if not path.exists():
        logger.warning(f"No REZ membership file ({path}); REZ comes from the seed only")
        return None
    return pd.read_feather(path)


def _pdf_to_text(pdf: Path) -> str:
    exe = shutil.which("pdftotext")
    if exe is None:
        raise RuntimeError("pdftotext (poppler) is needed to read the ELI appendix PDFs")
    return subprocess.run([exe, "-layout", str(pdf), "-"], check=True,
                          capture_output=True, text=True).stdout


def build(texts: dict[str, str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse {state: text} for all five appendices into one membership and forecast table."""
    parts = [parse_appendix_text(t, s) for s, t in texts.items()]
    membership = resolve_duplicates(pd.concat([p[0] for p in parts], ignore_index=True))
    forecasts = pd.concat([p[1] for p in parts], ignore_index=True)
    return membership, forecasts


def _download_texts(year: int) -> dict[str, str]:
    urls = config.ELI_REGIONAL_APPENDIX_URLS[year]
    texts = {}
    with tempfile.TemporaryDirectory() as tmp:
        for state, url in urls.items():
            resp = requests.get(url, timeout=config.REQUEST_TIMEOUT,
                                headers={"User-Agent": config.USER_AGENT})
            resp.raise_for_status()
            if not resp.content.startswith(b"%PDF"):
                raise RuntimeError(f"{url} did not return a PDF")
            pdf = Path(tmp) / f"{state}.pdf"
            pdf.write_bytes(resp.content)
            texts[state] = _pdf_to_text(pdf)
    return texts


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("texts", nargs="*",
                        help="pdftotext -layout output, one per appendix, named a3-*/a4-*/... "
                             "(default: download the PDFs)")
    parser.add_argument("--year", type=int, default=max(config.ELI_REGIONAL_APPENDIX_URLS))
    parser.add_argument("--cache-dir", default=str(PROJECT_ROOT / config.DATA_DIR))
    args = parser.parse_args(argv)

    if args.texts:
        by_prefix = {"a3": "NSW", "a4": "QLD", "a5": "SA", "a6": "TAS", "a7": "VIC"}
        texts = {by_prefix[Path(p).name[:2].lower()]: Path(p).read_text() for p in args.texts}
    else:
        texts = _download_texts(args.year)
    missing = set(config.ELI_REGIONAL_APPENDIX_URLS[args.year]) - set(texts)
    if missing:
        logger.error(f"Missing appendices for {sorted(missing)}")
        return 1

    membership, forecasts = build(texts)
    cache = Path(args.cache_dir)
    membership.to_feather(cache / MEMBERSHIP_FILE)
    forecasts.to_feather(cache / Path(config.REZ_FORECAST_CACHE).name)
    logger.info(f"Wrote {len(membership)} DUIDs in {membership['REZ_NAME'].nunique()} sections "
                f"and {len(forecasts)} REZ forecasts (ELI {args.year})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
