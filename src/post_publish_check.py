"""Post-publish check: turn the lane red when a newer ELI edition is confirmed.

deploy/run-update.sh runs this last, after anything publishable has been pushed (and on
its no-change exit paths), so a new ELI edition makes the lane fail without holding back
the MLF, actual-curtailment and generator-list updates (Zalen's decision 6(b)). The
output validator prints the same message as a warning.

    python -m src.post_publish_check [--cache-dir data]
"""

import argparse
import json
import sys
from pathlib import Path

from src import config

PROJECT_ROOT = Path(__file__).resolve().parent.parent
STATUS_FILE = "source_status.json"


def newer_edition_message(eli: dict) -> str | None:
    """What to do when the probe confirmed the next ELI edition, or None when it did not."""
    if not eli or eli.get("newer_edition_available") is not True:
        return None
    edition = eli.get("edition")
    new = (edition or 0) + 1
    return (f"ELI {new} has been published ({eli.get('probed_url')}) but the dashboard still uses "
            f"ELI {edition}. To wire it in: (1) in src/config.py add {new} to ELI_CHART_DATA_URLS "
            f"and ELI_REGIONAL_APPENDIX_URLS (the newest key is the edition used), with the file "
            f"names on AEMO's ELI page {config.ELI_PAGE_URL}; (2) rerun python -m src.eli_appendix "
            f"on the new appendix PDFs; (3) regenerate with python -m src.main --full-refresh")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--cache-dir", default=str(PROJECT_ROOT / config.DATA_DIR),
                        help="Directory holding source_status.json (default: data/)")
    args = parser.parse_args(argv)
    path = Path(args.cache_dir) / STATUS_FILE
    if not path.exists():
        # The output validator fails on a missing status file; nothing to add here
        print(f"post-publish check: {path} missing, ELI edition not checked")
        return 0
    eli = json.loads(path.read_text(encoding="utf-8")).get("eli", {})
    msg = newer_edition_message(eli)
    if msg:
        print(f"FAIL: {msg}", file=sys.stderr)
        return 1
    print(f"post-publish check: no ELI edition newer than {eli.get('edition')} confirmed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
