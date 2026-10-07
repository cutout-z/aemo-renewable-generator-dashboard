"""CLI orchestrator for AEMO Solar & Wind Curtailment Dashboard."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

from . import config, source_status
from .download_generators import fetch_generators
from .download_mlf import fetch_mlf_data
from .download_eli import fetch_eli_curtailment
from .download_rez import fetch_rez_forecasts
from .fetch_curtailment import fetch_curtailment_by_fy
from .merge import build_summary
from .excel_output import generate_all_workbooks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def run(full_refresh: bool = False, cache_dir: str | None = None,
        output_dir: str | None = None):
    """Main execution flow.

    cache_dir / output_dir default to the repo's data/ and outputs/; pass other
    directories to run the pipeline without touching the committed files.
    """
    cache_root = Path(cache_dir) if cache_dir else PROJECT_ROOT / config.DATA_DIR
    output_root = Path(output_dir) if output_dir else PROJECT_ROOT / config.OUTPUT_DIR
    cache_dir = str(cache_root)
    output_dir = str(output_root)
    summary_path = output_root / Path(config.SUMMARY_CSV).name

    # ── Step 1: Generator listing (the spine) ────────────────────────────
    # Rebuilt every run: the Registration List is re-downloaded each time so
    # new units appear (a failed download falls back to the last good copy).
    gen_cache = cache_root / Path(config.GENERATOR_CACHE).name
    try:
        generators = fetch_generators(cache_dir)
        gen_cache.parent.mkdir(parents=True, exist_ok=True)
        generators.reset_index(drop=True).to_feather(gen_cache)
        logger.info(f"Cached generator listing ({len(generators)} farms)")
    except Exception as e:
        if not gen_cache.exists():
            raise
        logger.error(f"Generator listing rebuild failed ({e}); using cached listing")
        generators = pd.read_feather(gen_cache)

    all_duids = set(generators["DUID"].unique())
    logger.info(f"Tracking {len(all_duids)} solar/wind generators")

    # ── Step 2: MLF data from aemo-mlf-tracker ───────────────────────────
    mlf_cache = cache_root / Path(config.MLF_CACHE).name
    if not full_refresh and mlf_cache.exists():
        logger.info("Loading cached MLF data...")
        mlf_data = pd.read_feather(mlf_cache)
    else:
        mlf_data = fetch_mlf_data(cache_dir, generator_duids=all_duids)
        if not mlf_data.empty:
            mlf_cache.parent.mkdir(parents=True, exist_ok=True)
            mlf_data.reset_index(drop=True).to_feather(mlf_cache)

    # ── Step 3: ELI projected curtailment ────────────────────────────────
    eli_cache = cache_root / Path(config.ELI_CURTAILMENT_CACHE).name
    if not full_refresh and eli_cache.exists():
        logger.info("Loading cached ELI curtailment data...")
        eli_data = pd.read_feather(eli_cache)
    else:
        try:
            eli_data = fetch_eli_curtailment(cache_dir)
            if not eli_data.empty:
                eli_cache.parent.mkdir(parents=True, exist_ok=True)
                eli_data.reset_index(drop=True).to_feather(eli_cache)
        except Exception as e:
            logger.warning(f"ELI curtailment download failed: {e}")
            if eli_cache.exists():
                logger.warning("Using cached ELI curtailment data after refresh failure")
                eli_data = pd.read_feather(eli_cache)
            else:
                eli_data = pd.DataFrame()

    # ── Step 4: REZ forecasts ────────────────────────────────────────────
    # Extracted from the ELI regional appendices by `python -m src.eli_appendix`
    rez_data = fetch_rez_forecasts(cache_dir)

    # ── Step 5: Actual curtailment from credit dashboard ────────────────
    curt_cache = cache_root / Path(config.CURTAILMENT_CACHE).name
    if not full_refresh and curt_cache.exists():
        logger.info("Loading cached actual curtailment data...")
        actual_curtailment = pd.read_feather(curt_cache)
    else:
        actual_curtailment = refresh_actual_curtailment(all_duids, cache_root, curt_cache)

    # ── Step 6: Build merged summary ─────────────────────────────────────
    summary = build_summary(
        generators=generators,
        mlf_data=mlf_data,
        eli_curtailment=eli_data,
        rez_forecasts=rez_data,
        actual_curtailment=actual_curtailment,
        cache_dir=cache_dir,
    )

    # ── Step 7: Save outputs ─────────────────────────────────────────────
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(summary_path, index=False)
    logger.info(f"Saved summary.csv ({len(summary)} rows × {len(summary.columns)} columns)")

    # ── Step 8: Generate Excel workbooks ─────────────────────────────────
    generate_all_workbooks(summary, output_dir)

    # ── Step 9: Publish source editions and fetch dates for the page footer ──
    # (a trimmed copy of data/source_status.json; deploy/run-update.sh commits it with
    # outputs/, so it only moves when summary.csv changes)
    status_path = source_status.publish(cache_root, output_root)
    logger.info(f"Published {status_path.name}")

    logger.info("Done.")


ACTUAL_STATUS_KEY = "actual_curtailment"


def refresh_actual_curtailment(all_duids: set[str], cache_root: Path,
                               curt_cache: Path) -> pd.DataFrame:
    """Fetch the upstream FY rollup; on failure fall back to the cached copy.

    Either way the outcome goes into source_status.json, so a run that
    republishes cached actuals says so and tests/validate_outputs.py can warn
    (fresh fallback) or fail (stale or no actuals at all).
    """
    previous = source_status.load(cache_root).get(ACTUAL_STATUS_KEY, {})
    record = {"attempted_at": source_status.now_iso(), "refreshed": False, "error": None,
              "fetched_at": previous.get("fetched_at"), "fys": previous.get("fys", []),
              "upstream_last_month": previous.get("upstream_last_month"),
              "used_cache": False}
    try:
        actual = fetch_curtailment_by_fy(all_duids)
        value_cols = [c for c in actual.columns if c.startswith("CURTAILMENT_ACTUAL_")]
        if not value_cols or actual[value_cols].isna().all().all():
            raise ValueError("upstream rollup has no complete-FY value for any tracked unit")
        curt_cache.parent.mkdir(parents=True, exist_ok=True)
        actual.reset_index(drop=True).to_feather(curt_cache)
        record.update(refreshed=True, fetched_at=record["attempted_at"],
                      fys=[c.replace("CURTAILMENT_ACTUAL_", "") for c in value_cols],
                      upstream_last_month=actual.attrs.get("upstream_last_month"))
    except Exception as e:
        record["error"] = str(e)
        if curt_cache.exists():
            actual = pd.read_feather(curt_cache)
            record["used_cache"] = True
            logger.error(f"ACTUAL CURTAILMENT REFRESH FAILED: {e}; republishing the cached "
                         f"copy (last good fetch {record['fetched_at'] or 'unknown'})")
        else:
            actual = pd.DataFrame()
            logger.error(f"ACTUAL CURTAILMENT REFRESH FAILED: {e}; no cached copy, the "
                         "actual-curtailment columns are left out")
    source_status.update(cache_root, ACTUAL_STATUS_KEY, record)
    return actual


def main():
    parser = argparse.ArgumentParser(
        description="AEMO Solar & Wind Curtailment Dashboard"
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Re-fetch all data from sources (default: use cached if available)",
    )
    parser.add_argument(
        "--cache-dir",
        help="Directory for cached inputs (default: data/)",
    )
    parser.add_argument(
        "--output-dir",
        help="Directory for summary.csv and the workbooks (default: outputs/)",
    )
    args = parser.parse_args()
    run(full_refresh=args.full_refresh, cache_dir=args.cache_dir,
        output_dir=args.output_dir)


if __name__ == "__main__":
    main()
