"""CLI orchestrator for AEMO Solar & Wind Curtailment Dashboard."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

from . import config
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
    gen_cache = cache_root / Path(config.GENERATOR_CACHE).name
    if not full_refresh and gen_cache.exists():
        logger.info("Loading cached generator listing...")
        generators = pd.read_feather(gen_cache)
    else:
        generators = fetch_generators(cache_dir)
        gen_cache.parent.mkdir(parents=True, exist_ok=True)
        generators.reset_index(drop=True).to_feather(gen_cache)
        logger.info(f"Cached generator listing ({len(generators)} farms)")

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
    rez_cache = cache_root / Path(config.REZ_FORECAST_CACHE).name
    if not full_refresh and rez_cache.exists():
        logger.info("Loading cached REZ forecast data...")
        rez_data = pd.read_feather(rez_cache)
    else:
        try:
            rez_data = fetch_rez_forecasts(cache_dir)
            if not rez_data.empty:
                rez_cache.parent.mkdir(parents=True, exist_ok=True)
                rez_data.reset_index(drop=True).to_feather(rez_cache)
        except Exception as e:
            logger.warning(f"REZ forecast download failed: {e}")
            if rez_cache.exists():
                logger.warning("Using cached REZ forecast data after refresh failure")
                rez_data = pd.read_feather(rez_cache)
            else:
                rez_data = pd.DataFrame()

    # ── Step 5: Actual curtailment from credit dashboard ────────────────
    curt_cache = cache_root / Path(config.CURTAILMENT_CACHE).name
    if not full_refresh and curt_cache.exists():
        logger.info("Loading cached actual curtailment data...")
        actual_curtailment = pd.read_feather(curt_cache)
    else:
        try:
            actual_curtailment = fetch_curtailment_by_fy(all_duids)
            if not actual_curtailment.empty:
                curt_cache.parent.mkdir(parents=True, exist_ok=True)
                actual_curtailment.reset_index(drop=True).to_feather(curt_cache)
        except Exception as e:
            logger.warning(f"Actual curtailment refresh failed: {e}")
            if curt_cache.exists():
                logger.warning("Using cached actual curtailment data after refresh failure")
                actual_curtailment = pd.read_feather(curt_cache)
            else:
                actual_curtailment = pd.DataFrame()

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

    logger.info("Done.")


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
