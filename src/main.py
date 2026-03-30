"""CLI orchestrator for AEMO Solar & Wind Curtailment Dashboard."""

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
from .download_curtailment import calculate_actual_curtailment
from .merge import build_summary
from .excel_output import generate_all_workbooks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def run(full_refresh: bool = False, skip_scada: bool = False):
    """Main execution flow."""
    cache_dir = str(PROJECT_ROOT / config.DATA_DIR)
    output_dir = str(PROJECT_ROOT / config.OUTPUT_DIR)
    summary_path = PROJECT_ROOT / config.SUMMARY_CSV

    # ── Step 1: Generator listing (the spine) ────────────────────────────
    gen_cache = PROJECT_ROOT / config.GENERATOR_CACHE
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
    mlf_cache = PROJECT_ROOT / config.MLF_CACHE
    if not full_refresh and mlf_cache.exists():
        logger.info("Loading cached MLF data...")
        mlf_data = pd.read_feather(mlf_cache)
    else:
        mlf_data = fetch_mlf_data(cache_dir, generator_duids=all_duids)
        if not mlf_data.empty:
            mlf_cache.parent.mkdir(parents=True, exist_ok=True)
            mlf_data.reset_index(drop=True).to_feather(mlf_cache)

    # ── Step 3: ELI projected curtailment ────────────────────────────────
    eli_cache = PROJECT_ROOT / config.ELI_CURTAILMENT_CACHE
    if not full_refresh and eli_cache.exists():
        logger.info("Loading cached ELI curtailment data...")
        eli_data = pd.read_feather(eli_cache)
    else:
        eli_data = fetch_eli_curtailment(cache_dir)
        if not eli_data.empty:
            eli_cache.parent.mkdir(parents=True, exist_ok=True)
            eli_data.reset_index(drop=True).to_feather(eli_cache)

    # ── Step 4: REZ forecasts ────────────────────────────────────────────
    rez_cache = PROJECT_ROOT / config.REZ_FORECAST_CACHE
    if not full_refresh and rez_cache.exists():
        logger.info("Loading cached REZ forecast data...")
        rez_data = pd.read_feather(rez_cache)
    else:
        rez_data = fetch_rez_forecasts(cache_dir)
        if not rez_data.empty:
            rez_cache.parent.mkdir(parents=True, exist_ok=True)
            rez_data.reset_index(drop=True).to_feather(rez_cache)

    # ── Step 5: Actual curtailment from SCADA + UIGF ────────────────────
    curt_cache = PROJECT_ROOT / config.CURTAILMENT_CACHE
    if skip_scada:
        logger.info("Skipping SCADA curtailment calculation (--skip-scada)")
        actual_curtailment = pd.DataFrame()
    elif not full_refresh and curt_cache.exists():
        logger.info("Loading cached actual curtailment data...")
        actual_curtailment = pd.read_feather(curt_cache)
    else:
        actual_curtailment = calculate_actual_curtailment(
            cache_dir, all_duids, full_refresh=full_refresh
        )
        if not actual_curtailment.empty:
            curt_cache.parent.mkdir(parents=True, exist_ok=True)
            actual_curtailment.reset_index(drop=True).to_feather(curt_cache)

    # ── Step 6: Build merged summary ─────────────────────────────────────
    summary = build_summary(
        generators=generators,
        mlf_data=mlf_data,
        eli_curtailment=eli_data,
        rez_forecasts=rez_data,
        actual_curtailment=actual_curtailment,
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
        help="Re-download all data (default: use cached if available)",
    )
    parser.add_argument(
        "--skip-scada",
        action="store_true",
        help="Skip SCADA/UIGF curtailment calculation (faster, uses cached or empty)",
    )
    args = parser.parse_args()
    run(full_refresh=args.full_refresh, skip_scada=args.skip_scada)


if __name__ == "__main__":
    main()
