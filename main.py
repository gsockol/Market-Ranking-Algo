"""
main.py
=======
HVLP Global Gym Market Opportunity Model — Orchestrator

Usage
-----
    python main.py [OPTIONS]

Options
-------
    --csv PATH        Path to country CSV (default: input_data.csv)
    --no-cache        Bypass API response cache and re-fetch all data
    --interactive     Prompt for missing values instead of silently applying Rule 3
    --output-dir DIR  Output directory for HTML and Excel (default: output)

Pipeline
--------
  1.  Load config
  2.  Ingest CSV  (ingestor)
  3.  Compute derived metrics  (calculator)
  4.  Fetch external data via World Bank API  (fetcher)
  5.  Load YAML manual overrides  (override_loader)
  6.  Merge all data sources + build audit trail  (override_loader)
  7.  Min-max normalise all scored variables  (normalizer)
  8.  Build per-country weight matrix (Rules 1-3)  (weighter)
  9.  Compute composite scores + tier labels  (scorer)
  10. Generate qualitative commentary  (commentary)
  11. Write self-contained HTML dashboard  (dashboard)
  12. Write multi-sheet Excel workbook  (exporter)
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("main")


# ---------------------------------------------------------------------------
# Config import (single source of truth)
# ---------------------------------------------------------------------------

import config as cfg


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_availability(df: pd.DataFrame, scored_vars: list) -> dict:
    """
    Build {country: {variable: bool}} availability matrix.
    True = a non-NaN value exists for that variable after all data merging.
    """
    matrix = {}
    for _, row in df.iterrows():
        country = row["country"]
        matrix[country] = {
            var: pd.notna(row.get(var)) for var in scored_vars
        }
    return matrix


def _print_summary(scores_df: pd.DataFrame) -> None:
    print("\n" + "=" * 64)
    print("  HVLP GLOBAL GYM MARKET OPPORTUNITY — RESULTS SUMMARY")
    print("=" * 64)
    print(f"  {'Rank':<5} {'Country':<20} {'Score':>7}  {'Tier'}")
    print("  " + "-" * 58)
    for _, row in scores_df.iterrows():
        score = f"{row['composite_score']:.1f}"
        print(f"  {row['rank']:<5} {row['country']:<20} {score:>7}  {row['tier']}")
    print("=" * 64 + "\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="HVLP Global Gym Market Opportunity Scoring Tool"
    )
    parser.add_argument(
        "--csv",
        default="input_data.csv",
        help="Path to country input CSV (default: input_data.csv)",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Bypass API cache and re-fetch all external data",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Prompt interactively for any missing data not covered by YAML overrides",
    )
    parser.add_argument(
        "--output-dir",
        default=cfg.OUTPUT_DIR,
        help=f"Output directory for HTML and Excel (default: {cfg.OUTPUT_DIR})",
    )
    parser.add_argument(
        "--skip-api",
        action="store_true",
        help="Skip all World Bank API calls; rely entirely on manual overrides and Rule 3",
    )
    args = parser.parse_args()

    scored_vars = list(cfg.WEIGHTS.keys())

    # ------------------------------------------------------------------
    # STEP 1 — Ingest CSV
    # ------------------------------------------------------------------
    logger.info("Step 1/10: Ingesting CSV: %s", args.csv)
    from src.ingestor import ingest_csv
    df = ingest_csv(args.csv, cfg.CSV_COLUMN_MAP)

    countries = df["country"].tolist()
    logger.info("Countries loaded: %s", countries)

    # ------------------------------------------------------------------
    # STEP 2 — Derived metrics
    # ------------------------------------------------------------------
    logger.info("Step 2/10: Computing derived metrics …")
    from src.calculator import calculate_derived_metrics
    df = calculate_derived_metrics(df, cfg.DUES_INCREASE_PCT)

    # ------------------------------------------------------------------
    # STEP 3 — Fetch external data
    # ------------------------------------------------------------------
    if args.skip_api:
        logger.info("Step 3/10: API fetch skipped (--skip-api). All API variables = None.")
        external_data = {c: {} for c in countries}
    else:
        logger.info("Step 3/10: Fetching World Bank data (cache TTL: %dh) …", cfg.CACHE_EXPIRY_HOURS)
        from src.fetcher import fetch_all_external_data
        external_data = fetch_all_external_data(
            countries=countries,
            country_iso3_map=cfg.COUNTRY_ISO3_MAP,
            wb_indicators=cfg.WB_INDICATORS,
            cache_dir=cfg.CACHE_DIR,
            ttl_hours=cfg.CACHE_EXPIRY_HOURS,
            no_cache=args.no_cache,
        )

    # ------------------------------------------------------------------
    # STEP 4 — Load YAML overrides
    # ------------------------------------------------------------------
    logger.info("Step 4/10: Loading manual overrides …")
    from src.override_loader import load_yaml_overrides, merge_overrides
    yaml_overrides = load_yaml_overrides("overrides/manual_inputs.yaml")

    # ------------------------------------------------------------------
    # STEP 5 — Merge all data sources
    # ------------------------------------------------------------------
    logger.info("Step 5/10: Merging data sources …")
    df, audit = merge_overrides(
        df=df,
        external_data=external_data,
        yaml_overrides=yaml_overrides,
        scored_variables=scored_vars,
        interactive=args.interactive,
    )

    # ------------------------------------------------------------------
    # STEP 6 — Normalise
    # ------------------------------------------------------------------
    logger.info("Step 6/10: Normalising variables (min-max, active dataset scope) …")
    from src.normalizer import normalize_all
    normalized_df = normalize_all(df, scored_vars, cfg.INVERTED_VARIABLES)

    # ------------------------------------------------------------------
    # STEP 7 — Build weight matrix (Rules 1-3)
    # ------------------------------------------------------------------
    logger.info("Step 7/10: Resolving per-country weights (Rules 1-3) …")
    from src.weighter import build_weight_matrix
    availability = _build_availability(df, scored_vars)
    weight_matrix = build_weight_matrix(
        countries=countries,
        availability_matrix=availability,
        base_weights=cfg.WEIGHTS,
        rule1_cfg=cfg.RULE1_MISSING_CAGR,
        rule2_cfg=cfg.RULE2_MISSING_CONCENTRATION,
        categories=cfg.VARIABLE_CATEGORIES,
    )

    # ------------------------------------------------------------------
    # STEP 8 — Composite scores
    # ------------------------------------------------------------------
    logger.info("Step 8/10: Computing composite scores …")
    from src.scorer import compute_scores
    scores_df = compute_scores(
        normalized_df=normalized_df,
        weight_matrix=weight_matrix,
        categories=cfg.VARIABLE_CATEGORIES,
        tier_thresholds=cfg.TIER_THRESHOLDS,
        tier_labels=cfg.TIER_LABELS,
    )

    # ------------------------------------------------------------------
    # STEP 9 — Commentary
    # ------------------------------------------------------------------
    logger.info("Step 9/10: Generating commentary …")
    from src.commentary import generate_commentary
    commentary = generate_commentary(
        scores_df=scores_df,
        full_df=df,
        normalized_df=normalized_df,
        weight_matrix=weight_matrix,
        audit=audit,
        categories=cfg.VARIABLE_CATEGORIES,
        inverted_variables=cfg.INVERTED_VARIABLES,
    )

    # ------------------------------------------------------------------
    # STEP 10 — Outputs
    # ------------------------------------------------------------------
    logger.info("Step 10/10: Writing outputs …")
    from src.dashboard import generate_dashboard
    dashboard_path = generate_dashboard(
        scores_df=scores_df,
        full_df=df,
        normalized_df=normalized_df,
        weight_matrix=weight_matrix,
        audit=audit,
        commentary=commentary,
        categories=cfg.VARIABLE_CATEGORIES,
        base_weights=cfg.WEIGHTS,
        tier_colors=cfg.TIER_COLORS,
        output_dir=args.output_dir,
        filename=cfg.DASHBOARD_FILENAME,
    )

    from src.exporter import export_excel
    excel_path = export_excel(
        scores_df=scores_df,
        full_df=df,
        normalized_df=normalized_df,
        weight_matrix=weight_matrix,
        audit=audit,
        categories=cfg.VARIABLE_CATEGORIES,
        base_weights=cfg.WEIGHTS,
        output_dir=args.output_dir,
        filename=cfg.EXCEL_FILENAME,
    )

    # ------------------------------------------------------------------
    # Print summary
    # ------------------------------------------------------------------
    _print_summary(scores_df)
    print(f"  Dashboard → {dashboard_path}")
    print(f"  Excel     → {excel_path}\n")


if __name__ == "__main__":
    sys.exit(main())
