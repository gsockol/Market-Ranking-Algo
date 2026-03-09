# MODEL STATUS: STABLE BASELINE (v1.0)
# This version passed full verification:
#   * Brazil ranks Top 5
#   * Portugal ranking improved
#   * No zero CAGR values
#   * Penetration override system active
#   * Colab execution verified
# Do NOT modify normalization or weights without creating a new version tag.

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


def _is_tty() -> bool:
    """True when stdin is a real interactive terminal (not piped or batch)."""
    return hasattr(sys.stdin, "isatty") and sys.stdin.isatty()


def _in_colab() -> bool:
    """True when running inside Google Colab."""
    try:
        import google.colab  # noqa: F401
        return True
    except ImportError:
        return False


def _collect_penetration_overrides(
    df: pd.DataFrame,
    yaml_path: str,
) -> dict:
    """
    Collect future gym penetration rate overrides from YAML and/or the user.

    Workflow
    --------
    1. Load any values already set in the YAML ``penetration_overrides`` section.
    2. Ask the user whether to apply custom rates (via ipywidgets in Colab,
       or a plain text prompt in terminals).  Skipped automatically when
       stdin is not a TTY (CI / piped runs).
    3. If the user answers yes, show per-country prompts for session-level
       adjustments on top of the YAML values.
    4. Print a table of all applied overrides for transparency.

    Returns
    -------
    dict  {country: future_penetration_fraction}
        Combined YAML + session overrides.  Empty if user declines or stdin
        is not interactive.
    """
    from src.override_loader import load_yaml_penetration_overrides

    yaml_overrides = load_yaml_penetration_overrides(yaml_path)
    session_overrides: dict = {}

    # ── Determine whether to show the override prompt ──────────────────────
    # Skip entirely in batch / piped mode to avoid hanging.
    if not _is_tty() and not _in_colab():
        if yaml_overrides:
            logger.info(
                "Non-interactive run: applying %d YAML penetration override(s) without prompt.",
                len(yaml_overrides),
            )
        return yaml_overrides

    # ── Colab: ipywidgets checkbox ─────────────────────────────────────────
    use_overrides = False
    if _in_colab():
        try:
            import ipywidgets as widgets
            from IPython.display import display

            cb = widgets.Checkbox(
                value=bool(yaml_overrides),
                description="Use custom future penetration assumptions",
                style={"description_width": "initial"},
            )
            display(cb)
            # Execution continues synchronously; checkbox reflects its
            # initial value.  Users can toggle it before running the next cell.
            use_overrides = cb.value
        except ImportError:
            # ipywidgets not available — fall through to text prompt
            pass

    # ── Terminal: plain text prompt ────────────────────────────────────────
    if not _in_colab() or not use_overrides:
        try:
            ans = input(
                "\nUse custom future penetration rates? (y/n) "
                "[YAML has %d preset]: " % len(yaml_overrides)
            ).strip().lower()
            use_overrides = ans.startswith("y")
        except EOFError:
            use_overrides = False

    if not use_overrides:
        return {}

    # ── Apply YAML values, then offer per-country session adjustments ──────
    print("\n  Penetration overrides  (fraction, e.g. 0.20 = 20%):")
    print(f"  {'Country':<22} {'Current':>8}  {'CSV Target':>10}  {'Override':>10}")
    print("  " + "-" * 58)
    for _, row in df.iterrows():
        country = row["country"]
        cur = row.get("current_penetration_pct")
        fut = row.get("future_penetration_pct")
        cur_str = f"{cur:.1%}" if pd.notna(cur) else "?"
        fut_str = f"{fut:.1%}" if pd.notna(fut) else "?"
        yaml_val = yaml_overrides.get(country)
        default_str = f"{yaml_val:.1%}" if yaml_val else "—"
        try:
            raw = input(
                f"  {country:<22} {cur_str:>8}  {fut_str:>10}  "
                f"[YAML={default_str}] new value or Enter to keep: "
            ).strip()
        except EOFError:
            raw = ""
        if raw:
            try:
                v = float(raw)
                # Auto-convert percentage
                if 1.0 < v <= 100.0:
                    v = round(v / 100.0, 6)
                if 0.0 < v <= 1.0:
                    session_overrides[country] = v
                else:
                    print(f"    {v:.4f} out of range (0, 1] — skipped.")
            except ValueError:
                print(f"    '{raw}' is not a valid number — skipped.")
        elif yaml_val is not None:
            session_overrides[country] = yaml_val

    # ── Summarise ──────────────────────────────────────────────────────────
    combined = {**yaml_overrides, **session_overrides}
    if combined:
        print("\n  Applied penetration overrides:")
        for c, v in combined.items():
            print(f"    {c:<22} → {v:.1%}")
    print()
    return combined


def _collect_gdp_growth_overrides(
    df: pd.DataFrame, interactive: bool
) -> dict:
    """
    Prompt (interactive mode only) for per-country GDP growth rate overrides.
    Returns {country: gdp_growth_rate_pct}.
    """
    gdp_growth_overrides: dict = {}
    if not interactive or not _is_tty():
        return gdp_growth_overrides

    print("\n" + "=" * 64)
    print("  GDP GROWTH OVERRIDES  (session-only)")
    print("=" * 64)
    yn2 = input("\nOverride GDP growth rate for any country? (y/n): ").strip().lower()
    if yn2 == "y":
        print("  Note: gym_membership_cagr = GDP_growth_rate × 1.4")
        for _, row in df.iterrows():
            country = row["country"]
            try:
                raw = input(
                    f"  {country:<22} GDP growth % (e.g. 4.5, or Enter to skip): "
                ).strip()
            except EOFError:
                raw = ""
            if raw:
                try:
                    gdp_growth_overrides[country] = float(raw)
                except ValueError:
                    print(f"    '{raw}' is not a valid number — skipped.")
    if gdp_growth_overrides:
        print(f"\n  GDP growth overrides: {len(gdp_growth_overrides)} countries.")
    print("=" * 64 + "\n")
    return gdp_growth_overrides


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
        "--refresh-api",
        action="store_true",
        help="Bypass API cache and force re-fetch of all external data (alias for --no-cache)",
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

    # ------------------------------------------------------------------
    # REPRODUCIBILITY: print model version tag and commit hash
    # ------------------------------------------------------------------
    try:
        import subprocess as _sp
        _tag = _sp.check_output(
            ["git", "describe", "--tags", "--exact-match", "HEAD"],
            stderr=_sp.DEVNULL, text=True,
        ).strip()
    except Exception:
        _tag = None
    try:
        _commit = _sp.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=_sp.DEVNULL, text=True,
        ).strip()
    except Exception:
        _commit = None

    if _tag or _commit:
        print(f"  Model Version : {_tag or '(untagged)'}")
        print(f"  Commit        : {_commit or 'unknown'}")
        print()

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
    # PRE-RUN: penetration overrides (YAML + optional interactive prompt)
    # ------------------------------------------------------------------
    # Always runs: loads YAML section, then prompts if stdin is a TTY.
    # In Colab: shows an ipywidgets checkbox when available.
    # In batch/CI: silently applies YAML-only values without prompting.
    penetration_overrides = _collect_penetration_overrides(
        df, "overrides/manual_inputs.yaml"
    )

    # GDP growth overrides — interactive-only (--interactive flag)
    gdp_growth_overrides = _collect_gdp_growth_overrides(df, args.interactive)

    # ------------------------------------------------------------------
    # STEP 2 — Derived metrics
    # ------------------------------------------------------------------
    logger.info("Step 2/10: Computing derived metrics …")
    from src.calculator import calculate_derived_metrics
    df = calculate_derived_metrics(
        df,
        cfg.DUES_INCREASE_PCT,
        penetration_overrides=penetration_overrides if penetration_overrides else None,
    )

    # ------------------------------------------------------------------
    # STEP 3 — Fetch external data
    # ------------------------------------------------------------------
    if args.skip_api:
        logger.info("Step 3/10: API fetch skipped (--skip-api). All API variables = None.")
        external_data = {c: {} for c in countries}
    else:
        logger.info(
            "Step 3/10: Fetching external data (WB / OECD / Trading Economics, "
            "cache TTL: %dh) …", cfg.CACHE_EXPIRY_HOURS
        )
        from src.fetcher import fetch_all_external_data
        external_data = fetch_all_external_data(
            countries=countries,
            country_iso3_map=cfg.COUNTRY_ISO3_MAP,
            wb_indicators=cfg.WB_INDICATORS,
            oecd_country_codes=cfg.OECD_COUNTRY_CODES,
            te_api_key=cfg.TRADING_ECONOMICS_API_KEY,
            cache_dir=cfg.CACHE_DIR,
            ttl_hours=cfg.CACHE_EXPIRY_HOURS,
            no_cache=args.no_cache or args.refresh_api,
            imf_country_codes=cfg.IMF_COUNTRY_CODES,
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
    # Convert GDP growth overrides → gym_membership_cagr via the same ×1.4 formula
    gui_cagr_overrides = (
        {c: round(v * 1.4, 4) for c, v in gdp_growth_overrides.items()}
        if gdp_growth_overrides
        else None
    )
    # Include composite input variables (labor, real_estate) so merge_overrides
    # populates them in df even though they're no longer individually scored.
    merge_vars = scored_vars + cfg.COMPOSITE_INPUT_VARIABLES
    df, audit = merge_overrides(
        df=df,
        external_data=external_data,
        yaml_overrides=yaml_overrides,
        scored_variables=merge_vars,
        interactive=args.interactive,
        gui_cagr_overrides=gui_cagr_overrides,
    )

    # ------------------------------------------------------------------
    # STEP 5b — Composite variables (need full merged dataset with YAML values)
    # ------------------------------------------------------------------
    logger.info("Step 5b/10: Computing composite variables …")
    from src.calculator import calculate_composite_variables
    df = calculate_composite_variables(df)

    # Patch audit trail: composite variables are computed here, not from any
    # external source, so merge_overrides recorded "missing" for them.
    # Also surface gym_membership_cagr_source as a dedicated DataFrame column
    # so the dashboard and Excel export can display it explicitly.
    for country in countries:
        crow = df.loc[df["country"] == country]
        if not crow.empty:
            if pd.notna(crow["operating_cost_composite"].values[0]):
                audit[country]["operating_cost_composite"] = "computed_composite"
            if pd.notna(crow["market_agility_bonus"].values[0]):
                audit[country]["market_agility_bonus"] = "computed_composite"

    # Add gym_membership_cagr_source column to df for transparent reporting
    cagr_sources = {c: audit[c].get("gym_membership_cagr", "unknown") for c in countries}
    df["gym_membership_cagr_source"] = df["country"].map(cagr_sources)

    # ------------------------------------------------------------------
    # STEP 6 — Normalise
    # ------------------------------------------------------------------
    logger.info("Step 6/10: Normalising variables (Z-score + percentile, active dataset scope) …")
    from src.normalizer import normalize_all
    normalized_df = normalize_all(
        df,
        scored_vars,
        cfg.INVERTED_VARIABLES,
        cfg.USA_BASELINE,
        outlier_cap_variables=cfg.OUTLIER_CAP_VARIABLES,
        outlier_cap_percentile=cfg.OUTLIER_CAP_PERCENTILE,
        pre_transforms=cfg.PRE_TRANSFORMS,
        clip_p05p95_variables=cfg.CLIP_P05P95_VARIABLES,
    )

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
    # BASELINE EXPORT — always overwrite so the most recent run is on disk
    # ------------------------------------------------------------------
    _out = Path(args.output_dir)
    _out.mkdir(parents=True, exist_ok=True)

    _baseline_ranking = _out / "baseline_ranking.csv"
    scores_df.to_csv(_baseline_ranking, index=False)

    # Full per-variable detail: scores + normalized values + raw CAGR source
    _full_cols = ["country", "composite_score", "rank", "tier"] + scored_vars
    _full_cols += ["gym_membership_cagr_source"] if "gym_membership_cagr_source" in df.columns else []
    _detail = scores_df[["country", "composite_score", "rank", "tier"]].copy()
    for var in scored_vars:
        if var in normalized_df.columns:
            _detail[f"norm_{var}"] = normalized_df[var].values
        if var in df.columns:
            _detail[f"raw_{var}"] = df[var].values
    if "gym_membership_cagr_source" in df.columns:
        _detail["gym_membership_cagr_source"] = df["gym_membership_cagr_source"].values
    _baseline_full = _out / "baseline_scores_full.csv"
    _detail.to_csv(_baseline_full, index=False)

    logger.info("Baseline exports written: %s, %s", _baseline_ranking, _baseline_full)

    # ------------------------------------------------------------------
    # Print summary
    # ------------------------------------------------------------------
    _print_summary(scores_df)
    print(f"  Dashboard         → {dashboard_path}")
    print(f"  Excel             → {excel_path}")
    print(f"  Baseline ranking  → {_baseline_ranking}")
    print(f"  Baseline full     → {_baseline_full}\n")


if __name__ == "__main__":
    sys.exit(main())
