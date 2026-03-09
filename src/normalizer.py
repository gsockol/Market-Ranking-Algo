# MODEL STATUS: STABLE BASELINE (v1.0)
# This version passed full verification:
#   * Brazil ranks Top 5
#   * Portugal ranking improved
#   * No zero CAGR values
#   * Penetration override system active
#   * Colab execution verified
# Do NOT modify normalization or weights without creating a new version tag.

"""
src/normalizer.py
=================
USA-baseline normalisation for all scored variables.

Normalisation pipeline — SIX-STEP ORDER (do not reorder)
---------------------------------------------------------
Each variable is processed through the following steps. Steps 1–3 are
conditional (only applied when the variable is in the relevant config set).

  Step 1 — Two-sided p05/p95 clip  [optional: clip_p05p95_variables]
      raw = raw.clip(p05, p95)
      Prevents extreme emerging-market outliers (e.g. Turkiye inflation 53.9%)
      from compressing all peer country scores into a narrow band.

  Step 2 — Winsorize upper tail    [optional: outlier_cap_variables]
      raw = raw.clip(upper=p95)
      One-sided cap for right-skewed TAM/concentration variables.
      Applied AFTER Step 1 so two-sided clip cannot widen the upper bound.

  Step 3 — Pre-transform           [optional: pre_transforms]
      "log"  : raw = log(raw + shift + 1e-9)  — compresses skewed distributions
      "sqrt" : raw = sqrt(raw + shift)         — partial compression
      Applied AFTER Steps 1–2 so transforms operate on clean, capped values.

  Step 4 — Z-score
      z = (x − mean(x)) / std(x)
      Centres and scales the distribution so all variables are comparable.

  Step 5 — Percentile conversion (0–100)
      p = rankdata(z, method="average") / n_valid × 100
      Maps each country's Z-score to its percentile rank within the set.
      A score of 50 means the country is exactly at the median.
      A score of 100 means the country has the best value for this variable.

  Step 6 — Invert for "lower is better" variables  [inverted_variables]
      p = 100 − p
      For variables where a lower raw value is better (inflation, currency
      volatility, corporate tax), the raw percentile is flipped so that
      100 still means "best".

USA_BASELINE is accepted for API compatibility and used by the audit trail
downstream; it does not affect the normalised percentile scores.

Edge cases
----------
- Variable not in data or all NaN  → column set to NaN (warning logged).
- std == 0 (all countries identical) → all get 50.0 (median by convention).
- NaN values for individual countries → excluded from rankdata; stay NaN in
  the output (handled by weighter Rule 3 — missing weight redistributed).
- Log transform: shift = max(0, -min + 1e-6) ensures positivity; 1e-9 epsilon
  guards against exact-zero values producing log(0) = -inf.
"""

import logging

import numpy as np
import pandas as pd
from scipy.stats import rankdata

logger = logging.getLogger(__name__)


def normalize_all(
    df: pd.DataFrame,
    variables: list,
    inverted_variables: set,
    usa_baseline: dict,
    outlier_cap_variables: set | None = None,
    outlier_cap_percentile: float = 0.90,
    pre_transforms: dict | None = None,
    clip_p05p95_variables: set | None = None,
) -> pd.DataFrame:
    """
    Normalise every variable in *variables* using the six-step pipeline
    documented in the module docstring.

    Parameters
    ----------
    df : pd.DataFrame
        Full merged data, one row per country.
    variables : list[str]
        Ordered list of internal variable keys to normalise.
    inverted_variables : set[str]
        Variable keys where lower raw = better score (Step 6 inversion).
    usa_baseline : dict
        {variable_key: usa_reference_value} — retained for API compatibility
        and downstream auditing; not used in normalisation math.
    outlier_cap_variables : set[str] or None
        Variable keys whose upper tail should be Winsorized (Step 2).
    outlier_cap_percentile : float
        Upper quantile for Winsorization (default 0.90 = 90th percentile).
    pre_transforms : dict[str, str] or None
        {variable_key: transform_name} applied at Step 3.
        Supported transforms: "log", "sqrt".
    clip_p05p95_variables : set[str] or None
        Variable keys receiving two-sided [p05, p95] clipping at Step 1.

    Returns
    -------
    pd.DataFrame
        Index matches *df*. Columns = *variables*.
        Values are percentile scores in [~0, 100] (or NaN for missing data).
        The scorer sums these directly — no further ×100 multiplication.
        Higher = better for every variable (including inverted ones).
    """
    result = pd.DataFrame(index=df.index)

    for var in variables:
        if var.startswith("_"):
            continue

        if var not in df.columns:
            logger.warning(
                "Variable '%s' not found in data — normalised column will be NaN.", var
            )
            result[var] = np.nan
            continue

        raw = df[var].copy().astype(float)
        valid_mask = raw.notna()
        n_valid = valid_mask.sum()

        if n_valid == 0:
            logger.warning(
                "Variable '%s': no data for any country — normalised column will be NaN.", var
            )
            result[var] = np.nan
            continue

        raw_valid = raw[valid_mask]

        # ══════════════════════════════════════════════════════════════════
        # NORMALIZATION PIPELINE — DO NOT REORDER STEPS
        # Step 1: Clip p05/p95 (two-sided, emerging-market outlier control)
        # Step 2: Winsorize upper tail (one-sided, TAM/concentration cap)
        # Step 3: Pre-transform — log or sqrt (compress skewed distributions)
        # Step 4: Z-score (mean=0, std=1 across valid countries)
        # Step 5: Percentile rank (rankdata / n * 100 → 0-100 scale)
        # Step 6: Invert if needed (score = 100 − percentile for cost/risk)
        # ══════════════════════════════════════════════════════════════════

        # ── Step 1: Two-sided p05/p95 clip ───────────────────────────────
        if clip_p05p95_variables and var in clip_p05p95_variables:
            p05 = float(np.percentile(raw_valid.values, 5))
            p95 = float(np.percentile(raw_valid.values, 95))
            raw_valid = raw_valid.clip(lower=p05, upper=p95)
            logger.debug(
                "Variable '%s': two-sided p05/p95 clip [%.4f, %.4f].", var, p05, p95
            )

        # ── Step 2: Winsorize upper tail ──────────────────────────────────
        if outlier_cap_variables and var in outlier_cap_variables:
            cap_val = float(np.percentile(raw_valid.values, outlier_cap_percentile * 100))
            raw_valid = raw_valid.clip(upper=cap_val)
            logger.debug(
                "Variable '%s': upper tail Winsorized at %.4f (%.0f-th pct).",
                var, cap_val, outlier_cap_percentile * 100,
            )

        # ── Step 3: Pre-transform (log / sqrt) ───────────────────────────
        if pre_transforms and var in pre_transforms:
            transform = pre_transforms[var]
            if transform == "log":
                # shift ensures all values > 0; 1e-9 epsilon prevents log(0)=-inf
                # when a value exactly equals the shift boundary after clipping.
                shift = max(0.0, -float(raw_valid.min()) + 1e-6)
                raw_valid = np.log(raw_valid + shift + 1e-9)
                logger.debug(
                    "Variable '%s': log transform applied (shift=%.6f).", var, shift
                )
            elif transform == "sqrt":
                shift = max(0.0, -float(raw_valid.min()))
                raw_valid = np.sqrt(raw_valid + shift)
                logger.debug(
                    "Variable '%s': sqrt transform applied (shift=%.6f).", var, shift
                )

        # ── Step 4: Z-score across all valid countries ────────────────────
        mean_val = raw_valid.mean()
        std_val = raw_valid.std(ddof=1)

        if std_val == 0 or np.isnan(std_val):
            # All countries have identical values — assign median percentile.
            logger.debug(
                "Variable '%s': std == 0 — all valid countries assigned 50.0.", var
            )
            pct = raw.copy()
            pct[valid_mask] = 50.0
            pct[~valid_mask] = np.nan
            result[var] = pct
            continue

        z_valid = (raw_valid - mean_val) / std_val

        # ── Step 5: Percentile conversion (0–100) ─────────────────────────
        ranks = rankdata(z_valid.values, method="average")
        pct_valid = ranks / n_valid * 100

        pct = pd.Series(np.nan, index=raw.index, dtype=float)
        pct[valid_mask] = pct_valid

        # ── Step 6: Invert for "lower is better" variables ────────────────
        if var in inverted_variables:
            pct[valid_mask] = 100.0 - pct[valid_mask]

        result[var] = pct

    logger.info(
        "Z-score + percentile normalisation complete: %d variables across %d countries.",
        len(variables),
        len(df),
    )
    return result
