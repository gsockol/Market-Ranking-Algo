"""
src/normalizer.py
=================
USA-baseline normalisation for all scored variables.

Normalisation model
-------------------
Each country's variable value is first expressed as a ratio relative to the USA
reference value defined in config.USA_BASELINE:

    Non-inverted (higher = better):
        ratio = country_value / usa_value       → USA ratio = 1.0

    Inverted (lower = better):
        ratio = usa_value / country_value       → USA ratio = 1.0
        Rationale: a country with *lower* cost than the USA gets ratio > 1.0,
        meaning it scores *better* than the USA.

Logarithmic compression is then applied so that extreme outliers have
diminishing marginal returns while keeping USA anchored at exactly 100:

    log_score = 100 × log1p(ratio) / log1p(1.0)
              = 100 × ln(1 + ratio) / ln(2)

USA (ratio = 1.0) → 100 × ln(2) / ln(2) = 100.
Countries better than USA (ratio > 1.0) → score > 100.
Countries worse than USA (ratio < 1.0) → score < 100.

Guard: log1p is undefined for ratio ≤ −1.  Ratios are clamped to −1 + ε
before the log is applied.  In practice this only affects WGI variables for
countries whose raw score is strongly negative while the USA reference is
positive.

Edge cases
----------
- Variable not in usa_baseline → stored as NaN (warning logged).
- country_value is NaN → stored as NaN (already handled by weighter Rule 3).
- Inverted + country_value == 0 → division by zero → stored as NaN.
- usa_value == 0 → cannot normalize → stored as NaN (warning logged).
- ratio ≤ −1 → clamped to −1 + 1e-9 before log1p (logged at DEBUG level).
"""

import logging

import numpy as np
import pandas as pd

# Normalising divisor so that USA (ratio = 1.0) always maps to exactly 100.
_LOG_SCALE = np.log1p(1.0)  # ln(2) ≈ 0.6931

logger = logging.getLogger(__name__)


def normalize_all(
    df: pd.DataFrame,
    variables: list,
    inverted_variables: set,
    usa_baseline: dict,
) -> pd.DataFrame:
    """
    Normalize every variable in *variables* using USA-baseline ratios.

    Parameters
    ----------
    df : pd.DataFrame
        Full merged data, one row per country. Must contain all columns
        listed in *variables* (missing columns produce NaN output).
    variables : list[str]
        Ordered list of internal variable keys to normalise.
    inverted_variables : set[str]
        Variable keys where lower raw = better score.
        For these: norm = usa_value / country_value.
    usa_baseline : dict
        {variable_key: usa_reference_value} from config.USA_BASELINE.

    Returns
    -------
    pd.DataFrame
        Index matches *df*. Columns = *variables*.
        Values are log-compressed scores: 100 × log1p(ratio) / log1p(1.0).
        USA = 100.0 exactly.  Scores can exceed 100 or be negative.
        The scorer sums these directly (no further ×100 multiplication).
    """
    result = pd.DataFrame(index=df.index)

    for var in variables:
        if var.startswith("_"):
            continue

        usa_val = usa_baseline.get(var)

        if usa_val is None:
            logger.warning(
                "Variable '%s' has no USA_BASELINE entry — normalised column will be NaN.",
                var,
            )
            result[var] = np.nan
            continue

        if usa_val == 0:
            logger.warning(
                "Variable '%s' USA baseline is 0 — cannot normalize — column will be NaN.",
                var,
            )
            result[var] = np.nan
            continue

        if var not in df.columns:
            logger.warning(
                "Variable '%s' not found in data — normalised column will be NaN.", var
            )
            result[var] = np.nan
            continue

        raw = df[var].copy().astype(float)
        n_valid = raw.notna().sum()

        if n_valid == 0:
            logger.warning(
                "Variable '%s': no data for any country — normalised column will be NaN.", var
            )
            result[var] = np.nan
            continue

        if var in inverted_variables:
            # Lower raw value = better score.
            # ratio = usa_value / country_value  →  USA ratio = 1.0
            # Guard against division by zero (country_value == 0).
            ratio = raw.copy()
            valid_nonzero = raw.notna() & (raw != 0)
            zero_mask = raw.notna() & (raw == 0)
            ratio[valid_nonzero] = usa_val / raw[valid_nonzero]
            ratio[zero_mask] = np.nan    # can't invert a zero value
            ratio[raw.isna()] = np.nan
        else:
            # Higher raw value = better score.
            # ratio = country_value / usa_value  →  USA ratio = 1.0
            ratio = raw / usa_val

        # Log compression: score = 100 × log1p(ratio) / log1p(1.0)
        # Clamp ratio to (-1, ∞) so log1p stays in its valid domain.
        safe_ratio = ratio.clip(lower=-1 + 1e-9)
        result[var] = 100.0 * np.log1p(safe_ratio) / _LOG_SCALE

    logger.info(
        "USA-baseline log-normalisation complete: %d variables across %d countries.",
        len(variables),
        len(df),
    )
    return result
