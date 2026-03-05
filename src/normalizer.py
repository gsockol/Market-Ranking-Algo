"""
src/normalizer.py
=================
USA-baseline normalisation for all scored variables.

Normalisation model
-------------------
Replaced log normalization with Z-score + percentile hybrid to handle extreme
outliers while preserving relative ranking and weighting.

Each variable is normalised independently across all countries in three steps:

  Step 1 — Z-score
      z = (x − mean(x)) / std(x)
      Centres and scales the distribution so all variables are comparable.

  Step 2 — Percentile conversion (0–100)
      p = rankdata(z, method="average") / n_valid × 100
      Maps each country's Z-score to its percentile rank within the set.
      A score of 50 means the country is exactly at the median.
      A score of 100 means the country has the best value for this variable.

  Step 3 — Invert for "lower is better" variables
      p = 100 − p
      For variables where a lower raw value is better (inflation, currency
      volatility, corporate tax, labor cost, real estate cost), the raw
      percentile is flipped so that 100 still means "best".

USA_BASELINE is accepted for API compatibility and used by the audit trail
downstream; it does not affect the normalised percentile scores.

Edge cases
----------
- Variable not in data or all NaN  → column set to NaN (warning logged).
- std == 0 (all countries identical) → all get 50.0 (median by convention).
- NaN values for individual countries → excluded from rankdata; stay NaN in
  the output (handled by weighter Rule 3 — missing weight redistributed).
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
) -> pd.DataFrame:
    """
    Normalise every variable in *variables* using Z-score + percentile hybrid.

    Parameters
    ----------
    df : pd.DataFrame
        Full merged data, one row per country.
    variables : list[str]
        Ordered list of internal variable keys to normalise.
    inverted_variables : set[str]
        Variable keys where lower raw = better score.
        These receive  p = 100 − p  after percentile conversion.
    usa_baseline : dict
        {variable_key: usa_reference_value} — retained for API compatibility
        and downstream auditing; not used in normalisation math.

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

        # ── Step 1: Z-score across all valid countries ────────────────────
        raw_valid = raw[valid_mask]
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

        # ── Step 2: Percentile conversion (0–100) ────────────────────────
        ranks = rankdata(z_valid.values, method="average")
        pct_valid = ranks / n_valid * 100

        pct = pd.Series(np.nan, index=raw.index, dtype=float)
        pct[valid_mask] = pct_valid

        # ── Step 3: Invert for "lower is better" variables ───────────────
        if var in inverted_variables:
            pct[valid_mask] = 100.0 - pct[valid_mask]

        result[var] = pct

    logger.info(
        "Z-score + percentile normalisation complete: %d variables across %d countries.",
        len(variables),
        len(df),
    )
    return result
