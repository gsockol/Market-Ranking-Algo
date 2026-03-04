"""
src/normalizer.py
=================
Min-max normalisation for all scored variables.

Scope rule (from MSD §6):
  Normalisation is calculated ONLY across countries present in the current
  dataset execution — not against any fixed global scale.

Inversion:
  Variables listed in INVERTED_VARIABLES are flipped after normalisation:
      normalised = 1 − normalised
  so that a lower raw value (e.g. lower inflation) maps to a higher score.

Edge cases:
  - All values for a variable are NaN → column stays NaN.
  - Only one non-NaN value exists → that country's normalised value = 0.5
    (placed at the midpoint; flagged in logs).
  - min == max (all countries have identical value) → normalised = 0.5 for all.

Returns:
  A new DataFrame with identical index to the input, containing one column
  per scored variable, values in [0.0, 1.0] or NaN.
"""

import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _minmax(series: pd.Series) -> pd.Series:
    """
    Min-max scale a numeric Series to [0, 1].
    NaN values are preserved.  Returns a new Series.
    """
    valid = series.dropna()
    if valid.empty:
        return series.copy()

    lo, hi = valid.min(), valid.max()

    if hi == lo:
        logger.debug(
            "Variable '%s': min == max (%.4f) across all countries → setting 0.5",
            series.name,
            lo,
        )
        out = series.copy()
        out[series.notna()] = 0.5
        return out

    return (series - lo) / (hi - lo)


def normalize_all(
    df: pd.DataFrame,
    variables: list,
    inverted_variables: set,
) -> pd.DataFrame:
    """
    Normalise every variable in *variables* across all rows of *df*.

    Parameters
    ----------
    df : pd.DataFrame
        Full merged data, one row per country.  Must contain all columns
        listed in *variables* (missing columns produce NaN output).
    variables : list[str]
        Ordered list of internal variable keys to normalise.
    inverted_variables : set[str]
        Variable keys where lower raw = better score; these are flipped
        after min-max scaling.

    Returns
    -------
    pd.DataFrame
        Index matches *df*.  Columns = *variables*.  Values in [0, 1] or NaN.
    """
    result = pd.DataFrame(index=df.index)

    for var in variables:
        if var not in df.columns:
            logger.warning(
                "Variable '%s' not found in data — normalised column will be NaN.", var
            )
            result[var] = np.nan
            continue

        raw = df[var].copy().astype(float)
        n_valid = raw.notna().sum()

        if n_valid == 0:
            logger.warning("Variable '%s': no data for any country — skipping normalisation.", var)
            result[var] = np.nan
            continue

        if n_valid == 1:
            logger.warning(
                "Variable '%s': only one country has data — normalised value set to 0.5.", var
            )
            norm = raw.copy()
            norm[raw.notna()] = 0.5
        else:
            norm = _minmax(raw)
            norm.name = var

        if var in inverted_variables:
            # Flip: high raw → low score, low raw → high score
            norm = norm.copy()
            norm[norm.notna()] = 1.0 - norm[norm.notna()]

        result[var] = norm

    logger.info(
        "Normalisation complete: %d variables across %d countries.",
        len(variables),
        len(df),
    )
    return result
