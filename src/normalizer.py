"""
src/normalizer.py
=================
USA-baseline normalisation for all scored variables.

Normalisation model
-------------------
Each country's variable value is expressed as a ratio relative to the USA
reference value defined in config.USA_BASELINE:

    Non-inverted (higher = better):
        norm = country_value / usa_value        → USA scores 1.0 (= 100 pts)

    Inverted (lower = better):
        norm = usa_value / country_value        → USA scores 1.0 (= 100 pts)
        Rationale: a country with *lower* cost than the USA gets a ratio > 1.0,
        meaning it scores *better* than the USA.

The scorer then multiplies by 100, so USA always contributes 100 × its weight.
Countries better than the USA score > 100; worse → < 100.
Scores can be negative for WGI variables when a country's raw value is negative
while the USA's reference is positive.

Edge cases
----------
- Variable not in usa_baseline → stored as NaN (warning logged).
- country_value is NaN → stored as NaN (already handled by weighter Rule 3).
- Inverted + country_value == 0 → division by zero → stored as NaN.
- usa_value == 0 → cannot normalize → stored as NaN (warning logged).
"""

import logging

import numpy as np
import pandas as pd

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
        Values are ratios relative to USA (USA = 1.0), or NaN.
        The scorer multiplies by 100 so USA → 100 pts.
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
            # norm = usa_value / country_value  →  USA = 1.0
            # Guard against division by zero (country_value == 0).
            norm = raw.copy()
            valid_nonzero = raw.notna() & (raw != 0)
            zero_mask = raw.notna() & (raw == 0)
            norm[valid_nonzero] = usa_val / raw[valid_nonzero]
            norm[zero_mask] = np.nan    # can't invert a zero value
            norm[raw.isna()] = np.nan
        else:
            # Higher raw value = better score.
            # norm = country_value / usa_value  →  USA = 1.0
            norm = raw / usa_val

        result[var] = norm

    logger.info(
        "USA-baseline normalisation complete: %d variables across %d countries.",
        len(variables),
        len(df),
    )
    return result
