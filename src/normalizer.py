# MODEL STATUS: v1.1 (Institutional Magnitude Update)
# Updates: 
#   * Upgraded Step 5 to "Magnitude Scaling" for Opportunity/TAM
#   * Hardened Log-Transform shift logic
#   * Preserved "Stable Baseline" Percentile logic for all other variables

"""
src/normalizer.py
=================
Institutional-grade normalisation for gym market scoring.

Normalisation pipeline — SIX-STEP ORDER
---------------------------------------
1. Two-sided p05/p95 clip (Outlier Control)
2. Winsorize upper tail (TAM Cap)
3. Pre-transform (Log/Sqrt for Magnitude compression)
4. Z-score (Standardization)
5. Magnitude vs. Rank Mapping (0–100)
6. Inversion (Lower is Better logic)
"""

import logging
import numpy as np
import pandas as pd
from scipy.stats import rankdata

logger = logging.getLogger(__name__)

# Variables where the 'Size' or 'Magnitude' matters more than just the rank.
# These will use Min-Max scaling to preserve the distance between countries.
MAGNITUDE_VARIABLES = {
    'Opportunity ($M)', 
    'Potential Market Size ($M)', 
    'Concentration (000s/gym)'
}

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
    
    result = pd.DataFrame(index=df.index)

    for var in variables:
        if var.startswith("_") or var not in df.columns:
            if not var.startswith("_"):
                logger.warning("Variable '%s' not found — setting to NaN.", var)
            result[var] = np.nan
            continue

        raw = df[var].copy().astype(float)
        valid_mask = raw.notna()
        n_valid = valid_mask.sum()

        if n_valid == 0:
            result[var] = np.nan
            continue

        raw_valid = raw[valid_mask]

        # ── Step 1: Two-sided p05/p95 clip (Stability Control) ──────────
        if clip_p05p95_variables and var in clip_p05p95_variables:
            p05, p95 = np.percentile(raw_valid, [5, 95])
            raw_valid = raw_valid.clip(lower=p05, upper=p95)

        # ── Step 2: Winsorize upper tail (TAM Cap) ─────────────────────
        if outlier_cap_variables and var in outlier_cap_variables:
            cap_val = np.percentile(raw_valid, outlier_cap_percentile * 100)
            raw_valid = raw_valid.clip(upper=cap_val)

        # ── Step 3: Pre-transform (Log / Sqrt) ────────────────────────
        # CRITICAL: This now affects the final score magnitude!
        if pre_transforms and var in pre_transforms:
            transform = pre_transforms[var]
            shift = max(0.0, -float(raw_valid.min()) + 1e-6)
            if transform == "log":
                raw_valid = np.log1p(raw_valid + shift)
            elif transform == "sqrt":
                raw_valid = np.sqrt(raw_valid + shift)

        # ── Step 4: Z-score (Standardization) ─────────────────────────
        mean_val = raw_valid.mean()
        std_val = raw_valid.std(ddof=1)

        if std_val == 0 or np.isnan(std_val):
            pct = pd.Series(50.0, index=raw.index)
            pct[~valid_mask] = np.nan
            result[var] = pct
            continue

        # ── Step 5: Magnitude vs Rank Mapping ─────────────────────────
        if var in MAGNITUDE_VARIABLES:
            # MAGNITUDE SCALE: Preserves the "Gap" between leaders
            # (e.g. Germany gets a much higher score than France if it's much bigger)
            v_min, v_max = raw_valid.min(), raw_valid.max()
            pct_valid = (raw_valid - v_min) / (v_max - v_min) * 100
        else:
            # RANK SCALE: Standard Percentile (Stable Baseline)
            # Useful for qualitative metrics like 'Political Stability'
            z_valid = (raw_valid - mean_val) / std_val
            ranks = rankdata(z_valid, method="average")
            pct_valid = (ranks / n_valid) * 100

        pct = pd.Series(np.nan, index=raw.index)
        pct[valid_mask] = pct_valid

        # ── Step 6: Invert if needed (Lower = Better) ──────────────────
        if var in inverted_variables:
            pct[valid_mask] = 100.0 - pct[valid_mask]

        result[var] = pct

    logger.info("Normalisation v1.1 complete for %d variables.", len(variables))
    return result
