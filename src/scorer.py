"""
src/scorer.py
=============
Composite score computation and tier assignment.

Score formula (per country):
    composite_score = Σ (normalised_value[v] × weight[v])  for all v
    result is a weighted sum of USA-relative ratios, × 100.
    USA scores 100; countries better than USA may exceed 100.

Only variables with a non-NaN normalised value contribute.
(Missing variables are already zeroed in the weight matrix by weighter.py,
so this naturally handles partial data without a separate branch.)

Tier assignment uses config.TIER_THRESHOLDS and config.TIER_LABELS.

Also computes per-category contributions for dashboard breakdown bars:
    category_contribution[cat] = Σ (norm[v] × weight[v]) for v in cat
    expressed as a fraction of the country's total composite score.
"""

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _assign_tier(score: float, thresholds: dict, tier_labels: dict) -> str:
    """
    Map a USA-benchmark score to a tier label.
    USA = 100 → Tier 2 (Competitive Alternative).
    Scores may exceed 100 (Tier 1 threshold = 110).
    """
    if score >= thresholds["tier1_min"]:        # >= 110
        return tier_labels[1]
    if score >= thresholds["tier2_min"]:        # >= 90
        return tier_labels[2]
    if score >= thresholds["tier3_min"]:        # >= 70
        return tier_labels[3]
    if score >= thresholds.get("tier4_min", 50):  # >= 50
        return tier_labels[4]
    return tier_labels.get(5, tier_labels[4])   # < 50 → Tier 5


def compute_scores(
    normalized_df: pd.DataFrame,
    weight_matrix: dict,
    categories: dict,
    tier_thresholds: dict,
    tier_labels: dict,
) -> pd.DataFrame:
    """
    Compute composite scores for all countries.

    Parameters
    ----------
    normalized_df : pd.DataFrame
        Output of normalizer.normalize_all.  Index aligns with original df.
        Columns = scored variable keys; values in [0, 1] or NaN.
    weight_matrix : dict
        {country: {variable_key: float}} from weighter.build_weight_matrix.
    categories : dict
        config.VARIABLE_CATEGORIES — for per-category contribution breakdown.
    tier_thresholds : dict
        config.TIER_THRESHOLDS.
    tier_labels : dict
        config.TIER_LABELS.

    Returns
    -------
    pd.DataFrame
        Columns: country, composite_score, tier, rank,
                 + one column per category: contrib_{cat_key}
    """
    records = []

    for _, row in normalized_df.iterrows():
        country = row.name if "country" not in row.index else None

    # Rebuild with country as a column if it's in the index
    if "country" in normalized_df.columns:
        norm_iter = normalized_df.iterrows()
    else:
        norm_iter = normalized_df.iterrows()

    # Work with the DataFrame that has country as a regular column
    # (normalizer returns a DataFrame indexed the same as the input df;
    #  we need country names — fetch from weight_matrix keys in order)
    countries_ordered = list(weight_matrix.keys())

    # Map country → row position in normalized_df
    # normalized_df has integer index aligned with the original df
    # We iterate weight_matrix in insertion order (same as countries list)
    for pos, country in enumerate(countries_ordered):
        if pos >= len(normalized_df):
            break

        norm_row = normalized_df.iloc[pos]
        weights = weight_matrix[country]

        composite = 0.0
        cat_contribs = {}

        # Per-category contribution (skip the _weight_sum sentinel key)
        for cat_key, cat_cfg in categories.items():
            vars_in_cat = cat_cfg if isinstance(cat_cfg, list) else cat_cfg.get("variables", [])
            cat_sum = 0.0
            for var in vars_in_cat:
                if var.startswith("_"):
                    continue
                w = weights.get(var, 0.0)
                v = norm_row.get(var, np.nan)
                if w > 0 and pd.notna(v):
                    cat_sum += v * w
            cat_contribs[f"contrib_{cat_key}"] = round(cat_sum * 100, 4)
            composite += cat_sum

        # Normalise by actual weight sum (handles partial-category-missing edge case)
        weight_sum = weights.get("_weight_sum", 1.0)
        if weight_sum > 0:
            composite = composite / weight_sum

        composite_score = round(composite * 100, 4)
        tier = _assign_tier(composite_score, tier_thresholds, tier_labels)

        record = {
            "country": country,
            "composite_score": composite_score,
            "tier": tier,
        }
        record.update(cat_contribs)
        records.append(record)

    scores_df = pd.DataFrame(records)
    scores_df = scores_df.sort_values("composite_score", ascending=False).reset_index(drop=True)
    scores_df.insert(0, "rank", range(1, len(scores_df) + 1))

    logger.info(
        "Scoring complete. Top: %s (%.1f). Bottom: %s (%.1f).",
        scores_df.iloc[0]["country"],
        scores_df.iloc[0]["composite_score"],
        scores_df.iloc[-1]["country"],
        scores_df.iloc[-1]["composite_score"],
    )
    return scores_df
