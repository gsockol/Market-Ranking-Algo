import pandas as pd
import numpy as np

def calculate_derived_metrics(df):
    """SECTION 7: Feature Engineering"""
    df_out = df.copy()
    # 7.5 Market Agility Bonus
    if 'potential_market_size' in df_out.columns:
        df_out['agility_bonus'] = 1 / np.sqrt(df_out['potential_market_size'].replace(0, 1))
    return df_out

def compute_scores(ndf, weights):
    """SECTION 10: Weighted Aggregation"""
    score = pd.Series(0.0, index=ndf.index)
    for var, weight in weights.items():
        if var in ndf.columns:
            score += ndf[var] * weight
    return score

def assign_tier(score):
    """SECTION 12: Tier Assignment"""
    if score >= 75: return "Tier 1 — High Opportunity"
    if score >= 60: return "Tier 2 — Above-Average Market"
    if score >= 45: return "Tier 3 — Speculative / Risk"
    return "Tier 4 — Avoid"
