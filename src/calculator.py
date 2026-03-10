import pandas as pd
import numpy as np

def calculate_derived_metrics(df):
    df_out = df.copy()
    # Market Agility Bonus (Section 7.5)
    if 'potential_market_size_usd_m' in df_out.columns:
        df_out['agility_bonus'] = 1 / np.sqrt(df_out['potential_market_size_usd_m'].clip(lower=1))
    return df_out

def compute_scores(ndf, weights):
    # Ensure we only multiply variables that exist
    score = pd.Series(0.0, index=ndf.index)
    for var, weight in weights.items():
        if var in ndf.columns:
            score += ndf[var] * weight
    return score

def assign_tier(score):
    if score >= 70: return "Tier 1 — High Opportunity"
    if score >= 55: return "Tier 2 — Strategic Growth"
    if score >= 40: return "Tier 3 — Speculative / Risk"
    return "Tier 4 — Avoid"
