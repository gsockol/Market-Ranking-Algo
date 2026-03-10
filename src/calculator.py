import pandas as pd

def compute_composite_scores(ndf, weights):
    """
    ENGINEERING SPEC SECTION 10: Vectorized Weighted Aggregation
    """
    score_series = pd.Series(0, index=ndf.index)
    for var, weight in weights.items():
        if var in ndf.columns:
            score_series += ndf[var] * weight
    return score_series

def assign_tiers(score):
    """
    ENGINEERING SPEC SECTION 12: Tier Assignment Logic
    """
    if score >= 75: return "Tier 1: Prime Target"
    if score >= 55: return "Tier 2: Strategic Growth"
    if score >= 40: return "Tier 3: High Risk"
    return "Tier 4: Avoid"

def calculate_derived_metrics(df, dues_increase=0.0):
    """
    ENGINEERING SPEC SECTION 7: Derived Variable Engineering
    """
    df_out = df.copy()
    # 7.1 Penetration Headroom
    if 'future_penetration_pct' in df_out.columns and 'current_penetration_pct' in df_out.columns:
        df_out['headroom'] = df_out['future_penetration_pct'] - df_out['current_penetration_pct']
    
    return df_out
