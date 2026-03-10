# =============================================================================
# HVLP GYM MARKET ENTRY SCORING TOOL — CONFIGURATION
# =============================================================================
# MODEL STATUS: v1.1 (Institutional Magnitude & Proxy Update)
# Verification Passed:
#   * Magnitude Scaling enabled for TAM/Opportunity
#   * Rule 0 Proxy logic active (Tax anchors Operating Costs)
#   * Log-transforms synchronized with Normalizer v1.1
# =============================================================================

import os

# -----------------------------------------------------------------------------
# 1. SCORING WEIGHTS (Must sum to 1.0)
# -----------------------------------------------------------------------------
WEIGHTS = {
    # --- Market Opportunity (46%) ---
    "opportunity_usd_m":        0.28,   # ($M) Potential Market Size − Current Market Size
    "potential_market_size":    0.18,   # ($M) Implied Future Members × Future Dues × 12

    # --- Penetration / Membership (24%) ---
    "gym_membership_cagr":      0.08,   # Growth Trend
    "penetration_headroom":     0.12,   # Future % − Current %
    "concentration":            0.04,   # Market Saturation (000s inhabitants/gym)

    # --- Demand Indicators (8%) ---
    "youth_population_pct":     0.02,   # Demographics
    "middle_class_pct":         0.04,   # Affordability Proxy
    "avg_gym_spend_pct_gdp":    0.02,   # Wallet Share

    # --- Cost Structure (8%) ---
    "operating_cost_composite": 0.06,   # Composite Labor/RE
    "corporate_tax_rate":       0.02,   # Statutory Rate (Inverted)

    # --- Market Agility (2%) ---
    "market_agility_bonus":     0.02,   # Small-market agility factor

    # --- Operational Risk (12%) ---
    "ease_of_doing_business":   0.03,
    "political_stability":      0.01,
    "rule_of_law":              0.02,
    "inflation_rate":           0.01,   # Inverted
    "currency_volatility":      0.01,   # Inverted
    "financing_accessibility":  0.04,
}

# -----------------------------------------------------------------------------
# 2. NORMALIZATION & MAGNITUDE LOGIC (New in v1.1)
# -----------------------------------------------------------------------------

# Variables where the "Gap" in size matters (Magnitude) vs. just the rank.
# These use Min-Max scaling
