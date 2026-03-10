# --- config.py Simulation ---
import numpy as np

WEIGHTS = {
    'market_scale': 0.25,        # Consolidated Opportunity + Potential Size
    'penetration_headroom': 0.15, 
    'institutional_risk': 0.10,  # Consolidated Governance + Stability
    'gym_membership_cagr': 0.10,
    'operating_cost_composite': 0.15, # Labor (0.6) + Real Estate (0.4)
    'youth_population_pct': 0.10,
    'middle_class_pct': 0.10,
    'fitness_spend_proxy': 0.05
}

# Metrics where LOWER value = BETTER score
INVERTED_VARIABLES = [
    'labor_cost_index', 
    'real_estate_cost_index', 
    'inflation_rate', 
    'currency_volatility',
    'corporate_tax_rate'
]

# Feature Engineering Transforms
PRE_TRANSFORMS = {
    "log": ["opportunity_usd_m", "potential_market_size", "concentration"],
    "sqrt": ["penetration_headroom"]
}
