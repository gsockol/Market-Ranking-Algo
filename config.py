# config.py - Optimized for HVLP Ranking
WEIGHTS = {
    # Update 1: Consolidated Market Scale (25%)
    'Opportunity ($M)': 0.15,
    'Potential Market Size ($M)': 0.10,
    
    # Update 3: Institutional Risk Consolidation (10%)
    'Ease of Doing Business': 0.03,
    'Political Stability': 0.03,
    'Rule of Law': 0.04,
    
    # Core HVLP Drivers (Increased Weights)
    'Penetration Headroom': 0.15,  # Priority for HVLP
    'Gym Membership CAGR': 0.10,   # Growth focus
    
    # Costs & Demographics (15% & 15%)
    'Real Estate Cost Index': 0.10,
    'Corporate Tax Rate': 0.05,
    'Youth / Working Age Population % (15–64)': 0.10,
    'Middle Class %': 0.05,
    
    # Operations & Macro (10%)
    'Ease of Financing (GFDD)': 0.05,
    'Avg Gym Spend as % of GDP': 0.05
}

# These columns should be ranked ascending (lower value is better)
INVERTED_METRICS = [
    'Corporate Tax Rate', 
    'Real Estate Cost Index', 
    'Inflation Rate', 
    'Currency Volatility'
]
