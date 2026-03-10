# config.py

# Optimized Weights for HVLP (Updates 1 & 3 applied)
# Scale reduced to 25%, Institutional reduced to 10%, Headroom & CAGR boosted.
WEIGHTS = {
    # Update 1: Consolidated Market Scale (25%)
    'Opportunity ($M)': 0.15,
    'Potential Market Size ($M)': 0.10,
    
    # Update 3: Institutional Risk Consolidation (10%)
    'Ease of Doing Business': 0.03,
    'Political Stability': 0.03,
    'Rule of Law': 0.04,
    
    # Growth & Strategy (Boosted)
    'Penetration Headroom': 0.15,
    'Gym Membership CAGR': 0.10,
    'Real Estate Cost Index': 0.07,
    
    # Secondary Indicators (Kept at current values)
    'Youth / Working Age Population % (15–64)': 0.05,
    'Middle Class %': 0.05,
    'Ease of Financing (GFDD)': 0.05,
    'Avg Gym Spend as % of GDP': 0.05,
    'Inflation Rate': 0.03,
    'Currency Volatility': 0.03,
    'Corporate Tax Rate': 0.03,
    'Labour Cost Index': 0.02,
    'Concentration (000s/gym)': 0.02
}

# Values where LOWER is BETTER
INVERTED_METRICS = [
    'Corporate Tax Rate', 
    'Real Estate Cost Index', 
    'Inflation Rate', 
    'Currency Volatility',
    'Labour Cost Index'
]
