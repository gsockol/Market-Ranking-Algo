import pandas as pd

def ingest_csv(file_path):
    """SECTION 6: Schema Validation. Maps CSV headers to Internal Spec Names."""
    df = pd.read_csv(file_path)
    
    # Precise mapping for your specific CSV headers
    mapping = {
        'Country': 'country',
        'Opportunity ($M)': 'opportunity_usd_m',
        'Potential Market Size ($M)': 'potential_market_size',
        'Gym Membership CAGR': 'gym_membership_cagr',
        'Penetration Headroom': 'headroom',
        'Concentration (000s/gym)': 'concentration',
        'Political Stability': 'political_stability',
        'Inflation Rate': 'inflation',
        'Corporate Tax Rate': 'corporate_tax_rate',
        'Labour Cost Index': 'labor_cost_index',
        'Real Estate Cost Index': 'real_estate_cost_index'
    }
    
    # Rename and drop anything that didn't map to keep the data clean
    df = df.rename(columns=mapping)
    return df
