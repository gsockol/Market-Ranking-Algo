import pandas as pd

def ingest_csv(file_path):
    """Hard-maps CSV headers to ensure no 0.0 results in report."""
    df = pd.read_csv(file_path)
    
    mapping = {
        'Country': 'country',
        'Opportunity ($M)': 'opportunity_usd_m',
        'Potential Market Size ($M)': 'potential_market_size_usd_m',
        'Gym Membership CAGR': 'gym_membership_cagr',
        'Penetration Headroom': 'penetration_headroom',
        'Concentration (000s/gym)': 'concentration_000s_gym',
        'Ease of Doing Business': 'ease_of_doing_business',
        'Political Stability': 'political_stability',
        'Inflation Rate': 'inflation_rate',
        'Currency Volatility': 'currency_volatility',
        'Rule of Law': 'rule_of_law',
        'Ease of Financing (GFDD)': 'ease_of_financing_gfdd',
        'Corporate Tax Rate': 'corporate_tax_rate',
        'Labour Cost Index': 'labour_cost_index',
        'Real Estate Cost Index': 'real_estate_cost_index',
        'Youth / Working Age Population % (15–64)': 'youth_population_pct',
        'Middle Class %': 'middle_class_pct',
        'Avg Gym Spend as % of GDP': 'gym_spend_pct_gdp'
    }
    
    df = df.rename(columns=mapping)
    
    # Clean data: Convert any text placeholders like 'CSV / Derived' to NaN then fill with 0
    cols_to_fix = [c for c in df.columns if c != 'country']
    for col in cols_to_fix:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        
    return df
