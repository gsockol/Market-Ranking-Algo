import pandas as pd
import logging

def ingest_csv(file_path):
    """
    SECTION 6: Data Loading & Schema Validation
    Maps raw CSV headers to internal system variables.
    """
    df = pd.read_csv(file_path)
    
    # Header Normalization
    df.columns = [c.strip() for c in df.columns]
    
    # Internal Mapping Table (Maps your CSV to Spec Section 4)
    mapping = {
        'Country': 'country',
        'Opportunity ($M)': 'opportunity_usd_m',
        'Potential Market Size ($M)': 'potential_market_size',
        'Gym Membership CAGR': 'gym_membership_cagr',
        'Penetration Headroom': 'headroom',
        'Concentration (000s/gym)': 'concentration',
        'Ease of Doing Business': 'ease_of_doing_business',
        'Political Stability': 'political_stability',
        'Inflation Rate': 'inflation',
        'Currency Volatility': 'currency_volatility',
        'Rule of Law': 'rule_of_law',
        'Ease of Financing (GFDD)': 'financing_access',
        'Corporate Tax Rate': 'corporate_tax_rate',
        'Labour Cost Index': 'labor_cost_index',
        'Real Estate Cost Index': 'real_estate_cost_index',
        'Youth / Working Age Population % (15–64)': 'youth_population_pct',
        'Middle Class %': 'middle_class_pct',
        'Avg Gym Spend as % of GDP': 'fitness_spend_proxy'
    }
    
    df = df.rename(columns=mapping)
    
    # Validate Required Primary Key
    if 'country' not in df.columns:
        raise KeyError("REQUIRED COLUMN MISSING: Could not find 'Country' in CSV.")
        
    return df
