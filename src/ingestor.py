import pandas as pd
import re

def clean_name(name):
    """Utility to turn 'Opportunity ($M)' into 'opportunity_usd_m'"""
    name = name.lower().strip()
    name = name.replace('($m)', 'usd_m').replace('$', 'usd').replace(' ', '_')
    name = re.sub(r'[^a-z0-9_]', '', name)
    return name

def ingest_csv(file_path):
    """SECTION 6: Schema Validation & Robust Mapping"""
    df = pd.read_csv(file_path)
    
    # 1. Strip and Clean Column Names
    raw_to_clean = {col: clean_name(col) for col in df.columns}
    df = df.rename(columns=raw_to_clean)
    
    # 2. Hard-Mapping for Spec Compliance (Section 4)
    spec_mapping = {
        'opportunity_usdm': 'opportunity_usd_m',
        'potential_market_size_usdm': 'potential_market_size',
        'penetration_headroom': 'headroom',
        'concentration_000sgym': 'concentration',
        'inflation_rate': 'inflation',
        'ease_of_financing_gfdd': 'financing_access',
        'labour_cost_index': 'labor_cost_index',
        'youth__working_age_population__1564': 'youth_population_pct',
        'middle_class_': 'middle_class_pct',
        'avg_gym_spend_as__of_gdp': 'fitness_spend_proxy'
    }
    
    # Apply spec mapping if those cleaned names exist
    df = df.rename(columns={k: v for k, v in spec_mapping.items() if k in df.columns})
    
    # Force 'country' column
    country_cols = [c for c in df.columns if 'country' in c]
    if country_cols:
        df = df.rename(columns={country_cols[0]: 'country'})
    
    return df
