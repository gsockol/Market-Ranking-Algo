import pandas as pd
import re

def clean_column_name(name):
    name = name.lower().strip()
    name = name.replace('($m)', 'usd_m').replace('$', 'usd').replace(' ', '_')
    name = name.replace('%', 'pct').replace('/', '').replace('(', '').replace(')', '')
    return re.sub(r'_+', '_', name).strip('_')

def ingest_csv(file_path):
    df = pd.read_csv(file_path)
    df.columns = [clean_column_name(c) for c in df.columns]
    
    # FORCE NUMERIC: If data contains text like "CSV / Derived", turn it to 0
    for col in df.columns:
        if col != 'country':
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
    return df
