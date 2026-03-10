import numpy as np
import pandas as pd

def normalize_pipeline(df, weights, inverted_vars, pre_transforms):
    ndf = df.copy()
    
    # Step 1: Pre-Transforms (Section 7 & 8.3)
    for col in pre_transforms.get("log", []):
        if col in ndf.columns:
            ndf[col] = np.log1p(ndf[col])
            
    for col in pre_transforms.get("sqrt", []):
        if col in ndf.columns:
            ndf[col] = np.sqrt(ndf[col])

    # Step 2: Z-Score & Percentile Mapping (Section 8.4 & 8.5)
    for var in weights.keys():
        if var in ndf.columns:
            mu, sigma = ndf[var].mean(), ndf[var].std()
            if sigma == 0: sigma = 1
            z = (ndf[var] - mu) / sigma
            # Percentile Mapping
            ndf[var] = (z.rank(pct=True) * 100)
            
            # Step 3: Inversion (Section 8.6)
            if var in inverted_vars:
                ndf[var] = 100 - ndf[var]
                
    return ndf
