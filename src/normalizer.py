import numpy as np
import pandas as pd

def normalize_all(df, weights, inverted_vars, pre_transforms):
    ndf = df.copy()
    
    # Log Transforms
    for col in pre_transforms.get("log", []):
        if col in ndf.columns:
            ndf[col] = np.log1p(ndf[col].clip(lower=0))

    # Rank-based Normalization (0-100)
    for var in weights.keys():
        if var in ndf.columns:
            # Create a percentile rank (0 to 100)
            ndf[var] = ndf[var].rank(pct=True) * 100
            
            # Invert costs/risks
            if var in inverted_vars:
                ndf[var] = 100 - ndf[var]
                
    return ndf
