import numpy as np
import pandas as pd

def normalize_all(df, weights, inverted_vars, pre_transforms):
    ndf = df.copy()
    
    # Apply Log Transforms to compress massive market gaps (Section 7.3)
    for col in pre_transforms.get("log", []):
        if col in ndf.columns:
            ndf[col] = np.log1p(ndf[col])

    # Rank-based Scoring: 0 to 100 relative to other countries
    for var in weights.keys():
        if var in ndf.columns:
            # Rank the countries (Higher Value = Higher Rank)
            ndf[var] = ndf[var].rank(pct=True) * 100
            
            # Invert if it's a cost or risk (Higher Value = Lower Rank)
            if var in inverted_vars:
                ndf[var] = 100 - ndf[var]
                
    return ndf
