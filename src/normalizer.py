import numpy as np
import pandas as pd

def normalize_all(df, weights, inverted_vars, pre_transforms):
    """SECTION 8: Normalization Engine"""
    ndf = df.copy()
    
    # 8.3 Pre-Transforms (Section 7.3 Log Compression)
    for col in pre_transforms.get("log", []):
        if col in ndf.columns:
            ndf[col] = np.log1p(ndf[col].clip(lower=0))

    # 8.4 & 8.5 Z-Score & Percentile Scaling
    for var in weights.keys():
        if var in ndf.columns:
            # Z-Score
            mu, sigma = ndf[var].mean(), ndf[var].std()
            if sigma == 0 or pd.isna(sigma): sigma = 1
            z = (ndf[var] - mu) / sigma
            
            # Map to 0-100 Score
            ndf[var] = z.rank(pct=True) * 100
            
            # 8.6 Inversion (High Tax = Low Score)
            if var in inverted_vars:
                ndf[var] = 100 - ndf[var]
                
    return ndf
