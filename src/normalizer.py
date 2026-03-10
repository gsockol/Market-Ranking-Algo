import numpy as np
import pandas as pd

def normalize_all(df, weights, inverted_vars, pre_transforms):
    """
    SECTION 8: Normalization Engine
    Pipeline: Log/Sqrt -> Z-score -> Percentile -> Inversion
    """
    ndf = df.copy()
    
    # 8.3: Pre-Transforms (TAM Compression & Headroom)
    for col in pre_transforms.get("log", []):
        if col in ndf.columns:
            ndf[col] = np.log1p(ndf[col].clip(lower=0))
            
    for col in pre_transforms.get("sqrt", []):
        if col in ndf.columns:
            ndf[col] = np.sqrt(ndf[col].clip(lower=0))

    # 8.4 & 8.5: Z-Score and Percentile Mapping (0-100)
    for var in weights.keys():
        if var in ndf.columns:
            # Handle concentration inversion logic (Section 7.2)
            if var == 'concentration':
                ndf[var] = np.log1p(ndf[var].clip(lower=0.1))
            
            # Z-Score
            mu, sigma = ndf[var].mean(), ndf[var].std()
            if sigma == 0 or pd.isna(sigma): sigma = 1
            z = (ndf[var] - mu) / sigma
            
            # Percentile Mapping (Deterministic)
            ndf[var] = z.rank(pct=True) * 100
            
            # 8.6: Inversion (Section 8.6)
            if var in inverted_vars:
                ndf[var] = 100 - ndf[var]
                
    return ndf
