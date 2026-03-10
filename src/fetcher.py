"""
src/fetcher.py (CLEAN VERSION - NO TRADING ECONOMICS)
====================================================
External data retrieval with World Bank primary sourcing.
"""

import logging
import json
import time
from datetime import datetime, timezone
from pathlib import Path
import requests
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

def fetch_all_external_data(countries, country_iso3_map, wb_indicators, oecd_country_codes, te_api_key=None, cache_dir=".cache", ttl_hours=720, no_cache=False, imf_country_codes=None):
    """
    Fetches World Bank data. All Trading Economics logic has been removed 
    to prevent UnboundLocalErrors.
    """
    result = {c: {} for c in countries}
    
    # 1. Unified Batch Fetch for World Bank
    for var_key, indicator in wb_indicators.items():
        try:
            # We fetch all countries in one go to be efficient
            url = f"https://api.worldbank.org/v2/country/all/indicator/{indicator}?format=json&per_page=300&date=2020:2024"
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                data = resp.json()[1]
                # Map results back to our countries
                for entry in data:
                    iso3 = entry['countryiso3code']
                    val = entry['value']
                    # Find which of our countries matches this ISO3
                    for c_name, c_iso in country_iso3_map.items():
                        if c_iso == iso3 and val is not None:
                            if c_name in result and var_key not in result[c_name]:
                                result[c_name][var_key] = val
        except Exception as e:
            logger.error(f"Error fetching WB {var_key}: {e}")

    # 2. Fill Missing GDP CAGR with World Bank GDP Growth
    # (Since we removed TE, we use NY.GDP.MKTP.KD.ZG as the primary proxy)
    for country in countries:
        if "inflation_rate" in result[country]:
             # Just an example of ensuring the dict structure exists
             pass
        
        # Ensure gym_membership_cagr doesn't crash by providing a null if missing
        if "gdp_cagr_proxy" not in result[country]:
            result[country]["gdp_cagr_proxy"] = result[country].get("gdp_growth_forecast", 0.0)

    return result

def compute_financing_scores(raw_data, countries):
    # Simple placeholder to maintain compatibility with main.py
    return {c: {"score": 50.0} for c in countries}
