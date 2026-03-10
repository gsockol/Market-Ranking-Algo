import requests
import logging

logger = logging.getLogger(__name__)

def fetch_all_external_data(countries, country_iso3_map, wb_indicators, oecd_country_codes=None, te_api_key=None, cache_dir=None, ttl_hours=None, no_cache=False, imf_country_codes=None):
    """
    ENGINEERING SPEC SECTION 5: Deterministic Macro Data Retrieval
    """
    result = {c: {} for c in countries}
    
    # --- ENGINE 1: WORLD BANK (Primary) ---
    for var_key, indicator in wb_indicators.items():
        try:
            url = f"https://api.worldbank.org/v2/country/all/indicator/{indicator}?format=json&per_page=500&date=2021:2024"
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200 and len(resp.json()) > 1:
                data = resp.json()[1]
                if data:
                    for entry in data:
                        iso3 = entry.get('countryiso3code')
                        val = entry.get('value')
                        for c_name, c_iso in country_iso3_map.items():
                            if c_iso == iso3 and val is not None and var_key not in result[c_name]:
                                result[c_name][var_key] = val
        except Exception as e:
            logger.warning(f"World Bank fetch failed for {var_key}: {e}")

    # --- ENGINE 2: IMF FALLBACK (Section 5 - Reliability) ---
    # Fills in GDP and Inflation if World Bank is down
    imf_indicators = {"gdp_growth_forecast": "NGDP_RPCH", "inflation_rate": "PCPIPIPCH"}
    for country in countries:
        iso3 = country_iso3_map.get(country)
        for var_key, imf_code in imf_indicators.items():
            if var_key not in result[country] or result[country][var_key] is None:
                try:
                    imf_url = f"https://www.imf.org/external/datamapper/api/v1/{imf_code}/{iso3}"
                    imf_resp = requests.get(imf_url, timeout=10)
                    if imf_resp.status_code == 200:
                        data = imf_resp.json().get('values', {}).get(imf_code, {}).get(iso3, {})
                        if data:
                            latest_year = sorted(data.keys())[-1]
                            result[country][var_key] = data[latest_year]
                except:
                    continue

    # --- ENGINE 3: HARD-CODED DEFAULTS (Section 16 - Failure Mode Protection) ---
    defaults = {"corporate_tax_rate": 25.0, "political_stability": 0.0, "inflation_rate": 3.0}
    for country in countries:
        for key, val in defaults.items():
            if key not in result[country] or result[country][key] is None:
                result[country][key] = val
                
    return result
