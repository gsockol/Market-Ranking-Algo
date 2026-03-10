import requests
import logging

def fetch_all_external_data(countries, country_iso3_map, wb_indicators):
    """SECTION 5: Data Sources (World Bank & IMF)"""
    result = {c: {} for c in countries}
    
    for var_key, indicator in wb_indicators.items():
        try:
            url = f"https://api.worldbank.org/v2/country/all/indicator/{indicator}?format=json&per_page=500&date=2021:2024"
            resp = requests.get(url, timeout=10).json()
            if len(resp) > 1 and resp[1]:
                for entry in resp[1]:
                    iso3 = entry.get('countryiso3code')
                    val = entry.get('value')
                    for c_name, c_iso in country_iso3_map.items():
                        if c_iso == iso3 and val is not None and var_key not in result[c_name]:
                            result[c_name][var_key] = val
        except: continue

    # Hardcoded Fallbacks for missing API data
    for country in countries:
        if "corporate_tax_rate" not in result[country]: result[country]["corporate_tax_rate"] = 25.0
        if "political_stability" not in result[country]: result[country]["political_stability"] = 0.0
            
    return result
