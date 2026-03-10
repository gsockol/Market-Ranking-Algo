# Synchronized with cleaned CSV headers
WEIGHTS = {
    "opportunity_usd_m": 0.225,
    "potential_market_size_usd_m": 0.125,
    "gym_membership_cagr": 0.05,
    "penetration_headroom": 0.08,
    "concentration_000sgym": 0.02,
    "ease_of_doing_business": 0.05,
    "political_stability": 0.04,
    "inflation_rate": 0.03,
    "currency_volatility": 0.03,
    "rule_of_law": 0.05,
    "ease_of_financing_gfdd": 0.05,
    "corporate_tax_rate": 0.03,
    "labour_cost_index": 0.02,
    "real_estate_cost_index": 0.05,
    "youth_population_pct": 0.05,
    "middle_class_pct": 0.05,
    "fitness_spend_proxy": 0.05
}

INVERTED_VARIABLES = ["corporate_tax_rate", "concentration_000sgym", "labour_cost_index", "real_estate_cost_index", "inflation_rate"]
PRE_TRANSFORMS = {"log": ["opportunity_usd_m", "potential_market_size_usd_m"]}

ISO_MAP = {
    "Austria": "AUT", "Belgium": "BEL", "France": "FRA", "Germany": "DEU",
    "Netherlands": "NLD", "Portugal": "PRT", "Switzerland": "CHE", "Turkiye": "TUR",
    "United Kingdom": "GBR", "Italy": "ITA", "Poland": "POL", "Brazil": "BRA",
    "Chile": "CHL", "Colombia": "COL", "India": "IND", "South Korea": "KOR",
    "Indonesia": "IDN", "Thailand": "THA", "Philippines": "PHL", "Japan": "JPN"
}
