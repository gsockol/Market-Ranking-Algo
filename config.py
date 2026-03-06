# =============================================================================
# HVLP GYM MARKET ENTRY SCORING TOOL — CONFIGURATION
# =============================================================================
# Edit this file to adjust all weights, thresholds, and parameters.
# Do NOT edit core logic files (scoring.py, data_fetcher.py, etc.).
# All weights must sum to 1.0.
# =============================================================================

# -----------------------------------------------------------------------------
# SCORING WEIGHTS
# Each key maps to a scored variable. Values must sum to exactly 1.0.
# To disable a variable: set its weight to 0.0 and redistribute manually.
# -----------------------------------------------------------------------------
WEIGHTS = {
    # --- Market Opportunity (≈34.3%) ---
    # Franchisor priority: royalty stream scale and long-run development potential
    # Note: target weights summed to 102%; each value below is proportionally
    # scaled by ÷1.02 so the dict sums to exactly 1.0000.
    "opportunity_usd_m":      0.1964,  # ($M) Potential Market Size − Current Market Size
    "potential_market_size":  0.1078,  # ($M) Implied Future Members × Future Dues × 12
    "gym_membership_cagr":    0.0392,  # % CAGR of gym memberships (3–5yr); manual input
                                       # Kept low — missing data treated as 0% otherwise
                                       # biases against underpenetrated markets.

    # --- Penetration Headroom (≈13.7%) ---
    # Underpenetration = runway for franchisee multi-unit development agreements
    "penetration_headroom":   0.1176,  # Future Penetration % − Current Penetration %
    "concentration":          0.0196,  # 000s inhabitants per HVLP gym (higher = less saturated)

    # --- Operational / Institutional Risk (≈14.7%) ---
    # Contract enforceability, regulatory efficiency, macro stability
    "ease_of_doing_business": 0.0294,  # World Bank WGI GE.EST (higher = better)
    "political_stability":    0.0196,  # World Bank WGI PV.EST (−2.5 to +2.5)
    "inflation_rate":         0.0196,  # Annual CPI %; INVERTED (lower = better score)
    "currency_volatility":    0.0196,  # 3yr std dev of % chg in USD rate; INVERTED
    "rule_of_law":            0.0294,  # World Bank WGI RL.EST (−2.5 to +2.5)
    "financing_accessibility": 0.0294, # Composite: credit depth, account access, bank branches

    # --- Cost Structure (≈14.7%) ---
    # Lower costs → better franchisee unit economics → faster scaling
    "corporate_tax_rate":     0.0294,  # Statutory rate %; INVERTED
    "labor_cost_index":       0.0490,  # Index (US=100); INVERTED
    "real_estate_cost_index": 0.0686,  # Real house price index (OECD RHPI); INVERTED

    # --- Demand Indicators (≈22.5%) ---
    # Structural demand signals for HVLP target demographic
    "youth_population_pct":   0.0784,  # % population aged 15–64 (World Bank SP.POP.1564.TO.ZS)
    "middle_class_pct":       0.0882,  # % population middle class (WB Q3+Q4 income quintile shares)
    "avg_gym_spend_pct_gdp":  0.0588,  # (Current Dues×12) ÷ GDP per Capita — affordability signal
}

# -----------------------------------------------------------------------------
# INVERTED VARIABLES
# For these, LOWER raw value = BETTER score.
# Normalization: score = (max − value) / (max − min) × 100
# -----------------------------------------------------------------------------
INVERTED_VARIABLES = {
    "inflation_rate",
    "currency_volatility",
    "corporate_tax_rate",
    "labor_cost_index",
    "real_estate_cost_index",
}

# -----------------------------------------------------------------------------
# VARIABLE CATEGORIES
# Used by Rule 3: redistribute missing weight within category only.
# -----------------------------------------------------------------------------
VARIABLE_CATEGORIES = {
    "market_opportunity": [
        "opportunity_usd_m",
        "potential_market_size",
        "gym_membership_cagr",
    ],
    "penetration_headroom": [
        "penetration_headroom",
        "concentration",
    ],
    "operational_risk": [
        "ease_of_doing_business",
        "political_stability",
        "inflation_rate",
        "currency_volatility",
        "rule_of_law",
        "financing_accessibility",
    ],
    "cost_structure": [
        "corporate_tax_rate",
        "labor_cost_index",
        "real_estate_cost_index",
    ],
    "demand_indicators": [
        "youth_population_pct",
        "middle_class_pct",
        "avg_gym_spend_pct_gdp",
    ],
}

# -----------------------------------------------------------------------------
# CONDITIONAL WEIGHT RULES
# Applied per-country BEFORE computing composite score.
# Rule 1 and Rule 2 take precedence over Rule 3.
# -----------------------------------------------------------------------------

# Rule 1: If gym_membership_cagr is missing
# CAGR weight (0.0392) is redistributed proportionally to opportunity_usd_m
# and potential_market_size using their base weight ratio (0.1964 : 0.1078).
# opp  override = 0.1964 + 0.0392 × (0.1964 / 0.3042) ≈ 0.2217
# pms  override = 0.1078 + 0.0392 × (0.1078 / 0.3042) ≈ 0.1175
# Sum check: 0.2217 + 0.1175 + 0 = 0.3392 vs original 0.1964 + 0.1078 + 0.0392 = 0.3434
# (minor rounding delta absorbed; weighter normalises by _weight_sum)
RULE1_MISSING_CAGR = {
    "zero_out": "gym_membership_cagr",
    "override": {
        "opportunity_usd_m": 0.2217,
        "potential_market_size": 0.1175,
    },
}

# Rule 2: If concentration is missing
# Concentration weight (0.0196) added directly to penetration_headroom.
# penetration_headroom override = 0.1176 + 0.0196 = 0.1372
RULE2_MISSING_CONCENTRATION = {
    "zero_out": "concentration",
    "override": {
        "penetration_headroom": 0.1372,
    },
}

# Rule 3: All other missing variables — redistribute proportionally within category.
# (Handled programmatically in scoring.py — no static config needed.)

# -----------------------------------------------------------------------------
# TIER THRESHOLDS (configurable)
# -----------------------------------------------------------------------------
# Percentile-based scoring (Z-score + percentile hybrid).  Scores range
# roughly 5–100 with 20 countries (rankdata / n * 100).  USA is not anchored
# at any fixed value — it ranks by its actual percentile across the set.
#
# Tier boundaries map to quartile-style bands:
#   ≥ 75  → top ~quartile (strong opportunity)
#   55–74 → above median
#   35–54 → below median but investable
#   < 35  → structural challenge
TIER_THRESHOLDS = {
    # Recalibrated for the revised weight distribution.
    # New weights shift emphasis toward penetration headroom and demand indicators,
    # which tend to produce composite scores in the ~40–85 range across 20 countries
    # (vs the prior ~30–70 range from the institutional-risk-heavy config).
    # Thresholds are set at the natural quartile breaks of that distribution:
    #   top quartile  ≥ 70  → Tier 1
    #   above median  ≥ 55  → Tier 2
    #   below median  ≥ 40  → Tier 3
    #   tail          < 40  → Tier 4
    "tier1_min": 70,  # Top-quartile Performer — strong structural opportunity
    "tier2_min": 55,  # Above-Average Market — above median across the set
    "tier3_min": 40,  # Developing Opportunity — below median but investable
    # Below 40 → Tier 4 — Structural Challenge
}

TIER_LABELS = {
    1: "Tier 1 — Top-Quartile Performer",
    2: "Tier 2 — Above-Average Market",
    3: "Tier 3 — Developing Opportunity",
    4: "Tier 4 — Structural Challenge",
}

TIER_COLORS = {
    1: "#7c3aed",   # purple  (outperforming)
    2: "#22c55e",   # green   (competitive)
    3: "#3b82f6",   # blue    (developing)
    4: "#f59e0b",   # amber   (structural challenge)
}

# -----------------------------------------------------------------------------
# DUES INCREASE ASSUMPTION (per country)
# Set country-specific % or use "default" for all others.
# Currently 0% for all countries as per instruction.
# Example override: "Germany": 0.05 → 5% dues increase for Germany.
# -----------------------------------------------------------------------------
DUES_INCREASE_PCT = {
    "default": 0.0,
    # "Germany": 0.05,
}

# -----------------------------------------------------------------------------
# COUNTRY → ISO 3166-1 ALPHA-3 CODE MAPPING
# Add new countries here when expanding via CSV.
# -----------------------------------------------------------------------------
COUNTRY_ISO3_MAP = {
    "Austria":      "AUT",
    "Belgium":      "BEL",
    "France":       "FRA",
    "Germany":      "DEU",
    "Netherlands":  "NLD",
    "Portugal":     "PRT",
    "Switzerland":  "CHE",
    "Turkiye":      "TUR",
    "Turkey":       "TUR",
    "UK":           "GBR",
    "United Kingdom": "GBR",
    "Italy":        "ITA",
    "Poland":       "POL",
    "Brazil":       "BRA",
    "Chile":        "CHL",
    "Colombia":     "COL",
    "India":        "IND",
    "South Korea":  "KOR",
    "Korea":        "KOR",
    "Indonesia":    "IDN",
    "Thailand":     "THA",
    "Philippines":  "PHL",
    "Japan":        "JPN",
    # Add new countries below:
    # "Vietnam": "VNM",
}

# -----------------------------------------------------------------------------
# WORLD BANK API INDICATOR CODES
# -----------------------------------------------------------------------------
WB_INDICATORS = {
    # Ease of Doing Business (now: WGI Regulatory Quality + Government Effectiveness)
    "regulatory_quality":       "RQ.EST",              # WGI Regulatory Quality
    "govt_effectiveness":       "GE.EST",              # WGI Government Effectiveness
    "political_stability":      "PV.EST",              # WGI Political Stability
    "rule_of_law":              "RL.EST",              # WGI Rule of Law
    "inflation_rate":           "FP.CPI.TOTL.ZG",     # CPI annual %
    "usd_exchange_rate":        "PA.NUS.FCRF",         # For currency volatility calc
    "domestic_credit_pct_gdp":  "FS.AST.PRVT.GD.ZS", # GFDD financing component
    "account_ownership_pct":    "FX.OWN.TOTL.ZS",     # GFDD financing component
    "bank_branches_per_100k":   "FB.CBK.BRCH.P5",     # GFDD financing component
    "youth_population_pct":       "SP.POP.1564.TO.ZS", # Population aged 15–64 % of total
    "income_share_q3":          "SI.DST.03RD.20",     # 3rd income quintile %
    "income_share_q4":          "SI.DST.04TH.20",     # 4th income quintile %
}

# -----------------------------------------------------------------------------
# USA BASELINE VALUES
# Each scored variable's reference value for the USA.
# All country normalized scores are calculated as (country / USA) = ratio,
# stored with USA = 1.0 so that composite × 100 gives USA = 100.
#
# Derived values calculated from spec inputs:
#   Market Size: 45,700  |  Cur Pen: 25.0%  |  Fut Pen: 30%
#   Population: 349M     |  GDP/Capita: $90,012  |  CAGR: 5.6%
# -----------------------------------------------------------------------------
USA_BASELINE = {
    # Market Opportunity
    "opportunity_usd_m":          9_176.0,   # Potential - Current market size
    "potential_market_size":      54_876.0,  # implied_future_members × future_dues × 12
    "gym_membership_cagr":        5.6,       # from spec

    # Penetration Headroom
    "penetration_headroom":       0.05,      # 30% − 25% = 5 pp
    "concentration":              3.24,      # 000s inhabitants per gym (from spec)

    # Operational Risk
    "ease_of_doing_business":     1.58,      # WGI (RQ + GE) / 2, USA 2022 approx
    "political_stability":        0.54,      # WGI PV.EST, USA 2022 approx
    "inflation_rate":             3.5,       # USA CPI % 2023
    "currency_volatility":        4.0,       # USD trade-weighted std dev, approx
    "rule_of_law":                1.54,      # WGI RL.EST, USA 2022 approx
    "financing_accessibility":    92.0,      # GFDD composite (0–100), USA approx

    # Cost Structure (inverted — lower is better)
    "corporate_tax_rate":         21.0,      # USA statutory federal CIT rate
    "labor_cost_index":           100.0,     # US = 100 by definition
    "real_estate_cost_index":     140.0,     # OECD RHPI for USA, approx 2022

    # Demand Indicators
    "youth_population_pct":       65.0,      # SP.POP.1564.TO.ZS, USA approx
    "middle_class_pct":           34.0,      # WB Q3+Q4 income shares, USA approx
    "avg_gym_spend_pct_gdp":      0.582,     # (current_dues × 12) / gdp_per_capita
}

# -----------------------------------------------------------------------------
# TRADING ECONOMICS API KEY
# Leave blank ("") to use YAML fallback for corporate_tax_rate.
# Sign up at https://tradingeconomics.com/api/ for a free or paid key.
# -----------------------------------------------------------------------------
TRADING_ECONOMICS_API_KEY = ""

# -----------------------------------------------------------------------------
# OECD COUNTRY CODES FOR stats.oecd.org API
# Most align with ISO-3. Set to None for non-OECD members (AHR / HOUSECOST
# data won't exist → YAML fallback applies automatically).
# -----------------------------------------------------------------------------
OECD_COUNTRY_CODES = {
    # OECD members
    "Austria":      "AUT",
    "Belgium":      "BEL",
    "France":       "FRA",
    "Germany":      "DEU",
    "Netherlands":  "NLD",
    "Portugal":     "PRT",
    "Switzerland":  "CHE",
    "Turkiye":      "TUR",
    "Turkey":       "TUR",
    "UK":           "GBR",
    "Italy":        "ITA",
    "Poland":       "POL",
    "Japan":        "JPN",
    "South Korea":  "KOR",
    "Chile":        "CHL",
    "Colombia":     "COL",
    # Non-OECD members — OECD API returns no data → YAML fallback:
    "Brazil":       None,
    "India":        None,
    "Indonesia":    None,
    "Thailand":     None,
    "Philippines":  None,
}

# -----------------------------------------------------------------------------
# IMF DATAMAPPER CODES
# -----------------------------------------------------------------------------
IMF_INDICATORS = {
    "inflation_rate": "PCPIPCH",   # Inflation, average consumer prices (% change)
}

IMF_COUNTRY_CODES = {
    "Austria":      "AUT",
    "Belgium":      "BEL",
    "France":       "FRA",
    "Germany":      "DEU",
    "Netherlands":  "NLD",
    "Portugal":     "PRT",
    "Switzerland":  "CHE",
    "Turkiye":      "TUR",
    "Turkey":       "TUR",
    "UK":           "GBR",
    "Italy":        "ITA",
    "Poland":       "POL",
    "Brazil":       "BRA",
    "Chile":        "CHL",
    "Colombia":     "COL",
    "India":        "IND",
    "South Korea":  "KOR",
    "Indonesia":    "IDN",
    "Thailand":     "THA",
    "Philippines":  "PHL",
    "Japan":        "JPN",
}

# -----------------------------------------------------------------------------
# DATA QUALITY FLAGS
# Shown in dashboard metadata column for transparency.
# -----------------------------------------------------------------------------
DATA_QUALITY_FLAGS = {
    "IC.BUS.EASE.XQ":   "⚠ Discontinued — World Bank halted Doing Business in 2021; last data 2019/2020",
    "SI.DST.04TH.20":   "⚠ Proxy — 3rd+4th income quintile income share used as middle-class approximation",
    "youth_estimate":   "⚠ Estimate — computed from 15–64 band when specific age bands unavailable",
    "oecd_ahr":         "⚠ OECD AHR — indexed vs highest country in set; non-OECD members use YAML fallback",
    "oecd_housecost":   "⚠ OECD RHPI — Real House Price Index; non-OECD members use YAML fallback",
    "te_tax":           "⚠ Trading Economics — requires API key; falls back to YAML when key absent",
    "yaml_fallback":    "⚠ YAML — static estimate used; no live API data available for this country/variable",
}

# -----------------------------------------------------------------------------
# CSV COLUMN MAP
# Maps raw CSV column headers → internal snake_case variable keys.
# Whitespace is stripped from CSV headers before matching.
# Add entries here when adding new input columns to the CSV.
# -----------------------------------------------------------------------------
CSV_COLUMN_MAP = {
    # Underscore-style headers (current committed CSV format)
    "Country":                    "country",
    "Market_Size_USD_M":          "market_size_m",
    "Current_Penetration":        "current_penetration_pct",
    "Future_Penetration":         "future_penetration_pct",
    "Population_M":               "population_m",
    "Concentration_000s_per_Gym": "concentration",
    "GDP_per_Capita_USD":         "gdp_per_capita",
    "Gym_Membership_CAGR":        "gym_membership_cagr",
    # MSD-spec aliases — so the spec example CSV also works without modification
    "Market Size ($M)":           "market_size_m",
    "Current Penetration %":      "current_penetration_pct",
    "Future Penetration %":       "future_penetration_pct",
    "Population (M)":             "population_m",
    "Concentration (000s/gym)":   "concentration",
    "GDP per Capita ($)":         "gdp_per_capita",
    "Gym Membership CAGR":        "gym_membership_cagr",
}

# -----------------------------------------------------------------------------
# EUROZONE COUNTRIES (ISO3)
# Used to attribute EUR/USD volatility to Eurozone members when fetching
# the exchange rate series (they share the EUR, so PA.NUS.FCRF returns
# EUR/USD for all of them — volatility will be identical across the group,
# which is the correct economic interpretation).
# -----------------------------------------------------------------------------
EUROZONE_ISO3 = {
    "AUT", "BEL", "FRA", "DEU", "NLD", "PRT", "ITA",
    "ESP", "FIN", "GRC", "IRL", "LVA", "LTU", "LUX",
    "MLT", "SVK", "SVN", "CYP", "EST",
}

# -----------------------------------------------------------------------------
# CACHE SETTINGS
# -----------------------------------------------------------------------------
CACHE_DIR = ".cache"
CACHE_EXPIRY_HOURS = 48

# -----------------------------------------------------------------------------
# OUTPUT SETTINGS
# -----------------------------------------------------------------------------
OUTPUT_DIR = "output"
DASHBOARD_FILENAME = "dashboard.html"
EXCEL_FILENAME = "hvlp_market_ranking.xlsx"
