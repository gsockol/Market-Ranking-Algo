# =============================================================================
# HVLP GYM MARKET ENTRY SCORING TOOL — CONFIGURATION
# =============================================================================
# Edit this file to adjust all weights, thresholds, and parameters.
# Do NOT edit core logic files (scoring.py, data_fetcher.py, etc.).
# All weights must sum to 1.0.
# =============================================================================

import os

# -----------------------------------------------------------------------------
# SCORING WEIGHTS
# Each key maps to a scored variable. Values must sum to exactly 1.0.
# To disable a variable: set its weight to 0.0 and redistribute manually.
# -----------------------------------------------------------------------------
WEIGHTS = {
    # --- Market Opportunity (46%) ---
    "opportunity_usd_m":        0.28,   # ($M) Potential Market Size − Current Market Size
    "potential_market_size":    0.18,   # ($M) Implied Future Members × Future Dues × 12

    # --- Penetration / Membership (24%) ---
    "gym_membership_cagr":      0.08,   # GDP_CAGR × 1.4; sourced TE → IMF → WB; never zero
    "penetration_headroom":     0.12,   # Future Penetration % − Current Penetration %
    "concentration":            0.04,   # Market concentration; 5%→4% (offset for agility bonus)

    # --- Demand Indicators (8%) ---
    "youth_population_pct":     0.02,   # % population aged 15–64; 3%→2% (offset for agility bonus)
    "middle_class_pct":         0.04,   # % population middle class — primary demand signal
    "avg_gym_spend_pct_gdp":    0.02,   # (Current Dues×12) ÷ GDP per Capita — affordability

    # --- Cost Structure (8%) ---
    "operating_cost_composite": 0.06,   # inverted_minmax(labor×0.6 + real_estate×0.4); replaces individual vars
    "corporate_tax_rate":       0.02,   # Statutory CIT rate %; INVERTED (lower = better)

    # --- Market Agility (2%) ---
    "market_agility_bonus":     0.02,   # 1/sqrt(potential_market_size) — rewards compact markets

    # --- Operational Risk (12%) ---
    "ease_of_doing_business":   0.03,   # WGI GE.EST + RQ.EST avg (higher = better)
    "political_stability":      0.01,   # World Bank WGI PV.EST (higher = better)
    "rule_of_law":              0.02,   # World Bank WGI RL.EST (−2.5 to +2.5)
    "inflation_rate":           0.01,   # Annual CPI %; INVERTED (lower = better score)
    "currency_volatility":      0.01,   # CoV of LCU/USD exchange rate; INVERTED
    "financing_accessibility":  0.04,   # Composite: credit depth, account access, bank branches
}
# sum = 0.28+0.18+0.08+0.12+0.04+0.02+0.04+0.02+0.06+0.02+0.02+0.03+0.01+0.02+0.01+0.01+0.04
#     = 1.0000 exactly ✓  (17 variables)
# Weight changes vs prior:
#   -0.03 labor_cost_index (removed — folded into operating_cost_composite)
#   -0.03 real_estate_cost_index (removed — folded into operating_cost_composite)
#   +0.06 operating_cost_composite (new composite at same total cost-structure weight)
#   +0.02 market_agility_bonus (new; funded by -0.01 concentration, -0.01 youth_population_pct)

# -----------------------------------------------------------------------------
# INVERTED VARIABLES
# For these, LOWER raw value = BETTER score.
# Normalization: score = (max − value) / (max − min) × 100
# -----------------------------------------------------------------------------
INVERTED_VARIABLES = {
    "inflation_rate",
    "currency_volatility",
    "corporate_tax_rate",
    # labor_cost_index and real_estate_cost_index removed — folded into
    # operating_cost_composite, which is already inverted in its formula
    # (higher composite score = cheaper operating cost = better).
}

# -----------------------------------------------------------------------------
# VARIABLE CATEGORIES
# Used by Rule 3: redistribute missing weight within category only.
# -----------------------------------------------------------------------------
VARIABLE_CATEGORIES = {
    "market_opportunity": [
        "opportunity_usd_m",
        "potential_market_size",
    ],
    "penetration_membership": [
        # gym_membership_cagr moved here (Rule 1 already handles it; Rule 3
        # redistribution stays within this category for any other edge cases)
        "gym_membership_cagr",
        "penetration_headroom",
        "concentration",
    ],
    "demand_indicators": [
        "youth_population_pct",
        "middle_class_pct",
        "avg_gym_spend_pct_gdp",
    ],
    "cost_structure": [
        "operating_cost_composite",   # replaces labor_cost_index + real_estate_cost_index
        "corporate_tax_rate",
    ],
    "market_agility": [
        "market_agility_bonus",
    ],
    "operational_risk": [
        "ease_of_doing_business",
        "political_stability",
        "rule_of_law",
        "inflation_rate",
        "currency_volatility",
        "financing_accessibility",
    ],
}

# -----------------------------------------------------------------------------
# CONDITIONAL WEIGHT RULES
# Applied per-country BEFORE computing composite score.
# Rule 1 and Rule 2 take precedence over Rule 3.
# -----------------------------------------------------------------------------

# Rule 1: If gym_membership_cagr is missing
# cagr weight (0.08) redistributed proportionally to remaining demand indicators.
# remaining demand = youth(0.02) + middle_class(0.04) + avg_gym_spend(0.02) = 0.08
# youth_override     = 0.02 + 0.08×(2/8)  = 0.0400
# middle_override    = 0.04 + 0.08×(4/8)  = 0.0800
# avg_spend_override = 0.02 + 0.08×(2/8)  = 0.0400   (sum = 0.16 ✓)
# NOTE: CAGR is now sourced from GDP_CAGR×1.4; Rule 1 triggers only when all
#       GDP sources (TE, IMF, WB) and manual overrides are unavailable.
RULE1_MISSING_CAGR = {
    "zero_out": "gym_membership_cagr",
    "override": {
        "youth_population_pct":  0.04,
        "middle_class_pct":      0.08,
        "avg_gym_spend_pct_gdp": 0.04,
    },
}

# Rule 2: If concentration is missing
# concentration weight (0.04) added to penetration_headroom: 0.12 + 0.04 = 0.16
RULE2_MISSING_CONCENTRATION = {
    "zero_out": "concentration",
    "override": {
        "penetration_headroom": 0.16,
    },
}

# Rule 3: All other missing variables — redistribute proportionally within category.
# (Handled programmatically in scoring.py — no static config needed.)

# -----------------------------------------------------------------------------
# OUTLIER CAPPING
# Applied to high-skew market-size variables before Z-scoring so that the
# two or three largest absolute markets do not anchor the entire percentile
# scale.  Only the upper tail is Winsorized; the lower tail is unchanged.
# This narrows the spread at the top without altering any country's rank
# among the non-capped majority.
# -----------------------------------------------------------------------------
OUTLIER_CAP_VARIABLES  = {"opportunity_usd_m", "potential_market_size", "concentration"}
OUTLIER_CAP_PERCENTILE = 0.95   # p95 cap (was 0.90); concentration added — caps South Korea
                                 # before log transform so it doesn't anchor the scale

# -----------------------------------------------------------------------------
# PRE-NORMALIZATION TRANSFORMS
# Applied AFTER outlier capping (Step 2), BEFORE Z-scoring (Step 4).
# "log"  : np.log — compresses right-skewed distributions (TAM, concentration)
# "sqrt" : np.sqrt — partial compression for early-adoption curves (headroom)
# Values must be > 0; normalizer applies a shift + 1e-9 epsilon automatically.
# See normalizer.py for the authoritative six-step pipeline order.
# -----------------------------------------------------------------------------
PRE_TRANSFORMS = {
    "concentration":         "log",   # p95-cap then log → South Korea doesn't anchor scale
    "penetration_headroom":  "sqrt",  # nonlinear early-adoption reward curve
    "opportunity_usd_m":     "log",   # prevents large-market absolute distortion
    "potential_market_size": "log",   # prevents large-market absolute distortion
}

# -----------------------------------------------------------------------------
# COMPOSITE INPUT VARIABLES
# Variables merged from YAML/API sources but NOT individually scored.
# Required as raw inputs to calculate_composite_variables() in calculator.py.
# Must be included in the merge_overrides() call alongside scored_variables.
# -----------------------------------------------------------------------------
COMPOSITE_INPUT_VARIABLES = [
    "labor_cost_index",        # input to operating_cost_composite (weight 60%)
    "real_estate_cost_index",  # input to operating_cost_composite (weight 40%)
]

# -----------------------------------------------------------------------------
# TWO-SIDED PERCENTILE CLIPPING VARIABLES  (Step 1 of normalization pipeline)
# Raw values clipped to [p05, p95] across all countries BEFORE Z-scoring.
# Prevents extreme emerging-market outliers (e.g. Turkiye inflation 53.9%,
# Colombia political_stability -1.57) from compressing peer country rankings.
# Applied before Winsorization (Step 2) and pre_transforms (Step 3).
# -----------------------------------------------------------------------------
CLIP_P05P95_VARIABLES = {
    "political_stability",
    "currency_volatility",
    "rule_of_law",
    "inflation_rate",
}

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
    "Austria":        "AUT",
    "Belgium":        "BEL",
    "Brazil":         "BRA",
    "Chile":          "CHL",
    "Colombia":       "COL",
    "France":         "FRA",
    "Germany":        "DEU",
    "India":          "IND",
    "Indonesia":      "IDN",
    "Italy":          "ITA",
    "Japan":          "JPN",
    "Netherlands":    "NLD",
    "Philippines":    "PHL",
    "Poland":         "POL",
    "Portugal":       "PRT",
    "South Korea":    "KOR",
    "Switzerland":    "CHE",
    "Thailand":       "THA",
    "Turkiye":        "TUR",
    "United Kingdom": "GBR",
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

    # Cost Structure
    "corporate_tax_rate":         21.0,      # USA statutory federal CIT rate; INVERTED
    "operating_cost_composite":   0.40,      # US is mid-range on both labor and real estate
    "market_agility_bonus":       0.004,     # 1/sqrt(54876) ≈ 0.004; large market = low bonus

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
TRADING_ECONOMICS_API_KEY = os.environ.get("TRADING_ECONOMICS_API_KEY", "")

# -----------------------------------------------------------------------------
# OECD COUNTRY CODES FOR stats.oecd.org API
# Most align with ISO-3. Set to None for non-OECD members (AHR / HOUSECOST
# data won't exist → YAML fallback applies automatically).
# -----------------------------------------------------------------------------
OECD_COUNTRY_CODES = {
    # OECD members
    "Austria":        "AUT",
    "Belgium":        "BEL",
    "Chile":          "CHL",
    "Colombia":       "COL",
    "France":         "FRA",
    "Germany":        "DEU",
    "Italy":          "ITA",
    "Japan":          "JPN",
    "Netherlands":    "NLD",
    "Poland":         "POL",
    "Portugal":       "PRT",
    "South Korea":    "KOR",
    "Switzerland":    "CHE",
    "Turkiye":        "TUR",
    "United Kingdom": "GBR",
    # Non-OECD members — OECD API returns no data → YAML fallback:
    "Brazil":         None,
    "India":          None,
    "Indonesia":      None,
    "Philippines":    None,
    "Thailand":       None,
}

# -----------------------------------------------------------------------------
# IMF DATAMAPPER CODES
# -----------------------------------------------------------------------------
IMF_INDICATORS = {
    "inflation_rate": "PCPIPCH",   # Inflation, average consumer prices (% change)
    "gdp_growth":     "NGDP_RPCH", # Real GDP growth (% change) — used as gym CAGR source
}

IMF_COUNTRY_CODES = {
    "Austria":        "AUT",
    "Belgium":        "BEL",
    "Brazil":         "BRA",
    "Chile":          "CHL",
    "Colombia":       "COL",
    "France":         "FRA",
    "Germany":        "DEU",
    "India":          "IND",
    "Indonesia":      "IDN",
    "Italy":          "ITA",
    "Japan":          "JPN",
    "Netherlands":    "NLD",
    "Philippines":    "PHL",
    "Poland":         "POL",
    "Portugal":       "PRT",
    "South Korea":    "KOR",
    "Switzerland":    "CHE",
    "Thailand":       "THA",
    "Turkiye":        "TUR",
    "United Kingdom": "GBR",
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
    "GDP_CAGR":                   "gdp_cagr_csv",          # Local CSV fallback for gym CAGR
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
CACHE_EXPIRY_HOURS = 720   # 30 days

# -----------------------------------------------------------------------------
# OUTPUT SETTINGS
# -----------------------------------------------------------------------------
OUTPUT_DIR = "output"
DASHBOARD_FILENAME = "dashboard.html"
EXCEL_FILENAME = "hvlp_market_ranking.xlsx"
