"""
src/commentary.py
=================
Generates per-country qualitative commentary bullets for the dashboard.

Commentary is purely deterministic — derived from scored values, weights,
and the audit trail.  No language model is used.

Each country gets:
  - A "score drivers" sentence naming the top 2-3 contributing variables.
  - A "risk flags" sentence for inverted variables with poor raw scores.
  - A "data notes" sentence listing any Rule 1/2/3 adjustments that were made.
"""

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Human-readable labels for each variable key
_VAR_LABELS = {
    "opportunity_usd_m":        "Market Opportunity ($M)",
    "potential_market_size":    "Potential Market Size ($M)",
    "gym_membership_cagr":      "Gym Membership CAGR",
    "penetration_headroom":     "Penetration Headroom",
    "concentration":            "Gym Concentration (inhab/gym)",
    "ease_of_doing_business":   "Ease of Doing Business",
    "political_stability":      "Political Stability",
    "inflation_rate":           "Inflation Rate",
    "currency_volatility":      "Currency Volatility",
    "rule_of_law":              "Rule of Law",
    "financing_accessibility": "Ease of Financing (GFDD)",
    "corporate_tax_rate":       "Corporate Tax Rate",
    "labor_cost_index":         "Labour Cost Index",
    "real_estate_cost_index":   "Real Estate Cost Index",
    "youth_population_pct":           "Youth / Working Age Population % (15–64)",
    "middle_class_pct":         "Middle Class %",
    "avg_gym_spend_pct_gdp":    "Avg Gym Spend as % of GDP",
}

_CATEGORY_LABELS = {
    "market_opportunity":   "Market Opportunity",
    "penetration_headroom": "Penetration Headroom",
    "operational_risk":     "Operational Risk",
    "cost_structure":       "Cost Structure",
    "demand_indicators":    "Demand Indicators",
}


def generate_commentary(
    scores_df: pd.DataFrame,
    full_df: pd.DataFrame,
    normalized_df: pd.DataFrame,
    weight_matrix: dict,
    audit: dict,
    categories: dict,
    inverted_variables: set,
) -> dict:
    """
    Build a commentary dict for every country.

    Returns
    -------
    dict  {country: {"drivers": str, "risks": str, "data_notes": str}}
    """
    commentary = {}

    # Build country → row index mapping for full_df and normalized_df
    country_to_idx = {
        row["country"]: i for i, row in full_df.iterrows()
    }

    for _, score_row in scores_df.iterrows():
        country = score_row["country"]
        idx = country_to_idx.get(country)
        if idx is None:
            continue

        weights = weight_matrix.get(country, {})
        norm_row = normalized_df.iloc[idx] if idx < len(normalized_df) else pd.Series()
        raw_row = full_df.iloc[idx]
        country_audit = audit.get(country, {})

        # ---- Score drivers (top 3 contributing variables) ------------------
        contributions = []
        for var, w in weights.items():
            if w <= 0:
                continue
            norm_val = norm_row.get(var, np.nan)
            if pd.notna(norm_val):
                contributions.append((var, norm_val * w * 100))

        contributions.sort(key=lambda x: x[1], reverse=True)
        top3 = contributions[:3]

        if top3:
            driver_parts = []
            for var, contrib in top3:
                label = _VAR_LABELS.get(var, var)
                driver_parts.append(f"{label} (+{contrib:.1f}pts)")
            drivers_str = "Score driven by: " + ", ".join(driver_parts) + "."
        else:
            drivers_str = "Insufficient data to identify score drivers."

        # ---- Risk flags (inverted variables with poor normalised score) ----
        risk_flags = []
        for var in inverted_variables:
            norm_val = norm_row.get(var, np.nan)
            raw_val = raw_row.get(var, np.nan)
            w = weights.get(var, 0.0)
            if pd.notna(norm_val) and norm_val < 0.35 and w > 0:
                label = _VAR_LABELS.get(var, var)
                if pd.notna(raw_val):
                    risk_flags.append(f"{label} ({raw_val:.1f})")
                else:
                    risk_flags.append(label)

        risks_str = (
            "Risk flags: " + "; ".join(risk_flags) + "."
            if risk_flags
            else "No material risk flags identified."
        )

        # ---- Data notes (missing variables and rules applied) --------------
        notes = []
        missing_vars = [
            var for var, src in country_audit.items() if src == "missing"
        ]
        if missing_vars:
            labels = [_VAR_LABELS.get(v, v) for v in missing_vars]
            notes.append(
                f"Rule 3 redistribution applied for: {', '.join(labels)}."
            )

        if not weights.get("gym_membership_cagr", 1.0) > 0:
            notes.append(
                "Rule 1: CAGR missing — Opportunity and Potential Market Size weights adjusted."
            )
        if not weights.get("concentration", 1.0) > 0:
            notes.append(
                "Rule 2: Concentration missing — Penetration Headroom weight adjusted to 10%."
            )

        financing_partial = raw_row.get("_financing_partial", False)
        if financing_partial:
            notes.append(
                "Financing score computed from partial GFDD data (≥1 component missing)."
            )

        manual_vars = [
            var for var, src in country_audit.items() if src in ("manual_yaml", "manual_prompt")
        ]
        if manual_vars:
            labels = [_VAR_LABELS.get(v, v) for v in manual_vars]
            notes.append(f"Manual inputs used: {', '.join(labels)}.")

        data_notes_str = " ".join(notes) if notes else "All data sourced automatically."

        commentary[country] = {
            "drivers": drivers_str,
            "risks": risks_str,
            "data_notes": data_notes_str,
        }

    return commentary
