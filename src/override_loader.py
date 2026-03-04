"""
src/override_loader.py
======================
Loads manual data overrides from YAML and optionally prompts the user
interactively for any variables that remain missing after API fetch.

Override priority order (highest → lowest):
  1. overrides/manual_inputs.yaml  (file-based, version-controllable)
  2. Interactive terminal prompt    (only when --interactive flag is set)
  3. Missing → Rule 3 redistribution handled by weighter.py

merge_overrides()
    Merges API data + YAML overrides into the full data DataFrame.
    Returns the updated DataFrame plus a per-country audit dict showing
    the source of every variable value.
"""

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

logger = logging.getLogger(__name__)

# Variables that must come from manual input (no reliable public API)
_MANUAL_PRIMARY = {
    "corporate_tax_rate",
    "labor_cost_index",
    "real_estate_cost_index",
    "middle_class_pct",
}


def load_yaml_overrides(yaml_path: str) -> dict:
    """
    Read the YAML override file.

    Returns
    -------
    dict  {country_name: {variable_key: value}}
          Empty dict if the file is missing or the "overrides" key is absent.
    """
    path = Path(yaml_path)
    if not path.exists():
        logger.warning(
            "Override file not found: '%s'. Manual variables will be missing.", yaml_path
        )
        return {}
    with open(path, encoding="utf-8") as f:
        doc = yaml.safe_load(f) or {}
    overrides = doc.get("overrides", {}) or {}
    logger.info(
        "Loaded manual overrides for %d countries from '%s'.", len(overrides), yaml_path
    )
    return overrides


def _prompt_value(country: str, variable: str) -> float | None:
    """
    Ask the user to type a numeric value for one variable.
    Returns None if the user leaves the input blank.
    """
    prompt = (
        f"  Enter value for [{country}] → {variable} "
        "(press Enter to skip and apply Rule 3 redistribution): "
    )
    raw = input(prompt).strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        print(f"  Invalid input '{raw}' — treated as missing.")
        return None


def merge_overrides(
    df: pd.DataFrame,
    external_data: dict,
    yaml_overrides: dict,
    scored_variables: list,
    interactive: bool = False,
) -> tuple[pd.DataFrame, dict]:
    """
    Merge all data sources into *df* and build an audit trail.

    Parameters
    ----------
    df : pd.DataFrame
        Output of calculator.calculate_derived_metrics — contains CSV-derived
        columns.
    external_data : dict
        {country: {variable: value|None}} from fetcher.fetch_all_external_data.
    yaml_overrides : dict
        {country: {variable: value}} from load_yaml_overrides.
    scored_variables : list[str]
        All variable keys that will be scored (from config.WEIGHTS).
    interactive : bool
        If True, prompt the user for any variable still missing after
        YAML override application.

    Returns
    -------
    (pd.DataFrame, dict)
        Updated DataFrame and audit dict:
        {country: {variable: "csv_derived"|"api"|"manual_yaml"|"manual_prompt"|"missing"}}
    """
    audit = {row["country"]: {} for _, row in df.iterrows()}

    # Initialise any scored variable columns that don't yet exist in df
    for var in scored_variables:
        if not var.startswith("_") and var not in df.columns:
            df[var] = np.nan

    for _, row in df.iterrows():
        country = row["country"]
        ext = external_data.get(country, {})
        yaml_ctry = yaml_overrides.get(country, {})

        for var in scored_variables:
            # Skip internal/derived flags
            if var.startswith("_"):
                continue

            # 1. Already in DataFrame (CSV-derived or previously computed)
            mask = df["country"] == country
            existing = df.loc[mask, var].values
            if len(existing) > 0 and pd.notna(existing[0]):
                audit[country][var] = "csv_derived"
                continue

            # 2. API-fetched external data
            api_val = ext.get(var)
            if api_val is not None and not (
                isinstance(api_val, float) and np.isnan(api_val)
            ):
                df.loc[mask, var] = api_val
                audit[country][var] = "api"
                continue

            # 3. YAML manual override
            yaml_val = yaml_ctry.get(var)
            if yaml_val is not None:
                df.loc[mask, var] = float(yaml_val)
                audit[country][var] = "manual_yaml"
                continue

            # 4. Interactive prompt
            if interactive:
                prompted = _prompt_value(country, var)
                if prompted is not None:
                    df.loc[mask, var] = prompted
                    audit[country][var] = "manual_prompt"
                    continue

            # 5. Genuinely missing → will trigger Rule 3 in weighter
            audit[country][var] = "missing"
            logger.info(
                "%s / %s: no data from any source — will apply Rule 3 redistribution.",
                country, var,
            )

    # Also copy financing metadata columns into DataFrame for dashboard use
    for _, row in df.iterrows():
        country = row["country"]
        ext = external_data.get(country, {})
        df.loc[df["country"] == country, "_financing_partial"] = ext.get(
            "_financing_partial", False
        )

    return df, audit
