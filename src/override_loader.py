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

from src.utils.country_normalization import normalize_country_name

logger = logging.getLogger(__name__)

# Minimum gym_membership_cagr when ALL GDP sources (TE, IMF, WB) return nothing.
# Corresponds to a 2 % GDP growth floor × 1.4 multiplier.
# Prevents Rule 1 from triggering on data gaps alone.
_CAGR_FLOOR = 2.8


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
    raw_overrides = doc.get("overrides", {}) or {}
    # Normalize country name keys so YAML entries like "UK:" or "Korea:"
    # are silently resolved to their canonical internal names.
    overrides = {normalize_country_name(k): v for k, v in raw_overrides.items()}
    logger.info(
        "Loaded manual overrides for %d countries from '%s'.", len(overrides), yaml_path
    )
    return overrides


def load_yaml_penetration_overrides(yaml_path: str) -> dict:
    """
    Read the ``penetration_overrides`` section from the YAML override file.

    Returns
    -------
    dict  {country_name: fraction}  — only countries where
          • penetration_overrides.enabled == true, AND
          • the per-country value is not null, AND
          • the value passes range validation (0 < v ≤ 1).
    Returns an empty dict when the section is absent or enabled == false.

    Values outside (0, 1] but inside (0, 100] are silently converted from
    a percentage to a fraction (e.g. 20.0 → 0.20).
    """
    path = Path(yaml_path)
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        doc = yaml.safe_load(f) or {}

    pen_section = doc.get("penetration_overrides", {}) or {}
    if not pen_section.get("enabled", False):
        return {}

    raw_values = pen_section.get("values", {}) or {}
    result = {}
    for country_raw, val in raw_values.items():
        if val is None:
            continue
        country = normalize_country_name(country_raw)
        try:
            v = float(val)
        except (TypeError, ValueError):
            logger.warning(
                "penetration_overrides: '%s' value '%s' is not numeric — skipped.",
                country_raw, val,
            )
            continue

        # Auto-convert percentage input to fraction
        if 1.0 < v <= 100.0:
            v = round(v / 100.0, 6)

        if not (0.0 < v <= 1.0):
            logger.warning(
                "penetration_overrides: '%s' value %.4f out of range (0, 1] — skipped.",
                country, v,
            )
            continue
        result[country] = v

    if result:
        logger.info(
            "YAML penetration overrides loaded for %d countries: %s",
            len(result),
            ", ".join(f"{c}={v:.1%}" for c, v in result.items()),
        )
    return result


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
    gui_cagr_overrides: dict | None = None,
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
    gui_cagr_overrides : dict | None
        Optional {country: float} from the GUI Overrides panel.  When provided,
        a matching entry takes precedence over API, YAML, and GDP-proxy values
        for gym_membership_cagr.

    Returns
    -------
    (pd.DataFrame, dict)
        Updated DataFrame and audit dict:
        {country: {variable: "csv_derived"|"api"|"gui_override"|"manual_yaml"
                            |"gdp_cagr_proxy"|"manual_prompt"|"missing"}}
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

            # 2.5. GUI CAGR override (highest non-CSV/API priority for CAGR)
            if var == "gym_membership_cagr" and gui_cagr_overrides:
                gui_val = gui_cagr_overrides.get(country)
                if gui_val is not None:
                    df.loc[mask, var] = float(gui_val)
                    audit[country][var] = "gui_override"
                    continue

            # 3. YAML manual override
            yaml_val = yaml_ctry.get(var)
            if yaml_val is not None:
                df.loc[mask, var] = float(yaml_val)
                audit[country][var] = "manual_yaml"
                continue

            # 3.5. GDP CAGR proxy — sourced TE (primary) → IMF (fallback) → WB (last
            #      resort) in fetcher.py; used only for gym_membership_cagr when no
            #      explicit value is available.  Formula: gym_cagr = GDP_growth × 1.4
            if var == "gym_membership_cagr":
                gdp_proxy = ext.get("gdp_cagr_proxy")
                gdp_source = ext.get("_gdp_source", "unknown")
                if gdp_proxy is not None and not (
                    isinstance(gdp_proxy, float) and np.isnan(gdp_proxy)
                ):
                    cagr_value = round(float(gdp_proxy) * 1.4, 4)
                    df.loc[mask, var] = cagr_value
                    audit[country][var] = f"gdp_cagr_proxy_x1.4 ({gdp_source})"
                    logger.info(
                        "%s / gym_membership_cagr: GDP growth %.2f%% [%s] × 1.4 = %.2f%%.",
                        country, gdp_proxy, gdp_source, cagr_value,
                    )
                    continue

            # 3.6. Local CSV fallback for GDP CAGR — used when all live API sources
            #      (TE, IMF, WB) are unavailable.  Column: GDP_CAGR in input_data.csv.
            #      Formula: gym_cagr = gdp_cagr_csv × 1.4
            if var == "gym_membership_cagr":
                csv_gdp = df.loc[mask, "gdp_cagr_csv"].values
                if len(csv_gdp) > 0 and pd.notna(csv_gdp[0]):
                    cagr_value = round(float(csv_gdp[0]) * 1.4, 4)
                    df.loc[mask, var] = cagr_value
                    audit[country][var] = "gdp_cagr_csv_x1.4"
                    logger.info(
                        "%s / gym_membership_cagr: CSV GDP_CAGR %.2f%% × 1.4 = %.2f%% "
                        "(local CSV fallback).",
                        country, float(csv_gdp[0]), cagr_value,
                    )
                    continue

            # 4. Interactive prompt
            if interactive:
                prompted = _prompt_value(country, var)
                if prompted is not None:
                    df.loc[mask, var] = prompted
                    audit[country][var] = "manual_prompt"
                    continue

            # 5. gym_membership_cagr: use floor (GDP growth minimum × 1.4) instead of
            #    zero so this variable never collapses and Rule 1 does not trigger on
            #    a data gap alone.
            if var == "gym_membership_cagr":
                df.loc[mask, var] = _CAGR_FLOOR
                audit[country][var] = "gdp_growth_floor"
                logger.warning(
                    "%s / gym_membership_cagr: all GDP sources unavailable — "
                    "using floor value %.1f%% (2%% GDP floor × 1.4).",
                    country, _CAGR_FLOOR,
                )
                continue

            # 6. Genuinely missing → will trigger Rule 3 in weighter
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
