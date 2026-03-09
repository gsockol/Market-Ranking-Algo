"""
src/calculator.py
=================
Derives all computed metrics from the raw CSV columns.

All formulas come verbatim from the MSD.  No values are hardcoded in this
module — dues increase rates are read from the caller-supplied dict.

Derived columns added to the DataFrame
---------------------------------------
penetration_headroom      : future_penetration_pct − current_penetration_pct
implied_members_current   : current_penetration_pct × population_m × 1 000 000
current_dues_monthly_usd  : market_size_m × 1e6 / (implied_members × 12)
dues_increase_pct         : per-country override or default 0.0
future_dues_monthly_usd   : current_dues × (1 + dues_increase_pct)
implied_members_future    : future_penetration_pct × population_m × 1 000 000
potential_market_size     : implied_members_future × future_dues × 12 / 1e6  ($M)
opportunity_usd_m         : potential_market_size − market_size_m             ($M)
avg_gym_spend_pct_gdp     : (current_dues × 12) / gdp_per_capita × 100       (%)
"""

import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _safe_divide(numerator, denominator):
    """Return numerator / denominator; return NaN when denominator is zero or NaN."""
    with np.errstate(divide="ignore", invalid="ignore"):
        result = np.where(
            (pd.isna(denominator)) | (denominator == 0),
            np.nan,
            numerator / denominator,
        )
    return result


def calculate_derived_metrics(
    df: pd.DataFrame,
    dues_increase_pct: dict,
    penetration_overrides: dict | None = None,
) -> pd.DataFrame:
    """
    Add all derived columns to *df* in-place and return it.

    Parameters
    ----------
    df : pd.DataFrame
        Output of ingestor.ingest_csv — must contain the raw CSV keys.
    dues_increase_pct : dict
        From config.DUES_INCREASE_PCT.
        Keys are country names; "default" key holds the fallback rate.
    penetration_overrides : dict | None
        Optional {country: target_penetration_fraction} from the GUI Overrides
        panel.  When provided the matching country's future_penetration_pct
        (i.e. the target penetration assumption) is replaced before
        penetration_headroom is computed.  Silently ignored if the override
        value is below the country's current_penetration_pct.

    Returns
    -------
    pd.DataFrame
        Original DataFrame with derived columns appended.
    """
    default_dues_inc = dues_increase_pct.get("default", 0.0)

    rows = []
    for _, row in df.iterrows():
        country = row["country"]
        mkt_size  = row.get("market_size_m")
        cur_pen   = row.get("current_penetration_pct")
        fut_pen   = row.get("future_penetration_pct")
        pop       = row.get("population_m")
        gdp_pc    = row.get("gdp_per_capita")

        # Apply penetration target override when user has provided one via UI
        if penetration_overrides and country in penetration_overrides:
            override_val = penetration_overrides[country]
            if pd.notna(cur_pen) and override_val < cur_pen:
                logger.warning(
                    "%s: penetration override %.4f < current %.4f — override ignored.",
                    country, override_val, cur_pen,
                )
            else:
                fut_pen = override_val

        derived = {}

        # --- Penetration Headroom -------------------------------------------
        if pd.notna(fut_pen) and pd.notna(cur_pen):
            derived["penetration_headroom"] = round(fut_pen - cur_pen, 6)
        else:
            derived["penetration_headroom"] = np.nan
            logger.warning("%s: missing penetration values → penetration_headroom = NaN", country)

        # --- Implied current membership base (absolute count) ---------------
        if pd.notna(cur_pen) and pd.notna(pop):
            implied_current = cur_pen * pop * 1_000_000
        else:
            implied_current = np.nan

        derived["implied_members_current"] = implied_current

        # --- Current monthly dues per member (USD) --------------------------
        if pd.notna(mkt_size) and pd.notna(implied_current) and implied_current > 0:
            cur_dues = (mkt_size * 1_000_000) / (implied_current * 12)
        elif pd.notna(mkt_size) and implied_current == 0:
            logger.warning(
                "%s: current penetration × population = 0 → cannot derive monthly dues. "
                "Set manually in overrides/manual_inputs.yaml if needed.",
                country,
            )
            cur_dues = np.nan
        else:
            cur_dues = np.nan

        derived["current_dues_monthly_usd"] = cur_dues

        # --- Per-country dues increase rate ---------------------------------
        inc_pct = dues_increase_pct.get(country, default_dues_inc)
        derived["dues_increase_pct"] = inc_pct

        # --- Future monthly dues per member (USD) ---------------------------
        if pd.notna(cur_dues):
            fut_dues = cur_dues * (1 + inc_pct)
        else:
            fut_dues = np.nan

        derived["future_dues_monthly_usd"] = fut_dues

        # --- Implied future membership base ---------------------------------
        if pd.notna(fut_pen) and pd.notna(pop):
            implied_future = fut_pen * pop * 1_000_000
        else:
            implied_future = np.nan

        derived["implied_members_future"] = implied_future

        # --- Potential Market Size ($M) -------------------------------------
        if pd.notna(implied_future) and pd.notna(fut_dues):
            pot_mkt = (implied_future * fut_dues * 12) / 1_000_000
        else:
            pot_mkt = np.nan
            logger.warning("%s: cannot compute potential_market_size → NaN", country)

        derived["potential_market_size"] = pot_mkt

        # --- Opportunity ($M) -----------------------------------------------
        if pd.notna(pot_mkt) and pd.notna(mkt_size):
            derived["opportunity_usd_m"] = round(pot_mkt - mkt_size, 4)
        else:
            derived["opportunity_usd_m"] = np.nan

        # --- Avg Gym Spend as % of GDP --------------------------------------
        if pd.notna(cur_dues) and pd.notna(gdp_pc) and gdp_pc > 0:
            derived["avg_gym_spend_pct_gdp"] = round(
                (cur_dues * 12) / gdp_pc * 100, 6
            )
        else:
            derived["avg_gym_spend_pct_gdp"] = np.nan

        rows.append(derived)

    derived_df = pd.DataFrame(rows, index=df.index)

    # Merge derived columns back — don't overwrite existing columns unless
    # explicitly computed here (opportunity and potential_market_size always
    # come from this module).
    for col in derived_df.columns:
        df[col] = derived_df[col]

    logger.info("Derived metrics computed for %d countries.", len(df))
    return df


def calculate_composite_variables(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute cross-country composite variables requiring dataset-wide min/max context.

    Must be called AFTER merge_overrides() so that YAML-supplied values for
    labor_cost_index and real_estate_cost_index are present in df.
    Must be called BEFORE normalize_all().

    Columns added
    -------------
    operating_cost_composite : float in [0, 1]
        Inverted min-max scaled composite: labor_cost_index (×0.6) + real_estate_cost_index (×0.4).
        Higher value = cheaper operating cost = better.
        NOT in INVERTED_VARIABLES — inversion is baked into the formula here.
    market_agility_bonus : float > 0
        1 / sqrt(potential_market_size).
        Higher for smaller potential markets — rewards compact, efficient entry opportunities.
        NOT in INVERTED_VARIABLES — higher raw value is already semantically better.

    Parameters
    ----------
    df : pd.DataFrame
        Merged DataFrame containing at minimum labor_cost_index,
        real_estate_cost_index, and potential_market_size columns.

    Returns
    -------
    pd.DataFrame
        Original DataFrame with two new columns appended.
    """
    # ── Operating cost composite ──────────────────────────────────────────────
    labor = df["labor_cost_index"].copy().astype(float)
    re    = df["real_estate_cost_index"].copy().astype(float)
    valid_labor = labor.notna()
    valid_re    = re.notna()

    def _inverted_minmax(series: pd.Series) -> pd.Series:
        """Scale series to [0, 1] with inversion: lower raw value → higher score."""
        lo, hi = series.min(), series.max()
        if hi == lo:
            return pd.Series(0.5, index=series.index)
        return (hi - series) / (hi - lo)

    labor_scaled = _inverted_minmax(labor[valid_labor]).reindex(df.index)
    re_scaled    = _inverted_minmax(re[valid_re]).reindex(df.index)

    composite = pd.Series(np.nan, index=df.index)
    both = valid_labor & valid_re
    composite[both] = labor_scaled[both] * 0.6 + re_scaled[both] * 0.4
    # Partial fallback: use whichever component is available at full weight
    labor_only = valid_labor & ~valid_re
    re_only    = ~valid_labor & valid_re
    composite[labor_only] = labor_scaled[labor_only]
    composite[re_only]    = re_scaled[re_only]
    df["operating_cost_composite"] = composite

    # ── Market agility bonus ──────────────────────────────────────────────────
    pot = df["potential_market_size"].copy().astype(float)
    df["market_agility_bonus"] = np.where(
        pot.notna() & (pot > 0),
        1.0 / np.sqrt(pot.clip(lower=0.01)),
        np.nan,
    )

    logger.info(
        "Composite variables computed: operating_cost_composite (%d/%d countries), "
        "market_agility_bonus (%d/%d countries).",
        both.sum(), len(df), pot.notna().sum(), len(df),
    )
    return df
