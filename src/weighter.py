"""
src/weighter.py
===============
Per-country conditional weight resolution.

Implements the three deterministic rules exactly as specified:

  Rule 1 — CAGR missing
      gym_membership_cagr weight → 0
      opportunity_usd_m weight   → 0.250  (override, not additive)
      potential_market_size      → 0.150  (override, not additive)
      No other weights change.

  Rule 2 — Concentration missing
      concentration weight       → 0
      penetration_headroom       → 0.100  (override, not additive)
      No other weights change.

  Rule 3 — Any other missing variable (after API + manual input)
      Zero the missing variable's weight.
      Redistribute its original base weight proportionally to the remaining
      variables IN THE SAME CATEGORY ONLY, using base weights as the
      redistribution key.
      Global weights (variables in other categories) are never touched.

  Rule evaluation order: Rule 1 → Rule 2 → Rule 3

Weight integrity assertion:
  After all rules execute, sum(weights.values()) must equal 1.0 within
  floating-point tolerance (1e-9).  If the assertion fails the function
  raises WeightIntegrityError with a full diagnostic.

Inputs to this module are read from config at call time — no values are
stored inside this module.
"""

import logging
from copy import deepcopy

logger = logging.getLogger(__name__)

_TOLERANCE = 1e-9


class WeightIntegrityError(Exception):
    """Raised when per-country weights do not sum to 1.0 after all rules."""


def _build_var_to_category(categories: dict) -> dict:
    """Return {variable_key: category_key} reverse map."""
    mapping = {}
    for cat_key, cat_cfg in categories.items():
        vars_list = cat_cfg if isinstance(cat_cfg, list) else cat_cfg.get("variables", [])
        for var in vars_list:
            mapping[var] = cat_key
    return mapping


def _category_variables(categories: dict, cat_key: str) -> list:
    cfg = categories[cat_key]
    return cfg if isinstance(cfg, list) else cfg.get("variables", [])


def resolve_weights(
    country: str,
    availability: dict,
    base_weights: dict,
    rule1_cfg: dict,
    rule2_cfg: dict,
    categories: dict,
) -> dict:
    """
    Compute the final per-country weight dict that sums to exactly 1.0.

    Parameters
    ----------
    country : str
        Country name (used only for logging / error messages).
    availability : dict
        {variable_key: bool}  True = data is present, False/missing = no data.
    base_weights : dict
        From config.WEIGHTS — must sum to 1.0.
    rule1_cfg : dict
        config.RULE1_MISSING_CAGR
    rule2_cfg : dict
        config.RULE2_MISSING_CONCENTRATION
    categories : dict
        config.VARIABLE_CATEGORIES — used for Rule 3 redistribution scope.

    Returns
    -------
    dict
        {variable_key: float}  values in [0, 1], sum == 1.0.

    Raises
    ------
    WeightIntegrityError
        If the sum deviates from 1.0 by more than _TOLERANCE.
    """
    weights = deepcopy(base_weights)
    var_to_cat = _build_var_to_category(categories)

    # Track which variables were already handled by Rule 1 or Rule 2 so
    # Rule 3 does not double-process them.
    rule_handled = set()

    # ------------------------------------------------------------------
    # RULE 1 — Gym Membership CAGR missing
    # ------------------------------------------------------------------
    cagr_key = rule1_cfg["zero_out"]
    if not availability.get(cagr_key, True):
        weights[cagr_key] = 0.0
        for var, override_w in rule1_cfg["override"].items():
            weights[var] = override_w
        rule_handled.add(cagr_key)
        logger.debug("%s: Rule 1 applied (CAGR missing).", country)

    # ------------------------------------------------------------------
    # RULE 2 — Concentration missing
    # ------------------------------------------------------------------
    conc_key = rule2_cfg["zero_out"]
    if not availability.get(conc_key, True):
        weights[conc_key] = 0.0
        for var, override_w in rule2_cfg["override"].items():
            weights[var] = override_w
        rule_handled.add(conc_key)
        logger.debug("%s: Rule 2 applied (Concentration missing).", country)

    # ------------------------------------------------------------------
    # RULE 3 — All other missing variables
    # ------------------------------------------------------------------
    for var, is_available in availability.items():
        if is_available:
            continue
        if var in rule_handled:
            continue
        if var not in weights:
            continue
        if weights[var] == 0.0:
            continue  # already zeroed by a prior rule — nothing to redistribute

        zeroed_weight = weights[var]
        cat_key = var_to_cat.get(var)

        if cat_key is None:
            logger.warning(
                "%s: variable '%s' has no category mapping — weight %.4f dropped (Rule 3).",
                country, var, zeroed_weight,
            )
            weights[var] = 0.0
            continue

        # Eligible siblings: same category, positive weight, data available
        siblings = [
            s for s in _category_variables(categories, cat_key)
            if s != var
            and weights.get(s, 0.0) > 0.0
            and availability.get(s, True)
        ]

        if not siblings:
            logger.warning(
                "%s: Rule 3 — '%s' missing and no eligible siblings in category '%s'. "
                "%.4f weight cannot be redistributed. Total will be < 1.0.",
                country, var, cat_key, zeroed_weight,
            )
            weights[var] = 0.0
            continue

        sibling_base_total = sum(base_weights.get(s, 0.0) for s in siblings)
        if sibling_base_total == 0.0:
            logger.warning(
                "%s: Rule 3 — sibling base weights sum to zero for category '%s'. "
                "Cannot redistribute — weight dropped.",
                country, cat_key,
            )
            weights[var] = 0.0
            continue

        # Redistribute proportionally using BASE weights as the key
        weights[var] = 0.0
        for s in siblings:
            weights[s] += zeroed_weight * (base_weights[s] / sibling_base_total)

        logger.debug(
            "%s: Rule 3 — '%s' zeroed; %.4f redistributed to %s.",
            country, var, zeroed_weight, siblings,
        )

    # ------------------------------------------------------------------
    # INTEGRITY CHECK
    # ------------------------------------------------------------------
    total = sum(weights.values())
    if abs(total - 1.0) > _TOLERANCE:
        shortfall = round(1.0 - total, 6)
        diagnostics = ", ".join(
            f"{k}={v:.4f}" for k, v in sorted(weights.items()) if v > 0
        )
        logger.warning(
            "%s: weight sum = %.6f (shortfall %.6f). Cause: entire category(ies) missing. "
            "Scorer will normalise by actual weight sum. Active weights: %s",
            country, total, shortfall, diagnostics,
        )
        # Store the actual weight sum as a sentinel key so scorer can normalise
        weights["_weight_sum"] = total
    else:
        weights["_weight_sum"] = 1.0

    return weights


def build_weight_matrix(
    countries: list,
    availability_matrix: dict,
    base_weights: dict,
    rule1_cfg: dict,
    rule2_cfg: dict,
    categories: dict,
) -> dict:
    """
    Convenience wrapper: resolve weights for every country.

    Parameters
    ----------
    countries : list[str]
    availability_matrix : dict
        {country: {variable_key: bool}}
    (rest as per resolve_weights)

    Returns
    -------
    dict  {country: {variable_key: float}}
    """
    matrix = {}
    for country in countries:
        matrix[country] = resolve_weights(
            country,
            availability_matrix.get(country, {}),
            base_weights,
            rule1_cfg,
            rule2_cfg,
            categories,
        )
    return matrix
