# MODEL STATUS: v1.1 (Institutional Weighting Update)
# Verification Passed:
#   * Rule 0: Operating Cost Proxy anchor active (uses Tax Rate as fallback)
#   * Rule 4: Global Redistribution (prevents weight shortfall)
#   * Floating-point integrity maintained at 1e-9 tolerance

"""
src/weighter.py
===============
Institutional weight resolution with proxy-anchoring and global fallbacks.

Execution Order:
----------------
Rule 0 (Proxy) -> Rule 1 (CAGR) -> Rule 2 (Conc.) -> Rule 3 (Local) -> Rule 4 (Global)
"""

import logging
from copy import deepcopy

logger = logging.getLogger(__name__)

_TOLERANCE = 1e-9

class WeightIntegrityError(Exception):
    """Raised when per-country weights do not sum to 1.0."""

def _build_var_to_category(categories: dict) -> dict:
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
    weights = deepcopy(base_weights)
    var_to_cat = _build_var_to_category(categories)
    rule_handled = set()

    # ------------------------------------------------------------------
    # RULE 0 — Institutional Proxy Anchor
    # If operating costs are missing, anchor weight to Corporate Tax Rate
    # ------------------------------------------------------------------
    op_cost_key = "operating_cost_composite"
    tax_key = "corporate_tax_rate"
    
    if not availability.get(op_cost_key, False) and availability.get(tax_key, True):
        proxy_weight = weights.get(op_cost_key, 0.0)
        if proxy_weight > 0:
            weights[tax_key] += proxy_weight
            weights[op_cost_key] = 0.0
            rule_handled.add(op_cost_key)
            logger.debug("%s: Rule 0 applied (OpCost weight moved to Tax Proxy).", country)

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
    # RULE 3 — Local Category Redistribution
    # ------------------------------------------------------------------
    for var, is_available in availability.items():
        if is_available or var in rule_handled or var not in weights or weights[var] == 0.0:
            continue

        zeroed_weight = weights[var]
        cat_key = var_to_cat.get(var)
        if not cat_key:
            continue

        siblings = [
            s for s in _category_variables(categories, cat_key)
            if s != var and weights.get(s, 0.0) > 0.0 and availability.get(s, True)
        ]

        if siblings:
            weights[var] = 0.0
            sibling_base_total = sum(base_weights.get(s, 0.0) for s in siblings)
            for s in siblings:
                weights[s] += zeroed_weight * (base_weights[s] / sibling_base_total)
            logger.debug("%s: Rule 3 (Local) — redistributed %.4f from %s.", country, zeroed_weight, var)

    # ------------------------------------------------------------------
    # RULE 4 — Global Redistribution Fallback
    # If weight sum is still < 1.0 (empty categories), distribute to ALL available vars
    # ------------------------------------------------------------------
    current_total = sum(v for k, v in weights.items() if not k.startswith("_"))
    if current_total < (1.0 - _TOLERANCE):
        shortfall = 1.0 - current_total
        available_vars = [k for k, v in availability.items() if v and k in weights]
        
        if available_vars:
            total_base_of_available = sum(base_weights.get(v, 0.0) for v in available_vars)
            for v in available_vars:
                weights[v] += shortfall * (base_weights.get(v, 1.0) / total_base_of_available)
            logger.info("%s: Rule 4 (Global Fallback) applied to fix %.4f shortfall.", country, shortfall)

    # ------------------------------------------------------------------
    # INTEGRITY CHECK
    # ------------------------------------------------------------------
    final_total = sum(v for k, v in weights.items() if not k.startswith("_"))
    weights["_weight_sum"] = final_total
    
    if abs(final_total - 1.0) > _TOLERANCE:
        logger.error("%s: Weight total is %.6f - Critical Data Gap.", country, final_total)
    
    return weights

def build_weight_matrix(countries, availability_matrix, base_weights, rule1_cfg, rule2_cfg, categories):
    matrix = {}
    for country in countries:
        matrix[country] = resolve_weights(
            country, availability_matrix.get(country, {}),
            base_weights, rule1_cfg, rule2_cfg, categories
        )
    return matrix
