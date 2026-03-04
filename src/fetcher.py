"""
src/fetcher.py
==============
World Bank API retrieval with file-based caching.

Fetches all external variables for every country and returns a flat dict:
    {country_name: {variable_key: float | None}}

Cache behaviour:
    Responses are stored as JSON under CACHE_DIR.
    Cache is considered valid while its file is younger than CACHE_EXPIRY_HOURS.
    Pass no_cache=True to bypass and re-fetch everything.

Variables retrieved
-------------------
ease_of_doing_business  : IC.BUS.EASE.XQ (discontinued — last data ≈2020; flagged)
political_stability     : PV.EST  (WGI)
rule_of_law             : RL.EST  (WGI)
inflation_rate          : FP.CPI.TOTL.ZG
currency_volatility     : derived — std dev of YoY % changes in PA.NUS.FCRF
youth_population_pct    : sum of SP.POP.{1519,2024,2529,3034}.TO.ZS age bands
ease_of_financing       : composite of FS.AST.PRVT.GD.ZS, FX.OWN.TOTL.ZS,
                          FB.CBK.BRCH.P5 — normalised per-component then averaged

Variables NOT fetched here (manual-only):
    corporate_tax_rate, labor_cost_index, real_estate_cost_index, middle_class_pct
    → supplied via overrides/manual_inputs.yaml
"""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import requests

logger = logging.getLogger(__name__)

_WB_BASE = "https://api.worldbank.org/v2"
_REQUEST_DELAY = 0.25   # seconds between API calls — polite rate limiting
_TIMEOUT = 20           # seconds per request


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _cache_path(cache_dir: Path, iso3: str, indicator: str, mrv: int) -> Path:
    safe_ind = indicator.replace(".", "_")
    return cache_dir / f"{iso3}__{safe_ind}__mrv{mrv}.json"


def _cache_valid(path: Path, ttl_hours: float) -> bool:
    if not path.exists():
        return False
    age_h = (
        datetime.now(timezone.utc).timestamp() - path.stat().st_mtime
    ) / 3600
    return age_h < ttl_hours


def _read_cache(path: Path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _write_cache(path: Path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)


def _wb_get(session: requests.Session, iso3: str, indicator: str, mrv: int = 10):
    """
    Call the World Bank API and return a list of {date, value} dicts,
    most-recent first.  Returns None on any network / parse error.
    """
    url = f"{_WB_BASE}/country/{iso3}/indicator/{indicator}"
    params = {"format": "json", "mrv": mrv, "per_page": mrv}
    try:
        resp = session.get(url, params=params, timeout=_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, list) or len(payload) < 2:
            return []
        records = payload[1] or []
        return [
            {"date": r["date"], "value": r["value"]}
            for r in records
            if isinstance(r, dict)
        ]
    except Exception as exc:
        logger.warning("WB API error %s/%s: %s", iso3, indicator, exc)
        return None


def _fetch_series(
    session, cache_dir, ttl_hours, no_cache, iso3, indicator, mrv=10
):
    """Return cached or freshly fetched series list.  None = API failure."""
    path = _cache_path(cache_dir, iso3, indicator, mrv)
    if not no_cache and _cache_valid(path, ttl_hours):
        return _read_cache(path)
    time.sleep(_REQUEST_DELAY)
    data = _wb_get(session, iso3, indicator, mrv)
    if data is not None:
        _write_cache(path, data)
    return data


def _latest_value(series):
    """Return the most-recent non-null float, or None."""
    if not series:
        return None
    for rec in series:
        val = rec.get("value")
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                continue
    return None


def _coefficient_of_variation(series):
    """
    Compute the std dev of year-over-year absolute % changes in an exchange
    rate series.  Returns None if fewer than 3 data points are available.
    """
    if not series or len(series) < 3:
        return None
    values = []
    for rec in series:
        v = rec.get("value")
        if v is not None:
            try:
                values.append(float(v))
            except (TypeError, ValueError):
                pass
    if len(values) < 3:
        return None
    # series is newest-first; reverse to chronological order
    values = values[::-1]
    pct_changes = []
    for i in range(1, len(values)):
        prev = values[i - 1]
        if prev != 0:
            pct_changes.append(abs((values[i] - prev) / prev) * 100)
    if len(pct_changes) < 2:
        return None
    return float(np.std(pct_changes, ddof=1))


# ---------------------------------------------------------------------------
# Financing Accessibility composite
# ---------------------------------------------------------------------------

_FINANCING_INDICATORS = {
    "domestic_credit": "FS.AST.PRVT.GD.ZS",
    "account_ownership": "FX.OWN.TOTL.ZS",
    "bank_branches": "FB.CBK.BRCH.P5",
}
_FINANCING_LABELS = {
    "domestic_credit": "Domestic Credit to Private Sector (% GDP)",
    "account_ownership": "Account Ownership (% adults 15+)",
    "bank_branches": "Bank Branches per 100k adults",
}


def _fetch_financing_components(session, cache_dir, ttl_hours, no_cache, iso3):
    """Return {component_key: float|None} for the three GFDD indicators."""
    result = {}
    for comp_key, indicator in _FINANCING_INDICATORS.items():
        series = _fetch_series(
            session, cache_dir, ttl_hours, no_cache, iso3, indicator, mrv=10
        )
        result[comp_key] = _latest_value(series)
    return result


def compute_financing_scores(raw_components: dict, countries: list) -> dict:
    """
    Build the ease_of_financing score for each country.

    Steps (MSD §6):
      1. For each of the three GFDD components, min-max normalise across all
         countries that have data for that component.
      2. Each country's score = mean of its available normalised components.
      3. Countries missing all three components get score = None.
      4. Countries missing ≥1 component are flagged "partial_financing".

    Parameters
    ----------
    raw_components : dict
        {country_name: {comp_key: float|None}}
    countries : list[str]
        Ordered list of all country names (defines the normalisation scope).

    Returns
    -------
    dict
        {country_name: {"score": float|None, "partial": bool,
                         "components": {comp_key: float|None}}}
    """
    comp_keys = list(_FINANCING_INDICATORS.keys())

    # Build per-component arrays for normalisation
    arrays = {ck: [] for ck in comp_keys}
    for country in countries:
        comps = raw_components.get(country, {})
        for ck in comp_keys:
            arrays[ck].append(comps.get(ck))

    # Min-max normalise each component across countries with non-None values
    def _minmax_list(vals):
        numeric = [v for v in vals if v is not None]
        if not numeric:
            return [None] * len(vals)
        lo, hi = min(numeric), max(numeric)
        if lo == hi:
            return [0.5 if v is not None else None for v in vals]
        return [
            (v - lo) / (hi - lo) if v is not None else None
            for v in vals
        ]

    norm_arrays = {ck: _minmax_list(arrays[ck]) for ck in comp_keys}

    # Build per-country result
    result = {}
    for i, country in enumerate(countries):
        norm_vals = {ck: norm_arrays[ck][i] for ck in comp_keys}
        available = [v for v in norm_vals.values() if v is not None]
        score = float(np.mean(available) * 100) if available else None
        result[country] = {
            "score": score,
            "partial": len(available) < len(comp_keys),
            "components": {
                ck: raw_components.get(country, {}).get(ck)
                for ck in comp_keys
            },
        }

    return result


# ---------------------------------------------------------------------------
# Youth population (sum of age-band indicators)
# ---------------------------------------------------------------------------

_YOUTH_INDICATORS = [
    "SP.POP.1519.TO.ZS",
    "SP.POP.2024.TO.ZS",
    "SP.POP.2529.TO.ZS",
    "SP.POP.3034.TO.ZS",
]


def _fetch_youth_pct(session, cache_dir, ttl_hours, no_cache, iso3):
    """Sum the four age-band percentage indicators.  Returns None if all fail."""
    total = 0.0
    any_data = False
    for ind in _YOUTH_INDICATORS:
        series = _fetch_series(
            session, cache_dir, ttl_hours, no_cache, iso3, ind, mrv=5
        )
        val = _latest_value(series)
        if val is not None:
            total += val
            any_data = True
    return round(total, 4) if any_data else None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def fetch_all_external_data(
    countries: list,
    country_iso3_map: dict,
    wb_indicators: dict,
    cache_dir: str,
    ttl_hours: float,
    no_cache: bool = False,
) -> dict:
    """
    Fetch all external data for every country.

    Returns
    -------
    dict
        {country_name: {variable_key: value}}
        where value is float | None.
        Also includes "_financing_components" key per country for transparency.
    """
    cache_path = Path(cache_dir)
    cache_path.mkdir(exist_ok=True)

    session = requests.Session()
    session.headers.update({"User-Agent": "HVLP-Market-Scorer/1.0"})

    result = {c: {} for c in countries}
    financing_raw = {}

    total = len(countries)
    for idx, country in enumerate(countries, 1):
        iso3 = country_iso3_map.get(country)
        if not iso3:
            logger.warning(
                "[%d/%d] %s: no ISO3 code in config.COUNTRY_ISO3_MAP — skipping API fetch.",
                idx, total, country,
            )
            result[country] = {k: None for k in [
                "ease_of_doing_business", "political_stability", "rule_of_law",
                "inflation_rate", "currency_volatility", "youth_population_pct",
            ]}
            financing_raw[country] = {}
            continue

        logger.info("[%d/%d] Fetching: %s (%s)", idx, total, country, iso3)
        d = result[country]

        # Ease of Doing Business (IC.BUS.EASE.XQ — discontinued ~2020)
        series = _fetch_series(
            session, cache_path, ttl_hours, no_cache,
            iso3, wb_indicators["ease_of_doing_business"], mrv=5
        )
        d["ease_of_doing_business"] = _latest_value(series)

        # Political Stability (WGI PV.EST)
        series = _fetch_series(
            session, cache_path, ttl_hours, no_cache,
            iso3, wb_indicators["political_stability"], mrv=5
        )
        d["political_stability"] = _latest_value(series)

        # Rule of Law (WGI RL.EST)
        series = _fetch_series(
            session, cache_path, ttl_hours, no_cache,
            iso3, wb_indicators["rule_of_law"], mrv=5
        )
        d["rule_of_law"] = _latest_value(series)

        # Inflation Rate
        series = _fetch_series(
            session, cache_path, ttl_hours, no_cache,
            iso3, wb_indicators["inflation_rate"], mrv=5
        )
        d["inflation_rate"] = _latest_value(series)

        # Currency Volatility — derived from exchange rate std dev
        series = _fetch_series(
            session, cache_path, ttl_hours, no_cache,
            iso3, wb_indicators["usd_exchange_rate"], mrv=10
        )
        d["currency_volatility"] = _coefficient_of_variation(series)

        # Youth Population % (15-34) — sum of four age bands
        d["youth_population_pct"] = _fetch_youth_pct(
            session, cache_path, ttl_hours, no_cache, iso3
        )

        # Financing components (raw — score computed after all countries fetched)
        financing_raw[country] = _fetch_financing_components(
            session, cache_path, ttl_hours, no_cache, iso3
        )

    # Compute financing scores across all countries simultaneously
    # (normalisation must be scoped to the full active country set)
    financing_scores = compute_financing_scores(financing_raw, countries)
    for country in countries:
        fs = financing_scores.get(country, {})
        result[country]["financing_accessibility"] = fs.get("score")
        result[country]["_financing_partial"] = fs.get("partial", False)
        result[country]["_financing_components"] = fs.get("components", {})

    logger.info("External data fetch complete for %d countries.", len(countries))
    return result
