"""
src/fetcher.py
==============
External data retrieval with file-based caching (48-hour TTL).

Sources
-------
World Bank WDI / WGI
    ease_of_doing_business, political_stability, rule_of_law,
    inflation_rate, currency_volatility, youth_population_pct (SP.POP.1564.TO.ZS),
    financing_accessibility, middle_class_pct (Q3+Q4 income shares)

OECD Stats API (stats.oecd.org)
    labor_cost_index     → AHR dataset (Average Hourly Remuneration)
    real_estate_cost_index → HOUSE_PRICES dataset (Real House Price Index)
    Only OECD member countries have data; non-members fall back to YAML.

Trading Economics API
    corporate_tax_rate   → /country-list/corporate-tax-rate
    Requires TRADING_ECONOMICS_API_KEY in config.py.
    Falls back to YAML when key is empty or API call fails.

Priority: API value → YAML override (handled in override_loader.py)

Performance
-----------
• All WB indicators are fetched in a single batched API call per indicator
  (all countries at once) instead of one call per country.  This cuts WB
  API calls from N_countries × N_indicators down to N_indicators only.
• A unified cache file (cache/external_data_cache.json) stores the complete
  result dict.  On subsequent runs within the TTL window the file is loaded
  directly — zero API calls.
• HTTP requests use a 60-second timeout and up to 3 retries with exponential
  backoff (2 s, 4 s, 8 s) before giving up and returning NaN for a value.
"""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import requests

logger = logging.getLogger(__name__)

_WB_BASE   = "https://api.worldbank.org/v2"
_TE_BASE   = "https://api.tradingeconomics.com"
_OECD_BASE = "https://stats.oecd.org/SDMX-JSON/data"

_REQUEST_DELAY = 0.25   # seconds — polite rate limiting between calls
_TIMEOUT       = 60     # seconds per HTTP request (raised from 20)
_MAX_RETRIES   = 3      # number of retry attempts after first failure


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _cache_path(cache_dir: Path, key: str) -> Path:
    safe = key.replace("/", "_").replace(".", "_").replace(" ", "_")
    return cache_dir / f"{safe}.json"


def _cache_valid(path: Path, ttl_hours: float) -> bool:
    if not path.exists():
        return False
    age_h = (datetime.now(timezone.utc).timestamp() - path.stat().st_mtime) / 3600
    return age_h < ttl_hours


def _read_cache(path: Path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _write_cache(path: Path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)


# ---------------------------------------------------------------------------
# HTTP helper with retry + exponential backoff
# ---------------------------------------------------------------------------

def _http_get_with_retry(
    session: requests.Session,
    url: str,
    params: dict,
    timeout: int = _TIMEOUT,
    max_retries: int = _MAX_RETRIES,
) -> requests.Response:
    """
    GET *url* with retry on timeout or connection errors.

    Waits 2 s → 4 s → 8 s between successive attempts.
    Raises the final exception if all retries are exhausted.
    """
    last_exc = None
    for attempt in range(max_retries + 1):
        try:
            return session.get(url, params=params, timeout=timeout)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            last_exc = exc
            if attempt < max_retries:
                wait = 2 ** (attempt + 1)   # 2, 4, 8
                logger.warning(
                    "HTTP timeout/error (attempt %d/%d), retrying in %ds: %s",
                    attempt + 1, max_retries + 1, wait, exc,
                )
                time.sleep(wait)
    raise last_exc


# ---------------------------------------------------------------------------
# World Bank helpers
# ---------------------------------------------------------------------------

def _wb_batch_get(
    session: requests.Session,
    iso3_list: list,
    indicator: str,
    mrv: int = 10,
) -> dict:
    """
    Fetch *indicator* for **all** countries in *iso3_list* in a single WB
    API call using the semicolon-separated multi-country endpoint.

    Returns
    -------
    dict
        {iso3: [{date, value}, ...]} — newest records first per country.
        Returns an empty dict on any error (caller treats missing as None).
    """
    if not iso3_list:
        return {}

    country_str = ";".join(iso3_list)
    url = f"{_WB_BASE}/country/{country_str}/indicator/{indicator}"
    # per_page must cover all countries × mrv years; 500 is a safe ceiling
    per_page = max(500, len(iso3_list) * mrv + 10)
    params = {"format": "json", "mrv": mrv, "per_page": per_page}

    try:
        resp = _http_get_with_retry(session, url, params)
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, list) or len(payload) < 2:
            return {}
        result: dict = {}
        for r in (payload[1] or []):
            if not isinstance(r, dict):
                continue
            iso3 = r.get("countryiso3code", "")
            if not iso3:
                continue
            result.setdefault(iso3, []).append(
                {"date": r.get("date"), "value": r.get("value")}
            )
        return result
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
        logger.warning(
            "WB API batch failed for indicator %s after %d retries: %s",
            indicator, _MAX_RETRIES, exc,
        )
        return {}
    except Exception as exc:
        logger.warning("WB API error for indicator %s: %s", indicator, exc)
        return {}


def _wb_get(session: requests.Session, iso3: str, indicator: str, mrv: int = 10):
    """Single-country WB fetch (kept for compatibility). Returns list of dicts or None."""
    url = f"{_WB_BASE}/country/{iso3}/indicator/{indicator}"
    params = {"format": "json", "mrv": mrv, "per_page": mrv}
    try:
        resp = _http_get_with_retry(session, url, params)
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, list) or len(payload) < 2:
            return []
        return [
            {"date": r["date"], "value": r["value"]}
            for r in (payload[1] or [])
            if isinstance(r, dict)
        ]
    except Exception as exc:
        logger.warning("WB API error %s/%s: %s", iso3, indicator, exc)
        return None


def _fetch_wb_series(session, cache_dir, ttl_hours, no_cache, iso3, indicator, mrv=10):
    """Return cached or freshly fetched WB series. None = API failure."""
    path = _cache_path(cache_dir, f"wb_{iso3}_{indicator}_mrv{mrv}")
    if not no_cache and _cache_valid(path, ttl_hours):
        return _read_cache(path)
    time.sleep(_REQUEST_DELAY)
    data = _wb_get(session, iso3, indicator, mrv)
    if data is not None:
        _write_cache(path, data)
    return data


def _latest_value(series):
    """Return the most-recent non-null float from a WB series, or None."""
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
    """Std dev of YoY absolute % changes in an exchange-rate series."""
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
    values = values[::-1]   # chronological order (series is newest-first)
    pct_changes = [
        abs((values[i] - values[i - 1]) / values[i - 1]) * 100
        for i in range(1, len(values))
        if values[i - 1] != 0
    ]
    return float(np.std(pct_changes, ddof=1)) if len(pct_changes) >= 2 else None


# ---------------------------------------------------------------------------
# Financing Accessibility composite (World Bank GFDD)
# ---------------------------------------------------------------------------

_FINANCING_INDICATORS = {
    "domestic_credit":   "FS.AST.PRVT.GD.ZS",
    "account_ownership": "FX.OWN.TOTL.ZS",
    "bank_branches":     "FB.CBK.BRCH.P5",
}


def _fetch_financing_components(session, cache_dir, ttl_hours, no_cache, iso3):
    result = {}
    for comp_key, indicator in _FINANCING_INDICATORS.items():
        series = _fetch_wb_series(session, cache_dir, ttl_hours, no_cache, iso3, indicator, mrv=10)
        result[comp_key] = _latest_value(series)
    return result


def compute_financing_scores(raw_components: dict, countries: list) -> dict:
    """
    Min-max normalise each of the three GFDD components across all countries,
    then average them per country to get the financing_accessibility score (0–100).
    """
    comp_keys = list(_FINANCING_INDICATORS.keys())
    arrays = {ck: [raw_components.get(c, {}).get(ck) for c in countries] for ck in comp_keys}

    def _mm(vals):
        numeric = [v for v in vals if v is not None]
        if not numeric:
            return [None] * len(vals)
        lo, hi = min(numeric), max(numeric)
        if lo == hi:
            return [0.5 if v is not None else None for v in vals]
        return [(v - lo) / (hi - lo) if v is not None else None for v in vals]

    norm_arrays = {ck: _mm(arrays[ck]) for ck in comp_keys}

    result = {}
    for i, country in enumerate(countries):
        norm_vals = {ck: norm_arrays[ck][i] for ck in comp_keys}
        available = [v for v in norm_vals.values() if v is not None]
        score = float(np.mean(available) * 100) if available else None
        result[country] = {
            "score": score,
            "partial": len(available) < len(comp_keys),
            "components": {ck: raw_components.get(country, {}).get(ck) for ck in comp_keys},
        }
    return result


# ---------------------------------------------------------------------------
# Youth population % (World Bank SP.POP.1564.TO.ZS, population ages 15–64)
# Uses the 15–64 working-age band as the youth demand proxy.
# ---------------------------------------------------------------------------

def _fetch_youth_pct(session, cache_dir, ttl_hours, no_cache, iso3):
    """Return % of population aged 15–64, or None."""
    series = _fetch_wb_series(
        session, cache_dir, ttl_hours, no_cache,
        iso3, "SP.POP.1564.TO.ZS", mrv=5
    )
    val = _latest_value(series)
    return round(val, 4) if val is not None else None


# ---------------------------------------------------------------------------
# Middle Class % (World Bank income quintile shares Q3 + Q4)
# ---------------------------------------------------------------------------

_MIDDLE_CLASS_INDICATORS = {
    "q3": "SI.DST.03RD.20",
    "q4": "SI.DST.04TH.20",
}


def _fetch_middle_class_pct(session, cache_dir, ttl_hours, no_cache, iso3):
    """
    Sum the 3rd and 4th income quintile shares (% of income held by middle 40%
    of earners) as a proxy for middle-class strength.
    Returns None if neither quintile series has data.
    """
    total = 0.0
    any_data = False
    for key, indicator in _MIDDLE_CLASS_INDICATORS.items():
        series = _fetch_wb_series(session, cache_dir, ttl_hours, no_cache, iso3, indicator, mrv=10)
        val = _latest_value(series)
        if val is not None:
            total += val
            any_data = True
    return round(total, 4) if any_data else None


# ---------------------------------------------------------------------------
# OECD Stats API helpers
# ---------------------------------------------------------------------------

def _oecd_sdmx_get(session: requests.Session, dataset: str, filter_str: str,
                   start: int = 2018, end: int = 2023) -> dict | None:
    """
    Call the OECD stats.oecd.org SDMX-JSON endpoint.
    Returns the raw JSON dict or None on error.
    """
    url = f"{_OECD_BASE}/{dataset}/{filter_str}/all"
    params = {
        "contentType": "json",
        "startTime": str(start),
        "endTime": str(end),
    }
    try:
        resp = _http_get_with_retry(session, url, params)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.debug("OECD API error %s/%s: %s", dataset, filter_str, exc)
        return None


def _parse_oecd_latest(data: dict) -> float | None:
    """
    Extract the most-recent non-null observation value from an OECD SDMX-JSON
    response that contains a single series (one country, one measure).
    """
    if not data:
        return None
    try:
        datasets = data.get("dataSets", [])
        if not datasets:
            return None
        series = datasets[0].get("series", {})
        if not series:
            return None
        series_key = next(iter(series))
        observations = series[series_key].get("observations", {})
        if not observations:
            return None
        # Keys are string indices; sort descending to get most recent first
        for idx in sorted(observations.keys(), key=lambda x: int(x), reverse=True):
            entry = observations[idx]
            val = entry[0] if isinstance(entry, list) else entry
            if val is not None:
                return float(val)
    except (KeyError, IndexError, TypeError, ValueError, StopIteration):
        pass
    return None


def _fetch_oecd_ahr(session, cache_dir, ttl_hours, no_cache, oecd_code):
    """
    Fetch Average Hourly Remuneration (AHR) for one country from OECD.
    Returns raw value (currency units per hour) or None.
    Non-OECD countries and errors return None → fall back to YAML.
    """
    if not oecd_code:
        return None
    path = _cache_path(cache_dir, f"oecd_ahr_{oecd_code}")
    if not no_cache and _cache_valid(path, ttl_hours):
        cached = _read_cache(path)
        return cached.get("value")
    time.sleep(_REQUEST_DELAY)
    # Try primary measure code first, then fallback
    for measure in ("AHR", "WAGE", "COMP"):
        data = _oecd_sdmx_get(session, "AHR", f"{oecd_code}.{measure}.A")
        val = _parse_oecd_latest(data)
        if val is not None:
            _write_cache(path, {"value": val})
            return val
    _write_cache(path, {"value": None})
    return None


def _fetch_oecd_housecost(session, cache_dir, ttl_hours, no_cache, oecd_code):
    """
    Fetch the Real House Price Index (RHPI) for one country from OECD.
    Returns the index value or None.
    Non-OECD countries and errors return None → fall back to YAML.
    """
    if not oecd_code:
        return None
    path = _cache_path(cache_dir, f"oecd_housecost_{oecd_code}")
    if not no_cache and _cache_valid(path, ttl_hours):
        cached = _read_cache(path)
        return cached.get("value")
    time.sleep(_REQUEST_DELAY)
    for dataset, measure in [("HOUSE_PRICES", "RHPI"), ("HOUSE_PRICES", "NHPI"),
                              ("HOUSECOST", "IDX")]:
        data = _oecd_sdmx_get(session, dataset, f"{oecd_code}.{measure}.A")
        val = _parse_oecd_latest(data)
        if val is not None:
            _write_cache(path, {"value": val})
            return val
    _write_cache(path, {"value": None})
    return None


def _normalize_oecd_ahr_to_index(raw_ahr: dict) -> dict:
    """
    Convert raw AHR values to a labour cost index relative to US = 100.
    If US data is unavailable, normalise to the highest country in the set = 100.
    Countries missing raw data keep None.
    """
    values = {c: v for c, v in raw_ahr.items() if v is not None}
    if not values:
        return {c: None for c in raw_ahr}
    # Use US as reference if available; otherwise use the dataset maximum
    reference = values.get("US") or values.get("United States") or max(values.values())
    if reference == 0:
        return {c: None for c in raw_ahr}
    return {
        c: round((v / reference) * 100, 2) if v is not None else None
        for c, v in raw_ahr.items()
    }


# ---------------------------------------------------------------------------
# Trading Economics — Corporate Tax Rate
# ---------------------------------------------------------------------------

_TE_COUNTRY_NAME_MAP = {
    # TE name → our internal country name
    # "United Kingdom" removed — TE name and internal name now match
    "Korea, South":    "South Korea",
    "Turkey":          "Turkiye",
    "Korea":           "South Korea",
    "Czech Republic":  "Czech Republic",
    "Slovak Republic": "Slovakia",
}


def _fetch_te_corporate_tax(session, cache_dir, ttl_hours, no_cache, api_key: str) -> dict:
    """
    Fetch corporate tax rates for all countries from Trading Economics API.
    Returns {country_name: rate_float} or empty dict if key missing / call fails.
    """
    if not api_key:
        logger.info("TRADING_ECONOMICS_API_KEY not set — corporate_tax_rate uses YAML fallback.")
        return {}

    path = _cache_path(cache_dir, "te_corporate_tax_all")
    if not no_cache and _cache_valid(path, ttl_hours):
        return _read_cache(path)

    url = f"{_TE_BASE}/country-list/corporate-tax-rate"
    try:
        resp = _http_get_with_retry(session, url, {"c": api_key, "f": "json"})
        resp.raise_for_status()
        records = resp.json()
        if not isinstance(records, list):
            logger.warning("TE API unexpected response format.")
            return {}
        result = {}
        for rec in records:
            te_name = rec.get("Country", "")
            our_name = _TE_COUNTRY_NAME_MAP.get(te_name, te_name)
            val = rec.get("LatestValue")
            if val is not None:
                try:
                    result[our_name] = float(val)
                except (TypeError, ValueError):
                    pass
        _write_cache(path, result)
        logger.info("Trading Economics: fetched tax rates for %d countries.", len(result))
        return result
    except Exception as exc:
        logger.warning("Trading Economics API error: %s — using YAML fallback.", exc)
        return {}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def fetch_all_external_data(
    countries: list,
    country_iso3_map: dict,
    wb_indicators: dict,
    oecd_country_codes: dict,
    te_api_key: str,
    cache_dir: str,
    ttl_hours: float,
    no_cache: bool = False,
) -> dict:
    """
    Fetch all external data for every country.

    On the first run data is fetched from APIs and saved to a unified cache
    file (``cache/external_data_cache.json``).  Subsequent runs within the
    TTL window load that file directly — **zero API calls**.

    World Bank requests are batched: one API call per indicator covers all
    countries simultaneously instead of one call per country per indicator.

    Returns
    -------
    dict
        {country_name: {variable_key: float | None}}
        Internal metadata keys prefixed with '_' are also included.
    """
    cache_path = Path(cache_dir)
    cache_path.mkdir(exist_ok=True)

    # ── Unified cache: skip all API calls when data is still fresh ────────────
    unified_cache_file = cache_path / "external_data_cache.json"
    if not no_cache and _cache_valid(unified_cache_file, ttl_hours):
        logger.info(
            "External data loaded from cache (age < %.0fh). Skipping API calls.",
            ttl_hours,
        )
        return _read_cache(unified_cache_file)

    logger.info("Fetching fresh external data for %d countries...", len(countries))

    session = requests.Session()
    session.headers.update({"User-Agent": "HVLP-Market-Scorer/1.0"})

    result = {c: {} for c in countries}

    # Build ordered list of ISO3 codes that have a mapping (deduplicated)
    seen_iso3: set = set()
    valid_iso3: list = []
    iso3_to_country: dict = {}
    for country in countries:
        iso3 = country_iso3_map.get(country)
        if iso3:
            iso3_to_country[iso3] = country   # last writer wins for duplicate iso3
            if iso3 not in seen_iso3:
                valid_iso3.append(iso3)
                seen_iso3.add(iso3)
        else:
            logger.warning(
                "%s: no ISO3 in COUNTRY_ISO3_MAP — skipping WB fetch.", country
            )

    # ── World Bank: batch-fetch each indicator for ALL countries at once ──────
    #
    # Each tuple: (wb_series_key, indicator_code, mrv)
    # wb_series_key is used to store results in wb_data[indicator_code][iso3]
    # and is logged for progress visibility.
    _WB_BATCH_PLAN = [
        ("govt_effectiveness",  wb_indicators["govt_effectiveness"],   5),
        ("political_stability", wb_indicators["political_stability"],  5),
        ("rule_of_law",         wb_indicators["rule_of_law"],          5),
        ("inflation_rate",      wb_indicators["inflation_rate"],       5),
        ("usd_exchange_rate",   wb_indicators["usd_exchange_rate"],   10),
        ("youth_population",    "SP.POP.1564.TO.ZS",                  5),
        ("gdp_growth",          "NY.GDP.MKTP.KD.ZG",                  7),  # GDP growth % — CAGR proxy
        ("fin_domestic_credit", _FINANCING_INDICATORS["domestic_credit"],   10),
        ("fin_account_ownership", _FINANCING_INDICATORS["account_ownership"], 10),
        ("fin_bank_branches",   _FINANCING_INDICATORS["bank_branches"],      10),
        ("mc_q3",               _MIDDLE_CLASS_INDICATORS["q3"],       10),
        ("mc_q4",               _MIDDLE_CLASS_INDICATORS["q4"],       10),
    ]

    # wb_data[indicator_code][iso3] = [{date, value}, ...]
    wb_data: dict = {}
    n_plans = len(_WB_BATCH_PLAN)
    for step_idx, (label, indicator, mrv) in enumerate(_WB_BATCH_PLAN, 1):
        logger.info(
            "WB batch [%d/%d] %s (%s) — %d countries",
            step_idx, n_plans, label, indicator, len(valid_iso3),
        )
        batch = _wb_batch_get(session, valid_iso3, indicator, mrv)
        wb_data[indicator] = batch   # {iso3: series}

    # ── Map batched WB results into per-country result dict ───────────────────
    financing_raw: dict = {}
    raw_ahr: dict = {}

    for country in countries:
        iso3 = country_iso3_map.get(country)
        if not iso3:
            financing_raw[country] = {}
            raw_ahr[country] = None
            continue

        d = result[country]

        def _get_series(indicator: str):
            return wb_data.get(indicator, {}).get(iso3)

        # Institutional / macro (latest scalar value)
        d["ease_of_doing_business"] = _latest_value(
            _get_series(wb_indicators["govt_effectiveness"])
        )
        d["political_stability"] = _latest_value(
            _get_series(wb_indicators["political_stability"])
        )
        d["rule_of_law"] = _latest_value(
            _get_series(wb_indicators["rule_of_law"])
        )
        d["inflation_rate"] = _latest_value(
            _get_series(wb_indicators["inflation_rate"])
        )

        # Currency volatility from exchange-rate time series
        fx_series = _get_series(wb_indicators["usd_exchange_rate"])
        d["currency_volatility"] = _coefficient_of_variation(fx_series)

        # Youth population %
        val = _latest_value(_get_series("SP.POP.1564.TO.ZS"))
        d["youth_population_pct"] = round(val, 4) if val is not None else None

        # Middle class: sum of Q3 + Q4 income shares
        q3 = _latest_value(_get_series(_MIDDLE_CLASS_INDICATORS["q3"]))
        q4 = _latest_value(_get_series(_MIDDLE_CLASS_INDICATORS["q4"]))
        if q3 is not None or q4 is not None:
            d["middle_class_pct"] = round((q3 or 0.0) + (q4 or 0.0), 4)
        else:
            d["middle_class_pct"] = None

        # GDP growth rate — 5-year average used as gym membership CAGR proxy
        gdp_growth_series = _get_series("NY.GDP.MKTP.KD.ZG")
        if gdp_growth_series:
            valid_rates = [
                obs["value"] for obs in gdp_growth_series
                if isinstance(obs, dict) and obs.get("value") is not None
            ][:5]  # use up to 5 most-recent years
            d["gdp_cagr_proxy"] = round(sum(valid_rates) / len(valid_rates), 4) if valid_rates else None
        else:
            d["gdp_cagr_proxy"] = None

        # Financing components (cross-country normalisation happens after loop)
        financing_raw[country] = {
            "domestic_credit": _latest_value(
                _get_series(_FINANCING_INDICATORS["domestic_credit"])
            ),
            "account_ownership": _latest_value(
                _get_series(_FINANCING_INDICATORS["account_ownership"])
            ),
            "bank_branches": _latest_value(
                _get_series(_FINANCING_INDICATORS["bank_branches"])
            ),
        }

        # OECD: labour cost (AHR) — indexed to US=100 after full country loop
        oecd_code = oecd_country_codes.get(country)
        raw_ahr[country] = _fetch_oecd_ahr(
            session, cache_path, ttl_hours, no_cache, oecd_code
        )

        # OECD: real estate cost (house price index)
        housecost = _fetch_oecd_housecost(
            session, cache_path, ttl_hours, no_cache, oecd_code
        )
        if housecost is not None:
            d["real_estate_cost_index"] = housecost

    # ── Trading Economics: one shot for all countries ─────────────────────────
    te_tax_rates = _fetch_te_corporate_tax(
        session, cache_path, ttl_hours, no_cache, te_api_key
    )
    for country in countries:
        tax_val = te_tax_rates.get(country)
        if tax_val is not None:
            result[country]["corporate_tax_rate"] = tax_val

    # ── Post-loop: financing scores (cross-country normalisation) ─────────────
    financing_scores = compute_financing_scores(financing_raw, countries)
    for country in countries:
        fs = financing_scores.get(country, {})
        result[country]["financing_accessibility"] = fs.get("score")
        result[country]["_financing_partial"] = fs.get("partial", False)
        result[country]["_financing_components"] = fs.get("components", {})

    # ── Post-loop: normalise AHR values to labour cost index (US = 100) ───────
    indexed_ahr = _normalize_oecd_ahr_to_index(raw_ahr)
    for country in countries:
        val = indexed_ahr.get(country)
        if val is not None:
            result[country]["labor_cost_index"] = val

    # ── Save unified cache so subsequent runs skip all API calls ──────────────
    _write_cache(unified_cache_file, result)
    logger.info(
        "External data fetch complete for %d countries. Cache saved to %s",
        len(countries), unified_cache_file,
    )
    return result
