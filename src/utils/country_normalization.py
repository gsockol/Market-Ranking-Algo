"""
src/utils/country_normalization.py
====================================
Canonical country name normalization.

All country names flowing into the scoring model must pass through
normalize_country_name() before any dataset join or lookup.

This module is intentionally isolated from the scoring engine — it
handles only string → string mapping with no side effects.

Design rules
------------
- Matching is case-sensitive (e.g. "UK" matches but "uk" does not).
- Unknown names are returned unchanged so the pipeline never silently
  drops a country it doesn't recognise.
- Adding new aliases here is sufficient — no other file needs updating.
"""

# ---------------------------------------------------------------------------
# Alias table  (non-canonical → canonical)
# ---------------------------------------------------------------------------
_ALIASES: dict[str, str] = {
    # United Kingdom
    "UK":                        "United Kingdom",
    "U.K.":                      "United Kingdom",
    "Great Britain":             "United Kingdom",
    "Britain":                   "United Kingdom",
    "England":                   "United Kingdom",

    # United States
    "USA":                       "United States",
    "U.S.":                      "United States",
    "U.S.A.":                    "United States",
    "United States of America":  "United States",

    # South Korea  (World Bank uses "Korea, Rep.")
    "Korea, Rep.":               "South Korea",
    "Republic of Korea":         "South Korea",
    "Korea":                     "South Korea",
    "Korea, South":              "South Korea",

    # Turkey / Türkiye  (Trading Economics uses "Turkey")
    "Turkey":                    "Turkiye",

    # Russia
    "Russian Federation":        "Russia",

    # Czech Republic  (some sources use "Czechia")
    "Czechia":                   "Czech Republic",

    # Slovakia  (OECD uses "Slovak Republic")
    "Slovak Republic":           "Slovakia",
}


def normalize_country_name(name: str) -> str:
    """
    Return the canonical internal country name for *name*.

    Parameters
    ----------
    name : str
        Raw country name as it appears in a data source.

    Returns
    -------
    str
        Canonical internal name (e.g. "United Kingdom"), or *name*
        unchanged if no alias is registered.
    """
    if not isinstance(name, str):
        return name
    stripped = name.strip()
    return _ALIASES.get(stripped, stripped)
