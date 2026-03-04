"""
src/ingestor.py
===============
Schema-agnostic CSV ingestion.

Responsibilities:
- Read any CSV file with pandas — no assumptions about row count or column count.
- Strip leading/trailing whitespace from all column headers.
- Rename columns from CSV labels to internal snake_case keys using the
  caller-supplied column_map.  Extra columns not in the map are preserved
  under their stripped name so the pipeline never silently drops user data.
- Parse blank / whitespace-only cells as NaN uniformly.
- Return a clean DataFrame ready for downstream calculation.
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


def ingest_csv(path: str, column_map: dict) -> pd.DataFrame:
    """
    Read *path* and return a normalised DataFrame.

    Parameters
    ----------
    path : str
        Absolute or relative path to the CSV file.
    column_map : dict
        {csv_header: internal_key} mapping from config.CSV_COLUMN_MAP.

    Returns
    -------
    pd.DataFrame
        One row per country.  All column names are internal snake_case keys.
        Unmapped extra columns are kept under their stripped CSV name.
    """
    try:
        raw = pd.read_csv(
            path,
            dtype=str,          # read everything as string first — avoids
            keep_default_na=True,  # silent type coercion surprises
            skipinitialspace=True,
        )
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Input CSV not found: '{path}'. "
            "Check --csv argument or place file at data/input_data.csv"
        )

    # Strip whitespace from all column headers
    raw.columns = [c.strip() for c in raw.columns]

    # Log any CSV columns that are not in column_map (not an error — just info)
    mapped_keys = set(column_map.keys())
    extra_cols = [c for c in raw.columns if c not in mapped_keys]
    if extra_cols:
        logger.debug("CSV contains unmapped columns (kept as-is): %s", extra_cols)

    # Rename mapped columns; leave extras untouched
    raw = raw.rename(columns=column_map)

    # Confirm all required internal keys are now present
    required = set(column_map.values())
    missing_keys = required - set(raw.columns)
    if missing_keys:
        raise ValueError(
            f"CSV is missing expected column(s) after mapping: {missing_keys}. "
            f"Available stripped headers: {list(raw.columns)}"
        )

    # Replace blank / whitespace-only strings with NaN uniformly
    raw = raw.apply(
        lambda col: col.str.strip().replace("", pd.NA) if col.dtype == object else col
    )

    # Convert numeric columns (everything except "country" and any string extras)
    numeric_keys = [k for k in column_map.values() if k != "country"]
    for key in numeric_keys:
        if key in raw.columns:
            raw[key] = pd.to_numeric(raw[key], errors="coerce")

    # Strip country name strings
    raw["country"] = raw["country"].str.strip()

    # Drop rows where the country name is blank
    before = len(raw)
    raw = raw.dropna(subset=["country"]).reset_index(drop=True)
    if len(raw) < before:
        logger.warning(
            "Dropped %d row(s) with blank country name.", before - len(raw)
        )

    logger.info("Ingested %d countries from '%s'.", len(raw), path)
    return raw
