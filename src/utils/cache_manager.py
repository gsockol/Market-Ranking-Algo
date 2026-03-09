"""
src/utils/cache_manager.py
===========================
Thin wrapper around JSON file-based caching for the unified external
API result dict.

Cache file : .cache/external_data_cache.json
Default TTL: 720 hours (30 days)

Usage
-----
    from src.utils.cache_manager import CacheManager

    cm = CacheManager(cache_dir=".cache", force_refresh=False)
    if cm.is_valid():
        data = cm.load()
    else:
        data = fetch_from_apis()
        cm.save(data)

    # Force-refresh (--refresh-api / --no-cache CLI flag):
    cm = CacheManager(cache_dir=".cache", force_refresh=True)

Notes
-----
- This class manages only the *unified* external_data_cache.json.
  Per-indicator OECD caches (used by fetcher.py for individual country
  OECD endpoint calls) remain as separate files managed by fetcher.py.
- Thread-safety: load/save are not protected by a lock; the model runs
  single-threaded at the fetch stage so this is sufficient.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_CACHE_FILENAME = "external_data_cache.json"
DEFAULT_TTL_HOURS: float = 720.0   # 30 days


class CacheManager:
    """
    Manages a single JSON cache file with a configurable TTL.

    Parameters
    ----------
    cache_dir : str
        Directory in which the cache file is stored (created if absent).
    force_refresh : bool
        When True, ``is_valid()`` always returns False so the caller
        fetches fresh data.  Corresponds to ``--refresh-api`` /
        ``--no-cache`` CLI flags.
    ttl_hours : float
        Cache lifetime in hours.  Defaults to 720 (30 days).
    """

    def __init__(
        self,
        cache_dir: str,
        force_refresh: bool = False,
        ttl_hours: float = DEFAULT_TTL_HOURS,
    ) -> None:
        self._dir = Path(cache_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._path = self._dir / _CACHE_FILENAME
        self._force_refresh = force_refresh
        self._ttl_hours = ttl_hours

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_valid(self) -> bool:
        """
        Return True iff a non-expired cache file exists and
        ``force_refresh`` is False.
        """
        if self._force_refresh:
            return False
        if not self._path.exists():
            return False
        age_h = (
            datetime.now(timezone.utc).timestamp() - self._path.stat().st_mtime
        ) / 3600.0
        valid = age_h < self._ttl_hours
        if not valid:
            logger.info(
                "Cache expired (age %.1fh > TTL %.0fh). Will re-fetch.",
                age_h,
                self._ttl_hours,
            )
        return valid

    def load(self) -> dict:
        """Load and return the cached dict."""
        with open(self._path, encoding="utf-8") as f:
            data = json.load(f)
        logger.info(
            "External data loaded from cache (%s). Skipping API calls.", self._path
        )
        return data

    def save(self, data: dict) -> None:
        """Persist *data* to the cache file."""
        with open(self._path, "w", encoding="utf-8") as f:
            json.dump(data, f)
        logger.info("External data cache saved to %s.", self._path)
