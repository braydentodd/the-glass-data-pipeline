"""
The Glass - NBA API Client

Wraps the nba_api library with browser header patching, dynamic endpoint
loading, retry logic, and parameter building.  Abstracts NBA-specific
HTTP concerns so the core pipeline never touches requests directly.

No classes -- all functions operate on plain data.
"""

import importlib
import logging
import time
import warnings
from typing import Any, Callable, Dict, Optional

from src.etl.nba.config import API_CONFIG, ENDPOINTS, RETRY_CONFIG

warnings.filterwarnings(
    "ignore",
    message="Failed to return connection to pool",
    module="urllib3",
)

logger = logging.getLogger(__name__)


# ============================================================================
# BROWSER HEADERS  (required by stats.nba.com)
# ============================================================================

_NBA_STATS_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Host": "stats.nba.com",
    "Origin": "https://www.nba.com",
    "Pragma": "no-cache",
    "Referer": "https://www.nba.com/",
    "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
}


# ============================================================================
# SESSION PATCHING
# ============================================================================

_session_patched = False


def _patch_nba_api_headers() -> None:
    """Apply browser-like headers to the nba_api library (idempotent)."""
    global _session_patched
    if _session_patched:
        return
    try:
        from nba_api.stats.library import http as _stats_http
        from nba_api.library import http as _base_http

        _stats_http.STATS_HEADERS = _NBA_STATS_HEADERS
        _stats_http.NBAStatsHTTP.headers = _NBA_STATS_HEADERS
        _stats_http.NBAStatsHTTP._session = None
        _base_http.NBAHTTP._session = None
        _session_patched = True
    except ImportError:
        logger.warning("nba_api not installed -- header patching skipped")


# ============================================================================
# ENDPOINT CLASS LOADING
# ============================================================================

_endpoint_class_cache: Dict[str, Any] = {}


def load_endpoint_class(endpoint_name: str) -> Optional[Any]:
    """Dynamically import and cache an nba_api endpoint class by name.

    Returns ``None`` (with a warning) if the module doesn't exist.
    """
    if endpoint_name in _endpoint_class_cache:
        return _endpoint_class_cache[endpoint_name]

    module_path = f"nba_api.stats.endpoints.{endpoint_name}"
    try:
        module = importlib.import_module(module_path)
    except ImportError:
        logger.warning("Could not import endpoint module: %s", module_path)
        return None

    class_name = endpoint_name[0].upper() + endpoint_name[1:]
    cls = getattr(module, class_name, None)
    if cls is None:
        cls = getattr(module, endpoint_name.upper(), None)
    if cls is None:
        logger.warning("No class found in %s", module_path)
        return None

    _endpoint_class_cache[endpoint_name] = cls
    return cls


# ============================================================================
# API CALL FACTORY
# ============================================================================

def create_api_call(
    endpoint_class: Any,
    params: Dict[str, Any],
    endpoint_name: str = '',
    timeout: Optional[int] = None,
) -> Callable:
    """Build a zero-arg callable that executes an NBA API request.

    Internal params (keys starting with ``_``) are stripped before the call.
    Returns raw JSON dict with ``resultSets``.
    """
    _patch_nba_api_headers()

    clean_params = {k: v for k, v in params.items() if not k.startswith('_')}
    call_timeout = timeout or API_CONFIG['timeout_default']

    def _call() -> Dict[str, Any]:
        result = endpoint_class(**clean_params, timeout=call_timeout)
        return result.get_dict()

    return _call


# ============================================================================
# RETRY WRAPPER
# ============================================================================

def with_retry(func: Callable, max_retries: Optional[int] = None) -> Any:
    """Execute *func* with exponential back-off on failure.

    Always applies the configured rate-limit delay before each attempt.
    Returns the first successful result or re-raises the last exception.
    """
    retries = max_retries or RETRY_CONFIG['max_retries']
    backoff = RETRY_CONFIG['backoff_base']

    for attempt in range(1, retries + 1):
        try:
            time.sleep(API_CONFIG['rate_limit_delay'])
            return func()
        except Exception:
            if attempt >= retries:
                raise
            wait = attempt * (backoff // API_CONFIG['backoff_divisor'])
            logger.warning(
                'Attempt %d failed, retrying in %ds...', attempt, wait,
            )
            time.sleep(wait)

    raise RuntimeError(f"with_retry exhausted {retries} attempts")


# ============================================================================
# PARAMETER BUILDER
# ============================================================================

def build_endpoint_params(
    endpoint_name: str,
    season: str,
    season_type_name: str,
    entity: str,
    extra_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Assemble the full parameter dict for an NBA API call.

    Merges standard parameters (season, league_id, per_mode, season_type)
    with endpoint-specific defaults and caller-supplied overrides.
    """
    ep_cfg = ENDPOINTS.get(endpoint_name, {})
    params: Dict[str, Any] = {'season': season}

    # Season type
    st_param = ep_cfg.get('season_type_param')
    if st_param:
        params['season_type_all_star'] = season_type_name

    # Per-mode
    pm_param = ep_cfg.get('per_mode_param')
    if pm_param and pm_param in API_CONFIG:
        params[pm_param] = API_CONFIG[pm_param]

    # Player / Team discriminator for shared endpoints
    if (
        'player' in ep_cfg.get('entity_types', [])
        and 'team' in ep_cfg.get('entity_types', [])
    ):
        if entity == 'player':
            params['player_or_team'] = 'Player'
        else:
            params['player_or_team'] = 'Team'

    # League ID
    params['league_id'] = API_CONFIG['league_id']

    # Caller overrides win
    if extra_params:
        params.update(extra_params)

    return params
