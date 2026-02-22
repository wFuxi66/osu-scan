import logging
import os
import time
import json
import requests

logger = logging.getLogger(__name__)

LEADERBOARD_RELEASE_URL = "https://github.com/wFuxi66/osu-scan/releases/download/latest-data/leaderboard.json"
_LEADERBOARD_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'leaderboard.json')
_remote_cache = {'data': None, 'last_fetch': 0}
CACHE_TTL = 3600  # 1 hour


def run_monthly_scan(progress_callback=None):
    """Placeholder for the monthly global scan (not yet reimplemented)."""
    logger.info("run_monthly_scan called — scan logic is not yet reimplemented.")
    return {'error': 'Monthly scan not yet reimplemented'}


def load_leaderboard_results():
    """Loads leaderboard data from GitHub Release (preferred) or local file."""
    global _remote_cache

    # Return cached data if still fresh
    if time.time() - _remote_cache['last_fetch'] < CACHE_TTL and _remote_cache['data']:
        logger.info("Returning cached leaderboard data.")
        return _remote_cache['data']

    # Try fetching from GitHub Release
    logger.info("Fetching leaderboard from GitHub Release...")
    try:
        r = requests.get(LEADERBOARD_RELEASE_URL, timeout=3)
        if r.status_code == 200:
            data = r.json()
            _remote_cache['data'] = data
            _remote_cache['last_fetch'] = time.time()
            logger.info("Leaderboard fetched successfully from GitHub Release.")
            return data
        else:
            logger.warning("GitHub Release returned status %s, falling back to local file.", r.status_code)
    except Exception as e:
        logger.warning("Error fetching remote leaderboard: %s", e)

    # Fallback to local file
    if os.path.exists(_LEADERBOARD_FILE):
        logger.info("Loading leaderboard from local file: %s", _LEADERBOARD_FILE)
        try:
            with open(_LEADERBOARD_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error("Error loading local leaderboard file: %s", e)
            return None

    logger.warning("No leaderboard data available (remote failed, no local file).")
    return None
