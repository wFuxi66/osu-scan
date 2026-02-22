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
    """Runs the global monthly scan and saves leaderboard data to files."""
    import scan_logic

    logger.info("run_monthly_scan started.")
    if progress_callback is not None:
        try:
            progress_callback("Starting global monthly scan...")
        except Exception:
            logger.exception("Progress callback raised an exception.")

    try:
        result = scan_logic.run_global_scan(progress_callback=progress_callback)
    except Exception as e:
        logger.exception("Unexpected error during global scan.")
        return {'error': str(e)}

    if 'error' in result:
        logger.error("Global scan returned error: %s", result['error'])
        return result

    # Persist results so the GitHub Actions workflow can upload them
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    os.makedirs(data_dir, exist_ok=True)

    leaderboard_file = os.path.join(data_dir, 'leaderboard.json')
    cache_file = os.path.join(data_dir, 'leaderboard_cache.json')

    try:
        with open(leaderboard_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.info("Saved leaderboard to %s", leaderboard_file)

        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.info("Saved cache to %s", cache_file)
    except Exception as e:
        logger.error("Error saving leaderboard files: %s", e)
        return {'error': str(e)}

    if progress_callback is not None:
        try:
            progress_callback("Scan complete!")
        except Exception:
            logger.exception("Progress callback raised an exception.")

    logger.info("run_monthly_scan finished. Sets scanned: %s", result.get('total_sets_scanned', 0))
    return result


def load_leaderboard_results():
    """Loads leaderboard data from GitHub Release (preferred) or local file."""
    global _remote_cache

    # Return cached data if still fresh
    if time.time() - _remote_cache['last_fetch'] < CACHE_TTL and _remote_cache['data']:
        logger.debug("Returning cached leaderboard data.")
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
            # Negative cache: avoid repeated remote fetch attempts during outages
            _remote_cache['last_fetch'] = time.time()
    except Exception as e:
        logger.warning("Error fetching remote leaderboard: %s", e)
        # Negative cache: avoid repeated remote fetch attempts during outages
        _remote_cache['last_fetch'] = time.time()

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
