import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import os
import json
import concurrent.futures
from collections import defaultdict
from itertools import combinations

logger = logging.getLogger(__name__)

def get_session():
    """Returns a requests Session equipped with robust retry logic."""
    session = requests.Session()
    # Retry on 429 (Rate Limit) and 5xx (Server Error)
    # 401 is NOT in forcelist - manual handling refreshes token and retries
    retries = Retry(total=5,
                    backoff_factor=1,
                    status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session

# Configuration constants
API_BASE = 'https://osu.ppy.sh/api/v2'
TOKEN_URL = 'https://osu.ppy.sh/oauth/token'
CACHE_VERSION = 2

# User Credentials - MUST be set via environment variables
# On Render: Set in Dashboard > Environment
# Locally: Create a .env file (see .env.example)
CLIENT_ID = os.environ.get('OSU_CLIENT_ID')
CLIENT_SECRET = os.environ.get('OSU_CLIENT_SECRET')

if not CLIENT_ID or not CLIENT_SECRET:
    logger.warning("OSU_CLIENT_ID and OSU_CLIENT_SECRET environment variables not set!")
    logger.warning("The app will not work without valid osu! API credentials.")

# Token Manager for long-running scans
class TokenManager:
    """Manages token refresh for long-running scans."""
    def __init__(self):
        self.token = None
        self.expires_at = 0

    def get_token(self):
        """Get current token or refresh if expired."""
        now = time.time()
        if self.token and now < self.expires_at - 300:  # Refresh 5 min before expiry
            return self.token

        data = {
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
            'grant_type': 'client_credentials',
            'scope': 'public'
        }
        try:
            response = requests.post(TOKEN_URL, data=data, timeout=10)
            response.raise_for_status()
            result = response.json()
            self.token = result['access_token']
            # Tokens typically expire in 86400 seconds (24 hours)
            self.expires_at = now + result.get('expires_in', 86400)
            return self.token
        except Exception as e:
            logger.error("Error authenticating: %s", e)
            return None

    def refresh_token(self):
        """Force token refresh (e.g., after 401 response)."""
        self.token = None  # Invalidate current token
        self.expires_at = 0
        return self.get_token()

# Global token manager instance
_token_manager = TokenManager()

def get_token():
    """Obtains a client credentials token from osu! API."""
    return _token_manager.get_token()

def get_user_id(username_or_id, token):
    """Resolves a username to an ID."""
    headers = {'Authorization': f'Bearer {token}'}
    session = get_session()
    
    # Try assuming it's a username key
    params = {'key': 'username'}
    url = f'{API_BASE}/users/{username_or_id}/osu'
    
    try:
        response = session.get(url, headers=headers, params=params, timeout=10)
        if response.status_code == 200:
            return response.json()['id'], response.json()['username']
    except:
        pass

    # If failed, maybe it was an ID?
    if str(username_or_id).isdigit():
        url = f'{API_BASE}/users/{username_or_id}'
        try:
            response = session.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                return response.json()['id'], response.json()['username']
        except:
            pass
            
    return None, None


def get_beatmapsets(user_id, token, cancel_event=None):
    """Fetches all beatmap sets for a user."""
    headers = {'Authorization': f'Bearer {token}'}
    all_sets = []
    set_types = ['ranked_and_approved', 'loved']
    session = get_session()
    
    for s_type in set_types:
        if cancel_event and cancel_event.is_set(): return []
        offset = 0
        limit = 50
        while True:
            if cancel_event and cancel_event.is_set(): return []
            params = {'limit': limit, 'offset': offset}
            url = f'{API_BASE}/users/{user_id}/beatmapsets/{s_type}'
            
            try:
                response = session.get(url, headers=headers, params=params, timeout=15)
                if response.status_code == 404:
                    break 
                
                response.raise_for_status()
                data = response.json()
                
                if not data:
                    break
                    
                for s in data:
                    s['status_category'] = s_type
                    all_sets.append(s)

                offset += limit
                time.sleep(0.1) 
            except Exception as e:
                logger.error("Failed to fetch %s sets for user %s: %s", s_type, user_id, e)
                raise e
                
                
    return all_sets

def get_nominated_beatmapsets(user_id, token, cancel_event=None):
    """Fetches all beatmap sets nominated by a user."""
    headers = {'Authorization': f'Bearer {token}'}
    all_sets = []
    session = get_session()
    
    offset = 0
    limit = 50 # Unknown limit for this endpoint, safe bet
    
    while True:
        if cancel_event and cancel_event.is_set(): return []
        
        # This is a hidden endpoint, pagination support is assumed but not guaranteed.
        # If pagination doesn't accept 'offset', we might only get the first page.
        # But most osu! endpoints use offset/limit.
        params = {'limit': limit, 'offset': offset}
        url = f'{API_BASE}/users/{user_id}/beatmapsets/nominated'
        
        try:
            response = session.get(url, headers=headers, params=params, timeout=15)
            if response.status_code == 404: break

            response.raise_for_status()
            data = response.json()

            if not data: break

            all_sets.extend(data)

            offset += limit
            time.sleep(0.1)
        except Exception as e:
            logger.error("Failed to fetch nominated sets for user %s: %s", user_id, e)
            raise e
            
    return all_sets

import concurrent.futures

def process_set(bset, host_id, token=None):
    """Deep scans a single set and finds unique GDers."""
    gds_in_set = []

    try:
        # Full Deep Fetch - get token dynamically
        token = _token_manager.get_token()
        if not token:
            logger.warning("Failed to get token for set %s, using search data", bset['id'])
            full_set = bset
        else:
            headers = {'Authorization': f'Bearer {token}'}
            url = f'{API_BASE}/beatmapsets/{bset["id"]}'
            session = get_session()
            r = session.get(url, headers=headers, timeout=10)

            # Handle 401 by refreshing token and retrying once
            if r.status_code == 401:
                logger.warning("Token expired for set %s, refreshing...", bset['id'])
                refreshed_token = _token_manager.refresh_token()
                if refreshed_token:
                    headers = {'Authorization': f'Bearer {refreshed_token}'}
                    r = session.get(url, headers=headers, timeout=10)

            if r.status_code == 200:
                full_set = r.json()
            else:
                # Preserve essential fields from search result when deep-fetch fails
                logger.warning("Deep-fetch failed for set %s with status %s, using search data", bset['id'], r.status_code)
                full_set = bset

    except Exception as e:
        logger.error("Error fetching deep set %s: %s", bset['id'], e)
        # Preserve essential fields from search result
        full_set = bset

    beats = full_set.get('beatmaps', [])
    if not beats:
        return []

    # Count each beatmap separately (no deduplication)
    # If a user has 2 diffs in one set, they get counted as 2
    for beatmap in beats:
        owners = beatmap.get('owners', [])
        mode = beatmap.get('mode', 'osu')
        date = beatmap['last_updated'].split('T')[0]

        if owners:
            for owner in owners:
                if owner['id'] != host_id:
                    gds_in_set.append({
                        'mapper_id': owner['id'],
                        'mapper_name': owner['username'],
                        'last_updated': date,
                        'modes': [mode]
                    })
        else:
            mapper_id = beatmap['user_id']
            if mapper_id != host_id:
                gds_in_set.append({
                    'mapper_id': mapper_id,
                    'mapper_name': None,
                    'last_updated': date,
                    'modes': [mode]
                })

    return gds_in_set

def analyze_sets(beatmapsets, host_id, token, progress_callback=None, cancel_event=None):
    """Finds GDs in the provided beatmap sets using Concurrent Futures for speed."""
    all_gds = []
    total = len(beatmapsets)
    if progress_callback: progress_callback(f"Deep Scanning {total} sets...")

    if cancel_event and cancel_event.is_set(): return []

    # Use ThreadPool to speed up I/O - reduced from 15 to 8 to avoid 429 errors
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        # Submit all tasks
        future_to_set = {executor.submit(process_set, bset, host_id, token): bset for bset in beatmapsets}

        completed = 0
        for future in concurrent.futures.as_completed(future_to_set):
            if cancel_event and cancel_event.is_set():
                executor.shutdown(wait=False, cancel_futures=True)
                return []

            completed += 1
            if completed % 5 == 0:
                if progress_callback: progress_callback(f"Scanning progress: {completed}/{total} sets...")
                time.sleep(0.05)  # Small sleep to reduce API pressure

            try:
                results = future.result()
                all_gds.extend(results)
            except Exception as e:
                logger.error("Set processing generated an exception: %s", e)

    return all_gds

def process_nominator_set(bset, token=None, session=None):
    """Deep fetches a set to find its nominators, GDers (with modes), and host.

    Returns a tuple of:
      nominations        – list of nomination dicts
      gd_entries         – list of (uid, mode) tuples, one per diff per non-host owner
      set_modes          – set of all modes present in the set
      mapset_host_id     – user_id of the set host (may be None on failure)
      host_modes         – set of modes where the host personally mapped a diff
    """
    nominations = []
    gd_entries = []     # list of (uid, mode) – one entry per diff per non-host owner
    set_modes = set()   # all modes present in this set
    mapset_host_id = None
    host_modes = set()

    local_session = get_session()
    try:
        # Get token dynamically
        token = _token_manager.get_token()
        if not token:
            logger.warning("Failed to get token for set %s", bset['id'])
            return nominations, gd_entries, set_modes, mapset_host_id, host_modes

        headers = {'Authorization': f'Bearer {token}'}
        url = f'{API_BASE}/beatmapsets/{bset["id"]}'
        r = local_session.get(url, headers=headers, timeout=20)

        # Handle 401 by refreshing token and retrying once
        if r.status_code == 401:
            logger.warning("Token expired for set %s, refreshing...", bset['id'])
            refreshed_token = _token_manager.refresh_token()
            if refreshed_token:
                headers = {'Authorization': f'Bearer {refreshed_token}'}
                r = local_session.get(url, headers=headers, timeout=20)

        if r.status_code == 200:
            data = r.json()
            current_noms = data.get('current_nominations', [])

            for nom in current_noms:
                nominations.append({
                    'nominator_id': nom['user_id'],
                    'set_id': bset['id'],
                    'set_title': f"{bset['artist']} - {bset['title']}",
                    'date': (bset.get('ranked_date') or bset.get('last_updated')).split('T')[0],
                    'rulesets': nom.get('rulesets', [])
                })

            # Fallback to events API when current_nominations is empty (historical/reset maps)
            if not current_noms:
                try:
                    events_url = f'{API_BASE}/beatmapsets/events'
                    events_params = {
                        'types[]': 'nominate',
                        'min_date': (bset.get('ranked_date') or bset.get('last_updated', '2007-01-01')).split('T')[0]
                    }
                    events_r = local_session.get(events_url, headers=headers, params=events_params, timeout=15)

                    # Handle 401 for events API too
                    if events_r.status_code == 401:
                        logger.warning("Token expired for events API (set %s), refreshing...", bset['id'])
                        refreshed_token = _token_manager.refresh_token()
                        if refreshed_token:
                            headers = {'Authorization': f'Bearer {refreshed_token}'}
                            events_r = local_session.get(events_url, headers=headers, params=events_params, timeout=15)

                    if events_r.status_code == 200:
                        events_data = events_r.json()
                        for event in events_data.get('events', []):
                            if event.get('beatmapset', {}).get('id') == bset['id']:
                                nom_user = event.get('user', {})
                                if nom_user and nom_user.get('id'):
                                    mode = event.get('discussion', {}).get('beatmap', {}).get('mode', 'osu')
                                    rulesets = mode if isinstance(mode, list) else [mode]
                                    nominations.append({
                                        'nominator_id': nom_user['id'],
                                        'set_id': bset['id'],
                                        'set_title': f"{bset['artist']} - {bset['title']}",
                                        'date': (event.get('created_at') or bset.get('ranked_date') or bset.get('last_updated')).split('T')[0],
                                        'rulesets': rulesets
                                    })
                        time.sleep(0.1)  # Small sleep after events API call
                except Exception as e:
                    logger.error("Error fetching events for set %s: %s", bset['id'], e)

            # Extract host, GDers, and modes
            mapset_host_id = data.get('user_id')
            # Use `is not None` so user_id=0 (deleted account) still triggers processing
            if mapset_host_id is not None:
                beatmaps = data.get('beatmaps', [])
                _debug = logger.isEnabledFor(logging.DEBUG)
                if _debug:
                    logger.debug("Set %s (%s - %s): %d diffs, host_id=%s",
                                 bset['id'], bset.get('artist', ''), bset.get('title', ''),
                                 len(beatmaps), mapset_host_id)
                for beatmap in beatmaps:
                    bm_mode = beatmap.get('mode', 'osu')
                    bm_id = beatmap.get('id', '?') if _debug else None
                    set_modes.add(bm_mode)
                    owners = beatmap.get('owners', [])
                    if owners:
                        # Collab diff: attribute each owner individually
                        for owner in owners:
                            uid = owner['id']
                            if uid == mapset_host_id:
                                host_modes.add(bm_mode)
                                if _debug:
                                    logger.debug("  diff %s [%s]: host %s", bm_id, bm_mode, uid)
                            else:
                                gd_entries.append((uid, bm_mode))
                                if _debug:
                                    logger.debug("  diff %s [%s]: GD owner %s (via owners field)", bm_id, bm_mode, uid)
                    else:
                        # Single creator: fall back to user_id field
                        diff_creator = beatmap.get('user_id')
                        if diff_creator == mapset_host_id:
                            host_modes.add(bm_mode)
                            if _debug:
                                logger.debug("  diff %s [%s]: host %s (via user_id)", bm_id, bm_mode, diff_creator)
                        elif diff_creator is not None and diff_creator != mapset_host_id:
                            gd_entries.append((diff_creator, bm_mode))
                            if _debug:
                                logger.debug("  diff %s [%s]: GD creator %s (via user_id)", bm_id, bm_mode, diff_creator)
                        elif _debug:
                            logger.debug("  diff %s [%s]: no creator info (user_id=%s)", bm_id, bm_mode, diff_creator)

                # Log at INFO if GDs or noms were found (useful for diagnosis), else DEBUG
                if gd_entries or nominations:
                    logger.info("Set %s: %d GD diffs, %d nominations, host_modes=%s",
                                bset['id'], len(gd_entries), len(nominations), host_modes)
                else:
                    logger.debug("Set %s: no GDs or nominations found", bset['id'])
            else:
                logger.warning("Set %s has no user_id in API response; skipping diff attribution.", bset['id'])
        else:
            logger.warning("Deep-fetch failed for set %s with status %s", bset['id'], r.status_code)

    except Exception as e:
        logger.error("Error fetching set %s: %s", bset['id'], e)
    finally:
        local_session.close()

    # NOTE: host_modes intentionally left empty when the host has no personal diffs.
    # A host who only gathered GDs should not be credited for those modes.

    return nominations, gd_entries, set_modes, mapset_host_id, host_modes
            
# Global Cache
USER_CACHE = {}

def resolve_users_parallel(user_ids, token, progress_callback=None):
    """Resolves a list of user IDs to usernames using threading, with caching."""
    headers = {'Authorization': f'Bearer {token}'}

    # Identify which IDs are missing from cache
    missing_ids = [uid for uid in user_ids if uid not in USER_CACHE and uid != 0]
    total_missing = len(missing_ids)

    if total_missing > 0:
        msg = f"Resolving {total_missing} usernames..."
        if progress_callback: progress_callback(msg)

        def fetch_user(uid):
            try:
                session = get_session()
                r = session.get(f'{API_BASE}/users/{uid}', headers=headers, timeout=15)
                if r.status_code == 200:
                    return (uid, r.json()['username'])
            except:
                pass
            return (uid, f"User_{uid}")

        # Reduced from 15 to 8 to avoid 429 errors
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            future_to_uid = {executor.submit(fetch_user, uid): uid for uid in missing_ids}

            completed = 0
            for future in concurrent.futures.as_completed(future_to_uid):
                completed += 1
                if completed % 10 == 0:
                     if progress_callback: progress_callback(f"Resolving names {completed}/{total_missing}...")
                     time.sleep(0.05)  # Small sleep to reduce API pressure

                try:
                    uid, name = future.result()
                    USER_CACHE[uid] = name
                except:
                    pass

    # Build result from cache
    return {uid: USER_CACHE.get(uid, f"User_{uid}") for uid in user_ids if uid != 0}

def analyze_nominators(beatmapsets, token, progress_callback=None, cancel_event=None):
    """Fetches nominators for the provided beatmap sets using threading."""
    all_nominations = []

    target_sets = [b for b in beatmapsets if b['status'] in ['ranked', 'loved', 'qualified', 'approved']]
    total = len(target_sets)

    msg = f"Scanning {total} sets for Nominators..."
    if progress_callback: progress_callback(msg)

    if cancel_event and cancel_event.is_set(): return []

    # Create a thread-local session factory or just pass a session?
    # Actually requests.Session is not thread-safe if shared across threads heavily?
    # Documentation says Session is thread-safe.
    # But for safety, we can create one session per thread if we want, but sharing is usually fine for read-only.
    # Let's try sharing one session to reuse connections.
    session = get_session()

    # Reduced from 15 to 8 to avoid 429 errors
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        future_to_set = {executor.submit(process_nominator_set, bset, token, session): bset for bset in target_sets}

        completed = 0
        for future in concurrent.futures.as_completed(future_to_set):
            completed += 1
            if completed % 5 == 0:
                if progress_callback: progress_callback(f"Scanning progress: {completed}/{total} sets...")
                time.sleep(0.05)  # Small sleep to reduce API pressure

            try:
                noms, _gd_entries, _set_modes, _host_id, _host_modes = future.result()
                all_nominations.extend(noms)
            except Exception as e:
                logger.error("Nominator scan exception: %s", e)

    session.close()
    return all_nominations

def resolve_and_aggregate_nominators(noms, token, progress_callback=None):
    """Resolves names and builds the nominator leaderboard using parallel resolution."""
    unique_ids = set(n['nominator_id'] for n in noms)
    
    # Use the new parallel resolver
    user_cache = resolve_users_parallel(unique_ids, token, progress_callback)
            
    stats = defaultdict(lambda: {'count': 0, 'last_date': '', 'modes': set(), 'mode_counts': defaultdict(int)})
    
    for n in noms:
        name = user_cache.get(n['nominator_id'], f"ID:{n['nominator_id']}")
        date = n['date']
        rulesets = n.get('rulesets', [])
        if not rulesets:
            rulesets = ['osu'] # fallback
            
        stats[name]['count'] += 1
        stats[name]['modes'].update(rulesets)
        for r in rulesets:
            stats[name]['mode_counts'][r] += 1
            
        if date and date > stats[name]['last_date']:
            stats[name]['last_date'] = date
            
    leaderboard = []
    for name, data in stats.items():
        leaderboard.append({
            'mapper_name': name, 
            'total_gds': data['count'], 
            'last_gd_date': data['last_date'],
            'modes': list(data['modes']),
            'mode_counts': dict(data['mode_counts'])
        })
        
    leaderboard.sort(key=lambda x: (-x['total_gds'], x['mapper_name']))
    return leaderboard

def generate_nominator_leaderboard_for_user(username_input, progress_callback=None, cancel_event=None):
    token = get_token()
    if not token:
        return {'error': 'Authentication failed'}
        
    user_id, username = get_user_id(username_input, token)
    if not user_id:
        return {'error': f'User {username_input} not found'}
        
    # Fetch sets
    if progress_callback: progress_callback(f"Fetching beatmap sets for {username}...")
    sets = get_beatmapsets(user_id, token, cancel_event)
    
    if cancel_event and cancel_event.is_set(): return {'error': 'Cancelled'}
    
    # Analyze
    noms = analyze_nominators(sets, token, progress_callback, cancel_event)
    
    if cancel_event and cancel_event.is_set(): return {'error': 'Cancelled'}
    
    if not noms:
         return {'username': username, 'leaderboard': []}
         
    leaderboard = resolve_and_aggregate_nominators(noms, token, progress_callback)
    
    return {
        'username': username,
        'leaderboard': leaderboard,
        'type': 'Nominators'
    }

def generate_bn_leaderboard_for_user(username_input, progress_callback=None, cancel_event=None):
    """New Mode: Find mappers nominated by this BN."""
    token = get_token()
    if not token: return {'error': 'Authentication failed'}
    
    if cancel_event and cancel_event.is_set(): return {'error': 'Cancelled'}
    
    user_id, username = get_user_id(username_input, token)
    if not user_id: return {'error': f'User {username_input} not found'}
    
    # 1. Fetch nominated sets
    if progress_callback: progress_callback(f"Fetching maps nominated by {username}...")
    sets = get_nominated_beatmapsets(user_id, token, cancel_event)
    
    if cancel_event and cancel_event.is_set(): return {'error': 'Cancelled'}
    
    if not sets:
         return {'username': username, 'leaderboard': []}

    # 2. Count mappers (user_id field in beatmapset)
    if progress_callback: progress_callback(f"Analyzing {len(sets)} nominations...")
    
    stats = defaultdict(lambda: {'count': 0, 'last_date': '', 'modes': set(), 'mode_counts': defaultdict(int)})
    mappers_to_resolve = set()
    
    for bset in sets:
        mapper_id = bset['user_id']
        mappers_to_resolve.add(mapper_id)
        
        # Approximate date (ranked_date or last_updated)
        date = (bset.get('ranked_date') or bset.get('last_updated') or '').split('T')[0]
        
        host_modes = set()
        for bm in bset.get('beatmaps', []):
            if bm.get('user_id') == mapper_id:
                host_modes.add(bm.get('mode', 'osu'))
                
        if not host_modes:
            host_modes = set(['osu'])
            
        # We store by ID temporarily
        stats[mapper_id]['count'] += 1
        stats[mapper_id]['modes'].update(host_modes)
        for m in host_modes:
            stats[mapper_id]['mode_counts'][m] += 1
            
        if date and date > stats[mapper_id]['last_date']:
            stats[mapper_id]['last_date'] = date
            
    # 3. Resolve names
    if progress_callback: progress_callback("Resolving mapper names...")
    user_cache = resolve_users_parallel(mappers_to_resolve, token, progress_callback)
    
    if cancel_event and cancel_event.is_set(): return {'error': 'Cancelled'}
    
    # 4. Build leaderboard
    leaderboard = []
    for mid, data in stats.items():
        name = user_cache.get(mid, f"ID:{mid}")
        leaderboard.append({
            'mapper_name': name,
            'total_gds': data['count'],
            'last_gd_date': data['last_date'],
            'modes': list(data['modes']),
            'mode_counts': dict(data['mode_counts'])
        })
        
    leaderboard.sort(key=lambda x: (-x['total_gds'], x['mapper_name']))
    
    return {
        'username': username,
        'leaderboard': leaderboard,
        'type': 'Nominations'
    }

def get_guest_beatmapsets(user_id, token, cancel_event=None):
    """Fetches all beatmap sets where the user has contributed a guest difficulty."""
    headers = {'Authorization': f'Bearer {token}'}
    all_sets = []
    session = get_session()
    
    offset = 0
    limit = 50
    
    while True:
        if cancel_event and cancel_event.is_set(): return []
        
        params = {'limit': limit, 'offset': offset}
        url = f'{API_BASE}/users/{user_id}/beatmapsets/guest'
        
        try:
            response = session.get(url, headers=headers, params=params, timeout=15)
            if response.status_code == 404: break

            response.raise_for_status()
            data = response.json()

            if not data: break

            all_sets.extend(data)

            offset += limit
            time.sleep(0.1)
        except Exception as e:
            logger.error("Failed to fetch guest sets for user %s: %s", user_id, e)
            raise e
            
    return all_sets

def get_user_ranked_beatmapsets(user_id, token, cancel_event=None):
    """Fetches ranked_and_approved beatmapsets for a user (used for host counting)."""
    headers = {'Authorization': f'Bearer {token}'}
    all_sets = []
    session = get_session()

    offset = 0
    limit = 50

    while True:
        if cancel_event and cancel_event.is_set():
            return []
        params = {'limit': limit, 'offset': offset}
        url = f'{API_BASE}/users/{user_id}/beatmapsets/ranked_and_approved'

        try:
            response = session.get(url, headers=headers, params=params, timeout=15)
            if response.status_code == 404:
                break
            response.raise_for_status()
            data = response.json()
            if not data:
                break
            all_sets.extend(data)
            offset += limit
            time.sleep(0.1)
        except Exception as e:
            logger.error("Failed to fetch ranked sets for user %s: %s", user_id, e)
            break

    return all_sets


def generate_gd_hosts_leaderboard_for_user(username_input, progress_callback=None, cancel_event=None):
    """New Mode: Find which mappers the user has made the most GDs for."""
    token = get_token()
    if not token: return {'error': 'Authentication failed'}
    
    if cancel_event and cancel_event.is_set(): return {'error': 'Cancelled'}
    
    user_id, username = get_user_id(username_input, token)
    if not user_id: return {'error': f'User {username_input} not found'}
    
    # 1. Fetch guest beatmapsets (maps where user contributed a GD)
    if progress_callback: progress_callback(f"Fetching GD sets for {username}...")
    sets = get_guest_beatmapsets(user_id, token, cancel_event)
    
    if cancel_event and cancel_event.is_set(): return {'error': 'Cancelled'}
    
    if not sets:
         return {'username': username, 'leaderboard': []}

    # 2. Count hosts (user_id field in each beatmapset = the host)
    if progress_callback: progress_callback(f"Analyzing {len(sets)} GD sets...")

    stats = defaultdict(lambda: {'count': 0, 'last_date': '', 'modes': set(), 'mode_counts': defaultdict(int)})
    hosts_to_resolve = set()

    for bset in sets:
        host_id = bset['user_id']
        # Skip sets where the scanned user is also the host
        if host_id == user_id:
            continue
        hosts_to_resolve.add(host_id)

        # Use ranked_date or last_updated as date
        date = (bset.get('ranked_date') or bset.get('last_updated') or '').split('T')[0]

        # Count per-difficulty, not per-set
        beatmaps = bset.get('beatmaps', [])
        if beatmaps:
            for bm in beatmaps:
                # Check if this beatmap belongs to the scanned user
                owners = bm.get('owners', [])
                is_user_beatmap = False
                bm_mode = bm.get('mode', 'osu')

                if owners:
                    # Check if user_id is in owners
                    for owner in owners:
                        if owner['id'] == user_id:
                            is_user_beatmap = True
                            break
                else:
                    # Fallback to user_id check
                    if bm.get('user_id') == user_id:
                        is_user_beatmap = True

                if is_user_beatmap:
                    stats[host_id]['count'] += 1
                    stats[host_id]['modes'].add(bm_mode)
                    stats[host_id]['mode_counts'][bm_mode] += 1

                    bm_date = bm.get('last_updated', date)
                    if bm_date and bm_date > stats[host_id]['last_date']:
                        stats[host_id]['last_date'] = bm_date
        else:
            # Fallback: if the API returns no per-difficulty `beatmaps` for this set,
            # we conservatively count it as a single difficulty in `osu` mode.
            # This avoids dropping the set entirely from stats, but may undercount
            # in cases where the user created multiple difficulties in the same set.
            # A more accurate approach would require an additional API call here to
            # fetch full beatmapset data (similar to other code paths) and count
            # each difficulty explicitly. We intentionally skip that here to keep
            # this scan lightweight and accept the approximation.
            stats[host_id]['count'] += 1
            stats[host_id]['modes'].add('osu')
            stats[host_id]['mode_counts']['osu'] += 1

            if date and date > stats[host_id]['last_date']:
                stats[host_id]['last_date'] = date
            
    # 3. Resolve host names
    if progress_callback: progress_callback("Resolving host names...")
    user_cache = resolve_users_parallel(hosts_to_resolve, token, progress_callback)
    
    if cancel_event and cancel_event.is_set(): return {'error': 'Cancelled'}
    
    # 4. Build leaderboard
    leaderboard = []
    for host_id, data in stats.items():
        name = user_cache.get(host_id, f"ID:{host_id}")
        leaderboard.append({
            'mapper_name': name,
            'total_gds': data['count'],
            'last_gd_date': data['last_date'],
            'modes': list(data['modes']),
            'mode_counts': dict(data['mode_counts'])
        })
        
    leaderboard.sort(key=lambda x: (-x['total_gds'], x['mapper_name']))
    
    return {
        'username': username,
        'leaderboard': leaderboard,
        'type': 'GD Hosts'
    }

def resolve_and_aggregate(gds, token, progress_callback=None):
    """Resolves names and builds the leaderboard using parallel resolution."""
    
    # Only resolve IDs that have no name
    unique_ids_to_resolve = set(gd['mapper_id'] for gd in gds if not gd['mapper_name'])
    
    # Use the new parallel resolver
    user_cache = resolve_users_parallel(unique_ids_to_resolve, token, progress_callback)
            
    # Aggregate
    stats = defaultdict(lambda: {'count': 0, 'last_date': '', 'modes': set(), 'mode_counts': defaultdict(int)})
    
    for gd in gds:
        # Use provided name, or lookup in cache, or fallback to ID
        if gd['mapper_name']:
            mapper_name = gd['mapper_name']
        else:
            mapper_name = user_cache.get(gd['mapper_id'], f"ID:{gd['mapper_id']}")
            
        date = gd['last_updated']
        modes = gd.get('modes', ['osu'])
        if not modes:
            modes = ['osu']
        
        stats[mapper_name]['count'] += 1
        stats[mapper_name]['modes'].update(modes)
        for m in modes:
            stats[mapper_name]['mode_counts'][m] += 1
            
        if date > stats[mapper_name]['last_date']:
            stats[mapper_name]['last_date'] = date

    # Sort
    leaderboard = []
    for mapper, data in stats.items():
        leaderboard.append({
            'mapper_name': mapper,
            'total_gds': data['count'],
            'last_gd_date': data['last_date'],
            'modes': list(data['modes']),
            'mode_counts': dict(data['mode_counts'])
        })
    
    leaderboard.sort(key=lambda x: (-x['total_gds'], x['mapper_name']))
    return leaderboard

def generate_leaderboard_for_user(username_input, progress_callback=None, cancel_event=None):
    """Main entry point for web app."""
    token = get_token()
    if not token:
        return {'error': 'Authentication failed'}
        
    user_id, username = get_user_id(username_input, token)
    if not user_id:
        return {'error': f'User {username_input} not found'}
        
    if progress_callback: progress_callback(f"Found User: {username}. Fetching sets...")
    
    sets = get_beatmapsets(user_id, token, cancel_event)
    
    if cancel_event and cancel_event.is_set(): return {'error': 'Cancelled'}
    
    gds = analyze_sets(sets, user_id, token, progress_callback, cancel_event)
    
    if cancel_event and cancel_event.is_set(): return {'error': 'Cancelled'}
    
    if not gds:
        return {'username': username, 'leaderboard': []}
        
    leaderboard = resolve_and_aggregate(gds, token, progress_callback)
    
    return {
        'username': username,
        'leaderboard': leaderboard
    }


def search_ranked_beatmapsets(token, statuses=None, progress_callback=None):
    """Fetches all beatmapsets of the given statuses using cursor-based pagination."""
    if statuses is None:
        statuses = ['ranked', 'approved']

    headers = {'Authorization': f'Bearer {token}'}
    session = get_session()
    all_sets = []
    seen_ids = set()
    try:
        for status in statuses:
            if progress_callback:
                progress_callback(f"Fetching {status} beatmapsets...")
            cursor_string = None
            page_count = 0

            while True:
                params = {'s': status, 'sort': 'ranked_asc'}
                if cursor_string:
                    params['cursor_string'] = cursor_string

                try:
                    r = session.get(f'{API_BASE}/beatmapsets/search', headers=headers, params=params, timeout=30)

                    if r.status_code == 401:
                        refreshed = _token_manager.refresh_token()
                        if refreshed:
                            headers = {'Authorization': f'Bearer {refreshed}'}
                            r = session.get(f'{API_BASE}/beatmapsets/search', headers=headers, params=params, timeout=30)

                    r.raise_for_status()
                    data = r.json()
                    beatmapsets = data.get('beatmapsets', [])

                    if not beatmapsets:
                        break

                    for bset in beatmapsets:
                        if bset['id'] not in seen_ids:
                            seen_ids.add(bset['id'])
                            all_sets.append(bset)

                    page_count += 1
                    if page_count % 10 == 0 and progress_callback:
                        progress_callback(f"Fetched {len(all_sets)} {status} sets so far...")

                    cursor_string = data.get('cursor_string')
                    if not cursor_string:
                        break

                    time.sleep(0.5)

                except Exception as e:
                    logger.error("Error searching %s beatmapsets (page %d): %s", status, page_count, e)
                    raise

        return all_sets
    finally:
        session.close()


def search_ranked_beatmapsets_since(token, since_date, statuses=None, progress_callback=None):
    """Fetches beatmapsets ranked strictly after since_date (for incremental scans).

    Paginates using ranked_desc order and stops as soon as a set's ranked_date is
    on or before since_date, so only newly-ranked sets are returned.
    """
    if statuses is None:
        statuses = ['ranked', 'approved']

    headers = {'Authorization': f'Bearer {token}'}
    session = get_session()
    all_sets = []
    seen_ids = set()
    try:
        for status in statuses:
            if progress_callback:
                progress_callback(f"Fetching new {status} beatmapsets since {since_date}...")
            cursor_string = None
            done = False
            page_count = 0

            while not done:
                params = {'s': status, 'sort': 'ranked_desc'}
                if cursor_string:
                    params['cursor_string'] = cursor_string

                try:
                    r = session.get(f'{API_BASE}/beatmapsets/search', headers=headers, params=params, timeout=30)

                    if r.status_code == 401:
                        refreshed = _token_manager.refresh_token()
                        if refreshed:
                            headers = {'Authorization': f'Bearer {refreshed}'}
                            r = session.get(f'{API_BASE}/beatmapsets/search', headers=headers, params=params, timeout=30)

                    r.raise_for_status()
                    data = r.json()
                    beatmapsets = data.get('beatmapsets', [])

                    if not beatmapsets:
                        break

                    for bset in beatmapsets:
                        ranked_date = (bset.get('ranked_date') or '').split('T')[0]
                        if ranked_date and ranked_date <= since_date:
                            done = True
                            break
                        if bset['id'] not in seen_ids:
                            seen_ids.add(bset['id'])
                            all_sets.append(bset)

                    if not done:
                        page_count += 1
                        if page_count % 5 == 0 and progress_callback:
                            progress_callback(f"Fetched {len(all_sets)} new {status} sets so far...")
                        cursor_string = data.get('cursor_string')
                        if not cursor_string:
                            break
                        time.sleep(0.5)

                except Exception as e:
                    logger.error("Error fetching %s sets since %s (page %d): %s", status, since_date, page_count, e)
                    raise

        return all_sets
    finally:
        session.close()


def fetch_all_nomination_events(token, since_date=None, progress_callback=None):
    """Paginates /beatmapsets/events?types[]=nominate to collect nominator-per-set data.

    Args:
        token: OAuth token.
        since_date: Optional ISO date string 'YYYY-MM-DD'. When provided only events
            on or after this date are fetched (incremental mode).
        progress_callback: Optional callable(str) for progress messages.

    Returns:
        (set_nominators, nominator_ids) where:
            set_nominators – dict {set_id: [uid, ...]} of unique nominators per set
            nominator_ids  – set of all unique nominator user IDs found
    """
    headers = {'Authorization': f'Bearer {token}'}
    session = get_session()
    set_nominators = defaultdict(list)
    nominator_ids = set()
    params = {'types[]': 'nominate', 'limit': 50}
    if since_date:
        params['min_date'] = since_date

    cursor_string = None
    page = 0
    try:
        while True:
            if cursor_string:
                params['cursor_string'] = cursor_string

            try:
                r = session.get(f'{API_BASE}/beatmapsets/events', headers=headers, params=params, timeout=20)

                if r.status_code == 401:
                    refreshed = _token_manager.refresh_token()
                    if refreshed:
                        headers = {'Authorization': f'Bearer {refreshed}'}
                        r = session.get(f'{API_BASE}/beatmapsets/events', headers=headers, params=params, timeout=20)

                r.raise_for_status()
                data = r.json()
                events = data.get('events', [])

                if not events:
                    break

                for event in events:
                    user_obj = event.get('user') or {}
                    uid = user_obj.get('id') if isinstance(user_obj, dict) else None
                    bset_obj = event.get('beatmapset') or {}
                    set_id = bset_obj.get('id') if isinstance(bset_obj, dict) else None
                    if uid and set_id:
                        nominator_ids.add(uid)
                        if uid not in set_nominators[set_id]:
                            set_nominators[set_id].append(uid)

                page += 1
                if page % 20 == 0 and progress_callback:
                    progress_callback(f"Nomination events fetched: ~{page * 50} so far...")

                cursor_string = data.get('cursor_string')
                if not cursor_string:
                    break

                time.sleep(0.3)

            except Exception as e:
                logger.error("Error fetching nomination events (page %d): %s", page, e)
                raise
    finally:
        session.close()

    return dict(set_nominators), nominator_ids


def run_global_scan(progress_callback=None, scan_state=None):
    """Runs an incremental (or full) global scan using per-user API endpoints.

    On the first run (scan_state is None or empty) every ranked/approved beatmapset
    is fetched and per-user scans are executed for all GD contributors, hosts, and BNs.

    On subsequent runs only newly-ranked sets are fetched and per-user scans are run
    only for the small number of users who appear in those new sets.  All other users'
    counts are taken directly from the saved scan_state checkpoint, making the monthly
    update fast while remaining accurate.

    Args:
        progress_callback: Optional callable(str) for status messages.
        scan_state: Optional dict loaded from scan_state.json from the previous run.
            Must contain at least 'last_ranked_date' for incremental mode to activate.

    Returns:
        dict with leaderboard data and a 'scan_state' key holding the new checkpoint.
    """
    token = get_token()
    if not token:
        return {'error': 'Authentication failed'}

    # --- Load existing checkpoint or start fresh ---
    is_incremental = bool(scan_state and scan_state.get('last_ranked_date'))
    last_ranked_date = (scan_state or {}).get('last_ranked_date', '')

    # Internal dicts keyed by integer user ID; stored as string keys in JSON
    gd_counts = {int(k): v for k, v in (scan_state or {}).get('gd_counts', {}).items()}
    gd_modes = {int(k): v for k, v in (scan_state or {}).get('gd_modes', {}).items()}
    gd_last_dates = {int(k): v for k, v in (scan_state or {}).get('gd_last_dates', {}).items()}
    host_counts = {int(k): v for k, v in (scan_state or {}).get('host_counts', {}).items()}
    host_modes = {int(k): v for k, v in (scan_state or {}).get('host_modes', {}).items()}
    host_last_dates = {int(k): v for k, v in (scan_state or {}).get('host_last_dates', {}).items()}
    bn_counts = {int(k): v for k, v in (scan_state or {}).get('bn_counts', {}).items()}
    bn_modes = {int(k): v for k, v in (scan_state or {}).get('bn_modes', {}).items()}
    bn_last_dates = {int(k): v for k, v in (scan_state or {}).get('bn_last_dates', {}).items()}
    # bn_duos keys are "id1-id2" strings (id1 < id2)
    bn_duos = dict((scan_state or {}).get('bn_duos', {}))

    if is_incremental:
        logger.info("Incremental scan — fetching sets ranked after %s", last_ranked_date)
        if progress_callback:
            progress_callback(f"Incremental scan — fetching sets ranked after {last_ranked_date}...")
        all_sets = search_ranked_beatmapsets_since(
            token, last_ranked_date, progress_callback=progress_callback
        )
    else:
        logger.info("Full global scan starting")
        if progress_callback:
            progress_callback("Full scan — fetching all ranked/approved beatmapsets...")
        all_sets = search_ranked_beatmapsets(
            token, statuses=['ranked', 'approved'], progress_callback=progress_callback
        )

    if not all_sets and not is_incremental:
        return {'error': 'No beatmapsets found'}

    total_new = len(all_sets)
    logger.info("Fetched %d beatmapsets to process.", total_new)
    if progress_callback:
        progress_callback(f"Found {total_new} beatmapsets to process.")

    # --- Step 1: Collect host IDs and GD contributor IDs from search results ---
    new_host_ids = set()
    new_gd_ids = set()
    new_last_ranked_date = last_ranked_date
    # Cache creator names directly from search results to skip extra API calls
    creator_names = {}

    for bset in all_sets:
        host_id = bset.get('user_id')
        if host_id is not None:
            new_host_ids.add(host_id)
            if bset.get('creator'):
                creator_names[host_id] = bset['creator']

        for bm in bset.get('beatmaps', []):
            owners = bm.get('owners', [])
            if owners:
                for owner in owners:
                    if owner['id'] != host_id:
                        new_gd_ids.add(owner['id'])
            else:
                diff_creator = bm.get('user_id')
                if diff_creator is not None and diff_creator != host_id:
                    new_gd_ids.add(diff_creator)

        ranked_date = (bset.get('ranked_date') or '').split('T')[0]
        if ranked_date and ranked_date > new_last_ranked_date:
            new_last_ranked_date = ranked_date

    logger.info("Collected %d unique host IDs and %d GD contributor IDs from search results.",
                len(new_host_ids), len(new_gd_ids))

    # --- Step 2: Fetch nomination events ---
    if progress_callback:
        progress_callback("Fetching nomination events...")
    events_since = last_ranked_date if is_incremental else None
    try:
        set_nominators, new_nominator_ids = fetch_all_nomination_events(
            token, since_date=events_since, progress_callback=progress_callback
        )
    except Exception as e:
        logger.error("Failed to fetch nomination events: %s", e)
        set_nominators = {}
        new_nominator_ids = set()

    logger.info("Collected %d unique nominator IDs from events.", len(new_nominator_ids))

    # --- Step 3: Per-user scans for affected users (parallel, max 8 workers) ---

    def _scan_gd(uid):
        tok = _token_manager.get_token()
        sets = get_guest_beatmapsets(uid, tok)
        count = len(sets)
        mc = defaultdict(int)
        last_date = ''
        for bset in sets:
            ranked_date = (bset.get('ranked_date') or bset.get('last_updated') or '').split('T')[0]
            if ranked_date and ranked_date > last_date:
                last_date = ranked_date
            for bm in bset.get('beatmaps', []):
                bm_mode = bm.get('mode', 'osu')
                owners = bm.get('owners', [])
                if owners:
                    for owner in owners:
                        if owner['id'] == uid:
                            mc[bm_mode] += 1
                else:
                    if bm.get('user_id') == uid:
                        mc[bm_mode] += 1
        return count, dict(mc), last_date

    def _scan_host(uid):
        tok = _token_manager.get_token()
        sets = get_user_ranked_beatmapsets(uid, tok)
        count = len(sets)
        mc = defaultdict(int)
        last_date = ''
        for bset in sets:
            ranked_date = (bset.get('ranked_date') or bset.get('last_updated') or '').split('T')[0]
            if ranked_date and ranked_date > last_date:
                last_date = ranked_date
            for bm in bset.get('beatmaps', []):
                mc[bm.get('mode', 'osu')] += 1
        return count, dict(mc), last_date

    def _scan_bn(uid):
        tok = _token_manager.get_token()
        sets = get_nominated_beatmapsets(uid, tok)
        count = len(sets)
        mc = defaultdict(int)
        last_date = ''
        for bset in sets:
            ranked_date = (bset.get('ranked_date') or bset.get('last_updated') or '').split('T')[0]
            if ranked_date and ranked_date > last_date:
                last_date = ranked_date
            bset_modes = {bm.get('mode', 'osu') for bm in bset.get('beatmaps', [])} or {'osu'}
            for mode in bset_modes:
                mc[mode] += 1
        return count, dict(mc), last_date

    def _run_parallel_scans(user_ids, scan_fn, counts_dict, modes_dict, last_dates_dict, label):
        total_u = len(user_ids)
        completed_u = [0]

        def _worker(uid):
            result = scan_fn(uid)
            completed_u[0] += 1
            if completed_u[0] % 100 == 0 and progress_callback:
                progress_callback(f"{label}: {completed_u[0]}/{total_u}...")
            return uid, result

        if not user_ids:
            return
        if progress_callback:
            progress_callback(f"Scanning {total_u} {label} users...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(_worker, uid): uid for uid in user_ids}
            for future in concurrent.futures.as_completed(futures):
                try:
                    uid, (count, mc, last_date) = future.result()
                    counts_dict[uid] = count
                    modes_dict[uid] = mc
                    last_dates_dict[uid] = last_date
                except Exception as e:
                    uid = futures[future]
                    logger.error("%s scan failed for user %s: %s", label, uid, e)

    _run_parallel_scans(new_gd_ids, _scan_gd, gd_counts, gd_modes, gd_last_dates, "GD")
    _run_parallel_scans(new_host_ids, _scan_host, host_counts, host_modes, host_last_dates, "host")
    _run_parallel_scans(new_nominator_ids, _scan_bn, bn_counts, bn_modes, bn_last_dates, "BN")

    # --- Step 4: Update duo counts from events ---
    for set_id, nominators in set_nominators.items():
        unique_noms = list(dict.fromkeys(nominators))  # deduplicate, preserve insertion order
        for i in range(len(unique_noms)):
            for j in range(i + 1, len(unique_noms)):
                id1 = min(unique_noms[i], unique_noms[j])
                id2 = max(unique_noms[i], unique_noms[j])
                duo_key = f"{id1}-{id2}"
                bn_duos[duo_key] = bn_duos.get(duo_key, 0) + 1

    logger.info("Computed %d duo pairs total.", len(bn_duos))

    # --- Step 5: Build new scan_state checkpoint ---
    new_scan_state = {
        'last_ranked_date': new_last_ranked_date,
        'gd_counts': {str(k): v for k, v in gd_counts.items()},
        'gd_modes': {str(k): v for k, v in gd_modes.items()},
        'gd_last_dates': {str(k): v for k, v in gd_last_dates.items()},
        'host_counts': {str(k): v for k, v in host_counts.items()},
        'host_modes': {str(k): v for k, v in host_modes.items()},
        'host_last_dates': {str(k): v for k, v in host_last_dates.items()},
        'bn_counts': {str(k): v for k, v in bn_counts.items()},
        'bn_modes': {str(k): v for k, v in bn_modes.items()},
        'bn_last_dates': {str(k): v for k, v in bn_last_dates.items()},
        'bn_duos': bn_duos,
    }

    # --- Step 6: Resolve usernames for all IDs in one batch ---
    if progress_callback:
        progress_callback("Resolving user names...")

    all_ids_to_resolve = set()
    all_ids_to_resolve.update(gd_counts.keys())
    all_ids_to_resolve.update(host_counts.keys())
    all_ids_to_resolve.update(bn_counts.keys())
    for duo_key in bn_duos:
        parts = duo_key.split('-')
        if len(parts) == 2:
            try:
                all_ids_to_resolve.update(int(p) for p in parts)
            except ValueError:
                pass

    # Strip IDs already known from creator_names to minimise API calls
    ids_missing_names = all_ids_to_resolve - set(creator_names.keys())
    user_cache = resolve_users_parallel(ids_missing_names, _token_manager.get_token(), progress_callback)
    user_cache.update(creator_names)

    # --- Step 7: Build leaderboards ---
    if progress_callback:
        progress_callback("Building leaderboards...")

    gd_leaderboard = []
    for uid, count in gd_counts.items():
        if count > 0:
            mc = gd_modes.get(uid, {})
            gd_leaderboard.append({
                'username': user_cache.get(uid, f"ID:{uid}"),
                'count': count,
                'last_date': gd_last_dates.get(uid, ''),
                'modes': list(mc.keys()),
                'mode_counts': mc,
            })
    gd_leaderboard.sort(key=lambda x: (-x['count'], x['username']))

    host_leaderboard = []
    for uid, count in host_counts.items():
        if count > 0:
            mc = host_modes.get(uid, {})
            host_leaderboard.append({
                'username': user_cache.get(uid, f"ID:{uid}"),
                'count': count,
                'last_date': host_last_dates.get(uid, ''),
                'modes': list(mc.keys()),
                'mode_counts': mc,
            })
    host_leaderboard.sort(key=lambda x: (-x['count'], x['username']))

    individual_leaderboard = []
    for uid, count in bn_counts.items():
        if count > 0:
            mc = bn_modes.get(uid, {})
            individual_leaderboard.append({
                'username': user_cache.get(uid, f"ID:{uid}"),
                'count': count,
                'last_date': bn_last_dates.get(uid, ''),
                'modes': list(mc.keys()),
                'mode_counts': mc,
            })
    individual_leaderboard.sort(key=lambda x: (-x['count'], x['username']))

    duo_leaderboard = []
    for duo_key, count in bn_duos.items():
        if count > 0:
            parts = duo_key.split('-')
            if len(parts) == 2:
                try:
                    id1, id2 = int(parts[0]), int(parts[1])
                except ValueError:
                    continue
                duo_leaderboard.append({
                    'bn1_name': user_cache.get(id1, f"ID:{id1}"),
                    'bn2_name': user_cache.get(id2, f"ID:{id2}"),
                    'count': count,
                    'last_date': '',
                    'bn1_modes': [],
                    'bn2_modes': [],
                    'modes': [],
                    'mode_counts': {},
                })
    duo_leaderboard.sort(key=lambda x: (-x['count'], x['bn1_name']))

    logger.info("Leaderboards built: %d GDers, %d hosts, %d BNs, %d duos.",
                len(gd_leaderboard), len(host_leaderboard), len(individual_leaderboard), len(duo_leaderboard))

    return {
        'leaderboard': duo_leaderboard,
        'individual_leaderboard': individual_leaderboard,
        'gd_leaderboard': gd_leaderboard,
        'host_leaderboard': host_leaderboard,
        'updated_at': time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime()),
        'total_sets_scanned': total_new,
        'total_duos': len(duo_leaderboard),
        'total_individuals': len(individual_leaderboard),
        'total_gders': len(gd_leaderboard),
        'total_hosts': len(host_leaderboard),
        'scan_state': new_scan_state,
    }

