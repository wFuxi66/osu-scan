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


def run_global_scan(progress_callback=None):
    """Runs the monthly global scan across all ranked beatmapsets.

    Returns a dict containing the four leaderboards (duo, individual BN, GD, host)
    plus metadata, or a dict with an 'error' key on failure.
    """
    token = get_token()
    if not token:
        return {'error': 'Authentication failed'}

    if progress_callback:
        progress_callback("Starting global scan — fetching all ranked beatmapsets...")

    all_sets = search_ranked_beatmapsets(token, statuses=['ranked', 'approved'], progress_callback=progress_callback)

    if not all_sets:
        return {'error': 'No beatmapsets found'}

    total = len(all_sets)
    if progress_callback:
        progress_callback(f"Found {total} beatmapsets. Processing nominations and GD data...")

    # Per-set data collectors
    all_nominations = []
    gd_stats = defaultdict(lambda: {'count': 0, 'last_date': '', 'modes': set(), 'mode_counts': defaultdict(int)})
    host_stats = defaultdict(lambda: {'count': 0, 'last_date': '', 'modes': set(), 'mode_counts': defaultdict(int)})
    # Pre-populate host names from search-result creator fields to avoid extra API calls
    host_names = {}

    completed = 0
    failed_sets = []

    def _accumulate(bset, result_tuple):
        noms, gd_entries, _set_modes, mapset_host_id, host_modes = result_tuple
        all_nominations.extend(noms)
        date = (bset.get('ranked_date') or bset.get('last_updated') or '').split('T')[0]
        # gd_entries is a list of (uid, mode) – one per diff per non-host owner
        for uid, mode in gd_entries:
            gd_stats[uid]['count'] += 1
            gd_stats[uid]['modes'].add(mode)
            gd_stats[uid]['mode_counts'][mode] += 1
            if date and date > gd_stats[uid]['last_date']:
                gd_stats[uid]['last_date'] = date
        # host_modes only contains modes where the host personally mapped a diff
        if mapset_host_id is not None:
            host_stats[mapset_host_id]['count'] += 1
            host_stats[mapset_host_id]['modes'].update(host_modes)
            for m in host_modes:
                host_stats[mapset_host_id]['mode_counts'][m] += 1
            if date and date > host_stats[mapset_host_id]['last_date']:
                host_stats[mapset_host_id]['last_date'] = date
            if bset.get('user_id') == mapset_host_id and bset.get('creator'):
                host_names[mapset_host_id] = bset['creator']

    # First pass — bounded in-flight concurrency
    max_workers = 8
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        set_iter = iter(all_sets)
        in_flight = {}

        for _ in range(max_workers):
            try:
                bset = next(set_iter)
            except StopIteration:
                break
            future = executor.submit(process_nominator_set, bset)
            in_flight[future] = bset

        while in_flight:
            for future in concurrent.futures.as_completed(list(in_flight)):
                bset = in_flight.pop(future)
                completed += 1
                if completed % 100 == 0 and progress_callback:
                    progress_callback(f"Processing sets: {completed}/{total}...")
                try:
                    _accumulate(bset, future.result())
                except Exception as e:
                    logger.error("Error processing set %s: %s", bset['id'], e)
                    failed_sets.append(bset)

                try:
                    next_bset = next(set_iter)
                except StopIteration:
                    next_bset = None

                if next_bset is not None:
                    next_future = executor.submit(process_nominator_set, next_bset)
                    in_flight[next_future] = next_bset

    # Retry failed sets with reduced concurrency
    if failed_sets:
        if progress_callback:
            progress_callback(f"Retrying {len(failed_sets)} failed sets...")
        retry_workers = 5
        with concurrent.futures.ThreadPoolExecutor(max_workers=retry_workers) as executor:
            retry_iter = iter(failed_sets)
            in_flight = {}

            for _ in range(retry_workers):
                try:
                    bset = next(retry_iter)
                except StopIteration:
                    break
                future = executor.submit(process_nominator_set, bset)
                in_flight[future] = bset

            while in_flight:
                for future in concurrent.futures.as_completed(list(in_flight)):
                    bset = in_flight.pop(future)
                    try:
                        _accumulate(bset, future.result())
                    except Exception as e:
                        logger.error("Retry failed for set %s: %s", bset['id'], e)

                    try:
                        next_bset = next(retry_iter)
                    except StopIteration:
                        next_bset = None

                    if next_bset is not None:
                        next_future = executor.submit(process_nominator_set, next_bset)
                        in_flight[next_future] = next_bset

    # Log scan summary before building leaderboards
    logger.info("Scan pass complete: %d sets processed, %d failed.", completed, len(failed_sets))
    logger.info("GD stats: %d unique GDers found.", len(gd_stats))
    logger.info("Host stats: %d unique hosts found.", len(host_stats))
    logger.info("Nominations: %d total nomination events.", len(all_nominations))

    # Build duo + individual leaderboards from nominations
    if progress_callback:
        progress_callback("Building BN leaderboards...")

    duo_stats = defaultdict(lambda: {'count': 0, 'last_date': '', 'bn1_modes': set(), 'bn2_modes': set(), 'modes': set(), 'mode_counts': defaultdict(int)})
    individual_stats = defaultdict(lambda: {'count': 0, 'last_date': '', 'modes': set(), 'mode_counts': defaultdict(int)})

    # Group nominations by set to find co-nominators
    set_nominations = defaultdict(list)
    for nom in all_nominations:
        set_nominations[nom['set_id']].append(nom)

    for _set_id, noms in set_nominations.items():
        for nom in noms:
            uid = nom['nominator_id']
            date = nom['date']
            rulesets = nom.get('rulesets') or ['osu']
            individual_stats[uid]['count'] += 1
            individual_stats[uid]['modes'].update(rulesets)
            for r in rulesets:
                individual_stats[uid]['mode_counts'][r] += 1
            if date and date > individual_stats[uid]['last_date']:
                individual_stats[uid]['last_date'] = date

        # Build duo pairs from unique nominators per set
        unique_noms = list({n['nominator_id']: n for n in noms}.values())
        for i, nom1 in enumerate(unique_noms):
            for nom2 in unique_noms[i + 1:]:
                id1 = min(nom1['nominator_id'], nom2['nominator_id'])
                id2 = max(nom1['nominator_id'], nom2['nominator_id'])
                duo_key = (id1, id2)
                date = max(nom1['date'], nom2['date'])
                if nom1['nominator_id'] == id1:
                    bn1_rulesets = nom1.get('rulesets') or ['osu']
                    bn2_rulesets = nom2.get('rulesets') or ['osu']
                else:
                    bn1_rulesets = nom2.get('rulesets') or ['osu']
                    bn2_rulesets = nom1.get('rulesets') or ['osu']
                duo_stats[duo_key]['count'] += 1
                duo_stats[duo_key]['bn1_modes'].update(bn1_rulesets)
                duo_stats[duo_key]['bn2_modes'].update(bn2_rulesets)
                duo_stats[duo_key]['modes'].update(bn1_rulesets + bn2_rulesets)
                for r in bn1_rulesets + bn2_rulesets:
                    duo_stats[duo_key]['mode_counts'][r] += 1
                if date and date > duo_stats[duo_key]['last_date']:
                    duo_stats[duo_key]['last_date'] = date

    # Resolve all user IDs in one batch
    if progress_callback:
        progress_callback("Resolving user names...")

    all_ids_to_resolve = set()
    all_ids_to_resolve.update(individual_stats.keys())
    all_ids_to_resolve.update(uid for key in duo_stats for uid in key)
    all_ids_to_resolve.update(gd_stats.keys())
    all_ids_to_resolve.update(uid for uid in host_stats if uid not in host_names)

    user_cache = resolve_users_parallel(all_ids_to_resolve, _token_manager.get_token(), progress_callback)
    user_cache.update(host_names)

    # Individual BN leaderboard
    individual_leaderboard = []
    for uid, data in individual_stats.items():
        individual_leaderboard.append({
            'username': user_cache.get(uid, f"ID:{uid}"),
            'count': data['count'],
            'last_date': data['last_date'],
            'modes': list(data['modes']),
            'mode_counts': dict(data['mode_counts']),
        })
    individual_leaderboard.sort(key=lambda x: (-x['count'], x['username']))

    # Duo leaderboard
    duo_leaderboard = []
    for (id1, id2), data in duo_stats.items():
        duo_leaderboard.append({
            'bn1_name': user_cache.get(id1, f"ID:{id1}"),
            'bn2_name': user_cache.get(id2, f"ID:{id2}"),
            'count': data['count'],
            'last_date': data['last_date'],
            'bn1_modes': list(data['bn1_modes']),
            'bn2_modes': list(data['bn2_modes']),
            'modes': list(data['modes']),
            'mode_counts': dict(data['mode_counts']),
        })
    duo_leaderboard.sort(key=lambda x: (-x['count'], x['bn1_name']))

    # GD leaderboard
    gd_leaderboard = []
    for uid, data in gd_stats.items():
        gd_leaderboard.append({
            'username': user_cache.get(uid, f"ID:{uid}"),
            'count': data['count'],
            'last_date': data['last_date'],
            'modes': list(data['modes']),
            'mode_counts': dict(data['mode_counts']),
        })
    gd_leaderboard.sort(key=lambda x: (-x['count'], x['username']))

    # Host leaderboard
    host_leaderboard = []
    for uid, data in host_stats.items():
        host_leaderboard.append({
            'username': user_cache.get(uid, f"ID:{uid}"),
            'count': data['count'],
            'last_date': data['last_date'],
            'modes': list(data['modes']),
            'mode_counts': dict(data['mode_counts']),
        })
    host_leaderboard.sort(key=lambda x: (-x['count'], x['username']))

    return {
        'leaderboard': duo_leaderboard,
        'individual_leaderboard': individual_leaderboard,
        'gd_leaderboard': gd_leaderboard,
        'host_leaderboard': host_leaderboard,
        'updated_at': time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime()),
        'total_sets_scanned': total,
        'total_duos': len(duo_leaderboard),
        'total_individuals': len(individual_leaderboard),
        'total_gders': len(gd_leaderboard),
        'total_hosts': len(host_leaderboard),
    }

