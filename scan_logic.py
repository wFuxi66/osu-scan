import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import os
import json
import concurrent.futures
from collections import defaultdict
from itertools import combinations

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
    print("WARNING: OSU_CLIENT_ID and OSU_CLIENT_SECRET environment variables not set!")
    print("The app will not work without valid osu! API credentials.")

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
            print(f"Error authenticating: {e}")
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
                print(f"Error: Failed to fetch {s_type} sets: {e}")
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
            print(f"Error: Failed to fetch nominated sets: {e}")
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
            print(f"Failed to get token for set {bset['id']}, using search data")
            full_set = bset
        else:
            headers = {'Authorization': f'Bearer {token}'}
            url = f'{API_BASE}/beatmapsets/{bset["id"]}'
            session = get_session()
            r = session.get(url, headers=headers, timeout=10)

            # Handle 401 by refreshing token and retrying once
            if r.status_code == 401:
                print(f"Token expired for set {bset['id']}, refreshing...")
                refreshed_token = _token_manager.refresh_token()
                if refreshed_token:
                    headers = {'Authorization': f'Bearer {refreshed_token}'}
                    r = session.get(url, headers=headers, timeout=10)

            if r.status_code == 200:
                full_set = r.json()
            else:
                # Preserve essential fields from search result when deep-fetch fails
                print(f"Deep-fetch failed for set {bset['id']} with status {r.status_code}, using search data")
                full_set = bset

    except Exception as e:
        print(f"Error fetching deep set {bset['id']}: {e}")
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
                print(f"Set processing generated an exception: {e}")

    return all_gds

def process_nominator_set(bset, token=None, session=None):
    """Deep fetches a set to find its nominators, GDers (with modes), and host."""
    nominations = []
    gd_user_modes = {}  # {gd_uid: set of mode strings}
    set_modes = set()   # all modes present in this set
    mapset_host_id = None
    host_modes = set()

    try:
        # Get token dynamically
        token = _token_manager.get_token()
        if not token:
            print(f"Failed to get token for set {bset['id']}")
            return nominations, gd_user_modes, set_modes, mapset_host_id, host_modes

        headers = {'Authorization': f'Bearer {token}'}
        url = f'{API_BASE}/beatmapsets/{bset["id"]}'
        # Use session if provided, else standard request
        req_func = session.get if session else get_session().get
        r = req_func(url, headers=headers, timeout=20)

        # Handle 401 by refreshing token and retrying once
        if r.status_code == 401:
            print(f"Token expired for set {bset['id']}, refreshing...")
            refreshed_token = _token_manager.refresh_token()
            if refreshed_token:
                headers = {'Authorization': f'Bearer {refreshed_token}'}
                r = req_func(url, headers=headers, timeout=20)

        if r.status_code == 200:
            data = r.json()
            current_noms = data.get('current_nominations', [])

            for nom in current_noms:
                nominations.append({
                    'nominator_id': nom['user_id'],
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
                    events_r = req_func(events_url, headers=headers, params=events_params, timeout=15)

                    # Handle 401 for events API too
                    if events_r.status_code == 401:
                        print(f"Token expired for events API (set {bset['id']}), refreshing...")
                        refreshed_token = _token_manager.refresh_token()
                        if refreshed_token:
                            headers = {'Authorization': f'Bearer {refreshed_token}'}
                            events_r = req_func(events_url, headers=headers, params=events_params, timeout=15)

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
                                        'set_title': f"{bset['artist']} - {bset['title']}",
                                        'date': (event.get('created_at') or bset.get('ranked_date') or bset.get('last_updated')).split('T')[0],
                                        'rulesets': rulesets
                                    })
                        time.sleep(0.1)  # Small sleep after events API call
                except Exception as e:
                    print(f"Error fetching events for set {bset['id']}: {e}")

            # Extract host, GDers, and modes
            mapset_host_id = data.get('user_id')
            if mapset_host_id:
                for beatmap in data.get('beatmaps', []):
                    bm_mode = beatmap.get('mode', 'osu')
                    set_modes.add(bm_mode)
                    owners = beatmap.get('owners', [])
                    if owners:
                        for owner in owners:
                            uid = owner['id']
                            if uid == mapset_host_id:
                                host_modes.add(bm_mode)
                            else:
                                if uid not in gd_user_modes:
                                    gd_user_modes[uid] = set()
                                gd_user_modes[uid].add(bm_mode)
                    else:
                        diff_creator = beatmap.get('user_id')
                        if diff_creator == mapset_host_id:
                            host_modes.add(bm_mode)
                        elif diff_creator and diff_creator != mapset_host_id:
                            if diff_creator not in gd_user_modes:
                                gd_user_modes[diff_creator] = set()
                            gd_user_modes[diff_creator].add(bm_mode)
        else:
            pass

    except Exception as e:
        print(f"Error fetching set {bset['id']}: {e}")

    # Simplify host_modes fallback: use set_modes if host_modes is empty
    if not host_modes:
        host_modes = set_modes if set_modes else {'osu'}

    return nominations, gd_user_modes, set_modes, mapset_host_id, host_modes
            
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
                noms, _gd_modes, _set_modes, _host_id, _host_modes = future.result()
                all_nominations.extend(noms)
            except Exception as e:
                print(f"Nominator scan exception: {e}")

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
            print(f"Error: Failed to fetch guest sets: {e}")
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


# ============================================================
# GLOBAL BN DUO LEADERBOARD
# ============================================================

# Path for persisted results
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
LEADERBOARD_FILE = os.path.join(DATA_DIR, 'leaderboard.json')

def search_ranked_beatmapsets(token, progress_callback=None):
    """Fetches ALL ranked/loved/approved/qualified beatmapsets using cursor pagination with restart capability."""
    headers = {'Authorization': f'Bearer {token}'}
    all_sets = []
    seen_set_ids = set()
    session = get_session()

    for status in ['ranked', 'qualified', 'loved', 'approved']:
        cursor_string = None
        page = 0
        page_cap = 500  # API hard cap
        last_ranked_date = None

        while True:
            params = {
                's': status,
                'sort': 'ranked_desc'  # Sort by ranked date descending
            }
            if cursor_string:
                params['cursor_string'] = cursor_string

            try:
                r = session.get(f'{API_BASE}/beatmapsets/search', headers=headers, params=params, timeout=15)
                if r.status_code != 200:
                    print(f"Search endpoint returned {r.status_code} for {status}")
                    break

                data = r.json()
                sets = data.get('beatmapsets', [])

                if not sets:
                    break

                # Preserve essential fields: user_id, creator, artist, title, ranked_date, last_updated, status
                for s in sets:
                    if s['id'] in seen_set_ids:
                        continue

                    seen_set_ids.add(s['id'])
                    all_sets.append({
                        'id': s['id'],
                        'user_id': s.get('user_id'),  # Host ID
                        'creator': s.get('creator', ''),  # Host name
                        'artist': s.get('artist', ''),
                        'title': s.get('title', ''),
                        'ranked_date': s.get('ranked_date'),
                        'last_updated': s.get('last_updated'),
                        'status': s.get('status', status)
                    })

                    # Track last ranked date for restart capability
                    if s.get('ranked_date'):
                        last_ranked_date = s['ranked_date']

                page += 1
                if progress_callback and page % 10 == 0:
                    progress_callback(f"Fetching {status} maps: {len(all_sets)} total, page {page}...")

                # Get next cursor
                cursor_string = data.get('cursor_string')
                if not cursor_string:
                    break

                # Check if we're approaching the page cap (~500 pages)
                # If so, we've likely hit the cap and should warn
                if page >= page_cap:
                    print(f"Warning: Reached page cap ({page_cap}) for {status}. Some maps may be missed.")
                    print(f"Last ranked date seen: {last_ranked_date}")
                    break

                time.sleep(0.1)  # Small sleep between pages

            except Exception as e:
                print(f"Error searching {status} beatmapsets: {e}")
                break

    return all_sets


LEADERBOARD_CACHE_FILE = os.path.join(DATA_DIR, 'leaderboard_cache.json')

def _load_cache():
    """Loads the scan cache (scanned set IDs + raw pair counts)."""
    if not os.path.exists(LEADERBOARD_CACHE_FILE):
        return {'scanned_ids': [], 'pair_counts': {}, 'cache_version': CACHE_VERSION}
    try:
        with open(LEADERBOARD_CACHE_FILE, 'r', encoding='utf-8') as f:
            cache = json.load(f)
        if cache.get('cache_version', 1) < CACHE_VERSION:
            print("Cache version outdated, resetting cache for a fresh scan.")
            return {'scanned_ids': [], 'pair_counts': {}, 'cache_version': CACHE_VERSION}
        return cache
    except Exception:
        return {'scanned_ids': [], 'pair_counts': {}, 'cache_version': CACHE_VERSION}

def _save_cache(cache):
    """Saves the scan cache to disk."""
    cache['cache_version'] = CACHE_VERSION
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(LEADERBOARD_CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False)


def global_bn_duo_scan(progress_callback=None):
    """
    Incremental global scan: loads cached results, fetches all ranked maps,
    skips already-scanned sets, deep-fetches only new ones, merges pair counts,
    rebuilds leaderboard, and saves everything.
    """
    if progress_callback: progress_callback("Authenticating...")
    token = get_token()
    if not token:
        return {'error': 'Authentication failed'}
    
    # 1. Load existing cache
    if progress_callback: progress_callback("Loading cache...")
    cache = _load_cache()
    scanned_ids = set(cache.get('scanned_ids', []))
    pair_counts = defaultdict(lambda: {'count': 0, 'last_date': '', 'mode_counts': defaultdict(int)})
    individual_counts = defaultdict(lambda: {'count': 0, 'last_date': '', 'mode_counts': defaultdict(int)})
    gd_counts = defaultdict(lambda: {'count': 0, 'last_date': '', 'mode_counts': defaultdict(int)})
    host_counts = defaultdict(lambda: {'count': 0, 'last_date': '', 'mode_counts': defaultdict(int)})
    user_modes = defaultdict(set)  # {user_id: set of mode strings}
    
    # helper for loading counts robustly
    def load_counts_to_dict(target_dict, cached_dict, key_cast=int, is_pair=False):
        for k_str, data in cached_dict.items():
            if is_pair:
                parts = k_str.split(',')
                if len(parts) == 2:
                    k = (int(parts[0]), int(parts[1]))
                else:
                    continue
            else:
                k = key_cast(k_str)

            if isinstance(data, int):
                target_dict[k]['count'] = data
            elif isinstance(data, dict):
                target_dict[k]['count'] = data.get('count', 0)
                target_dict[k]['last_date'] = data.get('last_date', '')
                target_dict[k]['mode_counts'] = defaultdict(int, data.get('mode_counts', {}))

    # Restore existing counts
    load_counts_to_dict(pair_counts, cache.get('pair_counts', {}), is_pair=True)
    load_counts_to_dict(individual_counts, cache.get('individual_counts', {}))
    load_counts_to_dict(gd_counts, cache.get('gd_counts', {}))
    load_counts_to_dict(host_counts, cache.get('host_counts', {}))

    # Restore user modes
    for uid, modes in cache.get('user_modes', {}).items():
        user_modes[int(uid)] = set(modes)
    
    if progress_callback: progress_callback(f"Cache loaded: {len(scanned_ids)} sets already scanned.")
    
    # 2. Search all ranked/loved beatmapsets
    if progress_callback: progress_callback("Fetching ranked beatmapsets...")
    all_sets = search_ranked_beatmapsets(token, progress_callback)
    
    total_sets = len(all_sets)
    if total_sets == 0:
        return {'error': 'No ranked beatmapsets found'}
    
    # 3. Filter to only new sets
    new_sets = [s for s in all_sets if s['id'] not in scanned_ids]
    
    if progress_callback: progress_callback(f"Found {total_sets} total sets, {len(new_sets)} new to scan.")
    
    if len(new_sets) == 0 and pair_counts:
        if progress_callback: progress_callback("No new sets to scan. Rebuilding leaderboard from cache...")
    elif len(new_sets) > 0:
        # 3.5. Count hosts directly from search results (before deep-fetch)
        # This decouples host leaderboard from deep-fetch, avoiding missing data on failures
        if progress_callback: progress_callback("Counting hosts from search results...")
        for s in new_sets:
            if s.get('user_id'):  # Host ID is directly available
                host_id = s['user_id']
                date = (s.get('ranked_date') or s.get('last_updated') or '').split('T')[0]

                # We'll update host_counts here with basic data
                # Mode info will be refined during deep-fetch if successful
                host_counts[host_id]['count'] += 1
                if date and date > host_counts[host_id]['last_date']:
                    host_counts[host_id]['last_date'] = date
                # Default to osu mode - will be updated during deep-fetch
                host_counts[host_id]['mode_counts']['osu'] += 1
                user_modes[host_id].add('osu')

        # 4. Deep-fetch only new sets with retry logic
        session = get_session()
        set_lookup = {s['id']: s for s in new_sets}
        failed_sets = []  # Track failed deep-fetches for retry

        # Reduced from 15 to 8 to avoid 429 errors
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            future_to_set = {
                executor.submit(process_nominator_set, s, token, session): s
                for s in new_sets
            }

            completed = 0
            for future in concurrent.futures.as_completed(future_to_set):
                completed += 1
                if completed % 50 == 0:
                    if progress_callback:
                        progress_callback(f"Deep-scanning: {completed}/{len(new_sets)} new sets...")
                    time.sleep(0.1)  # Small sleep to reduce API pressure

                bset = future_to_set[future]
                try:
                    noms, gd_user_modes, set_modes, mapset_host_id, host_modes = future.result()

                    # Track failed deep-fetches for retry
                    if mapset_host_id is None:
                        failed_sets.append(bset)
                        continue

                    scanned_ids.add(bset['id'])

                    set_data = set_lookup.get(bset['id'], {})
                    date = (set_data.get('ranked_date') or set_data.get('last_updated') or '').split('T')[0]

                    if noms:
                        # Count individual nominations (unique per bn per set)
                        unique_noms = {n['nominator_id']: n for n in noms}
                        for nid, n in unique_noms.items():
                            individual_counts[nid]['count'] += 1
                            if date and date > individual_counts[nid]['last_date']:
                                individual_counts[nid]['last_date'] = date

                            # Mode counts
                            rulesets = n.get('rulesets', []) or ['osu']
                            for r in rulesets:
                                individual_counts[nid]['mode_counts'][r] += 1
                            user_modes[nid].update(rulesets)

                        if len(unique_noms) >= 2:
                            for n1, n2 in combinations(sorted(unique_noms.values(), key=lambda x: x['nominator_id']), 2):
                                pair = (n1['nominator_id'], n2['nominator_id'])
                                pair_counts[pair]['count'] += 1
                                if date and date > pair_counts[pair]['last_date']:
                                    pair_counts[pair]['last_date'] = date

                                r1 = set(n1.get('rulesets', []) or ['osu'])
                                r2 = set(n2.get('rulesets', []) or ['osu'])
                                shared_modes = r1 & r2
                                if not shared_modes:
                                    shared_modes = r1 | r2

                                for r in shared_modes:
                                    pair_counts[pair]['mode_counts'][r] += 1

                    # Count GDers (+1 per set, not per difficulty)
                    for gd_uid, gd_modes in gd_user_modes.items():
                        gd_counts[gd_uid]['count'] += 1
                        if date and date > gd_counts[gd_uid]['last_date']:
                            gd_counts[gd_uid]['last_date'] = date

                        user_modes[gd_uid].update(gd_modes)
                        for m in gd_modes:
                            gd_counts[gd_uid]['mode_counts'][m] += 1

                    # Update host with refined mode data from deep-fetch
                    if mapset_host_id and host_modes:
                        # Adjust the default osu count for this set if we have real data
                        if mapset_host_id in host_counts:
                            osu_count = host_counts[mapset_host_id]['mode_counts'].get('osu')
                            if osu_count:
                                host_counts[mapset_host_id]['mode_counts']['osu'] = osu_count - 1

                        user_modes[mapset_host_id].update(host_modes)
                        for m in host_modes:
                            host_counts[mapset_host_id]['mode_counts'][m] += 1

                except Exception as e:
                    print(f"Error deep-fetching set {bset['id']}: {e}")
                    failed_sets.append(bset)

        # 4.5. Retry failed deep-fetches once
        if failed_sets:
            if progress_callback:
                progress_callback(f"Retrying {len(failed_sets)} failed deep-fetches...")
            time.sleep(2)  # Wait before retry

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as retry_executor:
                retry_futures = {
                    retry_executor.submit(process_nominator_set, s, token, session): s
                    for s in failed_sets
                }

                retry_completed = 0
                for future in concurrent.futures.as_completed(retry_futures):
                    retry_completed += 1
                    bset = retry_futures[future]
                    try:
                        noms, gd_user_modes, set_modes, mapset_host_id, host_modes = future.result()

                        if mapset_host_id is None:
                            continue  # Skip after retry

                        scanned_ids.add(bset['id'])

                        set_data = set_lookup.get(bset['id'], {})
                        date = (set_data.get('ranked_date') or set_data.get('last_updated') or '').split('T')[0]

                        # Same processing logic as above
                        if noms:
                            unique_noms = {n['nominator_id']: n for n in noms}
                            for nid, n in unique_noms.items():
                                individual_counts[nid]['count'] += 1
                                if date and date > individual_counts[nid]['last_date']:
                                    individual_counts[nid]['last_date'] = date
                                rulesets = n.get('rulesets', []) or ['osu']
                                for r in rulesets:
                                    individual_counts[nid]['mode_counts'][r] += 1
                                user_modes[nid].update(rulesets)

                            if len(unique_noms) >= 2:
                                for n1, n2 in combinations(sorted(unique_noms.values(), key=lambda x: x['nominator_id']), 2):
                                    pair = (n1['nominator_id'], n2['nominator_id'])
                                    pair_counts[pair]['count'] += 1
                                    if date and date > pair_counts[pair]['last_date']:
                                        pair_counts[pair]['last_date'] = date
                                    r1 = set(n1.get('rulesets', []) or ['osu'])
                                    r2 = set(n2.get('rulesets', []) or ['osu'])
                                    shared_modes = r1 & r2
                                    if not shared_modes:
                                        shared_modes = r1 | r2
                                    for r in shared_modes:
                                        pair_counts[pair]['mode_counts'][r] += 1

                        for gd_uid, gd_modes in gd_user_modes.items():
                            gd_counts[gd_uid]['count'] += 1
                            if date and date > gd_counts[gd_uid]['last_date']:
                                gd_counts[gd_uid]['last_date'] = date
                            user_modes[gd_uid].update(gd_modes)
                            for m in gd_modes:
                                gd_counts[gd_uid]['mode_counts'][m] += 1

                        if mapset_host_id and host_modes:
                            # Ensure mode_counts exists for this host without discarding prior data
                            if mapset_host_id not in host_counts or 'mode_counts' not in host_counts[mapset_host_id]:
                                host_counts[mapset_host_id]['mode_counts'] = defaultdict(int)
                            user_modes[mapset_host_id].update(host_modes)
                            for m in host_modes:
                                host_counts[mapset_host_id]['mode_counts'][m] += 1

                    except Exception as e:
                        print(f"Retry failed for set {bset['id']}: {e}")

        session.close()
    
    if not pair_counts and not individual_counts and not gd_counts and not host_counts:
        return {'error': 'No data found'}
    
    # 5. Save cache
    if progress_callback: progress_callback("Saving cache...")
    cache_data = {
        'scanned_ids': list(scanned_ids),
        'pair_counts': {f"{k[0]},{k[1]}": v for k, v in pair_counts.items()},
        'individual_counts': individual_counts,
        'gd_counts': gd_counts,
        'host_counts': host_counts,
        'user_modes': {str(uid): list(modes) for uid, modes in user_modes.items()}
    }
    try:
        _save_cache(cache_data)
    except Exception as e:
        print(f"Error saving cache: {e}")
    
    # 6. Resolve all user names
    if progress_callback: progress_callback("Resolving usernames...")
    all_user_ids = set()
    for (id1, id2) in pair_counts.keys():
        all_user_ids.add(id1)
        all_user_ids.add(id2)
    for uid in individual_counts.keys():
        all_user_ids.add(uid)
    for uid in gd_counts.keys():
        all_user_ids.add(uid)
    for uid in host_counts.keys():
        all_user_ids.add(uid)
    
    user_cache = resolve_users_parallel(all_user_ids, token, progress_callback)
    
    # 7. Build leaderboards
    if progress_callback: progress_callback("Building leaderboards...")
    
    # Duo leaderboard
    leaderboard = []
    for (id1, id2), data in pair_counts.items():
        name1 = user_cache.get(id1, f"User_{id1}")
        name2 = user_cache.get(id2, f"User_{id2}")
        
        if name1.lower() > name2.lower():
            name1, name2 = name2, name1
        
        # Duo modes = union of both BNs' nomination modes
        bn1_nom_modes = sorted(individual_counts[id1]['mode_counts'].keys()) if id1 in individual_counts else []
        bn2_nom_modes = sorted(individual_counts[id2]['mode_counts'].keys()) if id2 in individual_counts else []
        duo_modes = sorted(set(bn1_nom_modes) | set(bn2_nom_modes))
        leaderboard.append({
            'bn1_name': name1,
            'bn2_name': name2,
            'bn1_modes': bn1_nom_modes,
            'bn2_modes': bn2_nom_modes,
            'modes': duo_modes,
            'count': data['count'],
            'last_date': data['last_date'],
            'mode_counts': dict(data.get('mode_counts', {}))
        })
    leaderboard.sort(key=lambda x: (-x['count'], x['bn1_name']))

    # Individual BN leaderboard
    individual_leaderboard = []
    for uid, data in individual_counts.items():
        name = user_cache.get(uid, f"User_{uid}")
        individual_leaderboard.append({
            'username': name,
            'count': data['count'],
            'last_date': data['last_date'],
            'user_id': uid,
            'modes': sorted(data.get('mode_counts', {}).keys()),
            'mode_counts': dict(data.get('mode_counts', {}))
        })
    individual_leaderboard.sort(key=lambda x: (-x['count'], x['username']))

    # GDer leaderboard
    gd_leaderboard = []
    for uid, data in gd_counts.items():
        name = user_cache.get(uid, f"User_{uid}")
        gd_leaderboard.append({
            'username': name,
            'count': data['count'],
            'last_date': data['last_date'],
            'user_id': uid,
            'modes': sorted(data.get('mode_counts', {}).keys()),
            'mode_counts': dict(data.get('mode_counts', {}))
        })
    gd_leaderboard.sort(key=lambda x: (-x['count'], x['username']))

    # Host (Most Active Mapper) leaderboard
    host_leaderboard = []
    for uid, data in host_counts.items():
        name = user_cache.get(uid, f"User_{uid}")
        host_leaderboard.append({
            'username': name,
            'count': data['count'],
            'last_date': data['last_date'],
            'user_id': uid,
            'modes': sorted(data.get('mode_counts', {}).keys()),
            'mode_counts': dict(data.get('mode_counts', {}))
        })
    host_leaderboard.sort(key=lambda x: (-x['count'], x['username']))
    
    # 8. Save leaderboard JSON
    results = {
        'leaderboard': leaderboard,
        'individual_leaderboard': individual_leaderboard,
        'gd_leaderboard': gd_leaderboard,
        'host_leaderboard': host_leaderboard,
        'total_sets_scanned': len(scanned_ids),
        'total_duos': len(leaderboard),
        'total_individuals': len(individual_leaderboard),
        'total_gders': len(gd_leaderboard),
        'total_hosts': len(host_leaderboard),
        'updated_at': time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime()),
        'scan_duration_note': 'Monthly automated scan'
    }
    
    try:
        with open(LEADERBOARD_FILE, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        if progress_callback: progress_callback("Scan complete! Results saved.")
        print(f"Leaderboard scan complete. {len(new_sets)} new sets scanned, {len(leaderboard)} duos, {len(gd_leaderboard)} GDers, {len(host_leaderboard)} hosts. Saved to {LEADERBOARD_FILE}")
    except Exception as e:
        print(f"Error saving leaderboard results: {e}")
        return {'error': f'Failed to save results: {e}'}
    
    return results


LEADERBOARD_RELEASE_URL = "https://github.com/wFuxi66/osu-scan/releases/download/latest-data/leaderboard.json"
_remote_cache = {'data': None, 'last_fetch': 0}
CACHE_TTL = 3600  # 1 hour

def load_leaderboard_results():
    """Loads leaderboard data from GitHub Release (preferred) or local file."""
    global _remote_cache
    
    # 1. return cached data if fresh
    if time.time() - _remote_cache['last_fetch'] < CACHE_TTL and _remote_cache['data']:
        return _remote_cache['data']
        
    # 2. Try fetching from GitHub Release
    try:
        r = requests.get(LEADERBOARD_RELEASE_URL, timeout=3)
        if r.status_code == 200:
            data = r.json()
            _remote_cache['data'] = data
            _remote_cache['last_fetch'] = time.time()
            return data
    except Exception as e:
        print(f"Error fetching remote leaderboard: {e}")
    
    # 3. Fallback to local file (e.g. for local dev or if remote fails)
    if os.path.exists(LEADERBOARD_FILE):
        try:
            with open(LEADERBOARD_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading local leaderboard results: {e}")
            return None
            
    return None
