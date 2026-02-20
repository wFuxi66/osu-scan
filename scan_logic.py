import requests
import time
import os
import json
import concurrent.futures
from collections import defaultdict
from itertools import combinations

# Configuration constants
API_BASE = 'https://osu.ppy.sh/api/v2'
TOKEN_URL = 'https://osu.ppy.sh/oauth/token'

# User Credentials - MUST be set via environment variables
# On Render: Set in Dashboard > Environment
# Locally: Create a .env file (see .env.example)
CLIENT_ID = os.environ.get('OSU_CLIENT_ID')
CLIENT_SECRET = os.environ.get('OSU_CLIENT_SECRET')

if not CLIENT_ID or not CLIENT_SECRET:
    print("WARNING: OSU_CLIENT_ID and OSU_CLIENT_SECRET environment variables not set!")
    print("The app will not work without valid osu! API credentials.")

def get_token():
    """Obtains a client credentials token from osu! API."""
    data = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'client_credentials',
        'scope': 'public'
    }
    try:
        response = requests.post(TOKEN_URL, data=data, timeout=10)
        response.raise_for_status()
        return response.json()['access_token']
    except Exception as e:
        print(f"Error authenticating: {e}")
        return None

def get_user_id(username_or_id, token):
    """Resolves a username to an ID."""
    headers = {'Authorization': f'Bearer {token}'}
    
    # Try assuming it's a username key
    params = {'key': 'username'}
    url = f'{API_BASE}/users/{username_or_id}/osu'
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=5)
        if response.status_code == 200:
            return response.json()['id'], response.json()['username']
    except:
        pass

    # If failed, maybe it was an ID?
    if str(username_or_id).isdigit():
        url = f'{API_BASE}/users/{username_or_id}'
        try:
            response = requests.get(url, headers=headers, timeout=5)
            if response.status_code == 200:
                return response.json()['id'], response.json()['username']
        except:
            pass
            
    return None, None


def get_beatmapsets(user_id, token, cancel_event=None):
    """Fetches all beatmap sets for a user."""
    headers = {'Authorization': f'Bearer {token}'}
    all_sets = []
    set_types = ['ranked', 'loved']
    
    for s_type in set_types:
        if cancel_event and cancel_event.is_set(): return []
        offset = 0
        limit = 100
        while True:
            if cancel_event and cancel_event.is_set(): return []
            params = {'limit': limit, 'offset': offset}
            url = f'{API_BASE}/users/{user_id}/beatmapsets/{s_type}'
            
            try:
                response = requests.get(url, headers=headers, params=params, timeout=10)
                if response.status_code == 404:
                    break 
                
                response.raise_for_status()
                data = response.json()
                
                if not data:
                    break
                    
                for s in data:
                    s['status_category'] = s_type
                    all_sets.append(s)
                
                if len(data) < limit:
                    break
                
                offset += len(data)
                time.sleep(0.1) 
            except Exception as e:
                print(f"Warning: Failed to fetch {s_type} sets: {e}")
                break
                
                
    return all_sets

def get_nominated_beatmapsets(user_id, token, cancel_event=None):
    """Fetches all beatmap sets nominated by a user."""
    headers = {'Authorization': f'Bearer {token}'}
    all_sets = []
    
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
            response = requests.get(url, headers=headers, params=params, timeout=10)
            if response.status_code == 404: break
            
            response.raise_for_status()
            data = response.json()
            
            if not data: break
                
            all_sets.extend(data)
            
            if len(data) < limit: break
            
            offset += len(data)
            time.sleep(0.1)
        except Exception as e:
            print(f"Warning: Failed to fetch nominated sets: {e}")
            break
            
    return all_sets

import concurrent.futures

def process_set(bset, host_id, token):
    """Deep scans a single set and finds unique GDers."""
    headers = {'Authorization': f'Bearer {token}'}
    gds_in_set = []
    
    try:
        # Full Deep Fetch
        url = f'{API_BASE}/beatmapsets/{bset["id"]}'
        r = requests.get(url, headers=headers, timeout=10)
        
        if r.status_code == 200:
            full_set = r.json()
        else:
            full_set = bset
            
    except Exception as e:
        print(f"Error fetching deep set {bset['id']}: {e}")
        full_set = bset

    beats = full_set.get('beatmaps', [])
    if not beats:
        return []

    # Dedup tracker for THIS set
    # We only want to count a mapper ONCE per set.
    seen_mappers_in_set = set()

    for beatmap in beats:
        owners = beatmap.get('owners', [])
        
        if owners:
            for owner in owners:
                if owner['id'] != host_id and owner['id'] not in seen_mappers_in_set:
                    gd_entry = {
                        'mapper_id': owner['id'],
                        'mapper_name': owner['username'], 
                        'last_updated': beatmap['last_updated'].split('T')[0]
                    }
                    gds_in_set.append(gd_entry)
                    seen_mappers_in_set.add(owner['id'])
        else:
            mapper_id = beatmap['user_id']
            if mapper_id != host_id and mapper_id not in seen_mappers_in_set:
                gd_entry = {
                    'mapper_id': mapper_id,
                    'mapper_name': None, 
                    'last_updated': beatmap['last_updated'].split('T')[0]
                }
                gds_in_set.append(gd_entry)
                seen_mappers_in_set.add(mapper_id)
                
    return gds_in_set

def analyze_sets(beatmapsets, host_id, token, progress_callback=None, cancel_event=None):
    """Finds GDs in the provided beatmap sets using Concurrent Futures for speed."""
    all_gds = []
    total = len(beatmapsets)
    if progress_callback: progress_callback(f"Deep Scanning {total} sets...")
    
    if cancel_event and cancel_event.is_set(): return []
    
    # Use ThreadPool to speed up I/O
    # Max workers = 5 to be safe with rate limits (osu! is lenient but let's not push it)
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
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
            
            try:
                results = future.result()
                all_gds.extend(results)
            except Exception as e:
                print(f"Set processing generated an exception: {e}")
                
    return all_gds

def process_nominator_set(bset, token, session=None):
    """Deep fetches a set to find its nominators, GDers (with modes), and host."""
    headers = {'Authorization': f'Bearer {token}'}
    nominations = []
    gd_user_modes = {}  # {gd_uid: set of mode strings}
    set_modes = set()   # all modes present in this set
    mapset_host_id = None
    
    try:
        url = f'{API_BASE}/beatmapsets/{bset["id"]}'
        # Use session if provided, else standard request
        req_func = session.get if session else requests.get
        r = req_func(url, headers=headers, timeout=20)
        
        if r.status_code == 200:
            data = r.json()
            current_noms = data.get('current_nominations', [])
            
            for nom in current_noms:
                nominations.append({
                    'nominator_id': nom['user_id'],
                    'set_title': f"{bset['artist']} - {bset['title']}",
                    'date': (bset.get('ranked_date') or bset.get('last_updated')).split('T')[0]
                })
            
            # Extract host, GDers, and modes
            mapset_host_id = data.get('user_id')
            if mapset_host_id:
                for beatmap in data.get('beatmaps', []):
                    bm_mode = beatmap.get('mode', 'osu')
                    set_modes.add(bm_mode)
                    diff_creator = beatmap.get('user_id')
                    if diff_creator and diff_creator != mapset_host_id:
                        if diff_creator not in gd_user_modes:
                            gd_user_modes[diff_creator] = set()
                        gd_user_modes[diff_creator].add(bm_mode)
        else:
            pass
            
    except Exception as e:
        print(f"Error fetching set {bset['id']}: {e}")
        
    return nominations, gd_user_modes, set_modes, mapset_host_id
            
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
                r = requests.get(f'{API_BASE}/users/{uid}', headers=headers, timeout=10)
                if r.status_code == 200:
                    return (uid, r.json()['username'])
            except:
                pass
            return (uid, f"User_{uid}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_uid = {executor.submit(fetch_user, uid): uid for uid in missing_ids}
            
            completed = 0
            for future in concurrent.futures.as_completed(future_to_uid):
                completed += 1
                if completed % 10 == 0:
                     if progress_callback: progress_callback(f"Resolving names {completed}/{total_missing}...")
                
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
    session = requests.Session()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_set = {executor.submit(process_nominator_set, bset, token, session): bset for bset in target_sets}
        
        completed = 0
        for future in concurrent.futures.as_completed(future_to_set):
            completed += 1
            if completed % 5 == 0:
                if progress_callback: progress_callback(f"Scanning progress: {completed}/{total} sets...")
            
            try:
                noms, _gd_modes, _set_modes, _host_id = future.result()
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
            
    stats = defaultdict(lambda: {'count': 0, 'last_date': ''})
    
    for n in noms:
        name = user_cache.get(n['nominator_id'], f"ID:{n['nominator_id']}")
        date = n['date']
        
        stats[name]['count'] += 1
        if date and date > stats[name]['last_date']:
            stats[name]['last_date'] = date
            
    leaderboard = []
    for name, data in stats.items():
        leaderboard.append({
            'mapper_name': name, 
            'total_gds': data['count'], 
            'last_gd_date': data['last_date']
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
    
    stats = defaultdict(lambda: {'count': 0, 'last_date': ''})
    mappers_to_resolve = set()
    
    for bset in sets:
        mapper_id = bset['user_id']
        mappers_to_resolve.add(mapper_id)
        
        # Approximate date (ranked_date or last_updated)
        date = (bset.get('ranked_date') or bset.get('last_updated') or '').split('T')[0]
        
        # We store by ID temporarily
        stats[mapper_id]['count'] += 1
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
            'last_gd_date': data['last_date']
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
    
    offset = 0
    limit = 100
    
    while True:
        if cancel_event and cancel_event.is_set(): return []
        
        params = {'limit': limit, 'offset': offset}
        url = f'{API_BASE}/users/{user_id}/beatmapsets/guest'
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            if response.status_code == 404: break
            
            response.raise_for_status()
            data = response.json()
            
            if not data: break
                
            all_sets.extend(data)
            
            if len(data) < limit: break
            
            offset += len(data)
            time.sleep(0.1)
        except Exception as e:
            print(f"Warning: Failed to fetch guest sets: {e}")
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
    
    stats = defaultdict(lambda: {'count': 0, 'last_date': ''})
    hosts_to_resolve = set()
    
    for bset in sets:
        host_id = bset['user_id']
        hosts_to_resolve.add(host_id)
        
        # Use ranked_date or last_updated as date
        date = (bset.get('ranked_date') or bset.get('last_updated') or '').split('T')[0]
        
        stats[host_id]['count'] += 1
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
            'last_gd_date': data['last_date']
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
    stats = defaultdict(lambda: {'count': 0, 'last_date': ''})
    
    for gd in gds:
        # Use provided name, or lookup in cache, or fallback to ID
        if gd['mapper_name']:
            mapper_name = gd['mapper_name']
        else:
            mapper_name = user_cache.get(gd['mapper_id'], f"ID:{gd['mapper_id']}")
            
        date = gd['last_updated']
        
        stats[mapper_name]['count'] += 1
        if date > stats[mapper_name]['last_date']:
            stats[mapper_name]['last_date'] = date

    # Sort
    leaderboard = []
    for mapper, data in stats.items():
        leaderboard.append({
            'mapper_name': mapper,
            'total_gds': data['count'],
            'last_gd_date': data['last_date']
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
    """Fetches ALL ranked/loved beatmapsets using the search endpoint with cursor pagination."""
    headers = {'Authorization': f'Bearer {token}'}
    all_sets = []
    cursor_string = None
    page = 0
    
    for status in ['ranked', 'loved']:
        cursor_string = None
        page = 0
        
        while True:
            params = {'s': status}
            if cursor_string:
                params['cursor_string'] = cursor_string
            
            try:
                r = requests.get(f'{API_BASE}/beatmapsets/search', headers=headers, params=params, timeout=15)
                if r.status_code != 200:
                    print(f"Search endpoint returned {r.status_code}")
                    break
                
                data = r.json()
                sets = data.get('beatmapsets', [])
                
                if not sets:
                    break
                
                for s in sets:
                    all_sets.append({
                        'id': s['id'],
                        'artist': s.get('artist', ''),
                        'title': s.get('title', ''),
                        'ranked_date': s.get('ranked_date'),
                        'last_updated': s.get('last_updated'),
                        'status': s.get('status', status)
                    })
                
                page += 1
                if progress_callback and page % 5 == 0:
                    progress_callback(f"Fetching {status} maps: {len(all_sets)} total so far...")
                
                # Get next cursor
                cursor_string = data.get('cursor_string')
                if not cursor_string:
                    break
                
                time.sleep(0.15)  # Be gentle with rate limits
                
            except Exception as e:
                print(f"Error searching {status} beatmapsets: {e}")
                break
    
    return all_sets


LEADERBOARD_CACHE_FILE = os.path.join(DATA_DIR, 'leaderboard_cache.json')

def _load_cache():
    """Loads the scan cache (scanned set IDs + raw pair counts)."""
    if not os.path.exists(LEADERBOARD_CACHE_FILE):
        return {'scanned_ids': [], 'pair_counts': {}}
    try:
        with open(LEADERBOARD_CACHE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {'scanned_ids': [], 'pair_counts': {}}

def _save_cache(cache):
    """Saves the scan cache to disk."""
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
    pair_counts = defaultdict(lambda: {'count': 0, 'last_date': ''})
    individual_counts = defaultdict(lambda: {'count': 0, 'last_date': ''})
    gd_counts = defaultdict(lambda: {'count': 0, 'last_date': ''})
    host_counts = defaultdict(lambda: {'count': 0, 'last_date': ''})
    user_modes = defaultdict(set)  # {user_id: set of mode strings}
    
    # Restore existing pair counts from cache (keys are stored as "id1,id2")
    for key_str, data in cache.get('pair_counts', {}).items():
        parts = key_str.split(',')
        pair_key = (int(parts[0]), int(parts[1]))
        pair_counts[pair_key] = data

    # Restore individual counts
    for uid, data in cache.get('individual_counts', {}).items():
        if isinstance(data, int):
            individual_counts[int(uid)]['count'] = data
        else:
            individual_counts[int(uid)] = data

    # Restore GD counts
    for uid, data in cache.get('gd_counts', {}).items():
        if isinstance(data, int):
            gd_counts[int(uid)]['count'] = data
        else:
            gd_counts[int(uid)] = data

    # Restore host counts
    for uid, data in cache.get('host_counts', {}).items():
        if isinstance(data, int):
            host_counts[int(uid)]['count'] = data
        else:
            host_counts[int(uid)] = data

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
        # 4. Deep-fetch only new sets
        session = requests.Session()
        set_lookup = {s['id']: s for s in new_sets}
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_set = {
                executor.submit(process_nominator_set, s, token, session): s 
                for s in new_sets
            }
            
            completed = 0
            for future in concurrent.futures.as_completed(future_to_set):
                completed += 1
                if completed % 100 == 0:
                    if progress_callback: 
                        progress_callback(f"Deep-scanning: {completed}/{len(new_sets)} new sets...")
                
                bset = future_to_set[future]
                try:
                    noms, gd_user_modes, set_modes, mapset_host_id = future.result()
                    scanned_ids.add(bset['id'])
                    
                    set_data = set_lookup.get(bset['id'], {})
                    date = (set_data.get('ranked_date') or set_data.get('last_updated') or '').split('T')[0]
                    
                    if noms:
                        nom_ids = [n['nominator_id'] for n in noms]
                        
                        # Count individual nominations (unique per set)
                        unique_noms = set(nom_ids)
                        for nid in unique_noms:
                            individual_counts[nid]['count'] += 1
                            if date and date > individual_counts[nid]['last_date']:
                                individual_counts[nid]['last_date'] = date
                            # BN modes = modes of maps they nominate
                            user_modes[nid].update(set_modes)

                        if len(unique_noms) >= 2:
                            for pair in combinations(sorted(unique_noms), 2):
                                pair_counts[pair]['count'] += 1
                                if date and date > pair_counts[pair]['last_date']:
                                    pair_counts[pair]['last_date'] = date
                    
                    # Count GDers (+1 per set, not per difficulty)
                    for gd_uid, gd_modes in gd_user_modes.items():
                        gd_counts[gd_uid]['count'] += 1
                        if date and date > gd_counts[gd_uid]['last_date']:
                            gd_counts[gd_uid]['last_date'] = date
                        # GDer modes = modes of the diffs they made
                        user_modes[gd_uid].update(gd_modes)
                    
                    # Count host (+1 per ranked set)
                    if mapset_host_id:
                        host_counts[mapset_host_id]['count'] += 1
                        if date and date > host_counts[mapset_host_id]['last_date']:
                            host_counts[mapset_host_id]['last_date'] = date
                        # Host modes = all modes in the set
                        user_modes[mapset_host_id].update(set_modes)

                except Exception as e:
                    print(f"Error deep-fetching set {bset['id']}: {e}")
        
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
        
        # Duo modes = union of both BNs' modes
        duo_modes = sorted(user_modes.get(id1, set()) | user_modes.get(id2, set()))
        leaderboard.append({
            'bn1_name': name1,
            'bn2_name': name2,
            'bn1_modes': sorted(user_modes.get(id1, set())),
            'bn2_modes': sorted(user_modes.get(id2, set())),
            'modes': duo_modes,
            'count': data['count'],
            'last_date': data['last_date']
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
            'modes': sorted(user_modes.get(uid, set()))
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
            'modes': sorted(user_modes.get(uid, set()))
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
            'modes': sorted(user_modes.get(uid, set()))
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
