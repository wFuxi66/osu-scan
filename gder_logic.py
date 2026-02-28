import requests
import time
import os
from collections import defaultdict

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
    """Deep fetches a set to find its nominators."""
    headers = {'Authorization': f'Bearer {token}'}
    nominations = []
    
    try:
        url = f'{API_BASE}/beatmapsets/{bset["id"]}'
        # Use session if provided, else standard request
        req_func = session.get if session else requests.get
        r = req_func(url, headers=headers, timeout=20) # Increased timeout to 20s
        
        if r.status_code == 200:
            data = r.json()
            current_noms = data.get('current_nominations', [])
            
            for nom in current_noms:
                nominations.append({
                    'nominator_id': nom['user_id'],
                    'set_title': f"{bset['artist']} - {bset['title']}",
                    'date': (bset.get('ranked_date') or bset.get('last_updated')).split('T')[0]
                })
        else:
            # If fetch fails, we skip specific nominator data for this set
            pass
            
    except Exception as e:
        print(f"Error fetching set {bset['id']}: {e}")
        
    return nominations
            
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
                results = future.result()
                all_nominations.extend(results)
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
