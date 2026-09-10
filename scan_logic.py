import requests
import time
import os
import json
import concurrent.futures
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

TOKEN_CACHE_FILE = 'token_cache.json'


def get_token():
    """Obtains a client credentials token, reusing the cached one until it nears expiry.

    A token lasts a day, but the token endpoint throttles hard: a handful of requests earns
    a 429 with a ~28 minute Retry-After that reads exactly like bad credentials. Caching it
    to disk means every process and every re-run shares the one token.
    """
    try:
        with open(TOKEN_CACHE_FILE) as f:
            cached = json.load(f)
        if cached['expires_at'] > time.time() + 300:
            return cached['token']
    except (OSError, ValueError, KeyError, TypeError):
        pass

    data = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'client_credentials',
        'scope': 'public'
    }
    try:
        response = requests.post(TOKEN_URL, data=data, timeout=10)
        response.raise_for_status()
        payload = response.json()
        token = payload['access_token']
    except Exception as e:
        print(f"Error authenticating: {e}")
        return None

    try:
        with open(TOKEN_CACHE_FILE, 'w') as f:
            json.dump({'token': token, 'expires_at': time.time() + payload.get('expires_in', 86400)}, f)
        os.chmod(TOKEN_CACHE_FILE, 0o600)
    except OSError:
        pass  # a read-only disk costs us the reuse, not the token
    return token

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
    session = requests.Session()
    
    for s_type in set_types:
        if cancel_event and cancel_event.is_set():
            session.close()
            return []
        offset = 0
        limit = 100
        while True:
            if cancel_event and cancel_event.is_set():
                session.close()
                return []
            params = {'limit': limit, 'offset': offset}
            url = f'{API_BASE}/users/{user_id}/beatmapsets/{s_type}'
            
            try:
                response = session.get(url, headers=headers, params=params, timeout=10)
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
                time.sleep(0.05) 
            except Exception as e:
                print(f"Warning: Failed to fetch {s_type} sets: {e}")
                break
                
    session.close()
    return all_sets

def get_nominated_beatmapsets(user_id, token, cancel_event=None):
    """Fetches all beatmap sets nominated by a user."""
    headers = {'Authorization': f'Bearer {token}'}
    all_sets = []
    session = requests.Session()
    
    offset = 0
    limit = 50 # Unknown limit for this endpoint, safe bet
    
    while True:
        if cancel_event and cancel_event.is_set():
            session.close()
            return []
        
        # This is a hidden endpoint, pagination support is assumed but not guaranteed.
        # If pagination doesn't accept 'offset', we might only get the first page.
        # But most osu! endpoints use offset/limit.
        params = {'limit': limit, 'offset': offset}
        url = f'{API_BASE}/users/{user_id}/beatmapsets/nominated'
        
        try:
            response = session.get(url, headers=headers, params=params, timeout=10)
            if response.status_code == 404: break
            
            response.raise_for_status()
            data = response.json()
            
            if not data: break
                
            all_sets.extend(data)
            
            if len(data) < limit: break
            
            offset += len(data)
            time.sleep(0.05)
        except Exception as e:
            print(f"Warning: Failed to fetch nominated sets: {e}")
            break
            
    session.close()
    return all_sets

def process_set(bset, host_id, token=None):
    """Scans a single set and finds unique GDers."""
    gds_in_set = []
    
    beats = bset.get('beatmaps')
    if beats is None and token:
        headers = {'Authorization': f'Bearer {token}'}
        try:
            url = f'{API_BASE}/beatmapsets/{bset["id"]}'
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 200:
                full_set = r.json()
                beats = full_set.get('beatmaps', [])
        except Exception as e:
            print(f"Error fetching deep set {bset['id']}: {e}")
            beats = []

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
                        'mapper_name': owner.get('username'), 
                        'last_updated': beatmap.get('last_updated', '').split('T')[0]
                    }
                    gds_in_set.append(gd_entry)
                    seen_mappers_in_set.add(owner['id'])
        else:
            mapper_id = beatmap.get('user_id')
            if mapper_id and mapper_id != host_id and mapper_id not in seen_mappers_in_set:
                gd_entry = {
                    'mapper_id': mapper_id,
                    'mapper_name': None, 
                    'last_updated': beatmap.get('last_updated', '').split('T')[0]
                }
                gds_in_set.append(gd_entry)
                seen_mappers_in_set.add(mapper_id)
                
    return gds_in_set

def analyze_sets(beatmapsets, host_id, token=None, progress_callback=None, cancel_event=None):
    """Finds GDs in the provided beatmap sets directly from memory (instant)."""
    all_gds = []
    total = len(beatmapsets)
    if progress_callback: progress_callback(f"Analyzing {total} sets...")
    
    if cancel_event and cancel_event.is_set(): return []
    
    for bset in beatmapsets:
        if cancel_event and cancel_event.is_set(): return []
        results = process_set(bset, host_id, token)
        all_gds.extend(results)
                
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
    except Exception as e:
        print(f"Error fetching set {bset['id']}: {e}")
        
    return nominations
            
# User Cache with persistent file storage
USER_CACHE_FILE = 'user_cache.json'
USER_CACHE = {}

def load_user_cache():
    global USER_CACHE
    if os.path.exists(USER_CACHE_FILE):
        try:
            with open(USER_CACHE_FILE, 'r') as f:
                data = json.load(f)
                USER_CACHE = {int(k): v for k, v in data.items()}
        except Exception as e:
            print(f"Error loading user cache: {e}")

def save_user_cache():
    try:
        with open(USER_CACHE_FILE, 'w') as f:
            json.dump(USER_CACHE, f)
    except Exception as e:
        print(f"Error saving user cache: {e}")

load_user_cache()

# The bulk endpoint takes 50 ids per request, turning 8000 lookups into 160.
USER_BATCH = 50
# ponytail: a flat pause is enough to stay under the API's sustained rate; swap in a proper
# token bucket only if a scan ever has to share the budget with something else.
BATCH_PAUSE = 0.5


def fetch_users_batch(session, uids, headers, max_retries=6):
    """Read up to USER_BATCH usernames in one request.

    Returns {uid: username} for the users that exist, or None when the request could not be
    completed -- the caller must then leave those ids alone rather than cache a guess.
    """
    params = [('ids[]', uid) for uid in uids]
    for attempt in range(max_retries):
        try:
            r = session.get(f'{API_BASE}/users', headers=headers, params=params, timeout=20)
        except Exception:
            time.sleep(1 + attempt)
            continue
        if r.status_code == 429:
            time.sleep(min(int(r.headers.get('Retry-After', 2 ** attempt)), 30))
            continue
        if r.status_code != 200:
            return None
        return {u['id']: u.get('username') for u in r.json().get('users', [])}
    return None


def resolve_users_parallel(user_ids, token, progress_callback=None, refresh=False):
    """Resolves a list of user IDs to usernames, with caching.

    Pass refresh=True to re-fetch names that are already cached. Players rename, and a
    cached name is never otherwise revisited.

    Names are read 50 at a time from the bulk endpoint. One request per user earns a 429
    storm on any real scan, and a 429 leaves the previous placeholder in the cache forever
    because nothing ever revisits a name that is already there.
    """
    headers = {'Authorization': f'Bearer {token}'}

    # Identify which IDs are missing from cache
    missing_ids = [uid for uid in user_ids if (refresh or uid not in USER_CACHE) and uid != 0]
    total_missing = len(missing_ids)

    if total_missing > 0:
        msg = f"Resolving {total_missing} usernames..."
        if progress_callback: progress_callback(msg)

        session = requests.Session()
        new_entries = False
        done = 0

        for i in range(0, total_missing, USER_BATCH):
            if i:
                time.sleep(BATCH_PAUSE)  # fired back to back, the batches earn a 429 of their own
            batch = missing_ids[i:i + USER_BATCH]
            found = fetch_users_batch(session, batch, headers)
            done += len(batch)
            if progress_callback:
                progress_callback(f"Resolving names {done}/{total_missing}...")
            if found is None:
                continue  # rate-limited or timed out: leave the batch for the next scan
            for uid in batch:
                # A user the endpoint does not return is restricted or deleted, which is an
                # answer worth caching, exactly as a 404 is on the single-user endpoint.
                USER_CACHE[uid] = found.get(uid) or f'User_{uid}'
            new_entries = True

        session.close()

        if new_entries:
            save_user_cache()

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
    
    session = requests.Session()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
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
    session = requests.Session()
    
    offset = 0
    limit = 100
    
    while True:
        if cancel_event and cancel_event.is_set():
            session.close()
            return []
        
        params = {'limit': limit, 'offset': offset}
        url = f'{API_BASE}/users/{user_id}/beatmapsets/guest'
        
        try:
            response = session.get(url, headers=headers, params=params, timeout=10)
            if response.status_code == 404: break
            
            response.raise_for_status()
            data = response.json()
            
            if not data: break
                
            all_sets.extend(data)
            
            if len(data) < limit: break
            
            offset += len(data)
            time.sleep(0.05)
        except Exception as e:
            print(f"Warning: Failed to fetch guest sets: {e}")
            break
            
    session.close()
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
    """Main entry point for the scan engine."""
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


if __name__ == '__main__':
    class FakeResp:
        def __init__(self, status, payload=None, retry_after='0'):
            self.status_code, self._payload, self.headers = status, payload, {'Retry-After': retry_after}
        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self, *responses):
            self.responses, self.calls = list(responses), []
        def get(self, url, headers=None, params=None, timeout=None):
            self.calls.append(params)
            return self.responses.pop(0)

    ok = FakeSession(FakeResp(200, {'users': [{'id': 1, 'username': 'Kecco'}]}))
    assert fetch_users_batch(ok, [1, 2], {}) == {1: 'Kecco'}
    assert ok.calls == [[('ids[]', 1), ('ids[]', 2)]], 'ids must go out as a repeated param'

    # A 429 is retried, not cached: the batch that finally lands is the answer.
    retried = FakeSession(FakeResp(429), FakeResp(200, {'users': [{'id': 1, 'username': 'Kecco'}]}))
    assert fetch_users_batch(retried, [1], {}) == {1: 'Kecco'}
    assert fetch_users_batch(FakeSession(*[FakeResp(429)] * 4), [1], {}) is None, \
        'a batch that never lands must stay unresolved'

    USER_CACHE.clear()
    USER_CACHE[9] = 'stale'
    import unittest.mock
    with unittest.mock.patch(__name__ + '.fetch_users_batch', return_value=None), \
         unittest.mock.patch(__name__ + '.save_user_cache'):
        resolve_users_parallel([9], 'tok', refresh=True)
    assert USER_CACHE[9] == 'stale', 'a failed batch must not overwrite a known name'

    USER_CACHE.clear()
    with unittest.mock.patch(__name__ + '.fetch_users_batch', return_value={1: 'Kecco'}), \
         unittest.mock.patch(__name__ + '.save_user_cache'):
        out = resolve_users_parallel([1, 2], 'tok')
    assert out == {1: 'Kecco', 2: 'User_2'}, 'a user the endpoint omits is restricted'
    assert USER_CACHE[2] == 'User_2'

    print('scan_logic self-check OK')
