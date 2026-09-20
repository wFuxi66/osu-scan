import requests
import time
import os
import json
import concurrent.futures
import threading
from collections import defaultdict

# Configuration constants
API_BASE = 'https://osu.ppy.sh/api/v2'
TOKEN_URL = 'https://osu.ppy.sh/oauth/token'

# What a scan credits. Qualified is in because it ranks within the week; approved is ranked
# under an older name. Anything unfinished - pending, wip, graveyard - is not a credit and
# never enters a count, whichever endpoint hands it over.
COUNTED_STATUSES = ('ranked', 'approved', 'qualified', 'loved')
SETTLED_STATUSES = ('ranked', 'approved', 'loved')
# Loved sets are voted in by the Loved Project, not nominated: they have no nominators to
# find, and counting them would put sets in the denominator that can never be in the total.
NOMINATED_STATUSES = ('ranked', 'approved', 'qualified')

# User Credentials - MUST be set via environment variables
# On Render: Set in Dashboard > Environment
# Locally: Create a .env file (see .env.example)
CLIENT_ID = os.environ.get('OSU_CLIENT_ID')
CLIENT_SECRET = os.environ.get('OSU_CLIENT_SECRET')

if not CLIENT_ID or not CLIENT_SECRET:
    print("WARNING: OSU_CLIENT_ID and OSU_CLIENT_SECRET environment variables not set!")
    print("The app will not work without valid osu! API credentials.")

TOKEN_CACHE_FILE = 'token_cache.json'

# Set by get_token() when the token endpoint throttled us: the seconds it asked us to wait.
# Cleared to None on every other outcome. Callers that retry read this to tell "wait, then
# ask again" apart from "these credentials are wrong" - from a bare None the two are
# indistinguishable, and a scan that guesses wrong either gives up on a door that was about
# to open, or hammers one that is never going to.
token_retry_after = None


def get_token():
    """Obtains a client credentials token, reusing the cached one until it nears expiry.

    A token lasts a day, but the token endpoint throttles hard: a handful of requests earns
    a 429 with a ~28 minute Retry-After that reads exactly like bad credentials. Caching it
    to disk means every process and every re-run shares the one token.

    Returns None on any failure, and sets `token_retry_after` when that failure was a
    throttle rather than a refusal.
    """
    global token_retry_after
    token_retry_after = None
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
        if response.status_code == 429:
            try:
                token_retry_after = max(1.0, float(response.headers.get('Retry-After') or 1800))
            except ValueError:
                token_retry_after = 1800.0
            print(f"Token endpoint is throttling us, not refusing us: it wants "
                  f"{token_retry_after / 60:.0f} more minutes. Credentials are fine.")
            return None
        if response.status_code in (401, 403):
            # Worth saying plainly: this is the one token failure waiting cannot fix.
            print(f"Token endpoint refused the credentials ({response.status_code}). "
                  f"Check OSU_CLIENT_ID and OSU_CLIENT_SECRET.")
            return None
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
            data = response.json()
            return data['id'], data['username']
    except Exception:
        pass

    # If failed, maybe it was an ID?
    if str(username_or_id).isdigit():
        url = f'{API_BASE}/users/{username_or_id}'
        try:
            response = requests.get(url, headers=headers, timeout=5)
            if response.status_code == 200:
                data = response.json()
                return data['id'], data['username']
        except Exception:
            pass
            
    return None, None


def get_beatmapsets(user_id, token, cancel_event=None):
    """Every counted set on a user's profile, and whether the listing ran short.

    A page the API refuses is not a page with nothing on it. Returning the sets that did
    arrive without a word turns a throttled scan into a confident, wrong report - it prints
    "read across N sets" for an N nobody can tell is short.
    """
    headers = {'Authorization': f'Bearer {token}'}
    all_sets = []
    # Qualified sets have no bucket of their own: they sit in 'pending' alongside the wip and
    # pending ones, which the status filter below drops.
    set_types = ['ranked', 'loved', 'pending']
    truncated = False
    session = requests.Session()
    
    for s_type in set_types:
        if cancel_event and cancel_event.is_set():
            session.close()
            return [], False
        offset = 0
        limit = 100
        while True:
            if cancel_event and cancel_event.is_set():
                session.close()
                return [], False
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
                    # The same rule as everywhere else, rather than a rule per bucket: what
                    # a bucket is called is the API's business, the status is the answer.
                    if s.get('status') not in COUNTED_STATUSES:
                        continue
                    s['status_category'] = s.get('status') or s_type
                    all_sets.append(s)
                
                if len(data) < limit:
                    break
                
                offset += len(data)
                time.sleep(0.05) 
            except Exception as e:
                print(f"Warning: Failed to fetch {s_type} sets: {e}")
                truncated = True
                break
                
    session.close()
    return all_sets, truncated

def get_nominated_beatmapsets(user_id, token, cancel_event=None):
    """Every set a user nominated, and whether the listing ran short. See get_beatmapsets."""
    headers = {'Authorization': f'Bearer {token}'}
    all_sets = []
    truncated = False
    session = requests.Session()
    
    offset = 0
    limit = 50 # Unknown limit for this endpoint, safe bet
    
    while True:
        if cancel_event and cancel_event.is_set():
            session.close()
            return [], False
        
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
            truncated = True
            break
            
    session.close()
    return all_sets, truncated

def get_set_with_retry(url, headers, session=None):
    """Reads one set, waiting out a rate limit. None means the read did not happen."""
    req_func = session.get if session else requests.get
    for attempt in range(3):
        try:
            r = req_func(url, headers=headers, timeout=20)
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            time.sleep(1 + attempt)
            continue
        if r.status_code == 429:
            time.sleep(min(int(r.headers.get('Retry-After', 2 ** attempt)), 30))
            continue
        return r if r.status_code == 200 else None
    return None


def process_set(bset, host_id, token=None):
    """Scans a single set and finds unique GDers."""
    gds_in_set = []
    loved = bset.get('status') == 'loved'
    
    beats = bset.get('beatmaps')
    if beats is None and token:
        # These buckets carry the difficulties with them, so this is a fallback that rarely
        # runs - but when it does, losing it drops every guest mapper on the set.
        r = get_set_with_retry(f'{API_BASE}/beatmapsets/{bset["id"]}',
                               {'Authorization': f'Bearer {token}'})
        beats = r.json().get('beatmaps', []) if r else []

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
                        'last_updated': beatmap.get('last_updated', '').split('T')[0],
                        'loved': loved
                    }
                    gds_in_set.append(gd_entry)
                    seen_mappers_in_set.add(owner['id'])
        else:
            mapper_id = beatmap.get('user_id')
            if mapper_id and mapper_id != host_id and mapper_id not in seen_mappers_in_set:
                gd_entry = {
                    'mapper_id': mapper_id,
                    'mapper_name': None, 
                    'last_updated': beatmap.get('last_updated', '').split('T')[0],
                    'loved': loved
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


def indexed_nominator_ids(bset, nomination_index):
    """Return validated indexed nominators, or None when the set needs a live read."""
    if bset.get('status') not in SETTLED_STATUSES or not isinstance(nomination_index, dict):
        return None

    sets = nomination_index.get('sets')
    if not isinstance(sets, dict):
        return None

    ids = sets.get(str(bset['id']))
    if not isinstance(ids, list) or not ids:
        return None
    if any(type(uid) is not int for uid in ids) or len(set(ids)) != len(ids):
        return None

    # `nominations_summary.current` is not maintained on older ranked sets: it reads 0 where
    # the set still has its nominators on record, and a disqualified-then-re-nominated set
    # keeps the whole historical list in current_nominations while the summary counts only the
    # final pair. So it is a floor, not an equality. An entry below it is an index that dropped
    # a nominator and needs a live read; anything at or above it is the same list the live read
    # would return, and taking it spares one request per set against a very small rate budget.
    expected = bset.get('nominations_summary', {}).get('current')
    if type(expected) is int and expected > 0 and len(ids) < expected:
        return None
    return ids


def process_nominator_set(bset, token, session=None, nomination_index=None):
    """Find a set's nominators from settled data, falling back to a live read."""
    headers = {'Authorization': f'Bearer {token}'}
    sid = bset['id']
    # Both dates can be absent on a set that is still in qualification.
    date = (bset.get('ranked_date') or bset.get('last_updated') or '').split('T')[0]
    title = f"{bset['artist']} - {bset['title']}"

    def entries(ids):
        return [{'nominator_id': uid, 'set_title': title, 'date': date} for uid in ids]

    settled = bset.get('status') in SETTLED_STATUSES
    if settled:
        # Who nominated a settled set is settled history, so a live read of one is as good as
        # the published index and worth keeping: it is reused by every later scan, including
        # when an index entry is judged short. A qualified set is never written here, which is
        # what keeps a re-nomination from being served out of this cache.
        cached = NOM_CACHE.get(sid)
        if cached is not None:
            return entries(cached)

    indexed = indexed_nominator_ids(bset, nomination_index)
    if indexed is not None:
        return entries(indexed)

    # Eight threads reading a few hundred sets go through the rate limit. ponytail: a retry
    # per thread is enough while the cache absorbs the repeat scans; pace the pool if not.
    r = get_set_with_retry(f'{API_BASE}/beatmapsets/{sid}', headers, session)
    if r is None:
        # None rather than []: an unread set has unknown nominators, and counting it as a
        # set with none quietly shortens everybody's total.
        return None

    ids = [nom['user_id'] for nom in r.json().get('current_nominations', [])]
    if settled:
        NOM_CACHE[sid] = ids
    return entries(ids)

            
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

# Two scans running at once were writing these files on top of each other: a half-written
# JSON is unreadable next boot, and json.dump over a dict another thread is filling raises
# outright. One writer at a time, and the rename is what makes the swap atomic.
CACHE_WRITE_LOCK = threading.Lock()


def _save_json_atomic(path, data, label):
    try:
        with CACHE_WRITE_LOCK:
            tmp = f'{path}.tmp'
            with open(tmp, 'w') as f:
                json.dump(dict(data), f)
            os.replace(tmp, path)
    except Exception as e:
        print(f"Error saving {label}: {e}")


def save_user_cache():
    _save_json_atomic(USER_CACHE_FILE, USER_CACHE, 'user cache')

load_user_cache()

# Nominations of a settled set never change, and reading them costs one request per set -
# the slowest thing a scan does. Cached by set id, they cost nothing on every scan after
# the first. ponytail: grows without bound, one small list per set; prune it if the file
# ever gets heavy, the way user_cache.json never has.
NOM_CACHE_FILE = 'nominations_cache.json'
NOM_CACHE = {}

def load_nom_cache():
    global NOM_CACHE
    if os.path.exists(NOM_CACHE_FILE):
        try:
            with open(NOM_CACHE_FILE, 'r') as f:
                NOM_CACHE = {int(k): v for k, v in json.load(f).items()}
        except Exception as e:
            print(f"Error loading nomination cache: {e}")

def save_nom_cache():
    _save_json_atomic(NOM_CACHE_FILE, NOM_CACHE, 'nomination cache')

load_nom_cache()

# The bulk endpoint takes 50 ids per request, turning 8000 lookups into 160.
USER_BATCH = 50
# ponytail: a flat pause is enough to stay under the API's sustained rate; swap in a proper
# token bucket only if a scan ever has to share the budget with something else.
BATCH_PAUSE = 0.5

# What fetch_user_name returns for an account the API says is gone. An object, not a string,
# so it can never be mistaken for a username the caller should cache.
RESTRICTED = object()


def fetch_user_name(session, uid, headers, max_retries=4):
    """Read one username from the single-user endpoint, to tell a live account from a gone one.

    The bulk endpoint drops accounts it should return - a real, active mapper can simply be
    absent from a 200 - so a missing id there is not an answer. This endpoint is the
    authority: a 200 carries the name, a 404 is a restricted or deleted account.

    Returns RESTRICTED for a confirmed-gone account, a username otherwise, and None when the
    request never landed. None must stay unresolved rather than be published as restricted.
    """
    for attempt in range(max_retries):
        try:
            r = session.get(f'{API_BASE}/users/{uid}', headers=headers,
                            params={'key': 'id'}, timeout=20)
        except Exception:
            time.sleep(1 + attempt)
            continue
        if r.status_code == 429:
            time.sleep(min(int(r.headers.get('Retry-After', 2 ** attempt)), 30))
            continue
        if r.status_code == 404:
            return RESTRICTED
        if r.status_code != 200:
            return None
        return (r.json() or {}).get('username') or RESTRICTED
    return None


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
                name = found.get(uid)
                if name:
                    USER_CACHE[uid] = name
                    continue
                # A 200 from the bulk endpoint can still omit an active account, so a missing
                # id is not proof of restriction. Confirm it directly: only a 404 writes the
                # placeholder, and a request that never lands leaves the id for the next scan.
                time.sleep(BATCH_PAUSE)
                answer = fetch_user_name(session, uid, headers)
                if answer is RESTRICTED:
                    USER_CACHE[uid] = f'User_{uid}'
                elif answer:
                    USER_CACHE[uid] = answer
            new_entries = True

        session.close()

        if new_entries:
            save_user_cache()

    # Only a confirmed 404 is cached as User_<id>. An id that could not be read at all comes
    # back under a different marker, so a board can leave it unnamed instead of calling a
    # live account restricted.
    return {uid: USER_CACHE.get(uid, f"Unknown_{uid}") for uid in user_ids if uid != 0}


def analyze_nominators(beatmapsets, token, progress_callback=None, cancel_event=None,
                       nomination_index=None):
    """Fetches nominators for the provided sets, and reports how many it could not read."""
    all_nominations = []
    unread = 0
    
    target_sets = [b for b in beatmapsets if b['status'] in NOMINATED_STATUSES]
    total = len(target_sets)
    
    msg = f"Scanning {total} sets for Nominators..."
    if progress_callback: progress_callback(msg)
    
    # Two values, like every other exit: the caller unpacks them, and a bare [] here used to
    # turn a cancelled scan into "not enough values to unpack".
    if cancel_event and cancel_event.is_set(): return [], 0
    
    session = requests.Session()
    known = len(NOM_CACHE)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        future_to_set = {
            executor.submit(process_nominator_set, bset, token, session, nomination_index): bset
            for bset in target_sets
        }
        
        completed = 0
        for future in concurrent.futures.as_completed(future_to_set):
            completed += 1
            if completed % 5 == 0:
                if progress_callback: progress_callback(f"Scanning progress: {completed}/{total} sets...")
            
            try:
                results = future.result()
            except Exception as e:
                print(f"Nominator scan exception: {e}")
                results = None
            if results is None:
                unread += 1
            else:
                all_nominations.extend(results)
            
    session.close()
    # Written once, off the worker threads, and only when this scan actually learnt something.
    if len(NOM_CACHE) > known:
        save_nom_cache()
    return all_nominations, unread

def resolve_and_aggregate_nominators(noms, token, progress_callback=None):
    """Resolves names and builds the nominator leaderboard using parallel resolution."""
    unique_ids = set(n['nominator_id'] for n in noms)
    
    # Use the new parallel resolver
    user_cache = resolve_users_parallel(unique_ids, token, progress_callback)
            
    stats = defaultdict(lambda: {'count': 0, 'last_date': ''})
    
    for n in noms:
        date = n['date']
        
        stats[n['nominator_id']]['count'] += 1
        if date and date > stats[n['nominator_id']]['last_date']:
            stats[n['nominator_id']]['last_date'] = date
            
    leaderboard = []
    for uid, data in stats.items():
        leaderboard.append({
            'mapper_id': uid,
            'mapper_name': user_cache.get(uid, f"ID:{uid}"),
            'total_gds': data['count'], 
            'last_gd_date': data['last_date']
        })
        
    leaderboard.sort(key=lambda x: (-x['total_gds'], x['mapper_name']))
    return leaderboard

def generate_nominator_leaderboard_for_user(username_input, progress_callback=None, cancel_event=None,
                                             nomination_index=None):
    token = get_token()
    if not token:
        return {'error': 'Authentication failed'}
        
    user_id, username = get_user_id(username_input, token)
    if not user_id:
        return {'error': f'User {username_input} not found'}
        
    # Fetch sets
    if progress_callback: progress_callback(f"Fetching beatmap sets for {username}...")
    sets, truncated = get_beatmapsets(user_id, token, cancel_event)
    sets = [b for b in sets if b.get('status') in NOMINATED_STATUSES]
    
    if cancel_event and cancel_event.is_set(): return {'error': 'Cancelled'}
    
    # Analyze
    noms, unread = analyze_nominators(
        sets,
        token,
        progress_callback,
        cancel_event,
        nomination_index=nomination_index,
    )
    
    if cancel_event and cancel_event.is_set(): return {'error': 'Cancelled'}
    
    if not noms:
         return {'username': username, 'user_id': user_id, 'leaderboard': [],
                 'unread_sets': unread, 'listing_truncated': truncated}
         
    leaderboard = resolve_and_aggregate_nominators(noms, token, progress_callback)
    
    return {
        'username': username,
        'user_id': user_id,
        'leaderboard': leaderboard,
        'sets_read': len(sets),
        'unread_sets': unread,
        'listing_truncated': truncated,
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
    sets, truncated = get_nominated_beatmapsets(user_id, token, cancel_event)
    sets = [b for b in sets if b.get('status') in NOMINATED_STATUSES]
    
    if cancel_event and cancel_event.is_set(): return {'error': 'Cancelled'}
    
    if not sets:
         return {'username': username, 'user_id': user_id, 'leaderboard': [],
                 'listing_truncated': truncated}

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
            'mapper_id': mid,
            'mapper_name': name,
            'total_gds': data['count'],
            'last_gd_date': data['last_date']
        })
        
    leaderboard.sort(key=lambda x: (-x['total_gds'], x['mapper_name']))
    
    return {
        'username': username,
        'user_id': user_id,
        'leaderboard': leaderboard,
        'sets_read': len(sets),
        'listing_truncated': truncated,
        'type': 'Nominations'
    }

def get_guest_beatmapsets(user_id, token, cancel_event=None):
    """Every set the user guested on, and whether the listing ran short. See get_beatmapsets."""
    headers = {'Authorization': f'Bearer {token}'}
    all_sets = []
    truncated = False
    session = requests.Session()
    
    offset = 0
    limit = 100
    
    while True:
        if cancel_event and cancel_event.is_set():
            session.close()
            return [], False
        
        params = {'limit': limit, 'offset': offset}
        url = f'{API_BASE}/users/{user_id}/beatmapsets/guest'
        
        try:
            response = session.get(url, headers=headers, params=params, timeout=10)
            if response.status_code == 404: break
            
            response.raise_for_status()
            data = response.json()
            
            if not data: break
                
            # The bucket is whatever the endpoint feels like returning; only the four statuses
            # a scan counts are kept. A graveyard or wip set is not a credit.
            all_sets.extend(s for s in data if s.get('status') in COUNTED_STATUSES)
            
            if len(data) < limit: break
            
            offset += len(data)
            time.sleep(0.05)
        except Exception as e:
            print(f"Warning: Failed to fetch guest sets: {e}")
            truncated = True
            break
            
    session.close()
    return all_sets, truncated

def generate_gd_hosts_leaderboard_for_user(username_input, progress_callback=None, cancel_event=None):
    """New Mode: Find which mappers the user has made the most GDs for."""
    token = get_token()
    if not token: return {'error': 'Authentication failed'}
    
    if cancel_event and cancel_event.is_set(): return {'error': 'Cancelled'}
    
    user_id, username = get_user_id(username_input, token)
    if not user_id: return {'error': f'User {username_input} not found'}
    
    # 1. Fetch guest beatmapsets (maps where user contributed a GD)
    if progress_callback: progress_callback(f"Fetching GD sets for {username}...")
    sets, truncated = get_guest_beatmapsets(user_id, token, cancel_event)
    
    if cancel_event and cancel_event.is_set(): return {'error': 'Cancelled'}
    
    if not sets:
         return {'username': username, 'user_id': user_id, 'leaderboard': [],
                 'listing_truncated': truncated}

    # 2. Count hosts (user_id field in each beatmapset = the host)
    if progress_callback: progress_callback(f"Analyzing {len(sets)} GD sets...")
    
    stats = defaultdict(lambda: {'count': 0, 'loved': 0, 'last_date': ''})
    hosts_to_resolve = set()
    
    for bset in sets:
        host_id = bset['user_id']
        hosts_to_resolve.add(host_id)
        stats[host_id]['loved'] += bset.get('status') == 'loved'
        
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
            'mapper_id': host_id,
            'mapper_name': name,
            'total_gds': data['count'],
            'loved_gds': data['loved'],
            'last_gd_date': data['last_date']
        })
        
    leaderboard.sort(key=lambda x: (-x['total_gds'], x['mapper_name']))
    
    return {
        'username': username,
        'user_id': user_id,
        'leaderboard': leaderboard,
        'sets_read': len(sets),
        'listing_truncated': truncated,
        'type': 'GD Hosts'
    }

def resolve_and_aggregate(gds, token, progress_callback=None):
    """Resolves names and builds the leaderboard using parallel resolution."""
    
    # Only resolve IDs that have no name
    unique_ids_to_resolve = set(gd['mapper_id'] for gd in gds if not gd['mapper_name'])
    
    # Use the new parallel resolver
    user_cache = resolve_users_parallel(unique_ids_to_resolve, token, progress_callback)
            
    # Aggregate
    stats = defaultdict(lambda: {'count': 0, 'loved': 0, 'last_date': '', 'name': None})
    
    for gd in gds:
        row = stats[gd['mapper_id']]
        # The mapset carries a name for most of them; the cache covers the rest.
        row['name'] = row['name'] or gd['mapper_name']
        date = gd['last_updated']
        
        row['count'] += 1
        # In the total, as on the boards, but named under it: loved is not ranked.
        row['loved'] += bool(gd.get('loved'))
        if date > row['last_date']:
            row['last_date'] = date

    # Sort
    leaderboard = []
    for uid, data in stats.items():
        leaderboard.append({
            'mapper_id': uid,
            'mapper_name': data['name'] or user_cache.get(uid, f"ID:{uid}"),
            'total_gds': data['count'],
            'loved_gds': data['loved'],
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
    
    sets, truncated = get_beatmapsets(user_id, token, cancel_event)
    
    if cancel_event and cancel_event.is_set(): return {'error': 'Cancelled'}
    
    gds = analyze_sets(sets, user_id, token, progress_callback, cancel_event)
    
    if cancel_event and cancel_event.is_set(): return {'error': 'Cancelled'}
    
    if not gds:
        return {'username': username, 'user_id': user_id, 'leaderboard': [],
                'listing_truncated': truncated}
        
    leaderboard = resolve_and_aggregate(gds, token, progress_callback)
    
    return {
        'username': username,
        'user_id': user_id,
        'leaderboard': leaderboard,
        'sets_read': len(sets),
        'listing_truncated': truncated
    }


if __name__ == '__main__':
    class FakeResp:
        def __init__(self, status, payload=None, retry_after='0'):
            self.status_code, self._payload, self.headers = status, payload, {'Retry-After': retry_after}
        def json(self):
            return self._payload
        def raise_for_status(self):
            pass

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

    # fetch_user_name is the authority on a bulk omission: a 200 names a live account, a 404
    # marks a gone one, and anything unread stays unresolved.
    assert fetch_user_name(FakeSession(FakeResp(200, {'username': 'Kecco'})), 1, {}) == 'Kecco'
    assert fetch_user_name(FakeSession(FakeResp(404)), 1, {}) is RESTRICTED
    assert fetch_user_name(FakeSession(*[FakeResp(429)] * 4), 1, {}) is None, \
        'a user that could not be read stays unresolved'

    USER_CACHE.clear()
    USER_CACHE[9] = 'stale'
    import unittest.mock
    with unittest.mock.patch(__name__ + '.fetch_users_batch', return_value=None), \
         unittest.mock.patch(__name__ + '.save_user_cache'):
        resolve_users_parallel([9], 'tok', refresh=True)
    assert USER_CACHE[9] == 'stale', 'a failed batch must not overwrite a known name'

    # A user the bulk endpoint omits is not necessarily restricted. The single-user lookup
    # decides: a live account keeps its name, and only a 404 writes the User_<id> placeholder.
    USER_CACHE.clear()
    with unittest.mock.patch(__name__ + '.fetch_users_batch', return_value={1: 'Kecco'}), \
         unittest.mock.patch(__name__ + '.fetch_user_name',
                             side_effect=lambda s, uid, h: 'Cookiezi' if uid == 2 else RESTRICTED), \
         unittest.mock.patch(__name__ + '.save_user_cache'):
        out = resolve_users_parallel([1, 2, 3], 'tok')
    assert out == {1: 'Kecco', 2: 'Cookiezi', 3: 'User_3'}, \
        'an active account the bulk omitted must keep its name; only a 404 is restricted'
    assert USER_CACHE[2] == 'Cookiezi' and USER_CACHE[3] == 'User_3'

    # A confirmation that never lands leaves the id unnamed, not restricted.
    USER_CACHE.clear()
    with unittest.mock.patch(__name__ + '.fetch_users_batch', return_value={}), \
         unittest.mock.patch(__name__ + '.fetch_user_name', return_value=None), \
         unittest.mock.patch(__name__ + '.save_user_cache'):
        out = resolve_users_parallel([4], 'tok')
    assert out == {4: 'Unknown_4'}, 'an unread id must not be published as restricted'
    assert 4 not in USER_CACHE

    # A listing that gives up mid-way must say so. Reporting the sets that did arrive as if
    # they were all of them is what turns a throttled scan into a confident, wrong report.
    class PagedSession:
        def __init__(self, pages):
            self.pages, self.n = pages, 0
        def get(self, url, headers=None, params=None, timeout=None):
            # Out of scripted pages means an empty bucket, which is an answer, not a failure:
            # get_beatmapsets walks three of them.
            if self.n >= len(self.pages):
                return FakeResp(200, [])
            page = self.pages[self.n]
            self.n += 1
            if isinstance(page, Exception):
                raise page
            return FakeResp(200, page)
        def close(self):
            pass

    full = [{'id': i, 'status': 'ranked'} for i in range(100)]
    with unittest.mock.patch(__name__ + '.requests.Session',
                             lambda: PagedSession([full, requests.exceptions.Timeout('boom')])):
        sets, truncated = get_beatmapsets(1, 'tok')
    assert truncated is True, 'a listing that broke off must report it'
    assert len(sets) == 100, 'the sets that did arrive are still worth keeping'

    with unittest.mock.patch(__name__ + '.requests.Session',
                             lambda: PagedSession([full[:5]])):
        sets, truncated = get_beatmapsets(1, 'tok')
    assert truncated is False and len(sets) == 5, 'a listing that finished is not truncated'

    # Cancelling used to hand the caller a bare list, which it unpacks into two names.
    cancelled = threading.Event()
    cancelled.set()
    noms, unread = analyze_nominators([], 'tok', None, cancelled)
    assert (noms, unread) == ([], 0)
    sets, truncated = get_beatmapsets(1, 'tok', cancelled)
    assert (sets, truncated) == ([], False)

    print('scan_logic self-check OK')
