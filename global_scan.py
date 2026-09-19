import requests
import time
import os
import json
import concurrent.futures
from collections import defaultdict
from datetime import datetime, timezone

import bn_data
import scan_logic

# Firebase config
FIREBASE_URL = os.environ.get('FIREBASE_URL', '')
FIREBASE_SECRET = os.environ.get('FIREBASE_SECRET', '')
# Set this and a scan writes under its own prefix instead of over the live data, so a
# rehearsal run can be inspected before anything replaces what the site is serving.
FIREBASE_NS = os.environ.get('FIREBASE_NS', '').strip('/')

# ---- Firebase helpers ----

def remote_path(path):
    """Where this data lives in Firebase, namespace included."""
    return f'{FIREBASE_NS}/{path}' if FIREBASE_NS else path


def local_path(path):
    """The offline stand-in. Only the last segment names the file, so a namespaced path
    lands beside its unprefixed twin rather than in a directory that does not exist."""
    return f'{path.rsplit("/", 1)[-1]}_cache.json'


def save_to_firebase(data, path='leaderboard'):
    """Saves data to Firebase Realtime Database."""
    if not FIREBASE_URL or not FIREBASE_SECRET:
        print("Firebase not configured, saving to local file instead")
        with open(local_path(path), 'w') as f:
            json.dump(data, f)
        return True

    path = remote_path(path)
    url = f'{FIREBASE_URL}/{path}.json?auth={FIREBASE_SECRET}'
    try:
        r = requests.put(url, json=data, timeout=30)
        r.raise_for_status()
        print(f"Saved to Firebase at /{path}")
        return True
    except Exception as e:
        print(f"Error saving to Firebase: {e}")
        # Fallback to local file
        with open(local_path(path), 'w') as f:
            json.dump(data, f)
        return False

def load_from_firebase(path='leaderboard'):
    """Loads data from Firebase Realtime Database."""
    if not FIREBASE_URL:
        try:
            with open(local_path(path), 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return None

    # Use secret for reading too if available
    auth_suffix = f'?auth={FIREBASE_SECRET}' if FIREBASE_SECRET else ''
    url = f'{FIREBASE_URL}/{remote_path(path)}.json{auth_suffix}'
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        return data
    except Exception as e:
        print(f"Error loading from Firebase: {e}")
        return None


def publish_scan_results(result, nomination_index):
    """Publish the lookup separately so leaderboard downloads stay small."""
    if not save_to_firebase(nomination_index, path='nomination_index'):
        return False
    return save_to_firebase(result)

# ---- API Helpers with Rate Limit Resilience ----

def list_looks_truncated(found, known, floor=0.9):
    """Whether a list that should hold `known` entries came back too short to trust.

    Sources go down, and every one of them reports that as an empty list rather than as an
    error. Measured against the last good scan, so there is no fixed number to maintain, and
    disabled entirely when there is nothing to measure against - a first run has to publish.
    """
    return bool(known) and found < known * floor


def build_nomination_index(nominations_by_bn, scanned_at):
    """Build a compact, deterministic mapset-to-nominators lookup."""
    nominators_by_set = defaultdict(set)
    for bn_id, sets in nominations_by_bn.items():
        for bset in sets:
            nominators_by_set[bset['id']].add(bn_id)

    return {
        'last_scan': scanned_at,
        'sets': {
            str(set_id): sorted(nominator_ids)
            for set_id, nominator_ids in sorted(nominators_by_set.items())
        },
    }


class ApiUnavailable(Exception):
    """The endpoint could not be read at all.

    Distinct from a 404, which is a real answer meaning "nothing here". Conflating the two
    is how a rate-limited scan ends up looking like a BN who simply nominated nothing.
    """


# osu! answers a burst on these endpoints with a Retry-After measured in half hours. Capping
# the wait at 15s, as this used to, just spends every attempt inside the window and gives up
# still throttled - so the scan reported no nominations for whoever it was reading at the
# time. Waiting the full ask is slower and correct.
MAX_RETRY_AFTER = int(os.environ.get('BN_MAX_RETRY_AFTER', 1800))
# Single-threaded, so a plain gap between requests is the whole rate limiter it needs.
REQUEST_GAP = 60.0 / float(os.environ.get('OSU_RATE_PER_MIN', 60))


def safe_api_get(url, headers, params=None, timeout=15, session=None, max_retries=4):
    """Executes a GET request, waiting out rate limits.

    Returns the response, or None for a 404. Raises ApiUnavailable when the endpoint could
    not be read, so the caller can refuse to treat an unread page as an empty one.
    """
    req_func = session.get if session else requests.get
    current_headers = dict(headers)
    for attempt in range(max_retries):
        try:
            r = req_func(url, headers=current_headers, params=params, timeout=timeout)
            if r.status_code == 429:
                retry_after = min(int(r.headers.get('Retry-After', 60) or 60), MAX_RETRY_AFTER)
                print(f"[Rate Limited 429] waiting {retry_after}s "
                      f"(attempt {attempt+1}/{max_retries})...", flush=True)
                time.sleep(retry_after)
                continue
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise ApiUnavailable(f"{url}: {e}") from e
            time.sleep(1 + attempt)
    raise ApiUnavailable(f"{url}: still rate limited after {max_retries} attempts")

def fetch_bn_nominations(osu_id, token, cancel_event=None, session=None):
    """Fetches all nominated sets for a BN via osu! API or web endpoint with rate-limit resilience."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }
    if token:
        headers['Authorization'] = f'Bearer {token}'
        
    all_sets = []
    offset = 0
    limit = 100
    
    while True:
        if cancel_event and cancel_event.is_set():
            return []
        
        params = {'limit': limit, 'offset': offset}
        url = f'https://osu.ppy.sh/users/{osu_id}/beatmapsets/nominated'
        
        # Lets ApiUnavailable through on purpose. Breaking here instead would hand back the
        # pages that happened to load as though they were all of them, and a BN throttled
        # mid-pagination would silently lose the rest of their nominations.
        r = safe_api_get(url, headers=headers, params=params, timeout=15, session=session)
        if not r:
            break

        data = r.json()
        if not data:
            break

        all_sets.extend(data)

        if len(data) < limit:
            break

        offset += len(data)
        time.sleep(REQUEST_GAP)

    return all_sets

def deep_fetch_set(set_id, token, session=None):
    """Deep-fetches a beatmapset to get current_nominations with rate-limit retry."""
    headers = {'Authorization': f'Bearer {token}'}
    url = f'https://osu.ppy.sh/api/v2/beatmapsets/{set_id}'
    r = safe_api_get(url, headers=headers, timeout=15, session=session)
    if r and r.status_code == 200:
        return r.json()
    return None

def run_global_scan(progress_callback=None, cancel_event=None):
    """
    Main scan function. Always does a full scan.

    Args:
        progress_callback: function(msg) for progress updates
        cancel_event: threading.Event to cancel

    Returns:
        dict with leaderboard data
    """
    def progress(msg):
        print(msg, flush=True)
        if progress_callback:
            progress_callback(msg)
    
    # 1. Get osu! API token
    progress("Authenticating with osu! API...")
    token = scan_logic.get_token()
    if not token:
        return {'error': 'Failed to authenticate with osu! API'}
    
    # 2. Fetch all BNs
    progress("Fetching BN list from Mapper's Guild...")
    all_bns = bn_data.get_all_bns()

    # Mapper's Guild supplies about two thirds of this list, and both of its fetches answer a
    # refusal with an empty list - so an outage there arrives looking exactly like a community
    # that shrank overnight. Publishing that drops every BN it failed to mention, and every duo
    # they were half of, out of the ladder. Measured against what the last good scan found, so
    # there is no magic number to keep up to date.
    previous = load_from_firebase() or {}
    known = previous.get('total_bns_scanned') or 0
    if list_looks_truncated(len(all_bns), known):
        msg = (f"Only {len(all_bns)} nominators listed, against {known} in the last scan - "
               f"one of the sources is down. Previous leaderboard kept; re-run to try again.")
        progress(msg)
        return {'error': msg, 'total_bns': len(all_bns), 'previous_total_bns': known}

    if cancel_event and cancel_event.is_set():
        return {'error': 'Cancelled'}
    
    progress(f"Found {len(all_bns)} BNs. Fetching their nominations...")
    
    # 3. Fetch nominations for every BN with clean, polite pacing
    bn_nomination_sets = {}  # osu_id -> list of set dicts
    all_set_ids = set()
    unread_bns = []          # BNs the API would not answer for; checked before publishing
    total_bns = len(all_bns)
    
    session_bns = requests.Session()
    adapter_bns = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10)
    session_bns.mount('https://', adapter_bns)
    session_bns.mount('http://', adapter_bns)

    for i, bn in enumerate(all_bns):
        if cancel_event and cancel_event.is_set():
            session_bns.close()
            return {'error': 'Cancelled'}
        
        if (i + 1) % 50 == 0 or i == 0 or (i + 1) == total_bns:
            progress(f"Fetching nominations: {i + 1}/{total_bns} BNs...")
        
        uid = bn['osu_id']
        try:
            sets = fetch_bn_nominations(uid, token, cancel_event, session=session_bns)
        except ApiUnavailable as e:
            # Recorded, not swallowed. A BN whose nominations could not be read is not a BN
            # with no nominations, and the difference decides whether this pass may publish.
            unread_bns.append(uid)
            print(f"Could not read nominations for BN {uid}: {e}", flush=True)
            continue
        bn_nomination_sets[uid] = sets
        for s in sets:
            all_set_ids.add(s['id'])

        time.sleep(REQUEST_GAP)

        # Refresh token periodically (every 200 BNs) to keep fresh rate limit buckets
        if (i + 1) % 200 == 0:
            new_token = scan_logic.get_token()
            if new_token:
                token = new_token

    session_bns.close()
    
    progress(f"Found {len(all_set_ids)} unique sets. Building nomination counts...")

    if cancel_event and cancel_event.is_set():
        return {'error': 'Cancelled'}

    # 4. Build per-mode nomination counts and mapset nominator index directly in memory.
    # Build bn_lookup early so mode attribution can use BN's known modes
    bn_lookup = {bn['osu_id']: bn for bn in all_bns}

    def attribute_mode(bset, bn_modes):
        """Determine which mode a nomination belongs to for a given BN."""
        eligible = bset.get('nominations_summary', {}).get('eligible_main_rulesets', [])
        eligible = ['catch' if m == 'fruits' else m for m in eligible]
        if bn_modes:
            matched = [m for m in bn_modes if m in eligible]
            if matched:
                return matched[0]
            # BN mode not in eligible (old set / data gap) — use BN's primary mode
            return bn_modes[0]
        if eligible:
            return eligible[0]
        # Last resort: look at the first beatmap's mode
        beatmaps = bset.get('beatmaps', [])
        raw = beatmaps[0].get('mode', 'osu') if beatmaps else 'osu'
        return 'catch' if raw == 'fruits' else raw

    bn_mode_counts = defaultdict(lambda: defaultdict(int))
    set_mode_bns = defaultdict(lambda: defaultdict(list))

    for bn_id, sets in bn_nomination_sets.items():
        bn_modes = bn_lookup.get(bn_id, {}).get('modes', [])
        for bset in sets:
            mode = attribute_mode(bset, bn_modes)
            bn_mode_counts[bn_id][mode] += 1
            if bn_id not in set_mode_bns[bset['id']][mode]:
                set_mode_bns[bset['id']][mode].append(bn_id)

    # 5. Build Iconic BN Duos from in-memory nomination index
    progress(f"Building Iconic BN Duos from {len(set_mode_bns)} unique mapsets...")
    duo_counts = defaultdict(int)

    for set_id, modes_dict in set_mode_bns.items():
        for mode, nominators in modes_dict.items():
            if len(nominators) >= 2:
                for i in range(len(nominators)):
                    for j in range(i + 1, len(nominators)):
                        pair = tuple(sorted([nominators[i], nominators[j]]))
                        duo_key = f"{pair[0]}:{pair[1]}:{mode}"
                        duo_counts[duo_key] += 1

    # 7. Resolve unknown nominator names (nominators not in bn_lookup)
    # Try to resolve unknown BN IDs from the nomination data
    unknown_ids = [uid for uid in bn_mode_counts if uid not in bn_lookup]
    
    if unknown_ids:
        progress(f"Resolving {len(unknown_ids)} unknown nominator names...")
        resolved = scan_logic.resolve_users_parallel(unknown_ids, token, progress_callback)
        for uid in unknown_ids:
            username = resolved.get(uid, f'User_{uid}')
            bn_lookup[uid] = {'osu_id': uid, 'username': username, 'modes': [], 'is_current': False}
    
    # 8. Format Top BNs leaderboard
    top_bns = []
    for uid, mode_counts_dict in bn_mode_counts.items():
        bn_info = bn_lookup.get(uid, {'username': f'User_{uid}', 'modes': [], 'is_current': False})
        total = sum(mode_counts_dict.values())
        entry = {
            'osu_id': uid,
            'username': bn_info.get('username', f'User_{uid}'),
            'is_current': bn_info.get('is_current', False),
            'total': total,
            'by_mode': dict(mode_counts_dict),
        }
        top_bns.append(entry)
    
    top_bns.sort(key=lambda x: -x['total'])
    
    # 9. Format Iconic BN Duos leaderboard
    duos = []
    for duo_key, count in duo_counts.items():
        parts = duo_key.split(':')
        bn1_id, bn2_id, mode = int(parts[0]), int(parts[1]), parts[2]
        bn1_info = bn_lookup.get(bn1_id, {'username': f'User_{bn1_id}'})
        bn2_info = bn_lookup.get(bn2_id, {'username': f'User_{bn2_id}'})
        
        duos.append({
            'bn1_id': bn1_id,
            'bn1_name': bn1_info.get('username', f'User_{bn1_id}'),
            'bn2_id': bn2_id,
            'bn2_name': bn2_info.get('username', f'User_{bn2_id}'),
            'mode': mode,
            'count': count,
        })
    
    duos.sort(key=lambda x: -x['count'])
    
    scanned_at = datetime.now(timezone.utc).isoformat()
    result = {
        'last_scan': scanned_at,
        'total_bns_scanned': len(all_bns),
        'total_sets_scanned': len(all_set_ids),
        'top_bns': top_bns,
        'duos': duos,
    }
    nomination_index = build_nomination_index(bn_nomination_sets, scanned_at)
    
    # 10. Save to Firebase, but only a pass worth keeping.
    #
    # Every unread BN would publish as one with no nominations, dropping them down the ladder
    # and taking their duo pairings with them. Overwriting a good leaderboard with a
    # rate-limited one is worse than serving yesterday's, so a short pass keeps yesterday's.
    if unread_bns:
        msg = (f"Incomplete scan: {len(unread_bns)} of {total_bns} BNs could not be read "
               f"(the API kept refusing). Previous leaderboard kept; re-run to try again.")
        progress(msg)
        return {'error': msg, 'unread_bns': len(unread_bns), 'total_bns': total_bns}

    progress("Saving results to Firebase...")
    if not publish_scan_results(result, nomination_index):
        # The fallback file lands on whatever machine ran the scan, and on a CI runner that
        # machine is about to be deleted. Saying "complete" here is how two hours of work
        # disappear behind a green tick.
        msg = "Scan finished but its leaderboard and nomination index could not both be published."
        progress(msg)
        return {'error': msg}

    progress(f"Scan complete! {len(top_bns)} BNs ranked, {len(duos)} duo pairs found.")
    return result


if __name__ == '__main__':
    # A short source list must never reach Firebase, and a publish that did not happen must
    # never read as a finished scan. Both guards are one `if` each; this is what holds them.
    assert list_looks_truncated(300, 955), 'a third of the list missing is an outage'
    assert not list_looks_truncated(955, 955), 'the same list as last time is fine'
    assert not list_looks_truncated(900, 955), 'nominators do retire; a small drop is normal'
    assert not list_looks_truncated(0, 0), 'a first scan has nothing to measure against'
    assert not list_looks_truncated(1200, 955), 'a growing list is not a truncated one'

    import unittest.mock
    _saved = (FIREBASE_URL, FIREBASE_SECRET)
    globals()['FIREBASE_URL'], globals()['FIREBASE_SECRET'] = 'https://example.invalid', 'secret'
    try:
        with unittest.mock.patch.object(requests, 'put',
                                        side_effect=requests.exceptions.Timeout('down')):
            assert save_to_firebase({'x': 1}, path='selfcheck') is False, \
                'a refused publish must report failure, not fall back in silence'
        with unittest.mock.patch.object(requests, 'put',
                                        return_value=unittest.mock.Mock(status_code=200)):
            assert save_to_firebase({'x': 1}, path='selfcheck') is True
    finally:
        globals()['FIREBASE_URL'], globals()['FIREBASE_SECRET'] = _saved
        if os.path.exists('selfcheck_cache.json'):
            os.remove('selfcheck_cache.json')
    print('global_scan self-check OK')

    from dotenv import load_dotenv
    load_dotenv()
    
    # Quick test: scan only 5 BNs
    import bn_data as bd
    
    print("=== Quick test: scanning 5 BNs ===")
    token = scan_logic.get_token()
    if not token:
        print("Auth failed")
        exit(1)
    
    bns = bd.get_all_bns()[:5]
    
    for bn in bns:
        sets = fetch_bn_nominations(bn['osu_id'], token)
        print(f"{bn['username']}: {len(sets)} nominations")
