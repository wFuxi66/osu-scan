import requests
import time
import os
import json
import concurrent.futures
from collections import defaultdict
from datetime import datetime

import bn_data
import scan_logic

# Firebase config
FIREBASE_URL = os.environ.get('FIREBASE_URL', '')
FIREBASE_SECRET = os.environ.get('FIREBASE_SECRET', '')

# ---- Firebase helpers ----

def save_to_firebase(data, path='leaderboard'):
    """Saves data to Firebase Realtime Database."""
    if not FIREBASE_URL or not FIREBASE_SECRET:
        print("Firebase not configured, saving to local file instead")
        with open('leaderboard_cache.json', 'w') as f:
            json.dump(data, f)
        return True
    
    url = f'{FIREBASE_URL}/{path}.json?auth={FIREBASE_SECRET}'
    try:
        r = requests.put(url, json=data, timeout=30)
        r.raise_for_status()
        print(f"Saved to Firebase at /{path}")
        return True
    except Exception as e:
        print(f"Error saving to Firebase: {e}")
        # Fallback to local file
        with open('leaderboard_cache.json', 'w') as f:
            json.dump(data, f)
        return False

def load_from_firebase(path='leaderboard'):
    """Loads data from Firebase Realtime Database."""
    if not FIREBASE_URL:
        try:
            with open('leaderboard_cache.json', 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return None

    # Use secret for reading too if available
    auth_suffix = f'?auth={FIREBASE_SECRET}' if FIREBASE_SECRET else ''
    url = f'{FIREBASE_URL}/{path}.json{auth_suffix}'
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        return data
    except Exception as e:
        print(f"Error loading from Firebase: {e}")
        return None

# ---- Scan logic ----

def fetch_bn_nominations(osu_id, token, cancel_event=None):
    """Fetches all nominated sets for a BN via osu! API."""
    headers = {'Authorization': f'Bearer {token}'}
    all_sets = []
    offset = 0
    limit = 50
    
    while True:
        if cancel_event and cancel_event.is_set():
            return []
        
        params = {'limit': limit, 'offset': offset}
        url = f'https://osu.ppy.sh/api/v2/users/{osu_id}/beatmapsets/nominated'
        
        try:
            r = requests.get(url, headers=headers, params=params, timeout=15)
            if r.status_code == 404:
                break
            r.raise_for_status()
            data = r.json()
            
            if not data:
                break
            
            all_sets.extend(data)
            
            if len(data) < limit:
                break
            
            offset += len(data)
            time.sleep(0.05)
        except Exception as e:
            print(f"Error fetching nominations for user {osu_id}: {e}")
            break
    
    return all_sets

def deep_fetch_set(set_id, token):
    """Deep-fetches a beatmapset to get current_nominations."""
    headers = {'Authorization': f'Bearer {token}'}
    
    try:
        url = f'https://osu.ppy.sh/api/v2/beatmapsets/{set_id}'
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"Error deep-fetching set {set_id}: {e}")
    
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
        print(msg)
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
    
    if cancel_event and cancel_event.is_set():
        return {'error': 'Cancelled'}
    
    progress(f"Found {len(all_bns)} BNs. Fetching their nominations...")
    
    # 3. Fetch nominations for every BN
    # Collect all nominated set IDs and track which BN nominated which set
    bn_nomination_sets = {}  # osu_id -> list of set dicts
    all_set_ids = set()
    
    total_bns = len(all_bns)
    
    for i, bn in enumerate(all_bns):
        if cancel_event and cancel_event.is_set():
            return {'error': 'Cancelled'}
        
        if (i + 1) % 10 == 0 or i == 0:
            progress(f"Fetching nominations: {i + 1}/{total_bns} BNs...")
        
        sets = fetch_bn_nominations(bn['osu_id'], token, cancel_event)
        bn_nomination_sets[bn['osu_id']] = sets
        
        for s in sets:
            all_set_ids.add(s['id'])
        
        # Rate limiting
        time.sleep(0.05)
        
        # Refresh token periodically (every 200 BNs)
        if (i + 1) % 200 == 0:
            new_token = scan_logic.get_token()
            if new_token:
                token = new_token
    
    progress(f"Found {len(all_set_ids)} unique sets. Building nomination counts...")

    if cancel_event and cancel_event.is_set():
        return {'error': 'Cancelled'}

    # 4. Build per-mode nomination counts directly from API fetch results.
    # The /beatmapsets/nominated endpoint returns all sets a BN has ever nominated,
    # including already-ranked sets. current_nominations is cleared when a set ranks,
    # so counting from there would miss the majority of nominations.

    # Build bn_lookup early so mode attribution can use BN's known modes
    bn_lookup = {bn['osu_id']: bn for bn in all_bns}

    bn_mode_counts = defaultdict(lambda: defaultdict(int))

    for bn_id, sets in bn_nomination_sets.items():
        bn_modes = bn_lookup.get(bn_id, {}).get('modes', [])
        for bset in sets:
            # Determine which mode this nomination belongs to.
            # Intersect the set's eligible rulesets with the BN's known modes.
            # This prevents std-only BNs from being credited with mania/taiko nominations.
            eligible = bset.get('nominations_summary', {}).get('eligible_main_rulesets', [])
            # Normalize 'fruits' -> 'catch'
            eligible = ['catch' if m == 'fruits' else m for m in eligible]

            if bn_modes:
                # Use the intersection of BN's modes and set's eligible modes
                matched = [m for m in bn_modes if m in eligible]
                if matched:
                    mode = matched[0]
                else:
                    # BN mode not in eligible (old set / data gap) — use BN's primary mode
                    mode = bn_modes[0]
            elif eligible:
                mode = eligible[0]
            else:
                # Last resort: look at the first beatmap's mode
                beatmaps = bset.get('beatmaps', [])
                raw = beatmaps[0].get('mode', 'osu') if beatmaps else 'osu'
                mode = 'catch' if raw == 'fruits' else raw

            bn_mode_counts[bn_id][mode] += 1

    # 5. Deep-fetch all unique sets for duo counts only
    set_nominations = {}  # set_id -> list of {user_id, mode}
    
    set_ids_list = list(all_set_ids)
    total_sets = len(set_ids_list)
    
    def fetch_set_noms(set_id):
        data = deep_fetch_set(set_id, token)
        if data:
            noms = data.get('current_nominations', [])
            return (set_id, noms)
        return (set_id, [])
    
    completed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_set_noms, sid): sid for sid in set_ids_list}
        
        for future in concurrent.futures.as_completed(futures):
            if cancel_event and cancel_event.is_set():
                executor.shutdown(wait=False, cancel_futures=True)
                return {'error': 'Cancelled'}
            
            completed += 1
            if completed % 50 == 0:
                progress(f"Deep-fetching sets: {completed}/{total_sets}...")
            
            try:
                set_id, noms = future.result()
                if noms:
                    set_nominations[set_id] = noms
            except Exception as e:
                print(f"Deep-fetch error: {e}")
    
    progress(f"Deep-fetched {len(set_nominations)} sets. Building duo leaderboard...")

    # 6. Build duo counts (Iconic BN Duos) from current_nominations.
    # Note: current_nominations is only populated for pending/qualified sets.
    # (bn1_id, bn2_id, mode) -> count  (bn1 < bn2 to avoid duplicates)
    duo_counts = defaultdict(int)

    for set_id, noms in set_nominations.items():
        mode_groups = defaultdict(list)
        for nom in noms:
            user_id = nom.get('user_id')
            mode = nom.get('rulesets', [None])
            if isinstance(mode, list) and mode:
                mode = mode[0]
            elif not mode:
                mode = nom.get('mode', 'osu')
            if mode == 'fruits':
                mode = 'catch'
            if user_id:
                mode_groups[mode].append(user_id)

        for mode, nominators in mode_groups.items():
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
        headers = {'Authorization': f'Bearer {token}'}
        for uid in unknown_ids:
            try:
                r = requests.get(f'https://osu.ppy.sh/api/v2/users/{uid}', headers=headers, timeout=10)
                if r.status_code == 200:
                    user_data = r.json()
                    bn_lookup[uid] = {'osu_id': uid, 'username': user_data.get('username', f'User_{uid}'), 'modes': [], 'is_current': False}
                else:
                    bn_lookup[uid] = {'osu_id': uid, 'username': f'User_{uid}', 'modes': [], 'is_current': False}
                time.sleep(0.05)
            except:
                bn_lookup[uid] = {'osu_id': uid, 'username': f'User_{uid}', 'modes': [], 'is_current': False}
    
    # 9. Format Top BNs leaderboard
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
    
    # 10. Format Iconic BN Duos leaderboard
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
    
    result = {
        'last_scan': datetime.utcnow().isoformat(),
        'total_bns_scanned': len(all_bns),
        'total_sets_scanned': len(all_set_ids),
        'top_bns': top_bns,
        'duos': duos,
    }
    
    # 11. Save to Firebase
    progress("Saving results to Firebase...")
    save_to_firebase(result)
    
    progress(f"Scan complete! {len(top_bns)} BNs ranked, {len(duos)} duo pairs found.")
    return result


if __name__ == '__main__':
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
