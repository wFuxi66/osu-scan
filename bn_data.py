import requests
import time

MAPPERS_GUILD_API = 'https://bn.mappersguild.com/api/users'

def fetch_current_bns():
    """Fetches all current BNs and NATs from Mapper's Guild API."""
    try:
        r = requests.get(f'{MAPPERS_GUILD_API}/relevantInfo', timeout=30)
        r.raise_for_status()
        data = r.json()
        return data.get('users', [])
    except Exception as e:
        print(f"Error fetching current BNs: {e}")
        return []

def fetch_former_bns():
    """Fetches all former BNs and NATs from Mapper's Guild API."""
    try:
        r = requests.get(f'{MAPPERS_GUILD_API}/loadPreviousBnAndNat', timeout=30)
        r.raise_for_status()
        data = r.json()
        return data.get('users', [])
    except Exception as e:
        print(f"Error fetching former BNs: {e}")
        return []

def extract_bn_info(user):
    """Extracts relevant info from a Mapper's Guild user entry."""
    modes = []
    for mode_info in user.get('modesInfo', []):
        mode = mode_info.get('mode', '')
        if mode and mode != 'none':
            modes.append(mode)
    
    # Also extract modes from history for former BNs who might have empty modesInfo
    if not modes:
        history_modes = set()
        for entry in user.get('history', []):
            mode = entry.get('mode', '')
            if mode and mode != 'none':
                history_modes.add(mode)
        modes = list(history_modes)
    
    return {
        'osu_id': user['osuId'],
        'username': user['username'],
        'modes': modes,
        'is_current': user.get('isBn', False) or user.get('isNat', False),
        'bn_duration': user.get('bnDuration', 0),
        'nat_duration': user.get('natDuration', 0),
    }

def fetch_alumni():
    """Fetches osu! Alumni (group 16) from the groups page to catch old BNs not in Mapper's Guild."""
    import re
    
    alumni = []
    
    try:
        r = requests.get('https://osu.ppy.sh/groups/16', timeout=30)
        if r.status_code != 200:
            print(f"Alumni page returned status {r.status_code}")
            return []
        
        # osu! embeds user data in <script id="json-users"> tag
        match = re.search(r'<script\s+id="json-users"[^>]*>(.*?)</script>', r.text, re.DOTALL)
        if not match:
            print("Could not find json-users in Alumni page")
            return []
        
        import json
        users = json.loads(match.group(1))
        
        for user in users:
            alumni.append({
                'osu_id': user['id'],
                'username': user.get('username', f'User_{user["id"]}'),
                'modes': [],
                'is_current': False,
                'bn_duration': 0,
                'nat_duration': 0,
            })
        
        print(f"Found {len(alumni)} Alumni from osu! group 16")
    except Exception as e:
        print(f"Error fetching Alumni: {e}")
    
    return alumni

def get_all_bns():
    """Fetches and merges all BNs (current + former + alumni) into a deduplicated list."""
    print("Fetching current BNs from Mapper's Guild...")
    current = fetch_current_bns()
    print(f"Found {len(current)} current BNs/NATs")
    
    time.sleep(0.5)
    
    print("Fetching former BNs from Mapper's Guild...")
    former = fetch_former_bns()
    print(f"Found {len(former)} former BNs/NATs")
    
    # Merge and deduplicate by osuId
    seen_ids = set()
    all_bns = []
    
    # Current BNs first (they take priority for username/modes)
    for user in current:
        osu_id = user['osuId']
        if osu_id not in seen_ids:
            seen_ids.add(osu_id)
            all_bns.append(extract_bn_info(user))
    
    # Then former BNs (only if not already in the list)
    for user in former:
        osu_id = user['osuId']
        if osu_id not in seen_ids:
            seen_ids.add(osu_id)
            all_bns.append(extract_bn_info(user))
    
    # Then Alumni from osu! group 16 (catches old BNs missing from Mapper's Guild)
    print("Fetching Alumni from osu! group 16...")
    alumni = fetch_alumni()
    alumni_added = 0
    for al in alumni:
        if al['osu_id'] not in seen_ids:
            seen_ids.add(al['osu_id'])
            all_bns.append(al)
            alumni_added += 1
    print(f"Added {alumni_added} new Alumni not in Mapper's Guild")
    
    print(f"Total unique BNs/NATs: {len(all_bns)}")
    return all_bns


if __name__ == '__main__':
    bns = get_all_bns()
    current_count = sum(1 for bn in bns if bn['is_current'])
    former_count = len(bns) - current_count
    print(f"\nCurrent: {current_count}, Former: {former_count}")
    
    # Count by mode
    mode_counts = {}
    for bn in bns:
        for mode in bn['modes']:
            mode_counts[mode] = mode_counts.get(mode, 0) + 1
    
    print("\nBy mode:")
    for mode, count in sorted(mode_counts.items()):
        print(f"  {mode}: {count}")
