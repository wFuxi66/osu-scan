import requests
import csv
import time
import sys

# Configuration constants
API_BASE = 'https://osu.ppy.sh/api/v2'
TOKEN_URL = 'https://osu.ppy.sh/oauth/token'

# User Credentials
CLIENT_ID = '47165'
CLIENT_SECRET = '7EiKteLaFyLzBaXuBR6LFtcFvMX5A80giRoFROky'
TARGET_USER = '24230576'

def get_token(client_id, client_secret):
    """Obtains a client credentials token from osu! API."""
    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials',
        'scope': 'public'
    }
    try:
        response = requests.post(TOKEN_URL, data=data)
        response.raise_for_status()
        return response.json()['access_token']
    except requests.exceptions.RequestException as e:
        print(f"Error authenticating: {e}")
        try:
            print(f"Server response: {response.text}")
        except:
            pass
        sys.exit(1)

def get_user_id(username_or_id, token):
    """Resolves a username to an ID, or returns the ID if already one."""
    # If it's pure digits, assume it's already an ID, but verify via API to be safe/get display name?
    # Actually, let's just use the lookup endpoint which handles both nice usually,
    # or strictly use 'key' param if it's a username.
    
    headers = {'Authorization': f'Bearer {token}'}
    
    # Try assuming it's a username key
    params = {'key': 'username'}
    url = f'{API_BASE}/users/{username_or_id}/osu'
    
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()['id'], response.json()['username']
    except:
        pass

    # If failed, maybe it was an ID?
    if str(username_or_id).isdigit():
        url = f'{API_BASE}/users/{username_or_id}'
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return response.json()['id'], response.json()['username']
        except:
            pass
            
    print(f"Could not resolve user: {username_or_id}")
    sys.exit(1)

def get_beatmapsets(user_id, token):
    """Fetches all beatmap sets for a user."""
    headers = {'Authorization': f'Bearer {token}'}
    all_sets = []
    
    # Categories: graveyard, pending (wip), ranked, loved.
    # Note: 'pending' in API often covers WIP and Pending.
    set_types = ['graveyard', 'pending', 'ranked', 'loved']
    
    print("Scanning beatmap sets...")
    
    for s_type in set_types:
        offset = 0
        limit = 100 # API max usually 50 or 100
        while True:
            params = {'limit': limit, 'offset': offset}
            url = f'{API_BASE}/users/{user_id}/beatmapsets/{s_type}'
            
            try:
                response = requests.get(url, headers=headers, params=params)
                if response.status_code == 404:
                    break 
                
                response.raise_for_status()
                data = response.json()
                
                if not data:
                    break
                    
                for s in data:
                    # Enrich with status for context
                    s['status_category'] = s_type
                    all_sets.append(s)
                
                if len(data) < limit:
                    break
                
                offset += len(data)
                # Polite rate limiting
                time.sleep(0.2) 
            except Exception as e:
                print(f"Warning: Failed to fetch {s_type} sets at offset {offset}: {e}")
                break
                
    return all_sets

def analyze_sets(beatmapsets, host_id):
    """Finds GDs in the provided beatmap sets."""
    gds = []
    
    print(f"Analyzing {len(beatmapsets)} sets for Guest Difficulties...")
    
    for bset in beatmapsets:
        # Skip if for some reason the API returned a set not hosted by user (unlikely with this endpoint)
        if bset['user_id'] != host_id:
            continue
            
        set_title = f"{bset['artist']} - {bset['title']}"
        set_id = bset['id']
        
        beats = bset.get('beatmaps', [])
        if not beats:
            continue
            
        for beatmap in beats:
            mapper_id = beatmap['user_id']
            
            # If the mapper_id is different from the host_id, it's a GD
            if mapper_id != host_id:
                gd_entry = {
                    'mapper_id': mapper_id,
                    # API v2 often doesn't embed the full user profile in the beatmap list inside a set
                    # We will resolve names later to avoid N+1 if many diffs by same person
                    'mapper_name': None, 
                    'diff_name': beatmap['version'],
                    'set_title': set_title,
                    'status': bset['status_category'],
                    'last_updated': beatmap['last_updated'],
                    'link': f"https://osu.ppy.sh/beatmapsets/{set_id}#{beatmap['mode']}/{beatmap['id']}"
                }
                gds.append(gd_entry)
                
    return gds

def resolve_mapper_names(gds, token):
    """Resolves user IDs to usernames efficiently."""
    headers = {'Authorization': f'Bearer {token}'}
    unique_ids = set(gd['mapper_id'] for gd in gds)
    user_cache = {}
    
    print(f"Resolving {len(unique_ids)} unique GDers...")
    
    for i, uid in enumerate(unique_ids):
        # Check cache (not really needed for set structure but good if we rerun logic)
        if uid == 0: continue # Deleted user?
        
        if i % 10 == 0:
            print(f"Resolving {i}/{len(unique_ids)}...", end='\r')
            
        try:
            r = requests.get(f'{API_BASE}/users/{uid}', headers=headers, timeout=5)
            if r.status_code == 200:
                user_cache[uid] = r.json()['username']
            else:
                print(f"Failed to resolve {uid}: {r.status_code}")
                user_cache[uid] = f"User_{uid}"
            time.sleep(0.1) # Reduced sleep
        except Exception as e:
            print(f"Error resolving {uid}: {e}")
            user_cache[uid] = f"User_{uid}"
            
    # Apply back
    for gd in gds:
        gd['mapper_name'] = user_cache.get(gd['mapper_id'], f"ID:{gd['mapper_id']}")
        
    return gds

def main():
    print("Welcome to the osu! GDer Scanner")
    print("You need an osu! API Client ID and Secret.")
    print("Get them here: https://osu.ppy.sh/home/account/edit#oauth")
    print("-" * 50)
    
    # Input credentials
    client_id = CLIENT_ID
    client_secret = CLIENT_SECRET
    
    if not client_id or not client_secret:
        print("Credentials missing in script.")
        return
        
    # Support command line arg for user
    if len(sys.argv) > 1:
        target_user = sys.argv[1]
    else:
        target_user = TARGET_USER

    print("\nAuthenticating...")
    token = get_token(client_id, client_secret)
    
    print(f"Looking up user: {target_user}...")
    host_id, host_username = get_user_id(target_user, token)
    print(f"Found User: {host_username} (ID: {host_id})")
    
    sets = get_beatmapsets(host_id, token)
    if not sets:
        print("No beatmap sets found.")
        return
        
    gds = analyze_sets(sets, host_id)
    
    if gds:
        print(f"Found {len(gds)} likely Guest Difficulties. Fetching names...")
        gds = resolve_mapper_names(gds, token)
        
        # Sort by date
        gds.sort(key=lambda x: x['last_updated'], reverse=True)
        
        filename = f"gd_report_{host_username}.csv"
        fields = ['mapper_name', 'mapper_id', 'diff_name', 'set_title', 'status', 'last_updated', 'link']
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fields)
                writer.writeheader()
                for gd in gds:
                    writer.writerow(gd)
            print(f"\nSUCCESS! Report saved to: {os.path.abspath(filename)}")
        except IOError as e:
            print(f"Error writing file: {e}")
            
    else:
        print("\nNo Guest Difficulties found on your sets (where Beatmap Owner != Set Host).")

if __name__ == "__main__":
    import os
    main()
