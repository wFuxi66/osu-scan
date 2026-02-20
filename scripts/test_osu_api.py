import os
import sys
import requests
import time

# Add parent directory to path so we can import app/scan_logic
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scan_logic import get_token, get_user_id, API_BASE

def fetch_all(user_id, token, endpoint, limit=100):
    headers = {'Authorization': f'Bearer {token}'}
    all_sets = []
    offset = 0
    
    while True:
        params = {'limit': limit, 'offset': offset}
        r = requests.get(f'{API_BASE}/users/{user_id}/beatmapsets/{endpoint}', headers=headers, params=params)
        data = r.json()
        
        if not data:
            break
            
        all_sets.extend(data)
        print(f"{endpoint} retrieved page: {len(data)}, total so far: {len(all_sets)}")
        
        if len(data) < limit:
            break
            
        offset += len(data)
        time.sleep(0.1)
    
    return len(all_sets)

def test_api():
    token = get_token()
    if not token:
        print("No token!")
        return
        
    user_id_andrea, _ = get_user_id("Andrea", token)
    user_id_aki, _ = get_user_id("Akitoshi", token)
    
    print(f"Andrea ID: {user_id_andrea}")
    nom_count = fetch_all(user_id_andrea, token, "nominated", limit=50)
    print(f"Andrea nominated maps (limit=50): {nom_count}")
    
    nom_count_100 = fetch_all(user_id_andrea, token, "nominated", limit=100)
    print(f"Andrea nominated maps (limit=100): {nom_count_100}")

    rank_count = fetch_all(user_id_andrea, token, "ranked", limit=100)
    print(f"Andrea ranked maps (limit=100): {rank_count}")
    
    rank_and_approved_count = fetch_all(user_id_andrea, token, "ranked_and_approved", limit=100)
    print(f"Andrea ranked_and_approved maps: {rank_and_approved_count}")
    
    guest_count = fetch_all(user_id_aki, token, "guest", limit=100)
    print(f"Akitoshi guest maps (limit=100): {guest_count}")

if __name__ == "__main__":
    test_api()
