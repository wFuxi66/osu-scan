import os
import requests
from dotenv import load_dotenv

# Load env variables
load_dotenv()
CLIENT_ID = os.getenv('OSU_CLIENT_ID')
CLIENT_SECRET = os.getenv('OSU_CLIENT_SECRET')

def get_token():
    url = 'https://osu.ppy.sh/oauth/token'
    data = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'client_credentials',
        'scope': 'public'
    }
    response = requests.post(url, data=data)
    return response.json().get('access_token')

def debug_map(user_id, token):
    headers = {'Authorization': f'Bearer {token}'}
    # 1. Fetch one ranked set
    print(f"Fetching ranked sets for user {user_id}...")
    r = requests.get(f'https://osu.ppy.sh/api/v2/users/{user_id}/beatmapsets/ranked?limit=1', headers=headers)
    
    if r.status_code == 200:
        sets = r.json()
        if not sets:
            print("No ranked sets found.")
            return

        bset = sets[0]
        map_id = bset['id']
        print(f"Found Set: {bset['artist']} - {bset['title']} (ID: {map_id})")
        
        # 2. Deep fetch the set
        url = f'https://osu.ppy.sh/api/v2/beatmapsets/{map_id}'
        print(f"Deep fetching {url}...")
        r2 = requests.get(url, headers=headers)
        if r2.status_code == 200:
            data = r2.json()
            print(f"Status: {data.get('status')}")
            print(f"Current Nominations: {data.get('current_nominations')}")
            # Check for other relevant fields
            print(f"Nominations Summary: {data.get('nominations_summary')}")
            print(f"Recent Favourites: {len(data.get('recent_favourites', []))}")
        else:
            print(f"Error deep fetching: {r2.status_code}")
    else:
        print(f"Error fetching list: {r.status_code}")

if __name__ == "__main__":
    token = get_token()
    # Sotarks ID: 4908696 (Wait, verify ID)
    # Actually, I'll use the search to find ID if needed, but let's try the one from web search result or just search 'Sotarks'
    # Sotarks ID is 2262963 (Wait, I used getting the ID via script before)
    # Let's trust my previous knowledge or just resolve it dynamically.
    
    # Resolve Fuxi66 ID
    r = requests.get('https://osu.ppy.sh/api/v2/users/Fuxi66', headers={'Authorization': f'Bearer {token}'}, params={'key': 'username'})
    if r.status_code == 200:
        uid = r.json()['id']
        print(f"Fuxi66 ID: {uid}")
        debug_map(uid, token)
    else:
        print("Could not resolve Fuxi66 ID")
