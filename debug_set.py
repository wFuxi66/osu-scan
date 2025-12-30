import requests
import os

CLIENT_ID = '47165'
CLIENT_SECRET = '7EiKteLaFyLzBaXuBR6LFtcFvMX5A80giRoFROky'

def get_token():
    payload = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'client_credentials',
        'scope': 'public'
    }
    response = requests.post('https://osu.ppy.sh/oauth/token', data=payload)
    return response.json().get('access_token')

def debug_set(set_id):
    token = get_token()
    headers = {'Authorization': f'Bearer {token}'}
    
    # Get Set Info
    print(f"Fetching Set {set_id}...")
    url = f"https://osu.ppy.sh/api/v2/beatmapsets/{set_id}"
    r = requests.get(url, headers=headers)
    data = r.json()
    
    print("\n--- Set ID:", data['id'], "---")
    print("Title:", data['title'])
    print("Artist:", data['artist'])
    print("Creator:", data['user']['username'], f"(ID: {data['user_id']})")
    
    print("\n[RELATED USERS FIELD]")
    rel = data.get('related_users', [])
    for u in rel:
        print(f"- {u['username']} ({u['id']})")
        
    print("\n[DIFFICULTIES]")
    for diff in data['beatmaps']:
        print(f"\nDiff: {diff['version']} (ID: {diff['id']})")
        print(f"User ID: {diff['user_id']}")
        owners = diff.get('owners', [])
        if owners:
            print("Owners (API Field):")
            for o in owners:
                print(f"  - {o['username']} ({o['id']})")
        else:
            print("Owners: [] (Empty or None)")

if __name__ == '__main__':
    debug_set('2367460')
