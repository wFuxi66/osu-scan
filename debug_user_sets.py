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
    r = requests.post('https://osu.ppy.sh/oauth/token', data=payload)
    return r.json()['access_token']

def check_user_sets(user_id):
    token = get_token()
    headers = {'Authorization': f'Bearer {token}'}
    
    # Amats ID = 21662192
    print(f"Fetching ranked sets for user {user_id}...")
    url = f"https://osu.ppy.sh/api/v2/users/{user_id}/beatmapsets/ranked?limit=50"
    
    r = requests.get(url, headers=headers)
    sets = r.json()
    
    found = False
    for s in sets:
        if s['id'] == 2367460: # Adventure Starts Here
            found = True
            print("\nFound Set: Adventure Starts Here")
            print(f"Related Users: {s.get('related_users')}")
            for b in s['beatmaps']:
                if b['id'] == 5124304: # FukA's Cute Oak
                    print(f"Checking Diff: {b['version']}")
                    print(f"Diff User ID: {b['user_id']}")
                    
                    owners = b.get('owners')
                    print(f"Owners field: {owners}")
                    
                    if not owners:
                        print("Confirmed: 'owners' field is MISSING in this endpoint.")
                    else:
                        print("Wait, 'owners' field IS present.")
    
    if not found:
        print("Set not found in first 50 results.")

if __name__ == '__main__':
    check_user_sets(21662192)
