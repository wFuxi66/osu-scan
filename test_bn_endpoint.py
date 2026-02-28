import os
import requests
from dotenv import load_dotenv

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

def check_bn_endpoint(bn_username, token):
    headers = {'Authorization': f'Bearer {token}'}
    
    # 1. Get ID
    r = requests.get(f'https://osu.ppy.sh/api/v2/users/{bn_username}', headers=headers, params={'key': 'username'})
    if r.status_code != 200:
        print(f"User {bn_username} not found")
        return
    
    uid = r.json()['id']
    print(f"BN {bn_username} ID: {uid}")
    
    # 2. Try 'nominated' endpoint
    url = f'https://osu.ppy.sh/api/v2/users/{uid}/beatmapsets/nominated'
    print(f"Testing URL: {url}")
    
    r = requests.get(url, headers=headers)
    print(f"Status Code: {r.status_code}")
    
    if r.status_code == 200:
        data = r.json()
        print(f"Found {len(data)} nominated maps!")
        if len(data) > 0:
            print(f"First map: {data[0]['artist']} - {data[0]['title']}")
    else:
        print("Endpoint failed or does not exist.")

if __name__ == "__main__":
    token = get_token()
    # "Andrea" is a known seasoned BN/NAT (or ex-BN). 
    # Or "Sotarks" (also BN at some point).
    # Let's try "Andrea".
    check_bn_endpoint("Andrea", token)
