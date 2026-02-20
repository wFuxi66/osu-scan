import os
import sys
import requests
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scan_logic import get_token, API_BASE

def test_user_modes():
    token = get_token()
    headers = {'Authorization': f'Bearer {token}'}
    
    # Try Andrea (33599) and Sotarks (4452992)
    for uid in [33599, 4452992]:
        print(f"--- Fetching {uid} ---")
        r = requests.get(f'{API_BASE}/users/{uid}', headers=headers)
        if r.status_code == 200:
            data = r.json()
            print(f"Username: {data.get('username')}")
            print(f"Default Playmode: {data.get('playmode')}")
            
            groups = data.get('groups', [])
            for g in groups:
                print(f"Group: {g.get('name')} | Modes: {g.get('playmodes')}")
                
            # print all top level keys
            # print(data.keys())
        else:
            print("Failed.")

if __name__ == "__main__":
    test_user_modes()
