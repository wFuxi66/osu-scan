import os
import sys
import json
import requests

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scan_logic import get_token, API_BASE, get_session

def check_nominations():
    token = get_token()
    headers = {'Authorization': f'Bearer {token}'}
    session = get_session()
    
    # Let's get a recent nominated mapset from Sotarks (4452992)
    url = f'{API_BASE}/users/4452992/beatmapsets/nominated'
    r = session.get(url, headers=headers, params={'limit': 1, 'offset': 0})
    if r.status_code == 200:
        data = r.json()
        if data:
            bset_id = data[0]['id']
            print(f"Checking Set ID: {bset_id}")
            
            r2 = session.get(f'{API_BASE}/beatmapsets/{bset_id}', headers=headers)
            if r2.status_code == 200:
                full_set = r2.json()
                noms = full_set.get('current_nominations', [])
                print(json.dumps(noms, indent=2))
            else:
                print("Failed deep fetch")
        else:
            print("No nominated sets found")
            

if __name__ == "__main__":
    check_nominations()
