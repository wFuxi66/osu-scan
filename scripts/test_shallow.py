import os
import sys
import requests
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scan_logic import get_token, API_BASE

def test():
    token = get_token()
    headers = {'Authorization': f'Bearer {token}'}
    # Andrea
    url = f'{API_BASE}/users/33599/beatmapsets/nominated'
    r = requests.get(url, headers=headers, params={'limit': 1, 'offset': 0})
    data = r.json()
    bset = data[0]
    print(bset.keys())
    if 'beatmaps' in bset:
        print("YES! Beatmaps included in shallow fetch.")
        print(f"Modes in beatmaps: {set(b['mode'] for b in bset['beatmaps'])}")
    else:
        print("NO beatmaps embedded in shallow fetch.")

if __name__ == "__main__":
    test()
