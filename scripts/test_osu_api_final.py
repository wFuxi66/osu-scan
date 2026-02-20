import os
import sys
import requests

# Add parent directory to path so we can import app/scan_logic
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scan_logic import get_token, get_user_id, get_nominated_beatmapsets, get_guest_beatmapsets, get_beatmapsets

def test_api():
    token = get_token()
    if not token:
        print("No token!")
        return
        
    user_id_andrea, _ = get_user_id("Andrea", token)
    user_id_aki, _ = get_user_id("Akitoshi", token)
    
    print(f"Testing Andrea ({user_id_andrea}) nominated maps...")
    nom_sets = get_nominated_beatmapsets(user_id_andrea, token)
    print(f"Andrea nominated maps: {len(nom_sets)}")
    
    print(f"Testing Andrea ({user_id_andrea}) ranked maps...")
    ranked_sets = get_beatmapsets(user_id_andrea, token)
    # The returned list contains both ranked_and_approved and loved. Filter to ranked_and_approved.
    ranked_only = [s for s in ranked_sets if s.get('status_category') == 'ranked_and_approved']
    print(f"Andrea ranked maps: {len(ranked_only)}")
    
    print(f"Testing Akitoshi ({user_id_aki}) guest maps...")
    guest_sets = get_guest_beatmapsets(user_id_aki, token)
    print(f"Akitoshi guest maps: {len(guest_sets)}")

if __name__ == "__main__":
    test_api()
