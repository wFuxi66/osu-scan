import os
import sys
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scan_logic import generate_bn_leaderboard_for_user

def test():
    # Test Sotarks (4452992), he was a standard BN so any hybrid maps he nominates shouldn't give him Taiko points inside the leaderboard
    print("Fetching Sotarks BN Leaderboard...")
    res = generate_bn_leaderboard_for_user('4452992')
    
    if 'error' in res:
        print("Error:", res['error'])
        return
        
    lb = res.get('leaderboard', [])
    print(f"Total entries: {len(lb)}")
    if len(lb) > 0:
        print("Sample Entry:")
        print(json.dumps(lb[0], indent=2))
        
        # Verify mode_counts structure is present
        assert 'mode_counts' in lb[0], "mode_counts not found in entry!"
        assert 'modes' in lb[0], "modes not found in entry!"
        
    print("Test passed.")

if __name__ == '__main__':
    test()
