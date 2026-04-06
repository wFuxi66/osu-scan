#!/usr/bin/env python
"""
Check how many of Larto's sets would be filtered by the last_scan date
"""
import os
from datetime import datetime

os.environ['OSU_CLIENT_ID'] = '47165'
os.environ['OSU_CLIENT_SECRET'] = 'REDACTED_OSU_SECRET'
os.environ['FIREBASE_URL'] = 'https://osu-scan-default-rtdb.firebaseio.com'
os.environ['FIREBASE_SECRET'] = 'REDACTED_FIREBASE_SECRET'

import scan_logic
import global_scan

token = scan_logic.get_token()
user_id, username = scan_logic.get_user_id('Larto', token)

print("Fetching Larto's %d nominated sets..." % 921)
sets = global_scan.fetch_bn_nominations(user_id, token)
print("[OK] Fetched %d sets\n" % len(sets))

# Load Firebase to get last_scan date
data = global_scan.load_from_firebase()
if not data:
    print("No Firebase data")
    exit(1)

last_scan = data.get('last_scan', '')
print("Last scan timestamp: %s" % last_scan)
print("Filtering sets by: ranked_date > %s\n" % last_scan)

print("=" * 60)
print("Checking which sets would be filtered")
print("=" * 60)

filtered_out = 0
kept = 0
old_sets = []

for bset in sets:
    ranked_date = (bset.get('ranked_date') or '').split('T')[0]  # Get just the date part

    # The comparison in global_scan is: (s.get('ranked_date') or '') > since_date
    # since_date would be something like '2026-04-01' from last_scan
    since_date_str = last_scan.split('T')[0]  # Just date part

    if ranked_date > since_date_str:
        kept += 1
    else:
        filtered_out += 1
        if len(old_sets) < 10:  # Store first 10 for display
            old_sets.append({
                'id': bset['id'],
                'title': bset.get('title', 'Unknown'),
                'ranked_date': ranked_date
            })

print("\nSets kept: %d" % kept)
print("Sets filtered out (ranked before %s): %d" % (since_date_str, filtered_out))
print("Difference (921 - %d = %d)" % (kept, 921 - kept))

if old_sets:
    print("\nExample old sets that would be filtered:")
    for s in old_sets:
        print("  - %s (ID: %d, ranked: %s)" % (s['title'][:40], s['id'], s['ranked_date']))
