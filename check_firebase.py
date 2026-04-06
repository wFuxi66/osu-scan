#!/usr/bin/env python
"""
Check Firebase leaderboard data to see last_scan date and scan metadata
"""
import os
import json

os.environ['OSU_CLIENT_ID'] = '47165'
os.environ['OSU_CLIENT_SECRET'] = 'REDACTED_OSU_SECRET'
os.environ['FIREBASE_URL'] = 'https://osu-scan-default-rtdb.firebaseio.com'
os.environ['FIREBASE_SECRET'] = 'REDACTED_FIREBASE_SECRET'

import global_scan

data = global_scan.load_from_firebase()

if not data:
    print("No data in Firebase")
else:
    print("=" * 60)
    print("FIREBASE LEADERBOARD METADATA")
    print("=" * 60)
    print("Last scan: %s" % data.get('last_scan', 'N/A'))
    print("Total BNs scanned: %s" % data.get('total_bns_scanned', 'N/A'))
    print("Total sets scanned: %s" % data.get('total_sets_scanned', 'N/A'))

    print("\n" + "=" * 60)
    print("TOP BNS (first 10)")
    print("=" * 60)
    top_bns = data.get('top_bns', [])
    for i, bn in enumerate(top_bns[:10], 1):
        print("%2d. %s: %d nominations" % (i, bn.get('username', 'Unknown'), bn.get('total', 0)))

    # Find Larto's ranking
    larto_rank = None
    for i, bn in enumerate(top_bns, 1):
        if bn.get('username') == 'Larto':
            larto_rank = i
            larto_count = bn.get('total', 0)
            break

    if larto_rank:
        print("\n[FOUND] Larto is ranked #%d with %d nominations" % (larto_rank, larto_count))
    else:
        print("\n[NOT FOUND] Larto not in top BNs list")
