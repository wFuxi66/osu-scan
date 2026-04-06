#!/usr/bin/env python
"""
Deep test: check how many of Larto's 921 nominated sets still have current_nominations
"""
import os
import sys

os.environ['OSU_CLIENT_ID'] = '47165'
os.environ['OSU_CLIENT_SECRET'] = 'REDACTED_OSU_SECRET'

import scan_logic
import global_scan
import time

def main():
    token = scan_logic.get_token()
    if not token:
        print("[FAIL] Auth failed")
        sys.exit(1)

    user_id, username = scan_logic.get_user_id('Larto', token)
    print("Found user: %s (ID: %s)\n" % (username, user_id))

    print("Fetching Larto's %d nominated sets..." % 921)
    sets = global_scan.fetch_bn_nominations(user_id, token)
    print("[OK] Fetched %d sets\n" % len(sets))

    print("=" * 60)
    print("Deep-fetching ALL sets to count current_nominations")
    print("=" * 60)

    total_sets = len(sets)
    sets_with_noms = 0
    sets_without_noms = 0
    total_larto_noms = 0

    for i, bset in enumerate(sets):
        if (i + 1) % 100 == 0:
            print("Progress: %d/%d sets..." % (i + 1, total_sets))

        set_id = bset['id']
        full_set = global_scan.deep_fetch_set(set_id, token)

        if full_set:
            noms = full_set.get('current_nominations', [])
            if noms:
                sets_with_noms += 1
                # Count Larto's nominations in this set
                for nom in noms:
                    if nom.get('user_id') == user_id:
                        total_larto_noms += 1
            else:
                sets_without_noms += 1
        else:
            sets_without_noms += 1

        time.sleep(0.01)

    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print("Total sets fetched: %d" % total_sets)
    print("Sets with current_nominations: %d" % sets_with_noms)
    print("Sets WITHOUT current_nominations: %d" % sets_without_noms)
    print("\nTotal Larto nominations found: %d" % total_larto_noms)
    print("Expected (from osu!): 921")
    print("Difference: %d" % (921 - total_larto_noms))

if __name__ == '__main__':
    main()
