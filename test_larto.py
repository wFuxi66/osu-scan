#!/usr/bin/env python
"""
Quick test script to debug Larto's nomination data.
"""
import os
import sys
import json

os.environ['OSU_CLIENT_ID'] = '47165'
os.environ['OSU_CLIENT_SECRET'] = 'REDACTED_OSU_SECRET'

import scan_logic
import global_scan

def main():
    token = scan_logic.get_token()
    if not token:
        print("[FAIL] Auth failed")
        sys.exit(1)

    print("[OK] Authenticated\n")

    user_id, username = scan_logic.get_user_id('Larto', token)
    if not user_id:
        print("[FAIL] User Larto not found")
        sys.exit(1)

    print("Found user: %s (ID: %s)\n" % (username, user_id))

    print("Fetching Larto's nominated sets...")
    sets = global_scan.fetch_bn_nominations(user_id, token)
    print("[OK] Fetched %d nominated sets\n" % len(sets))

    print("=" * 60)
    print("Inspecting current_nominations structure")
    print("=" * 60)

    total_noms = 0
    total_missing_user_id = 0

    for i in range(min(5, len(sets))):
        bset = sets[i]
        print("\n--- Set %d: %s (ID: %d) ---" % (i+1, bset.get('title', 'Unknown'), bset['id']))

        full_set = global_scan.deep_fetch_set(bset['id'], token)
        if full_set:
            noms = full_set.get('current_nominations', [])
            print("current_nominations count: %d" % len(noms))
            total_noms += len(noms)

            if noms:
                print("\nFirst nomination structure:")
                print(json.dumps(noms[0], indent=2, default=str))

                first = noms[0]
                print("\nKey fields:")
                print("  - user_id: %s" % first.get('user_id'))
                print("  - mode: %s" % first.get('mode'))
                print("  - rulesets: %s" % first.get('rulesets'))
                print("  - user: %s" % first.get('user'))

                for nom in noms:
                    if not nom.get('user_id'):
                        total_missing_user_id += 1
        else:
            print("[FAIL] Could not deep-fetch")

    print("\n" + "=" * 60)
    print("SUMMARY FROM FIRST 5 SETS")
    print("=" * 60)
    print("Total nominations: %d" % total_noms)
    print("Missing user_id: %d" % total_missing_user_id)
    print("Valid user_id: %d" % (total_noms - total_missing_user_id))

    if total_missing_user_id > 0:
        print("\n[WARN] Found %d nominations missing user_id!" % total_missing_user_id)
        print("This could explain the discrepancy!")
    else:
        print("\n[OK] All nominations have user_id")

if __name__ == '__main__':
    main()
