#!/usr/bin/env python
"""
Quick sample test: check 50 random Larto nominated sets
"""
import os
import sys
import random

os.environ['OSU_CLIENT_ID'] = '47165'
os.environ['OSU_CLIENT_SECRET'] = 'REDACTED_OSU_SECRET'

import scan_logic
import global_scan

def main():
    token = scan_logic.get_token()
    user_id, username = scan_logic.get_user_id('Larto', token)

    print("Fetching Larto's nominated sets...")
    sets = global_scan.fetch_bn_nominations(user_id, token)
    print("[OK] Fetched %d sets\n" % len(sets))

    print("=" * 60)
    print("Sampling 50 random sets for deep-fetch analysis")
    print("=" * 60)

    sample = random.sample(sets, min(50, len(sets)))
    sets_with_noms = 0
    sets_without_noms = 0

    for i, bset in enumerate(sample, 1):
        set_id = bset['id']
        full_set = global_scan.deep_fetch_set(set_id, token)

        if full_set:
            noms = full_set.get('current_nominations', [])
            has_noms = len(noms) > 0
            sets_with_noms += has_noms
            sets_without_noms += (1 - has_noms)

            status = "HAS NOMS (%d)" % len(noms) if has_noms else "NO NOMS"
            print("%2d. Set %d (%s): %s" % (i, set_id, bset.get('title', '?')[:30], status))
        else:
            sets_without_noms += 1

    print("\n" + "=" * 60)
    print("SAMPLE RESULTS (50 random sets)")
    print("=" * 60)
    print("Sets WITH current_nominations: %d (%.1f%%)" % (sets_with_noms, sets_with_noms*100.0/50))
    print("Sets WITHOUT current_nominations: %d (%.1f%%)" % (sets_without_noms, sets_without_noms*100.0/50))

    if sets_with_noms > 0:
        estimated_total = int(sets_with_noms * len(sets) / 50.0)
        print("\nEstimated sets with nominations: ~%d out of %d" % (estimated_total, len(sets)))
        print("Estimated missing nominations: ~%d" % (len(sets) - estimated_total))

if __name__ == '__main__':
    main()
