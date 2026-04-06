#!/usr/bin/env python
"""
Check Larto's detailed breakdown in Firebase
"""
import os
import json

os.environ['FIREBASE_URL'] = 'https://osu-scan-default-rtdb.firebaseio.com'
os.environ['FIREBASE_SECRET'] = 'REDACTED_FIREBASE_SECRET'

import global_scan

data = global_scan.load_from_firebase()

if not data:
    print("No Firebase data")
    exit(1)

# Find Larto's full entry
top_bns = data.get('top_bns', [])
larto_entry = None

for bn in top_bns:
    if bn.get('username') == 'Larto':
        larto_entry = bn
        break

if not larto_entry:
    print("Larto not found in top_bns")
    exit(1)

print("=" * 60)
print("LARTO'S FULL BREAKDOWN")
print("=" * 60)
print("Username: %s" % larto_entry.get('username'))
print("Total: %d" % larto_entry.get('total'))
print("Is Current BN: %s" % larto_entry.get('is_current'))

print("\nBy mode:")
by_mode = larto_entry.get('by_mode', {})
for mode, count in sorted(by_mode.items()):
    print("  %s: %d" % (mode, count))

print("\nSum of by_mode: %d" % sum(by_mode.values()))

# Also check some other BNs to see the pattern
print("\n" + "=" * 60)
print("COMPARING WITH OTHER BNs")
print("=" * 60)
for bn in top_bns[:5]:
    username = bn.get('username')
    total = bn.get('total')
    by_mode = bn.get('by_mode', {})
    mode_sum = sum(by_mode.values())
    print("%s: total=%d, by_mode_sum=%d, modes=%s" % (username, total, mode_sum, list(by_mode.keys())))
