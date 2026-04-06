#!/usr/bin/env python
"""
Quick test script to debug Larto's nomination data.
"""
import os
from dotenv import load_dotenv
import scan_logic
import global_scan
import json

load_dotenv()

# Get Larto's info
token = scan_logic.get_token()
if not token:
    print("Auth failed")
    exit(1)

# Fetch Larto by username
user_id, username = scan_logic.get_user_id('Larto', token)
print(f"Found user: {username} (ID: {user_id})")

# Fetch all sets nominated by Larto
print("\nFetching Larto's nominated sets...")
sets = global_scan.fetch_bn_nominations(user_id, token)
print(f"Fetched {len(sets)} nominated sets")

# Deep-fetch first 5 sets to inspect current_nominations structure
print("\nInspecting current_nominations structure...")
for i, bset in enumerate(sets[:5]):
    print(f"\n--- Set {i+1}: {bset.get('title', 'Unknown')} (ID: {bset['id']}) ---")
    full_set = global_scan.deep_fetch_set(bset['id'], token)
    if full_set:
        noms = full_set.get('current_nominations', [])
        print(f"current_nominations count: {len(noms)}")
        if noms:
            print(f"First nomination structure: {json.dumps(noms[0], indent=2, default=str)}")
            # Check for common fields
            first = noms[0]
            print(f"  - user_id: {first.get('user_id')}")
            print(f"  - mode: {first.get('mode')}")
            print(f"  - rulesets: {first.get('rulesets')}")
            print(f"  - user (nested): {first.get('user')}")
    else:
        print("Failed to deep-fetch set")

# Count manually from first 5 sets
print("\n--- Manual count from first 5 sets ---")
total_noms = 0
missing_user_id = 0
for bset in sets[:5]:
    full_set = global_scan.deep_fetch_set(bset['id'], token)
    if full_set:
        noms = full_set.get('current_nominations', [])
        total_noms += len(noms)
        for nom in noms:
            if not nom.get('user_id'):
                missing_user_id += 1
                print(f"  Missing user_id in: {nom}")

print(f"Total noms in first 5 sets: {total_noms}")
print(f"Noms missing user_id: {missing_user_id}")
