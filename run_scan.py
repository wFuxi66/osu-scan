import global_scan
import mapper_scan
import os
from dotenv import load_dotenv

if __name__ == '__main__':
    load_dotenv()
    print("Starting GitHub Actions global scan (full scan)...")

    failed = False

    # Independent scans: a failure in one must not skip the other.
    # Mapper scan first: it takes ~10 min, the BN scan hours — don't let it eat the runner budget.
    for name, fn in (("Mapper scan", mapper_scan.run_mapper_scan), ("BN scan", global_scan.run_global_scan)):
        try:
            fn()
            print(f"{name} finished successfully.")
        except Exception as e:
            print(f"Error during {name}: {e}")
            failed = True

    if failed:
        exit(1)
