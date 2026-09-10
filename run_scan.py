"""Entry point for the scheduled scans. `python run_scan.py [mappers|bn]`, default both."""
import sys

from dotenv import load_dotenv

# Before importing anything that reads them: global_scan copies FIREBASE_URL and
# FIREBASE_NS into module constants at import time, so loading .env afterwards leaves a
# local run writing nothing and a namespaced run writing over the live data.
load_dotenv()

import global_scan
import mapper_scan

SCANS = {
    # Mapper scan first: it is the resumable one, and the BN scan runs for hours.
    'mappers': ("Mapper scan", mapper_scan.run_mapper_scan),
    'bn': ("BN scan", global_scan.run_global_scan),
}


def main(names):
    failed = False
    for name in names:
        label, fn = SCANS[name]
        print(f"Starting {label}...")
        try:
            result = fn()
            if isinstance(result, dict) and result.get('error'):
                # An incomplete mapper scan keeps its checkpoint; a retry continues it.
                print(f"{label} did not finish: {result['error']}")
                failed = True
            else:
                print(f"{label} finished successfully.")
        except Exception as e:
            print(f"Error during {label}: {e}")
            failed = True
    return 1 if failed else 0


if __name__ == '__main__':
    requested = sys.argv[1:] or list(SCANS)
    unknown = [n for n in requested if n not in SCANS]
    if unknown:
        sys.exit(f"Unknown scan(s): {', '.join(unknown)}. Choose from: {', '.join(SCANS)}")
    sys.exit(main(requested))
