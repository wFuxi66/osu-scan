import global_scan
import os
from dotenv import load_dotenv

if __name__ == '__main__':
    load_dotenv()
    print("Starting GitHub Actions global scan (full scan)...")

    try:
        global_scan.run_global_scan()
        print("Scan finished successfully.")
    except Exception as e:
        print(f"Error during scan: {e}")
        exit(1)
