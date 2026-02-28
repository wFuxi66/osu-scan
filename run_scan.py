import global_scan
import os
from dotenv import load_dotenv

if __name__ == '__main__':
    load_dotenv()
    print("Starting GitHub Actions global scan...")
    
    # Fetch existing data to know if we can do an incremental scan
    existing = global_scan.load_from_firebase()
    since_date = existing.get('last_scan') if existing else None
    
    if since_date:
        print(f"Running incremental scan since {since_date}")
    else:
        print("Running full scan")
        
    try:
        global_scan.run_global_scan(since_date=since_date)
        print("Scan finished successfully.")
    except Exception as e:
        print(f"Error during scan: {e}")
        exit(1)
