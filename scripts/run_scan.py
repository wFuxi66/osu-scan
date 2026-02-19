import os
import sys

# Add parent directory to path so we can import app/gder_logic
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gder_logic import global_bn_duo_scan

def main():
    print("Starting Global BN Duo Scan via CLI...")
    
    # Check for required env vars
    if not os.environ.get('OSU_CLIENT_ID') or not os.environ.get('OSU_CLIENT_SECRET'):
        print("Error: OSU_CLIENT_ID and OSU_CLIENT_SECRET must be set.")
        sys.exit(1)

    # Run the scan
    # We pass a simple print as the progress callback
    result = global_bn_duo_scan(progress_callback=print)
    
    if 'error' in result:
        print(f"Scan failed: {result['error']}")
        sys.exit(1)
    
    print("Scan finished successfully.")

if __name__ == "__main__":
    main()
