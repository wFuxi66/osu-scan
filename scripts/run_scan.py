import os
import sys
import logging

# Add parent directory to path so we can import scan modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from monthly_scan import run_monthly_scan

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting monthly scan via CLI...")

    # Check for required env vars
    if not os.environ.get('OSU_CLIENT_ID') or not os.environ.get('OSU_CLIENT_SECRET'):
        logger.error("OSU_CLIENT_ID and OSU_CLIENT_SECRET must be set.")
        sys.exit(1)

    result = run_monthly_scan(progress_callback=lambda msg: logger.info(msg))

    if 'error' in result:
        logger.error("Scan failed: %s", result['error'])
        sys.exit(1)

    logger.info("Scan finished successfully.")

if __name__ == "__main__":
    main()
