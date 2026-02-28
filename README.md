# osu!scan

Analytical tool for osu! mappers to evaluate beatmap and nomination history.

**Live instance:** [https://osu-scan.onrender.com](https://osu-scan.onrender.com)

## Features
- Guest difficulty analysis (for users and hosts)
- Beatmap Nominator activity and history tracking
- Multi-threaded processing and result caching
- Exportable HTML reports

## Setup
1. Install dependencies: `pip install -r requirements.txt`
2. Configure environment: Create `.env` with `OSU_CLIENT_ID` and `OSU_CLIENT_SECRET`
3. Run: `python app.py`

## Technical Stack
Python, Flask, Concurrent Futures, osu! API v2.
