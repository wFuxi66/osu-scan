# osu!scan

Analytical tool for osu! mappers to evaluate beatmap and nomination history.

**Live instance:** [https://osu-scan.onrender.com](https://osu-scan.onrender.com)

## Features
- Guest difficulty analysis (for users and hosts)
- Beatmap Nominator activity and history tracking
- Global BN Leaderboards with automated monthly scanning
- Multi-threaded processing and result caching
- Exportable HTML reports

## Setup
1. Install dependencies: `pip install -r requirements.txt`
2. Configure environment: Create `.env` based on `.env.example`:
   - `OSU_CLIENT_ID` & `OSU_CLIENT_SECRET`: osu! API credentials
   - `FIREBASE_URL` & `FIREBASE_SECRET`: Realtime database
   - `SCAN_SECRET`: Key for manual scan triggers
3. Run: `python app.py`

## Technical Stack
Python, Flask, Concurrent Futures, osu! API v2, Firebase Realtime Database.
