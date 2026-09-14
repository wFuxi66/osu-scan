# osu!scan

Analytical tool for osu! mappers to evaluate beatmap and nomination history.

**Live instance:** [https://osu-scan.onrender.com](https://osu-scan.onrender.com)

## Features
- Guest difficulty analysis (for users and hosts)
- Beatmap Nominator activity and history tracking
- Leaderboards, all with a per-mode filter:
  - Nominators, by nominations
  - Nominator duos, by mapsets nominated together
  - Mappers, by all-time difficulty playcount (loved on/off)
  - Mappers, by mapsets (loved on/off)
  - Mappers, by favourites on the sets they hosted (loved on/off)
- Automated scans feeding the leaderboards: nominators nightly,
  mappers twice a week
- Multi-threaded processing and result caching

## Setup
1. Install dependencies: `pip install -r requirements.txt`
2. Configure environment: Create `.env` based on `.env.example`:
   - `OSU_CLIENT_ID` & `OSU_CLIENT_SECRET`: osu! API credentials
   - `FIREBASE_URL` & `FIREBASE_SECRET`: Realtime database
3. Run: `python app.py`

## Technical Stack
Python, Flask, Concurrent Futures, osu! API v2, Firebase Realtime Database.
