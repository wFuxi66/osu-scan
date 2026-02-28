# osu!scan

osu!scan is a web-based analytical tool designed for osu! mappers to evaluate their beatmap and nomination history using the osu! API v2.

**Live Instance:** [https://osu-scan.onrender.com](https://osu-scan.onrender.com)

## Core Capabilities

### Guest Difficulty Analysis
Identifies contributors for guest difficulties within a user's beatmap sets.
- Detects collaborations through deep-set inspection.
- Provides a comprehensive leaderboard of contributors.

### Guest Difficulty Host Analytics
Analyzes a user's contributions to other mappers' sets.
- Tracks which hosts a user has collaborated with most frequently.
- Aggregates contribution data into a ranked leaderboard.

### Nominator Analysis
Identifies the Beatmap Nominators who have most frequently nominated a user's mapsets.
- Useful for tracking nomination history and identifying consistent supporters.

### Beatmap Nominator Analytics
Evaluates a specific Beatmap Nominator's activity.
- Lists mappers most frequently nominated by a specific BN.
- Provides insight into a nominator's stylistic preferences and active collaborations.

## System Features

### High Performance
Utilizes multi-threaded parallel processing to analyze extensive datasets efficiently.

### Intelligent Caching
Implements a caching layer for usernames and scan results to minimize API overhead and improve response times.

### Data Portability
Supports exporting analysis results as standalone HTML reports for offline viewing and archival purposes.

## Local Development

### Prerequisites
- Python 3.8+
- osu! API v2 Client Credentials

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/wFuxi66/osu-scan.git
   cd osu-scan
   ```

2. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Environment Configuration:
   Create a `.env` file in the root directory with your osu! API credentials:
   ```env
   OSU_CLIENT_ID=your_id_here
   OSU_CLIENT_SECRET=your_secret_here
   ```

4. Execution:
   ```bash
   python app.py
   ```
   The application will be accessible at `http://127.0.0.1:5000`.

## Technical Stack
- **Backend**: Python, Flask, Concurrent Futures
- **API**: osu! API v2 (OAuth2)
- **Frontend**: HTML5, CSS3
- **Deployment**: Render
