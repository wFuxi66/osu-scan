# osu!scan

A web tool for osu! mappers to analyze their beatmap nominations and guest difficulties.

**Live:** [https://osu-scan.onrender.com](https://osu-scan.onrender.com)

## Features

- **GD Scanner** — Find who made the most guest difficulties in your sets. Detects collaborations and recursively checks beatmaps where the set owner might not be the creator.
- **GD Hosts Scanner** — Find which mapper you made the most GD for.
- **Nominator Scanner** — Find which BN nominated your maps the most.
- **BN Scanner** — Enter a BN's username to see which mappers they nominated the most.
- **BN Duo Leaderboard** — Global monthly leaderboard of BN pairs who co-nominated the most maps, available at `/bn-duos`.

## Running Locally

1. Clone the repository:
    ```bash
    git clone https://github.com/wFuxi66/osu-scan.git
    cd osu-scan
    ```

2. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3. Create a `.env` file with your osu! API credentials ([get them here](https://osu.ppy.sh/home/account/edit#oauth)):
    ```env
    OSU_CLIENT_ID=your_id
    OSU_CLIENT_SECRET=your_secret
    ```

4. Run the app:
    ```bash
    gunicorn app:app --workers 1 --threads 8
    ```
    Or `python app.py` for development.

## Technologies

- Python, Flask, Gunicorn, APScheduler
- osu! API v2 (OAuth Client Credentials)
- HTML, CSS

---
*Made by [Fuxi66](https://osu.ppy.sh/users/24230576)*
