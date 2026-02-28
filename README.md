# osu!scan

**osu!scan** is a powerful web tool designed to help osu! mappers and players analyze their beatmap history.

**Live Site:** [https://osu-scan.onrender.com](https://osu-scan.onrender.com)

## Features

### 🔍 Guest Difficulty Scanner
Find who made the most GD in your sets.
-   Detects **Collaborations** hidden in the description.
-   Recursively checks beatmaps where the set owner might not be the creator.

### 🎯 GD Hosts Scanner (New!)
Find which mapper you made the most GD for.
-   See a leaderboard of mappers whose sets you contributed to.
-   Perfect for seeing who you collaborate with the most!

### 🏆 Nominator Scanner
Find which BN nominated your maps the most.
-   Generates a leaderboard of your "Top Nominators".
-   See who has nominated the most of your mapsets.

### 🕵️‍♂️ BN Scanner
Find which mappers a BN nominated the most.
-   Enter a BN's username to see **every map they have nominated**.
-   View a leaderboard of which mappers they nominate the most.
-   Perfect for finding BNs who might be interested in your style.

### ⚡ Smart & Fast
-   **Parallel Processing**: Uses multi-threading to scan thousands of maps in seconds.
-   **Smart Caching**: Remembers usernames to speed up repeat scans.
-   **Exportable**: Download your results as an HTML file to keep forever.

---

## Running Locally

If you want to run this code on your own machine:

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/wFuxi66/osu-scan.git
    cd osu-scan
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configure Environment:**
    -   Create a `.env` file in the root folder.
    -   Add your osu! API credentials (get them [here](https://osu.ppy.sh/home/account/edit#oauth)):
        ```env
        OSU_CLIENT_ID=your_id
        OSU_CLIENT_SECRET=your_secret
        ```

4.  **Run the app:**
    ```bash
    # Run with Gunicorn (Recommended for stability like production)
    gunicorn app:app --workers 1 --threads 8
    
    # Or plain Python for debugging
    python app.py
    ```

5.  Open `http://127.0.0.1:8000` (or `5000` if using `python app.py`)

## Technologies

-   **Backend**: Python (Flask), Gunicorn, Threading
-   **Data**: osu! API v2 (OAuth Client Credentials)
-   **Frontend**: HTML5, CSS3 (Responsive Design)
-   **Deployment**: Render (Auto-deploy from GitHub)

---
*Made with ❤️ by [Fuxi66](https://osu.ppy.sh/users/24230576)*
