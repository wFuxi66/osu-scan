# osu!scan

**osu!scan** is a simple web tool to analyze your osu! beatmap contributions.

**Live Site:** [https://osu-scan.onrender.com](https://osu-scan.onrender.com)

## Features

- **Guest Difficulty Scanner**: Finds every map you have guest difficultied for other people.
- **Nominator Scanner**: Finds which Beatmap Nominators (BNs) have nominated your sets the most.
- **Leaderboard**: Displays a clean ranking of your top collaborators.
- **Download**: Export the results as an HTML file.

## How it works

1. Enter a generic osu! username or ID.
2. The scan runs in real-time (fetching data from osu! API v2).
3. View the results directly on the web page.

## Running Locally

If you want to run this code on your own machine:

1. Install Python 3.10+
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Create a `.env` file with your osu! API credentials (see `.env.example`).
4. Run the app:
   ```bash
   python app.py
   ```
5. Open `http://127.0.0.1:5000`

## Technologies

- **Python (Flask)**
- **osu! API v2**
- **HTML/CSS (Simple UI)**
- Hosted on **Render**

---
*Created by Fuxi66*
