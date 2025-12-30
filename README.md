# osu!scan

**osu!scan** is a simple web tool to analyze your osu! beatmap contributions.

**Live Site:** [https://osu-scan.onrender.com](https://osu-scan.onrender.com)

## Features

- **Guest Difficulty Scanner**: Finds every map you have guest difficultied (including hidden Collabs!).
- **Nominator Scanner**: Finds which Beatmap Nominators (BNs) have nominated your sets.
- **Deep Scan**: Checks every single set in detail to ensure 100% accuracy.

## Running Locally

If you want to run this code on your own machine:

1. **Clone the repository:**
   ```bash
   git clone https://github.com/wFuxi66/osu-scan.git
   cd osu-scan
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Environment:**
   - Create a `.env` file in the folder.
   - Add your osu! API credentials (get them [here](https://osu.ppy.sh/home/account/edit#oauth)):
     ```
     OSU_CLIENT_ID=your_id
     OSU_CLIENT_SECRET=your_secret
     ```

4. **Run the app:**
   ```bash
   python app.py
   ```
5. Open `http://127.0.0.1:5000`

## Technologies

- **Python (Flask)**
- **osu! API v2**
- **HTML/CSS (Simple UI)**
- Hosted on **Render** (Auto-deploys from GitHub)

---
*Made with ❤️ by Fuxi66*
