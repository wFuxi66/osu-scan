# osu! Guest Difficulty Scanner

This tool scans your osu! beatmap sets (Graveyard, Pending, Ranked, Loved) and identifies all Guest Difficulties (GDs), exporting a list of GDers and dates to a CSV file.

## Setup

1.  **Install Python**: Ensure you have Python installed.
2.  **Install Dependencies**:
    Open a terminal in this folder and run:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

1.  **Get API Credentials**:
    - Go to [osu! Account Settings - OAuth](https://osu.ppy.sh/home/account/edit#oauth).
    - Under "OAuth", create a new "New OAuth Application".
    - **Name**: `GD Scanner` (or anything).
    - **Callback URL**: `http://localhost` (doesn't matter for this script).
    - Click **Register**.
    - You will see a `Client ID` (number) and `Client Secret` (long string). **Keep these ready.**

2.  **Run the Script**:
    ```bash
    python scan_gds.py
    ```

3.  **Follow the Prompts**:
    - Enter your Client ID.
    - Enter your Client Secret.
    - Enter your osu! Username.

4.  **View Results**:
    - The script will generate a file named `gd_report_YOUR_USERNAME.csv`.
    - Open this file in Excel, Google Sheets, or any text editor.

## What it collects
- **Mapper Name**: The user who owns the difficulty.
- **Set Title**: The beatmap set.
- **Diff Name**: The name of the difficulty.
- **Date**: The `last_updated` date of the difficulty.
- **Link**: Direct link to the map.
