from flask import Flask, render_template, request, Response
from dotenv import load_dotenv
load_dotenv()  # Load .env file for local development

import gder_logic
import uuid

app = Flask(__name__)

# Simple in-memory cache for results
RESULTS_CACHE = {}

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

@app.route('/search', methods=['POST'])
def search():
    username = request.form.get('username')
    mode = request.form.get('mode', 'gd')
    
    if not username:
        return render_template('index.html', error="Please enter a username.")
    
    # Run the scan (this blocks until done)
    if mode == 'nominators':
        result = gder_logic.generate_nominator_leaderboard_for_user(username)
        title_prefix = "Nominator"
    else:
        result = gder_logic.generate_leaderboard_for_user(username)
        title_prefix = "Guest Difficulty"
    
    if 'error' in result:
        return render_template('index.html', error=result['error'])
    
    # Cache results for download
    cache_id = str(uuid.uuid4())
    RESULTS_CACHE[cache_id] = {
        'username': result['username'], 
        'leaderboard': result['leaderboard'],
        'title_prefix': title_prefix
    }
        
    return render_template('results.html', 
                           username=result['username'], 
                           leaderboard=result['leaderboard'],
                           title_prefix=title_prefix,
                           cache_id=cache_id)

@app.route('/download/<cache_id>')
def download_report(cache_id):
    data = RESULTS_CACHE.get(cache_id)
    if not data:
        return "Results expired."
        
    html = render_template('results.html', 
                           username=data['username'], 
                           leaderboard=data['leaderboard'],
                           title_prefix=data['title_prefix'],
                           cache_id=None)
                           
    filename = f"leaderboard_{data['username']}_{data.get('title_prefix', 'GD')}.html"
    
    return Response(
        html,
        mimetype="text/html",
        headers={"Content-Disposition": f"attachment;filename={filename}"}
    )

if __name__ == '__main__':
    print("Starting osu!scan...")
    print("Open http://127.0.0.1:5000 in your browser")
    app.run(debug=True, port=5000, threaded=True)
