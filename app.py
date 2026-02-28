from flask import Flask, render_template, request, Response, jsonify
from dotenv import load_dotenv
import threading
import time
import uuid
import gder_logic

load_dotenv()

app = Flask(__name__)

# JOBS storage: { 'job_id': { 'status': 'running', 'message': '...', 'result': ... } }
JOBS = {}
RESULTS_CACHE = {}

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

def run_scan_job(job_id, username, mode):
    """Background thread function."""
    def update_progress(msg):
        if job_id in JOBS:
            JOBS[job_id]['message'] = msg
            
    try:
        if mode == 'nominators':
            result = gder_logic.generate_nominator_leaderboard_for_user(username, progress_callback=update_progress)
            title_prefix = "Nominator"
        else:
            result = gder_logic.generate_leaderboard_for_user(username, progress_callback=update_progress)
            title_prefix = "Guest Difficulty"
            
        if 'error' in result:
             JOBS[job_id]['status'] = 'error'
             JOBS[job_id]['error'] = result['error']
        else:
            # Done
            RESULTS_CACHE[job_id] = {
                'username': result['username'], 
                'leaderboard': result['leaderboard'],
                'title_prefix': title_prefix
            }
            JOBS[job_id]['status'] = 'done'
            JOBS[job_id]['result_id'] = job_id
            
    except Exception as e:
        JOBS[job_id]['status'] = 'error'
        JOBS[job_id]['error'] = str(e)

@app.route('/api/start_scan', methods=['POST'])
def start_scan():
    username = request.form.get('username')
    mode = request.form.get('mode', 'gd')
    
    if not username:
        return jsonify({'error': 'Username required'}), 400
        
    job_id = str(uuid.uuid4())
    JOBS[job_id] = {'status': 'running', 'message': 'Starting...'}
    
    # Start background thread
    thread = threading.Thread(target=run_scan_job, args=(job_id, username, mode))
    thread.start()
    
    return jsonify({'job_id': job_id})

@app.route('/api/status/<job_id>')
def job_status(job_id):
    job = JOBS.get(job_id)
    if not job:
        return jsonify({'status': 'unknown'}), 404
    return jsonify(job)

@app.route('/results_view/<cache_id>')
def results_view(cache_id):
    data = RESULTS_CACHE.get(cache_id)
    if not data:
        return "Results expired or not found. <a href='/'>Go Back</a>"
    return render_template('results.html', 
                           username=data['username'], 
                           leaderboard=data['leaderboard'],
                           title_prefix=data['title_prefix'],
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
