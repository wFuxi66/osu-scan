from flask import Flask, render_template, request, Response, jsonify
from dotenv import load_dotenv
import threading
import time
import uuid
import gder_logic
from flask_limiter import Limiter

load_dotenv()

app = Flask(__name__)

# Get real IP behind Render's proxy
def get_real_ip():
    # X-Forwarded-For contains: "client_ip, proxy1, proxy2..."
    forwarded = request.headers.get('X-Forwarded-For', '')
    if forwarded:
        # Get the first IP (real client)
        return forwarded.split(',')[0].strip()
    return request.remote_addr or '127.0.0.1'

# Rate Limiter Configuration (per user IP)
limiter = Limiter(
    key_func=get_real_ip,
    app=app,
    default_limits=["2000 per day", "500 per hour"],
    storage_uri="memory://"
)

# JOBS storage: { 'job_id': { 'status', 'message', 'result', 'cancel_event', 'created_at' } }
JOBS = {}
RESULTS_CACHE = {}

# TTL for cleanup (10 minutes)
CACHE_TTL_SECONDS = 600

def cleanup_old_entries():
    """Remove jobs and results older than CACHE_TTL_SECONDS."""
    now = time.time()
    
    # Cleanup old jobs
    old_jobs = [jid for jid, job in JOBS.items() 
                if now - job.get('created_at', now) > CACHE_TTL_SECONDS]
    for jid in old_jobs:
        del JOBS[jid]
    
    # Cleanup old results
    old_results = [cid for cid, result in RESULTS_CACHE.items() 
                   if now - result.get('created_at', now) > CACHE_TTL_SECONDS]
    for cid in old_results:
        del RESULTS_CACHE[cid]
    
    if old_jobs or old_results:
        print(f"Cleanup: removed {len(old_jobs)} jobs, {len(old_results)} cached results")

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

def run_scan_job(job_id, username, mode, cancel_event):
    """Background thread function."""
    def update_progress(msg):
        if job_id in JOBS:
            if cancel_event.is_set():
                JOBS[job_id]['status'] = 'cancelled'
                JOBS[job_id]['message'] = 'Cancelled.'
                return
            JOBS[job_id]['message'] = msg
            
    try:
        if mode == 'nominators':
            result = gder_logic.generate_nominator_leaderboard_for_user(username, progress_callback=update_progress, cancel_event=cancel_event)
            title_prefix = "Nominators"
        elif mode == 'bn':
            result = gder_logic.generate_bn_leaderboard_for_user(username, progress_callback=update_progress, cancel_event=cancel_event)
            title_prefix = "Mappers Nominated by"
        else:
            result = gder_logic.generate_leaderboard_for_user(username, progress_callback=update_progress, cancel_event=cancel_event)
            title_prefix = "Guest Difficulties"
            
        if cancel_event.is_set():
            JOBS[job_id]['status'] = 'cancelled'
            JOBS[job_id]['message'] = 'Scan cancelled by user.'
        elif 'error' in result:
             JOBS[job_id]['status'] = 'error'
             JOBS[job_id]['error'] = result['error']
        else:
            # Done
            RESULTS_CACHE[job_id] = {
                'username': result['username'], 
                'leaderboard': result['leaderboard'],
                'title_prefix': title_prefix,
                'created_at': time.time()
            }
            JOBS[job_id]['status'] = 'done'
            JOBS[job_id]['result_id'] = job_id
            
    except Exception as e:
        JOBS[job_id]['status'] = 'error'
        JOBS[job_id]['error'] = str(e)

@app.route('/api/start_scan', methods=['POST'])
@limiter.limit("30 per minute") # Max 30 scans per minute per IP
def start_scan():
    # Run cleanup before starting new scan
    cleanup_old_entries()
    
    username = request.form.get('username')
    mode = request.form.get('mode', 'gd')
    
    if not username:
        return jsonify({'error': 'Username required'}), 400
        
    job_id = str(uuid.uuid4())
    cancel_event = threading.Event()
    
    JOBS[job_id] = {
        'status': 'running', 
        'message': 'Starting...',
        'cancel_event': cancel_event,
        'created_at': time.time()
    }
    
    # Start background thread
    thread = threading.Thread(target=run_scan_job, args=(job_id, username, mode, cancel_event))
    thread.start()
    
    return jsonify({'job_id': job_id})

@app.route('/api/cancel_scan/<job_id>', methods=['POST'])
def cancel_scan(job_id):
    job = JOBS.get(job_id)
    if job and 'cancel_event' in job:
        job['cancel_event'].set()
        job['status'] = 'cancelled'
        job['message'] = 'Cancelling...'
        return jsonify({'status': 'cancelled'})
    return jsonify({'error': 'Job not found'}), 404

@app.route('/api/status/<job_id>')
@limiter.exempt  # Status polling must not be rate-limited
def job_status(job_id):
    job = JOBS.get(job_id)
    if not job:
        return jsonify({'status': 'unknown'}), 404
    
    # Return a safe copy without non-serializable objects (like threading.Event)
    safe_job = {k: v for k, v in job.items() if k != 'cancel_event'}
    return jsonify(safe_job)

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
