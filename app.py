from flask import Flask, render_template, request, Response, jsonify, redirect, url_for
from dotenv import load_dotenv
import threading
import time
import uuid
import os
import scan_logic
from flask_limiter import Limiter
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

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

# Scan result cache: { 'username_lower:mode': { 'result': {...}, 'created_at': timestamp } }
# This caches actual scan results to avoid re-scanning same user
SCAN_CACHE = {}
SCAN_CACHE_TTL = 1800  # 30 minutes - cached scans stay valid this long

# TTL for cleanup (10 minutes for jobs/results view)
CACHE_TTL_SECONDS = 600

def cleanup_old_entries():
    """Remove jobs and results older than their TTLs."""
    now = time.time()
    
    # Cleanup old jobs
    old_jobs = [jid for jid, job in JOBS.items() 
                if now - job.get('created_at', now) > CACHE_TTL_SECONDS]
    for jid in old_jobs:
        del JOBS[jid]
    
    # Cleanup old results (view cache)
    old_results = [cid for cid, result in RESULTS_CACHE.items() 
                   if now - result.get('created_at', now) > CACHE_TTL_SECONDS]
    for cid in old_results:
        del RESULTS_CACHE[cid]
    
    # Cleanup old scan cache
    old_scans = [key for key, data in SCAN_CACHE.items()
                 if now - data.get('created_at', now) > SCAN_CACHE_TTL]
    for key in old_scans:
        del SCAN_CACHE[key]
    
    if old_jobs or old_results or old_scans:
        print(f"Cleanup: removed {len(old_jobs)} jobs, {len(old_results)} results, {len(old_scans)} cached scans")

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
            result = scan_logic.generate_nominator_leaderboard_for_user(username, progress_callback=update_progress, cancel_event=cancel_event)
            title_prefix = "Nominated for"
        elif mode == 'bn':
            result = scan_logic.generate_bn_leaderboard_for_user(username, progress_callback=update_progress, cancel_event=cancel_event)
            title_prefix = "Nominated by"
        elif mode == 'gd_hosts':
            result = scan_logic.generate_gd_hosts_leaderboard_for_user(username, progress_callback=update_progress, cancel_event=cancel_event)
            title_prefix = "Guest Difficulties by"
        else:
            result = scan_logic.generate_leaderboard_for_user(username, progress_callback=update_progress, cancel_event=cancel_event)
            title_prefix = "Guest Difficulties for"
            
        if cancel_event.is_set():
            JOBS[job_id]['status'] = 'cancelled'
            JOBS[job_id]['message'] = 'Scan cancelled by user.'
        elif 'error' in result:
             JOBS[job_id]['status'] = 'error'
             JOBS[job_id]['error'] = result['error']
        else:
            # Done - save to results cache
            RESULTS_CACHE[job_id] = {
                'username': result['username'], 
                'leaderboard': result['leaderboard'],
                'title_prefix': title_prefix,
                'created_at': time.time()
            }
            JOBS[job_id]['status'] = 'done'
            JOBS[job_id]['result_id'] = job_id
            
            # Also save to SCAN_CACHE for future requests
            cache_key = f"{username.lower().strip()}:{mode}"
            SCAN_CACHE[cache_key] = {
                'result': {
                    'username': result['username'],
                    'leaderboard': result['leaderboard'],
                    'title_prefix': title_prefix
                },
                'created_at': time.time()
            }
            
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
    
    # Check if we have a cached result for this user+mode
    cache_key = f"{username.lower().strip()}:{mode}"
    cached = SCAN_CACHE.get(cache_key)
    
    if cached and (time.time() - cached['created_at'] < SCAN_CACHE_TTL):
        # Return cached result instantly!
        job_id = str(uuid.uuid4())
        RESULTS_CACHE[job_id] = {
            'username': cached['result']['username'],
            'leaderboard': cached['result']['leaderboard'],
            'title_prefix': cached['result']['title_prefix'],
            'created_at': time.time()
        }
        JOBS[job_id] = {
            'status': 'done',
            'message': 'Loaded from cache',
            'result_id': job_id,
            'created_at': time.time()
        }
        return jsonify({'job_id': job_id, 'cached': True})
        
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
                           
    # Filename matches the page title: "{title_prefix} {username}.html"
    title_prefix = data.get('title_prefix', 'Results')
    username = data['username']
    filename = f"{title_prefix} {username}.html"
    
    return Response(
        html,
        mimetype="text/html",
        headers={"Content-Disposition": f"attachment;filename={filename}"}
    )

# ============================================================
# GLOBAL LEADERBOARDS
# ============================================================

# Secret key for admin trigger (set via env var or default)
GLOBAL_SCAN_SECRET = os.environ.get('GLOBAL_SCAN_SECRET', 'osuscan_admin_secret')

# Track global scan status
GLOBAL_SCAN_STATUS = {'running': False, 'message': '', 'last_run': None}

def run_global_bn_duo_scan():
    """Background function for the global BN duo scan."""
    if GLOBAL_SCAN_STATUS['running']:
        print("Global BN duo scan already running, skipping.")
        return
    
    GLOBAL_SCAN_STATUS['running'] = True
    GLOBAL_SCAN_STATUS['message'] = 'Starting global BN duo scan...'
    
    def progress(msg):
        GLOBAL_SCAN_STATUS['message'] = msg
        print(f"[BN Duo Scan] {msg}")
    
    try:
        result = scan_logic.global_bn_duo_scan(progress_callback=progress)
        if 'error' in result:
            print(f"Global BN duo scan failed: {result['error']}")
        else:
            print(f"Global BN duo scan completed successfully.")
    except Exception as e:
        print(f"Global BN duo scan error: {e}")
    finally:
        GLOBAL_SCAN_STATUS['running'] = False
        GLOBAL_SCAN_STATUS['message'] = 'Idle'
        GLOBAL_SCAN_STATUS['last_run'] = time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())

@app.route('/leaderboards')
def leaderboards_page():
    mode = request.args.get('mode', 'duo')
    page = request.args.get('page', 1, type=int)
    search = request.args.get('search', '').strip()
    game_mode = request.args.get('game_mode', '')
    per_page = 50
    data = scan_logic.load_leaderboard_results()
    
    pagination = None
    display_data = None

    if data:
        if mode == 'individual':
            full_list = data.get('individual_leaderboard', [])
        elif mode == 'gd':
            full_list = data.get('gd_leaderboard', [])
        elif mode == 'host':
            full_list = data.get('host_leaderboard', [])
        else:
            full_list = data.get('leaderboard', [])

        # Stamp original rank before filtering
        for i, entry in enumerate(full_list):
            entry['rank'] = i + 1

        # Server-side filtering
        if search:
            search_lower = search.lower()
            if mode == 'duo' or mode not in ['individual', 'gd', 'host']:
                full_list = [e for e in full_list if search_lower in e.get('bn1_name','').lower() or search_lower in e.get('bn2_name','').lower()]
            else:
                full_list = [e for e in full_list if search_lower in e.get('username','').lower()]
        
        if game_mode:
            full_list = [e for e in full_list if game_mode in e.get('modes', [])]

        total_entries = len(full_list)
        total_pages = (total_entries + per_page - 1) // per_page
        
        # Ensure page is valid
        if total_pages > 0:
            page = max(1, min(page, total_pages))
        else:
            page = 1
            
        start = (page - 1) * per_page
        end = start + per_page
        
        # Create shallow copy for display
        display_data = data.copy()
        display_data['leaderboard'] = full_list[start:end]
        
        pagination = {
            'current_page': page,
            'total_pages': total_pages,
            'total_entries': total_entries,
            'start_rank': start + 1
        }
    else:
        display_data = data

    return render_template('leaderboards.html', data=display_data, scan_status=GLOBAL_SCAN_STATUS, pagination=pagination, mode=mode, search=search, game_mode=game_mode)

@app.route('/bn-duos')
@app.route('/bn-leaderboard')
@app.route('/gder-leaderboard')
def legacy_redirect():
    return redirect(url_for('leaderboards_page', **request.args))

@app.route('/api/trigger_global_scan', methods=['GET', 'POST'])
def trigger_global_scan():
    key = request.args.get('key') or request.form.get('key')
    if key != GLOBAL_SCAN_SECRET:
        return jsonify({'error': 'Unauthorized'}), 403
    
    if GLOBAL_SCAN_STATUS['running']:
        return jsonify({'error': 'Scan already running', 'status': GLOBAL_SCAN_STATUS['message']})
    
    thread = threading.Thread(target=run_global_bn_duo_scan, daemon=True)
    thread.start()
    return jsonify({'status': 'Scan started'})

@app.route('/api/global_scan_status')
@limiter.exempt
def global_scan_status():
    return jsonify(GLOBAL_SCAN_STATUS)

# Monthly scheduler - runs on the 1st of each month at 06:00 UTC
scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(
    run_global_bn_duo_scan,
    trigger=CronTrigger(day=1, hour=6, minute=0),
    id='monthly_bn_duo_scan',
    name='Monthly BN Duo Scan',
    replace_existing=True
)
scheduler.start()
print("APScheduler started - BN Duo scan scheduled for 1st of each month at 06:00 UTC")

if __name__ == '__main__':
    print("Starting osu!scan...")
    print("Open http://127.0.0.1:5000 in your browser")
    app.run(debug=True, port=5000, threaded=True)
