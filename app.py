from flask import Flask, render_template, request, Response, jsonify, redirect
import gzip
import hmac
import json
from dotenv import load_dotenv
import threading
import time
import uuid
import os

# Before importing anything that reads credentials: scan_logic copies OSU_CLIENT_ID into a
# module constant at import time, so loading .env afterwards leaves it empty and every scan
# dies with "Authentication failed".
load_dotenv()

import scan_logic
import global_scan
from flask_limiter import Limiter

app = Flask(__name__)

# The stylesheet and the two marks only change when the repo does. Without this Flask asks
# the browser to revalidate each one on every page view: three round trips to be told
# nothing moved.
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 86400

# Get real IP behind Render's proxy
def get_real_ip():
    """The address the rate limiter counts against.

    X-Forwarded-For reads "client, proxy1, proxy2...", and the left of it is whatever the
    caller sent - a client that writes its own header gets a fresh identity per request and
    the limit stops existing. Only the rightmost entry was appended by our own proxy, so
    that is the one entry nobody outside can forge.
    """
    forwarded = request.headers.get('X-Forwarded-For', '')
    if forwarded:
        return forwarded.split(',')[-1].strip()
    return request.remote_addr or '127.0.0.1'

# Rate Limiter Configuration (per user IP)
limiter = Limiter(
    key_func=get_real_ip,
    app=app,
    default_limits=["2000 per day", "500 per hour"],
    storage_uri="memory://"
)

# One scan is minutes of osu! API calls held in a thread of its own, so accepting them as
# fast as they arrive is how one visitor takes the dyno down - and how the API starts
# throttling everyone else's. Turning them away is the honest answer; they retry in a moment.
MAX_CONCURRENT_SCANS = 6
SCANS_RUNNING = 0
SCANS_LOCK = threading.Lock()

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
        # pop, not del: two cleanups racing would have the second one raise KeyError.
        JOBS.pop(jid, None)
    
    # Cleanup old results (view cache)
    old_results = [cid for cid, result in RESULTS_CACHE.items() 
                   if now - result.get('created_at', now) > CACHE_TTL_SECONDS]
    for cid in old_results:
        RESULTS_CACHE.pop(cid, None)
    
    # Cleanup old scan cache
    old_scans = [key for key, data in SCAN_CACHE.items()
                 if now - data.get('created_at', now) > SCAN_CACHE_TTL]
    for key in old_scans:
        SCAN_CACHE.pop(key, None)
    
    if old_jobs or old_results or old_scans:
        print(f"Cleanup: removed {len(old_jobs)} jobs, {len(old_results)} results, {len(old_scans)} cached scans")

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

def run_scan_job(job_id, username, mode, cancel_event):
    """Background thread function."""
    # A scan that outlives its own job entry - the cleanup drops them after CACHE_TTL_SECONDS -
    # used to finish, raise KeyError writing its result, and throw the whole scan away. The
    # work is done by then; the least it can do is not crash on the way out.
    def set_job(**fields):
        job = JOBS.get(job_id)
        if job is not None:
            job.update(fields)

    def update_progress(msg):
        if cancel_event.is_set():
            set_job(status='cancelled', message='Cancelled.')
            return
        set_job(message=msg)
            
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
            set_job(status='cancelled', message='Scan cancelled by user.')
        elif 'error' in result:
            set_job(status='error', error=result['error'])
        else:
            payload = {
                'username': result['username'],
                'user_id': result.get('user_id'),
                'leaderboard': result['leaderboard'],
                'sets_read': result.get('sets_read'),
                'unread_sets': result.get('unread_sets'),
                'listing_truncated': result.get('listing_truncated'),
                'title_prefix': title_prefix
            }
            RESULTS_CACHE[job_id] = dict(payload, created_at=time.time())
            set_job(status='done', result_id=job_id)
            
            # Also save to SCAN_CACHE for future requests
            cache_key = f"{username.lower().strip()}:{mode}"
            SCAN_CACHE[cache_key] = {'result': payload, 'created_at': time.time()}
            
    except Exception as e:
        set_job(status='error', error=str(e))
    finally:
        global SCANS_RUNNING
        with SCANS_LOCK:
            SCANS_RUNNING -= 1

@app.route('/api/start_scan', methods=['POST'])
@limiter.limit("30 per minute") # Max 30 scans per minute per IP
def start_scan():
    global SCANS_RUNNING
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
        RESULTS_CACHE[job_id] = dict(cached['result'], created_at=time.time())
        JOBS[job_id] = {
            'status': 'done',
            'message': 'Loaded from cache',
            'result_id': job_id,
            'created_at': time.time()
        }
        return jsonify({'job_id': job_id, 'cached': True})
        
    with SCANS_LOCK:
        if SCANS_RUNNING >= MAX_CONCURRENT_SCANS:
            return jsonify({'error': 'Too many scans running right now. Try again in a minute.'}), 429
        SCANS_RUNNING += 1

    job_id = str(uuid.uuid4())
    cancel_event = threading.Event()
    
    JOBS[job_id] = {
        'status': 'running', 
        'message': 'Starting...',
        'cancel_event': cancel_event,
        'created_at': time.time()
    }
    
    # Start background thread. Daemon: a scan wedged on a slow API must not hold up a restart.
    thread = threading.Thread(target=run_scan_job, args=(job_id, username, mode, cancel_event),
                              daemon=True)
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
        return render_template('results.html', expired=True), 404
    return render_template('results.html',
                           username=data['username'],
                           user_id=data.get('user_id'),
                           leaderboard=data['leaderboard'],
                           title_prefix=data['title_prefix'],
                           sets_read=data.get('sets_read'),
                           unread_sets=data.get('unread_sets'),
                           listing_truncated=data.get('listing_truncated'))


# ---- Global BN Leaderboard ----

GLOBAL_SCAN_RUNNING = False

# Server-side cache for leaderboard data (avoids hitting Firebase on every request)
LEADERBOARD_CACHE = {}
LEADERBOARD_CACHE_TTL = 300  # 5 minutes

def get_leaderboard_data(path='leaderboard'):
    """Get leaderboard data with server-side caching."""
    now = time.time()
    entry = LEADERBOARD_CACHE.get(path)
    if entry and entry['data'] is not None and (now - entry['fetched_at'] < LEADERBOARD_CACHE_TTL):
        return entry['data']
    data = global_scan.load_from_firebase(path)
    LEADERBOARD_CACHE[path] = {'data': data, 'fetched_at': now}
    return data

@app.route('/leaderboard')
def leaderboard():
    return render_template('leaderboard.html')

# ---- Firebase namespace ----

# The site still reads the prefix the redesign was rehearsed against. The unprefixed paths
# hold an older scan with no profile_* fields, so pointing at them would put crawl figures
# back on the sets board. Set NEXT_FIREBASE_NS='' to cut over once a scan has filled them.
NEXT_NS = os.environ.get('NEXT_FIREBASE_NS', 'preprod').strip('/')


def next_path(name):
    return f'{NEXT_NS}/{name}' if NEXT_NS else name


# ---- Redirects from the /next preview, whose links are already out in the wild ----

@app.route('/next')
def next_index():
    return redirect('/', code=301)


@app.route('/next/leaderboard')
def next_leaderboard():
    # Keeps ?board=... intact, which is the form of the links people shared.
    return redirect(f'/leaderboard?{request.query_string.decode()}'
                    if request.query_string else '/leaderboard', code=301)


@app.route('/next/results_view/<cache_id>')
def next_results_view(cache_id):
    return redirect(f'/results_view/{cache_id}', code=301)


@app.route('/api/next/leaderboard_data')
@limiter.exempt
def next_leaderboard_data():
    return redirect('/api/leaderboard_data', code=301)


@app.route('/api/next/mappers_data')
@limiter.exempt
def next_mappers_data():
    return redirect('/api/mappers_data', code=301)


@app.route('/api/leaderboard_data')
@limiter.exempt
def leaderboard_data():
    """Returns full leaderboard JSON. Client handles filtering/pagination."""
    # The duo list alone runs to 18k pairs: ~1.9MB of JSON that gzips to ~0.2MB.
    return gzipped_json(get_leaderboard_data(next_path('leaderboard')), 'bns')

# Both ladders run to megabytes of JSON - every mapper in the game on one, every pair of
# nominators on the other. gzip cuts them by 7-9x; they only change when a scan lands, so
# compress once and reuse.
GZIP_CACHE = {}

def gzipped_json(payload, cache_key):
    """JSON response, gzipped when the caller accepts it.

    Serialising megabytes per request would dwarf the work of serving them, so both the
    encoded and the compressed body are kept until the scan behind them changes.
    """
    version = (payload or {}).get('last_scan')
    cached = GZIP_CACHE.get(cache_key)
    if not cached or cached['version'] != version:
        body = json.dumps(payload, separators=(',', ':')).encode()
        cached = {'version': version, 'raw': body, 'gzip': gzip.compress(body, 6)}
        GZIP_CACHE[cache_key] = cached

    # Held exactly as long as the server-side copy. Someone moving between the boards and
    # the scan page was pulling the same megabytes down each time. An empty payload is a
    # Firebase hiccup rather than a result, so that is the one thing no browser keeps.
    headers = {'Vary': 'Accept-Encoding',
               'Cache-Control': f'public, max-age={LEADERBOARD_CACHE_TTL}' if payload else 'no-store'}

    if 'gzip' not in request.headers.get('Accept-Encoding', ''):
        return Response(cached['raw'], mimetype='application/json', headers=headers)
    return Response(cached['gzip'], mimetype='application/json',
                    headers={**headers, 'Content-Encoding': 'gzip'})

@app.route('/api/mappers_data')
@limiter.exempt
def mappers_data():
    """Returns the global mapper playcount leaderboard. Client handles filtering/pagination."""
    return gzipped_json(get_leaderboard_data(next_path('mappers')), 'mappers')

@app.route('/api/run_global_scan', methods=['POST'])
def trigger_global_scan():
    global GLOBAL_SCAN_RUNNING
    
    # Check secret key
    secret = request.form.get('secret') or request.args.get('secret') or ''
    expected = os.environ.get('SCAN_SECRET', '')
    
    if not expected or not hmac.compare_digest(secret, expected):
        return jsonify({'error': 'Unauthorized'}), 403
    
    if GLOBAL_SCAN_RUNNING:
        return jsonify({'error': 'Scan already running'}), 409
    
    GLOBAL_SCAN_RUNNING = True

    def run():
        global GLOBAL_SCAN_RUNNING
        try:
            global_scan.run_global_scan()
        except Exception as e:
            print(f"Global scan error: {e}")
        finally:
            GLOBAL_SCAN_RUNNING = False
    
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    
    return jsonify({'status': 'started'})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print("Starting osu!scan...")
    print(f"Server is binding to 0.0.0.0:{port}")
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
