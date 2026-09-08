"""Global mapper leaderboard: total playcount across every ranked/loved difficulty a mapper made.

Guest difficulties count for whoever made them, and a collab difficulty counts in full for
every one of its authors.

The scan is built to survive a bad night: it checkpoints as it goes, so a crash, a
rate-limit wall or a dead runner costs only the pages since the last checkpoint, and it
refuses to publish a leaderboard it could not finish.
"""
import os
import pickle
import time
from collections import defaultdict
from datetime import datetime

import requests

import scan_logic
from global_scan import save_to_firebase

# Two ways to page the same corpus of leaderboarded maps (ranked + loved).
# The API takes a token and gets a far higher rate limit; the public search needs nothing
# but throttles hard, which matters over the ~1200 pages a full scan takes.
API_SEARCH_URL = 'https://osu.ppy.sh/api/v2/beatmapsets/search'
API_BEATMAPS_URL = 'https://osu.ppy.sh/api/v2/beatmaps'
API_BEATMAPSET_URL = 'https://osu.ppy.sh/api/v2/beatmapsets'
SEARCH_URL = 'https://osu.ppy.sh/beatmapsets/search'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
}
TOP_N = None  # keep every mapper: the whole ladder is ~8k entries

# Retry waits in seconds. The monthly job has hours to spare, so a request gets ~18 minutes
# of escalating patience before the scan gives up on it and checkpoints instead.
RETRY_WAITS = (0, 5, 15, 45, 120, 300, 600)
CHECKPOINT_EVERY = 20  # pages
STATE_PATH = os.environ.get('MAPPER_SCAN_STATE', 'mapper_scan_state.pickle')
BEATMAP_IDS_PER_CALL = 50  # the API's documented cap for ids[]
# Bumped whenever the checkpoint layout changes, so an old one is discarded, not misread.
STATE_VERSION = 2


class AuthRejected(Exception):
    """The API refused our token. Waiting cannot fix that, so never retry it."""


def get(session, url, headers, params=None):
    """A GET that keeps trying. Returns the response, or None once the patience runs out."""
    for wait in RETRY_WAITS:
        if wait:
            print(f"Request failed ({url}), waiting {wait}s...", flush=True)
            time.sleep(wait)
        try:
            r = session.get(url, headers=headers, params=params, timeout=30)
        except requests.exceptions.RequestException:
            continue
        if r.status_code == 200:
            return r
        if r.status_code in (401, 403):
            raise AuthRejected(f"{r.status_code} from {url}")
        if r.status_code == 404:
            return None
    return None


# ---- Checkpointing ----

def _int_dd():
    """Module-level so the stats structure stays picklable."""
    return defaultdict(int)


# Every difficulty falls in one of these four buckets. Keeping them apart is what lets the
# ladder answer "with or without guest difficulties" without a second scan.
BUCKETS = (('ranked', 'own'), ('ranked', 'guest'), ('loved', 'own'), ('loved', 'guest'))


def new_stats():
    """Per-mapper totals, split by map status and by whether the mapper hosted the set."""
    return {bucket: {
        'pc': defaultdict(int),
        'maps': defaultdict(int),
        'mode_pc': defaultdict(_int_dd),
        'mode_maps': defaultdict(_int_dd),
    } for bucket in BUCKETS}


def save_state(state, path=None):
    """Write the checkpoint atomically, so a crash mid-write cannot corrupt it."""
    state['version'] = STATE_VERSION
    path = path or STATE_PATH
    tmp = f'{path}.tmp'
    with open(tmp, 'wb') as f:
        pickle.dump(state, f, protocol=pickle.HIGHEST_PROTOCOL)
    os.replace(tmp, path)


def load_state(path=None):
    """Read a checkpoint left by an interrupted run, or None to start fresh."""
    try:
        with open(path or STATE_PATH, 'rb') as f:
            state = pickle.load(f)
    except (FileNotFoundError, EOFError, pickle.UnpicklingError, AttributeError, ImportError):
        return None
    if state.get('version') != STATE_VERSION:
        print("Checkpoint was written by an older scan layout; starting fresh.", flush=True)
        return None
    return state


def clear_state(path=None):
    try:
        os.remove(path or STATE_PATH)
    except FileNotFoundError:
        pass


# ---- Owners ----

def detect_owners_mode(session, token, sample_diff_id, sample_set_id):
    """Pick the cheapest endpoint that exposes a difficulty's full author list.

    'bulk' resolves 50 difficulties per call, 'set' needs one call per mapset, and 'none'
    means falling back to the single author osu! stores on each difficulty.
    """
    if not token:
        return 'none'
    headers = {**HEADERS, 'Authorization': f'Bearer {token}'}

    try:
        r = get(session, API_BEATMAPS_URL, headers, {'ids[]': [sample_diff_id]}) if sample_diff_id else None
    except AuthRejected:
        return 'none'
    if r is not None:
        maps = r.json().get('beatmaps') or []
        if maps and 'owners' in maps[0]:
            return 'bulk'

    r = get(session, f'{API_BEATMAPSET_URL}/{sample_set_id}', headers)
    if r is not None:
        for bmap in r.json().get('beatmaps') or []:
            if 'owners' in bmap:
                return 'set'
    return 'none'


def resolve_owners(session, beatmapsets, token, mode):
    """Map every difficulty on these mapsets to the user ids credited with it.

    Returns None if a lookup could not be completed, so the caller checkpoints rather than
    aggregating a page with half of its collabs missing.
    """
    if mode == 'none' or not beatmapsets:
        return {}
    if not token:
        return None
    headers = {**HEADERS, 'Authorization': f'Bearer {token}'}
    owners = {}

    try:
        if mode == 'bulk':
            ids = [bmap['id'] for bset in beatmapsets for bmap in bset.get('beatmaps', [])]
            for i in range(0, len(ids), BEATMAP_IDS_PER_CALL):
                r = get(session, API_BEATMAPS_URL, headers, {'ids[]': ids[i:i + BEATMAP_IDS_PER_CALL]})
                if r is None:
                    return None
                for bmap in r.json().get('beatmaps') or []:
                    owners[bmap['id']] = [o['id'] for o in (bmap.get('owners') or [])]
            return owners

        for bset in beatmapsets:
            r = get(session, f'{API_BEATMAPSET_URL}/{bset["id"]}', headers)
            if r is None:
                return None
            for bmap in r.json().get('beatmaps') or []:
                owners[bmap['id']] = [o['id'] for o in (bmap.get('owners') or [])]
        return owners
    except AuthRejected:
        return None


def diff_owners(bmap, owners_by_diff=None):
    """Everyone credited with a difficulty.

    osu! stores one `user_id` per difficulty but lists every author in `owners`, which the
    search endpoint omits. When an owners lookup is supplied a collab credits all of them;
    otherwise it falls back to the single stored author.
    """
    owners = (owners_by_diff or {}).get(bmap.get('id')) or [o['id'] for o in (bmap.get('owners') or [])]
    return owners or ([bmap['user_id']] if bmap.get('user_id') else [])


# ---- Aggregation ----

def aggregate_page(beatmapsets, stats, names, owners_by_diff=None):
    """Folds one search page into the running per-mapper totals."""
    for bset in beatmapsets:
        if bset.get('creator'):
            names[bset['user_id']] = bset['creator']
        for bmap in bset.get('beatmaps', []):
            # ponytail: a loved set can hold non-loved diffs; those land in the ranked bucket. ~0.1% of diffs.
            status = bmap.get('status') or bset.get('status')
            state = 'loved' if status == 'loved' else 'ranked'
            mode = 'catch' if bmap.get('mode') == 'fruits' else bmap.get('mode', 'osu')
            pc = bmap.get('playcount') or 0
            # A collab counts in full for every author, so each co-mapper shows its plays.
            for uid in diff_owners(bmap, owners_by_diff):
                # Hosting the set makes it your own map; anyone else on it is a guest mapper.
                b = stats[(state, 'own' if uid == bset.get('user_id') else 'guest')]
                b['pc'][uid] += pc
                b['maps'][uid] += 1
                b['mode_pc'][uid][mode] += pc
                b['mode_maps'][uid][mode] += 1


def prefer_name(resolved, stored):
    """The API's current username wins, except when the lookup failed.

    A failed lookup comes back as the placeholder User_<id>, which is worse than the
    possibly-outdated name the mapset carries.
    """
    if resolved and not resolved.startswith('User_'):
        return resolved
    return stored or resolved


def merge_modes(*dicts):
    """Sums several {mode: count} dicts into one."""
    out = defaultdict(int)
    for d in dicts:
        for mode, count in d.items():
            out[mode] += count
    return dict(out)


def fetch_page(session, cursor, token):
    """One page of search results.

    Returns (data, token). The token comes back None once the API has refused it, so the
    caller stops paying the auth round trip on every remaining page.
    """
    params = {'sort': 'ranked_desc', 'nsfw': 'true'}
    if cursor:
        params['cursor_string'] = cursor

    if token:
        try:
            r = get(session, API_SEARCH_URL, {**HEADERS, 'Authorization': f'Bearer {token}'}, params)
            if r is not None:
                return r.json(), token
        except AuthRejected:
            print("API refused the token; continuing on the public search.", flush=True)
            token = None

    try:
        r = get(session, SEARCH_URL, HEADERS, params)
    except AuthRejected:
        # The public search should never ask for auth; treat it as a dead page, not a crash.
        return None, token
    return (r.json() if r is not None else None), token


def run_mapper_scan(progress_callback=None, cancel_event=None, max_pages=None, resume=True):
    """Pages through every ranked/loved mapset and ranks mappers by total difficulty playcount.

    Resumes from the last checkpoint unless `resume` is False, and never publishes a
    leaderboard built from an incomplete pass.
    """
    def progress(msg):
        print(msg, flush=True)
        if progress_callback:
            progress_callback(msg)

    token = scan_logic.get_token()
    session = requests.Session()

    state = load_state() if resume else None
    if state:
        progress(f"Resuming from checkpoint: {len(state['seen'])} mapsets already scanned.")
    else:
        state = {'cursor': None, 'pages': 0, 'seen': set(), 'reported_total': None,
                 'stats': new_stats(), 'names': {}, 'owners_mode': None}

    stats, names = state['stats'], state['names']
    truncated = None

    if state['owners_mode'] not in (None, 'none') and not token:
        msg = ("Checkpoint was built with collab credit but no API token is available now; "
               "fix the credentials and re-run so the pass stays consistent.")
        progress(msg)
        session.close()
        return {'error': msg}

    progress("Fetching ranked & loved mapsets..." + ("" if token else " (no token: public search, slower)"))
    while True:
        if cancel_event and cancel_event.is_set():
            save_state(state)
            session.close()
            return {'error': 'Cancelled - checkpoint kept for the next run'}

        # Checked before fetching, so a resumed run that is already at the limit does nothing.
        if max_pages and state['pages'] >= max_pages:
            truncated = f"stopped early at the {max_pages}-page limit"
            break

        data, still_valid = fetch_page(session, state['cursor'], token)
        if still_valid is None and token is not None:
            token = None
            if state['owners_mode'] not in (None, 'none'):
                # Half the pass would carry collab credit and half would not: stop instead.
                truncated = "the API token stopped working, so collab credit would be inconsistent"
                break
        if data is None:
            truncated = f"search stopped responding after {state['pages']} pages"
            break

        page_sets = data.get('beatmapsets', [])
        if not page_sets:
            break
        if state['reported_total'] is None:
            state['reported_total'] = data.get('total')

        if state['owners_mode'] is None:
            probe = next((b for b in page_sets if b.get('beatmaps')), None)
            state['owners_mode'] = detect_owners_mode(
                session, token, probe['beatmaps'][0]['id'] if probe else None,
                probe['id'] if probe else None)
            progress({
                'bulk': "Collab credit on: reading difficulty owners 50 at a time.",
                'set': "Collab credit on: reading difficulty owners one mapset at a time (slow).",
                'none': "No owners endpoint reachable: collabs credit only their stored author.",
            }[state['owners_mode']])

        # Cursor pages can overlap; counting a set twice would double its mappers' playcount.
        fresh = [b for b in page_sets if b['id'] not in state['seen']]
        owners = resolve_owners(session, fresh, token, state['owners_mode'])
        if owners is None:
            truncated = f"owners lookup stopped responding after {state['pages']} pages"
            break

        aggregate_page(fresh, stats, names, owners)
        state['seen'].update(b['id'] for b in page_sets)
        state['pages'] += 1
        state['cursor'] = data.get('cursor_string')

        if state['pages'] % CHECKPOINT_EVERY == 0:
            save_state(state)
        if state['pages'] % 50 == 0:
            progress(f"Scanned {len(state['seen'])}/{state['reported_total'] or '?'} mapsets...")

        if not state['cursor']:
            break

    session.close()
    total_sets = len(state['seen'])

    # A short scan would quietly replace a good ladder with a wrong one, so refuse to save it.
    if truncated is None and state['reported_total'] and total_sets < state['reported_total'] * 0.99:
        truncated = f"only reached {total_sets} of {state['reported_total']} mapsets"
    if truncated:
        save_state(state)
        msg = (f"Incomplete scan ({truncated}). Previous leaderboard kept; "
               f"{total_sets} mapsets checkpointed, re-run to continue.")
        progress(msg)
        return {'error': msg, 'total_sets_scanned': total_sets, 'reported_total': state['reported_total']}

    all_ids = set()
    for b in stats.values():
        all_ids |= set(b['pc'])
    progress(f"Aggregated {total_sets} mapsets, {len(all_ids)} mappers. Ranking...")

    def total_pc(uid):
        return sum(stats[b]['pc'][uid] for b in BUCKETS)

    top_ids = sorted(all_ids, key=lambda uid: -total_pc(uid))
    if TOP_N:
        top_ids = top_ids[:TOP_N]

    # A mapset carries the name its host had when it ranked, so `creator` goes stale on every
    # rename. Re-resolve every mapper against the API; the stored name is only a fallback.
    names_current = False
    if token:
        progress(f"Refreshing {len(top_ids)} usernames...")
        resolved = scan_logic.resolve_users_parallel(top_ids, token, progress_callback, refresh=True)
        for uid, name in resolved.items():
            names[uid] = prefer_name(name, names.get(uid))
        names_current = True
    else:
        progress("No API token: usernames stay as they were stored on each mapset.")

    # Guest-difficulty figures are the totals minus the "own" ones, so they need no storage.
    def row(uid):
        own = [('ranked', 'own'), ('loved', 'own')]
        loved_b = [('loved', 'own'), ('loved', 'guest')]
        pick = lambda bs, key: sum(stats[b][key][uid] for b in bs)
        modes = lambda bs, key: merge_modes(*(stats[b][key][uid] for b in bs))
        return {
            'osu_id': uid,
            'username': names.get(uid, f'User_{uid}'),
            'playcount': pick(BUCKETS, 'pc'),
            'maps': pick(BUCKETS, 'maps'),
            'by_mode': modes(BUCKETS, 'mode_pc'),
            'maps_by_mode': modes(BUCKETS, 'mode_maps'),
            'loved_playcount': pick(loved_b, 'pc'),
            'loved_by_mode': modes(loved_b, 'mode_pc'),
            'own_playcount': pick(own, 'pc'),
            'own_maps': pick(own, 'maps'),
            'own_by_mode': modes(own, 'mode_pc'),
            'own_maps_by_mode': modes(own, 'mode_maps'),
            'own_loved_playcount': stats[('loved', 'own')]['pc'][uid],
            'own_loved_by_mode': dict(stats[('loved', 'own')]['mode_pc'][uid]),
        }

    mappers = [row(uid) for uid in top_ids]

    result = {
        'last_scan': datetime.utcnow().isoformat(),
        'total_sets_scanned': total_sets,
        'reported_total': state['reported_total'],
        'total_mappers': len(all_ids),
        'collab_credit': state['owners_mode'] != 'none',
        'names_current': names_current,
        'mappers': mappers,
    }

    progress("Saving mapper leaderboard to Firebase...")
    save_to_firebase(result, path='mappers')
    clear_state()
    progress(f"Mapper scan complete! Top mapper: {mappers[0]['username']} ({mappers[0]['playcount']:,} plays)"
             if mappers else "No mappers found.")
    return result


if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()

    stats, names = new_stats(), {}
    aggregate_page([
        {'user_id': 1, 'creator': 'Host', 'status': 'ranked', 'beatmaps': [
            {'id': 11, 'user_id': 1, 'mode': 'osu', 'playcount': 10, 'status': 'ranked'},
            {'id': 12, 'user_id': 2, 'mode': 'osu', 'playcount': 5, 'status': 'ranked'},
            {'id': 13, 'user_id': 2, 'mode': 'fruits', 'playcount': 3, 'status': 'ranked'},
        ]},
        {'user_id': 3, 'creator': 'LovedHost', 'status': 'loved', 'beatmaps': [
            {'id': 14, 'user_id': 2, 'mode': 'osu', 'playcount': 100, 'status': 'loved'},
            {'id': 15, 'user_id': 3, 'mode': 'osu', 'playcount': 7, 'status': 'graveyard'},
        ]},
        {'user_id': 4, 'creator': 'CollabHost', 'status': 'ranked', 'beatmaps': [
            {'id': 99, 'user_id': 4, 'mode': 'osu', 'playcount': 50, 'status': 'ranked',
             'owners': [{'id': 4}, {'id': 5}, {'id': 6}]},
        ]},
    ], stats, names)
    own, guest = stats[('ranked', 'own')], stats[('ranked', 'guest')]
    # Host 1 mapped one diff on their own set; user 2 guest-mapped two diffs on it.
    assert dict(own['pc']) == {1: 10, 3: 7, 4: 50}, dict(own['pc'])
    assert dict(guest['pc']) == {2: 8, 5: 50, 6: 50}, dict(guest['pc'])
    # The collab's 50 plays land in full on all three authors, not split between them,
    # and only the host counts them as his own map.
    assert own['maps'][4] == 1 and guest['maps'][5] == 1 and guest['maps'][6] == 1
    assert diff_owners({'id': 99, 'user_id': 4}, {99: [4, 5]}) == [4, 5]
    assert diff_owners({'id': 7, 'user_id': 4}) == [4], "no owners info falls back to the stored author"
    assert dict(stats[('loved', 'guest')]['pc']) == {2: 100}
    # A non-loved diff inside a loved set falls in the ranked bucket, credited to its host.
    assert dict(stats[('loved', 'own')]['pc']) == {}
    assert own['pc'][3] == 7
    assert dict(guest['mode_pc'][2]) == {'osu': 5, 'catch': 3}
    assert merge_modes(guest['mode_pc'][2], stats[('loved', 'guest')]['mode_pc'][2]) == {'osu': 105, 'catch': 3}
    assert names == {1: 'Host', 3: 'LovedHost', 4: 'CollabHost'}

    # Renames are why the ladder cannot trust the name stored on a mapset.
    assert prefer_name('Andrea', 'osuplayer111') == 'Andrea', "a current name must win"
    assert prefer_name('User_33599', 'osuplayer111') == 'osuplayer111', "a failed lookup must not erase a real name"
    assert prefer_name('User_33599', None) == 'User_33599'
    assert prefer_name(None, 'osuplayer111') == 'osuplayer111'

    # A checkpoint must come back with its counters and totals intact.
    probe = 'mapper_scan_state.selfcheck'
    save_state({'pages': 7, 'seen': {1, 2, 3}, 'stats': stats, 'names': names}, probe)
    back = load_state(probe)
    assert back['pages'] == 7 and back['seen'] == {1, 2, 3}
    assert back['stats'][('ranked', 'guest')]['pc'][5] == 50, "stats did not survive the checkpoint"
    # A checkpoint from an older layout must be discarded rather than misread.
    import pickle as _p
    with open(probe, 'wb') as f:
        _p.dump({'version': 0, 'pages': 1}, f)
    assert load_state(probe) is None, "a stale checkpoint layout should be ignored"
    clear_state(probe)
    assert load_state(probe) is None
    print("self-check OK")

    # A refused token must fail instantly: retrying auth errors used to sleep ~18 minutes a page.
    class Refusing:
        status_code = 401
        def get(self, *a, **k):
            Refusing.calls = getattr(Refusing, 'calls', 0) + 1
            return self
    refusing = Refusing()
    started = time.time()
    try:
        get(refusing, 'http://example.invalid', {})
        raise AssertionError("a 401 should raise AuthRejected")
    except AuthRejected:
        pass
    assert Refusing.calls == 1, f"auth error was retried {Refusing.calls} times"
    assert time.time() - started < 1, "auth error slept before giving up"
    data, token_after = fetch_page(refusing, None, 'bad-token')
    assert token_after is None, "a refused token must be dropped for the rest of the scan"
    assert data is None, "a page that cannot be fetched must come back empty, not raise"
    assert resolve_owners(refusing, [{'id': 1, 'beatmaps': [{'id': 2}]}], 'bad', 'bulk') is None
    print("auth handling OK: refused token fails fast and stops collab credit")

    # Live check of the network path; a full run is what run_mapper_scan is for.
    page, _ = fetch_page(requests.Session(), None, scan_logic.get_token())
    assert page and page['beatmapsets'], "search returned nothing"
    print(f"live fetch OK: {len(page['beatmapsets'])} sets of {page.get('total')} reported")
