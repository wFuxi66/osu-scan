"""Global mapper leaderboard: total playcount across every ranked/loved difficulty a mapper made (GDs included)."""
import time
from collections import defaultdict
from datetime import datetime

import requests

import scan_logic
from global_scan import safe_api_get, save_to_firebase

# The guest search scope is exactly the leaderboarded maps: ranked + loved. No token needed.
SEARCH_URL = 'https://osu.ppy.sh/beatmapsets/search'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
}
TOP_N = 1000


def new_stats():
    """Per-mapper totals, split into a 'ranked' and a 'loved' bucket."""
    return {bucket: {
        'pc': defaultdict(int),
        'maps': defaultdict(int),
        'mode_pc': defaultdict(lambda: defaultdict(int)),
        'mode_maps': defaultdict(lambda: defaultdict(int)),
    } for bucket in ('ranked', 'loved')}


def aggregate_page(beatmapsets, stats, names):
    """Folds one search page into the running per-mapper totals."""
    for bset in beatmapsets:
        if bset.get('creator'):
            names[bset['user_id']] = bset['creator']
        for bmap in bset.get('beatmaps', []):
            uid = bmap.get('user_id')
            if not uid:
                continue
            # ponytail: a loved set can hold non-loved diffs; those land in the ranked bucket. ~0.1% of diffs.
            status = bmap.get('status') or bset.get('status')
            b = stats['loved' if status == 'loved' else 'ranked']
            mode = 'catch' if bmap.get('mode') == 'fruits' else bmap.get('mode', 'osu')
            pc = bmap.get('playcount') or 0
            b['pc'][uid] += pc
            b['maps'][uid] += 1
            b['mode_pc'][uid][mode] += pc
            b['mode_maps'][uid][mode] += 1


def merge_modes(*dicts):
    """Sums several {mode: count} dicts into one."""
    out = defaultdict(int)
    for d in dicts:
        for mode, count in d.items():
            out[mode] += count
    return dict(out)


def run_mapper_scan(progress_callback=None, cancel_event=None, max_pages=None):
    """Pages through every ranked/loved mapset and ranks mappers by total difficulty playcount."""
    def progress(msg):
        print(msg, flush=True)
        if progress_callback:
            progress_callback(msg)

    stats = new_stats()
    names = {}

    session = requests.Session()
    cursor = None
    pages = 0
    total_sets = 0

    progress("Fetching ranked & loved mapsets...")
    while True:
        if cancel_event and cancel_event.is_set():
            session.close()
            return {'error': 'Cancelled'}

        params = {'sort': 'ranked_desc'}
        if cursor:
            params['cursor_string'] = cursor

        r = safe_api_get(SEARCH_URL, headers=HEADERS, params=params, timeout=20, session=session)
        if not r:
            break

        data = r.json()
        page_sets = data.get('beatmapsets', [])
        if not page_sets:
            break

        aggregate_page(page_sets, stats, names)
        total_sets += len(page_sets)
        pages += 1

        if pages % 50 == 0:
            progress(f"Scanned {total_sets}/{data.get('total', '?')} mapsets...")

        cursor = data.get('cursor_string')
        if not cursor or (max_pages and pages >= max_pages):
            break
        time.sleep(0.1)

    session.close()

    ranked, loved = stats['ranked'], stats['loved']
    all_ids = set(ranked['pc']) | set(loved['pc'])
    progress(f"Aggregated {total_sets} mapsets, {len(all_ids)} mappers. Ranking...")

    top_ids = sorted(all_ids, key=lambda uid: -(ranked['pc'][uid] + loved['pc'][uid]))[:TOP_N]

    # Most mappers are known from a set they hosted; resolve the rest of the top N by API.
    unknown = [uid for uid in top_ids if uid not in names]
    if unknown:
        token = scan_logic.get_token()
        if token:
            names.update(scan_logic.resolve_users_parallel(unknown, token, progress_callback))

    mappers = [{
        'osu_id': uid,
        'username': names.get(uid, f'User_{uid}'),
        'playcount': ranked['pc'][uid] + loved['pc'][uid],
        'loved_playcount': loved['pc'][uid],
        'maps': ranked['maps'][uid] + loved['maps'][uid],
        'loved_maps': loved['maps'][uid],
        'by_mode': merge_modes(ranked['mode_pc'][uid], loved['mode_pc'][uid]),
        'loved_by_mode': dict(loved['mode_pc'][uid]),
        'maps_by_mode': merge_modes(ranked['mode_maps'][uid], loved['mode_maps'][uid]),
    } for uid in top_ids]

    result = {
        'last_scan': datetime.utcnow().isoformat(),
        'total_sets_scanned': total_sets,
        'total_mappers': len(all_ids),
        'mappers': mappers,
    }

    progress("Saving mapper leaderboard to Firebase...")
    save_to_firebase(result, path='mappers')
    progress(f"Mapper scan complete! Top mapper: {mappers[0]['username']} ({mappers[0]['playcount']:,} plays)" if mappers else "No mappers found.")
    return result


if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()

    stats, names = new_stats(), {}
    aggregate_page([
        {'user_id': 1, 'creator': 'Host', 'status': 'ranked', 'beatmaps': [
            {'user_id': 1, 'mode': 'osu', 'playcount': 10, 'status': 'ranked'},
            {'user_id': 2, 'mode': 'osu', 'playcount': 5, 'status': 'ranked'},
            {'user_id': 2, 'mode': 'fruits', 'playcount': 3, 'status': 'ranked'},
        ]},
        {'user_id': 3, 'creator': 'LovedHost', 'status': 'loved', 'beatmaps': [
            {'user_id': 2, 'mode': 'osu', 'playcount': 100, 'status': 'loved'},
            {'user_id': 3, 'mode': 'osu', 'playcount': 7, 'status': 'graveyard'},
        ]},
    ], stats, names)
    assert dict(stats['ranked']['pc']) == {1: 10, 2: 8, 3: 7}, stats['ranked']['pc']
    assert dict(stats['loved']['pc']) == {2: 100}, stats['loved']['pc']
    assert dict(stats['loved']['maps']) == {2: 1}
    assert dict(stats['ranked']['mode_pc'][2]) == {'osu': 5, 'catch': 3}
    assert merge_modes(stats['ranked']['mode_pc'][2], stats['loved']['mode_pc'][2]) == {'osu': 105, 'catch': 3}
    assert names == {1: 'Host', 3: 'LovedHost'}
    print("self-check OK")

    run_mapper_scan(max_pages=3)
