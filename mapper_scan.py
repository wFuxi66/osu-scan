"""Global mapper leaderboard: total playcount across every ranked difficulty a mapper made (GDs included)."""
import time
from collections import defaultdict
from datetime import datetime

import requests

import scan_logic
from global_scan import safe_api_get, save_to_firebase

# ponytail: guest search only exposes the ranked category (loved/approved are login-gated),
# so loved maps are excluded. Add an authenticated s=loved pass if that ever matters.
SEARCH_URL = 'https://osu.ppy.sh/beatmapsets/search'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
}
TOP_N = 1000


def aggregate_page(beatmapsets, playcount, maps, mode_playcount, mode_maps, names):
    """Folds one search page into the running per-mapper totals."""
    for bset in beatmapsets:
        if bset.get('creator'):
            names[bset['user_id']] = bset['creator']
        for bmap in bset.get('beatmaps', []):
            uid = bmap.get('user_id')
            if not uid:
                continue
            mode = 'catch' if bmap.get('mode') == 'fruits' else bmap.get('mode', 'osu')
            pc = bmap.get('playcount') or 0
            playcount[uid] += pc
            maps[uid] += 1
            mode_playcount[uid][mode] += pc
            mode_maps[uid][mode] += 1


def run_mapper_scan(progress_callback=None, cancel_event=None, max_pages=None):
    """Pages through every ranked mapset and ranks mappers by total difficulty playcount."""
    def progress(msg):
        print(msg, flush=True)
        if progress_callback:
            progress_callback(msg)

    playcount = defaultdict(int)
    maps = defaultdict(int)
    mode_playcount = defaultdict(lambda: defaultdict(int))
    mode_maps = defaultdict(lambda: defaultdict(int))
    names = {}

    session = requests.Session()
    cursor = None
    pages = 0
    total_sets = 0

    progress("Fetching ranked mapsets...")
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

        aggregate_page(page_sets, playcount, maps, mode_playcount, mode_maps, names)
        total_sets += len(page_sets)
        pages += 1

        if pages % 50 == 0:
            progress(f"Scanned {total_sets}/{data.get('total', '?')} mapsets...")

        cursor = data.get('cursor_string')
        if not cursor or (max_pages and pages >= max_pages):
            break
        time.sleep(0.1)

    session.close()
    progress(f"Aggregated {total_sets} mapsets, {len(playcount)} mappers. Ranking...")

    top_ids = sorted(playcount, key=lambda uid: -playcount[uid])[:TOP_N]

    # Most mappers are known from a set they hosted; resolve the rest of the top N by API.
    unknown = [uid for uid in top_ids if uid not in names]
    if unknown:
        token = scan_logic.get_token()
        if token:
            names.update(scan_logic.resolve_users_parallel(unknown, token, progress_callback))

    mappers = [{
        'osu_id': uid,
        'username': names.get(uid, f'User_{uid}'),
        'playcount': playcount[uid],
        'maps': maps[uid],
        'by_mode': dict(mode_playcount[uid]),
        'maps_by_mode': dict(mode_maps[uid]),
    } for uid in top_ids]

    result = {
        'last_scan': datetime.utcnow().isoformat(),
        'total_sets_scanned': total_sets,
        'total_mappers': len(playcount),
        'mappers': mappers,
    }

    progress("Saving mapper leaderboard to Firebase...")
    save_to_firebase(result, path='mappers')
    progress(f"Mapper scan complete! Top mapper: {mappers[0]['username']} ({mappers[0]['playcount']:,} plays)" if mappers else "No mappers found.")
    return result


if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()

    playcount, maps = defaultdict(int), defaultdict(int)
    mode_playcount, mode_maps, names = defaultdict(lambda: defaultdict(int)), defaultdict(lambda: defaultdict(int)), {}
    aggregate_page([{
        'user_id': 1, 'creator': 'Host', 'beatmaps': [
            {'user_id': 1, 'mode': 'osu', 'playcount': 10},
            {'user_id': 2, 'mode': 'osu', 'playcount': 5},
            {'user_id': 2, 'mode': 'fruits', 'playcount': 3},
        ]}], playcount, maps, mode_playcount, mode_maps, names)
    assert playcount == {1: 10, 2: 8}, playcount
    assert maps == {1: 1, 2: 2}, maps
    assert dict(mode_playcount[2]) == {'osu': 5, 'catch': 3}
    assert names == {1: 'Host'}
    print("self-check OK")

    run_mapper_scan(max_pages=3)
