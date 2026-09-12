"""Global mapper leaderboard: total playcount across every ranked/loved difficulty a mapper made.

Guest difficulties count for whoever made them, and a collab difficulty counts in full for
every one of its authors.

The scan is built to survive a bad night: it checkpoints as it goes, so a crash, a
rate-limit wall or a dead runner costs only the pages since the last checkpoint, and it
refuses to publish a leaderboard it could not finish.
"""
import concurrent.futures
import os
import pickle
import threading
import time
from collections import Counter, defaultdict
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
API_USER_URL = 'https://osu.ppy.sh/api/v2/users'
SEARCH_URL = 'https://osu.ppy.sh/beatmapsets/search'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
}
TOP_N = None  # keep every mapper: the whole ladder is ~8k entries

# Retry waits in seconds. The monthly job has hours to spare, so a request gets ~18 minutes
# of escalating patience before the scan gives up on it and checkpoints instead.
RETRY_WAITS = (0, 5, 15, 45, 120, 300, 600)
# The token endpoint throttles too, and it hands back None rather than raising.
TOKEN_WAITS = (0, 30, 90, 300, 600)
# Longest throttle worth sitting through, in case a stray Retry-After says something absurd.
# The observed wait is ~28 minutes; the scan has hours, so the cap is slack, not a budget.
TOKEN_THROTTLE_CAP = 40 * 60
CHECKPOINT_EVERY = 20  # pages
STATE_PATH = os.environ.get('MAPPER_SCAN_STATE', 'mapper_scan_state.pickle')
# Verified cap: asking for 51 ids returns 50 with no error, so never chunk larger.
BEATMAP_IDS_PER_CALL = 50
# Workers cannot outrun the shared limiter - it hands out one slot at a time - so they only
# buy latency overlap. What extra ones do buy is waste: every thread already in flight when
# a 429 lands burns a retry on it and reports the same hold. The 2026-09-11 run logged the
# throttle eight times in a row, once per worker, for one throttle.
PROFILE_WORKERS = int(os.environ.get('PROFILE_WORKERS', 3))
# Every request in this module goes through one shared throttle, so the rate is a property
# of the scan rather than of whichever phase happens to be running.
#
# The old per-call sleeps paced each thread on its own: 8 profile workers 0.5s apart came to
# ~960 requests a minute. That sits under the burst the API refuses outright but far over
# what it sustains, so an hour in it started throttling everything, and each thread then
# backed off alone while the other seven kept the pressure on. The 2026-09-11 run spent
# nearly four hours that way and never finished.
RATE_PER_MIN = float(os.environ.get('OSU_RATE_PER_MIN', 300))
# The profile pass is enrichment on top of a finished crawl, so it gets a clock. Without one
# a throttled API keeps it retrying until the runner's job timeout kills the whole run and
# the ladder is never saved at all - the crawl's work thrown away for the optional part.
PROFILE_BUDGET = float(os.environ.get('PROFILE_BUDGET_MIN', 90)) * 60
# Bumped whenever the checkpoint layout changes, so an old one is discarded, not misread.
STATE_VERSION = 5


class AuthRejected(Exception):
    """The API refused our token. Waiting cannot fix that, so never retry it."""


def authenticate():
    """Get an API token, insisting when credentials exist.

    Without credentials the scan runs on the public search, losing collab credit and
    explicit mapsets. That is a fine fallback for someone who never configured any, but a
    silent one for someone who did, whose token endpoint is merely throttled.
    """
    configured = bool(scan_logic.CLIENT_ID and scan_logic.CLIENT_SECRET)
    throttled = False
    for wait in TOKEN_WAITS:
        if wait:
            print(f"Could not get an API token, retrying in {wait}s...", flush=True)
            time.sleep(wait)
        token = scan_logic.get_token()
        if token:
            return token

        held = scan_logic.token_retry_after
        if held:
            # TOKEN_WAITS adds up to ~17 minutes, but a throttled token endpoint asks for
            # ~28. Escalating politely through the rest of the attempts just spends them all
            # on a door that is not open yet, and then blames the credentials. Wait the time
            # it actually asked for instead.
            throttled = True
            held = min(held, TOKEN_THROTTLE_CAP)
            print(f"Waiting out the {held / 60:.0f} min throttle rather than spending the "
                  f"remaining attempts on a closed door.", flush=True)
            time.sleep(held)
            token = scan_logic.get_token()
            if token:
                return token

        if not configured:
            return None
    raise RuntimeError(
        "osu! credentials are configured but no token could be obtained"
        + (", and the token endpoint was throttling rather than refusing - so this is a rate"
           " limit to wait out, not a credentials problem." if throttled else
           ". The endpoint refused them rather than throttling, so check OSU_CLIENT_ID and"
           " OSU_CLIENT_SECRET.")
        + " Refusing to fall back to the public search, which would publish a ladder "
          "without collab credit or explicit mapsets.")


class RateLimiter:
    """One throttle shared by every thread, so the scan has a single overall request rate.

    Spacing requests here rather than sleeping inside each caller is what keeps the total
    rate flat no matter how many workers are running. `pause` exists because a 429 is a
    statement about the whole scan, not about the one thread that happened to receive it:
    backing that thread off alone leaves the others hammering and the throttle never lifts.
    """

    def __init__(self, per_minute):
        self.gap = 60.0 / per_minute if per_minute > 0 else 0.0
        self._lock = threading.Lock()
        self._next_at = 0.0

    def wait(self, deadline=None):
        """Take the next slot. Returns False if that slot falls past `deadline`.

        A 429 can park the queue for half an hour. Without this check the workers sit out
        the whole hold and only then notice their budget expired, so a 90 minute profile
        pass could run 120 - the deadline is not a deadline if the sleep ignores it.
        """
        with self._lock:
            now = time.monotonic()
            due = max(now, self._next_at)
            if deadline is not None and due > deadline:
                return False        # slot not consumed: it belongs to whoever still has time
            self._next_at = due + self.gap
        if due > now:
            time.sleep(due - now)
        return True

    def pause(self, seconds):
        """Hold every thread back for `seconds`, starting now."""
        with self._lock:
            self._next_at = max(self._next_at, time.monotonic() + seconds)


RATE = RateLimiter(RATE_PER_MIN)


def get(session, url, headers, params=None, deadline=None):
    """A GET that keeps trying. Returns the response, or None once the patience runs out.

    `deadline` (a time.monotonic() stamp) caps that patience for callers whose work is
    optional: past it the retries stop immediately instead of sitting out another backoff.
    """
    for wait in RETRY_WAITS:
        if deadline and time.monotonic() > deadline:
            return None
        if wait:
            time.sleep(wait)
        if not RATE.wait(deadline):
            return None
        try:
            r = session.get(url, headers=headers, params=params, timeout=30)
        except requests.exceptions.RequestException as e:
            print(f"Request error ({url}): {e.__class__.__name__}, retrying...", flush=True)
            continue
        if r.status_code == 200:
            return r
        if r.status_code in (401, 403):
            raise AuthRejected(f"{r.status_code} from {url}")
        if r.status_code == 404:
            return None
        if r.status_code == 429:
            # Retry-After is the API telling us exactly how long it wants; guessing shorter
            # just spends the retry budget re-tripping the same limit.
            try:
                held = max(1.0, float(r.headers.get('Retry-After') or 60))
            except ValueError:
                held = 60.0
            print(f"Rate limited (429), holding every thread {held:.0f}s...", flush=True)
            RATE.pause(held)
            continue
        # The status code is the difference between "slow down" and "the API is down",
        # so it goes in the log rather than a bare "request failed".
        print(f"Request failed ({r.status_code}) for {url}, retrying...", flush=True)
    return None


# ---- Checkpointing ----

def _int_dd():
    """Module-level so the stats structure stays picklable."""
    return defaultdict(int)


# Every difficulty falls in one of these four buckets. Keeping them apart is what lets the
# ladder answer "with or without guest difficulties" without a second scan.
BUCKETS = (('ranked', 'own'), ('ranked', 'guest'), ('loved', 'own'), ('loved', 'guest'))


ROLES = ('own', 'guest')


def new_stats():
    """Per-mapper totals, split by map status and by whether the mapper hosted the set."""
    stats = {bucket: {
        'pc': defaultdict(int),
        'maps': defaultdict(int),
        'mode_pc': defaultdict(_int_dd),
        'mode_maps': defaultdict(_int_dd),
    } for bucket in BUCKETS}
    # Mapset counts are keyed by role alone: a set holding both loved and ranked
    # difficulties would otherwise be counted once per status.
    stats['sets'] = {role: {'count': defaultdict(int), 'modes': defaultdict(_int_dd)}
                     for role in ROLES}
    return stats


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


# ---- Cross-run cache ----

# The run checkpoint above is deleted on success; this file is not. It holds only the parts
# of a scan that do not change between runs, so a daily scan can re-read every playcount -
# the number the ladder is actually built on, and the cheap half of the scan - while
# skipping the lookups that cost 25x more and return the same answer.
#
# Nothing volatile belongs in here. Playcounts are never cached at any depth: they come free
# with the search pages the crawl reads in full every single run.
CACHE_PATH = os.environ.get('MAPPER_SCAN_CACHE', 'mapper_scan_cache.pickle')
CACHE_VERSION = 1
# A difficulty's author list is fixed when it ranks - but not absolutely: mappers do get
# added to or removed from one afterwards. So the cache is never trusted indefinitely. Every
# run re-reads the slice whose id falls due, which turns the whole cache over this many runs
# and puts a hard ceiling on how stale any entry can be. ~140 requests a day at 30.
# Set to 0 to distrust the cache entirely and re-read every difficulty.
OWNERS_ROTATION = int(os.environ.get('OWNERS_ROTATION', 30))
# Same idea for profile counts. The crawl predicts most of their movement, but not all: a set
# leaving qualified, or a graveyard set revived, moves a profile count without touching
# anything the crawl can see. The rotation is what catches those.
PROFILE_ROTATION = int(os.environ.get('PROFILE_ROTATION', 30))


def new_cache():
    return {'version': CACHE_VERSION, 'runs': 0,
            'owners': {}, 'profiles': {}, 'profile_basis': {}}


def load_cache(path=None):
    """Read the cross-run cache, or a fresh empty one.

    Every doubt about it - missing, truncated, half-written, left by an older layout -
    resolves to an empty cache. That costs a full scan, which is slow; the alternative is a
    wrong ladder, which is worse and silent.
    """
    try:
        with open(path or CACHE_PATH, 'rb') as f:
            cache = pickle.load(f)
    except (FileNotFoundError, EOFError, pickle.UnpicklingError, AttributeError, ImportError):
        return new_cache()
    if cache.get('version') != CACHE_VERSION:
        print("Cache was written by an older scan layout; rebuilding it.", flush=True)
        return new_cache()
    # A cache missing a key it should have is a cache we do not understand.
    if not all(k in cache for k in ('runs', 'owners', 'profiles', 'profile_basis')):
        return new_cache()
    return cache


def save_cache(cache, path=None):
    """Write the cache atomically, so a killed runner cannot leave a torn one behind."""
    cache['version'] = CACHE_VERSION
    path = path or CACHE_PATH
    tmp = f'{path}.tmp'
    with open(tmp, 'wb') as f:
        pickle.dump(cache, f, protocol=pickle.HIGHEST_PROTOCOL)
    os.replace(tmp, path)


def due_for_recheck(key, run, rotation):
    """Whether this id is in the slice being re-read from the API this run.

    Spreading by id rather than by a stored timestamp means uniform coverage, no per-entry
    bookkeeping, and a guarantee that every entry comes up exactly once per `rotation` runs.
    """
    if rotation <= 0:
        return True
    return key % rotation == run % rotation


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


def resolve_owners(session, beatmapsets, token, mode, need=None):
    """Map every difficulty on these mapsets to the user ids credited with it.

    `need`, when given, limits the lookup to the difficulties actually worth a request; the
    caller supplies the rest from its cross-run cache. An empty `need` means every
    difficulty on the page was already known, and the page costs nothing.

    Returns None if a lookup could not be completed, so the caller checkpoints rather than
    aggregating a page with half of its collabs missing.
    """
    if mode == 'none' or not beatmapsets:
        return {}
    if need is not None and not need:
        return {}
    if not token:
        return None
    headers = {**HEADERS, 'Authorization': f'Bearer {token}'}
    owners = {}

    try:
        if mode == 'bulk':
            ids = [bmap['id'] for bset in beatmapsets for bmap in bset.get('beatmaps', [])]
            if need is not None:
                ids = [i for i in ids if i in need]
            for i in range(0, len(ids), BEATMAP_IDS_PER_CALL):
                r = get(session, API_BEATMAPS_URL, headers, {'ids[]': ids[i:i + BEATMAP_IDS_PER_CALL]})
                if r is None:
                    return None
                for bmap in r.json().get('beatmaps') or []:
                    owners[bmap['id']] = [o['id'] for o in (bmap.get('owners') or [])]
            return owners

        for bset in beatmapsets:
            # One call returns the whole mapset, so it is only worth making when at least one
            # difficulty on it is actually wanted.
            if need is not None and not any(b.get('id') in need
                                            for b in bset.get('beatmaps') or []):
                continue
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
    owners = owners or ([bmap['user_id']] if bmap.get('user_id') else [])
    # An id repeated in owners would credit the same mapper twice for one difficulty.
    return list(dict.fromkeys(owners))


# ---- Official profile counts ----

# What osu! prints on a profile, and what a mapper will compare our ladder against.
# Per category: the field we store, osu!'s own field, and where its mode split goes.
PROFILE_KINDS = {
    'ranked': ('profile_ranked_sets', 'ranked_beatmapset_count', 'profile_ranked_by_mode'),
    'loved': ('profile_loved_sets', 'loved_beatmapset_count', 'profile_loved_by_mode'),
    'guest': ('profile_guest_sets', 'guest_beatmapset_count', 'profile_guest_by_mode'),
}
SETS_PAGE = 100
# Ties go by this order, so the same set never changes column between scans.
MODE_ORDER = ('osu', 'taiko', 'catch', 'mania')
# What the crawl's corpus is made of. A set outside these is not missing from the ladder:
# qualified and graveyard sets were never meant to be on it.
CORPUS_STATUS = ('ranked', 'approved', 'loved')


def dominant_mode(modes):
    """The one mode a set counts under, given a mapper's difficulty counts per mode.

    Ties go by MODE_ORDER so the same set never changes column between scans. Shared by the
    crawl and the profile pass: two routes to the same figure have to file a set the same
    way, or the per-mode columns disagree with each other depending on which one ran.
    """
    if not modes:
        return None
    return max(modes, key=lambda m: (modes[m], -(MODE_ORDER + (m,)).index(m)))


def set_mode(diffs, uid=None):
    """The one mode a set counts under: where its mapper put the most difficulties.

    A set is a set. Filing a hybrid under every mode it touches is what stops the columns
    adding up to the total osu! prints, and a mapper who did the taiko half of a std set
    ranked one taiko set, not one of each.
    """
    modes = Counter('catch' if b.get('mode') == 'fruits' else b.get('mode')
                    for b in diffs if uid is None or b.get('user_id') == uid)
    modes.pop(None, None)
    return dominant_mode(modes)


def fetch_profile_counts(session, uids, token, progress=None, budget=None,
                         seen=None, harvest=None):
    """Read each mapper's mapset counts off their profile, split by the mode they mapped in.

    Our crawl pages the search index, which runs short of what a profile prints: it never
    sees qualified sets, and misses some ranked ones outright. The profile is the number a
    mapper checks us against, so it wins, and the category listings behind it are the only
    place the split by mode can come from honestly.

    A set is counted once, under the mode the mapper actually worked in on it. Counting the
    modes a set *contains* instead would file one hybrid set under two modes and the columns
    would stop adding up to the total, which is the whole complaint this answers.

    Given `seen` (the set ids the crawl reached) and a `harvest` dict, every leaderboarded
    set in these listings that the crawl never saw is stashed there for the caller to fold
    in. They cost nothing: the listings are being read anyway, and they carry each set in
    full, difficulty playcounts included.
    """
    if not token:
        return {}
    headers = {**HEADERS, 'Authorization': f'Bearer {token}'}
    counts = {}
    deadline = time.monotonic() + (PROFILE_BUDGET if budget is None else budget)

    def own_modes(uid, kind, count, host, found=None):
        """Which mode a mapper worked in on each of their sets in one category.

        osu! stores one author per difficulty and lists the rest under `owners`, which the
        category listing omits, so a collab looks like it belongs to nobody. Those sets get
        a second look through the beatmaps endpoint rather than being dropped.
        """
        modes = defaultdict(int)
        collabs = []
        for offset in range(0, count, SETS_PAGE):
            r = get(session, f'{API_USER_URL}/{uid}/beatmapsets/{kind}', headers,
                    {'limit': SETS_PAGE, 'offset': offset}, deadline=deadline)
            if r is None:
                return None  # A short read would under-report; drop the split, keep the count.
            for bset in r.json() or []:
                # A set the crawl never reached. A DMCA takedown leaves a mapset ranked,
                # playable and counted on its mapper's profile, but drops it from every
                # search page - so this listing is the only place its plays turn up.
                if found is not None and bset.get('id') not in seen \
                        and bset.get('status') in CORPUS_STATUS:
                    found[bset['id']] = bset
                diffs = bset.get('beatmaps') or []
                mine = set_mode(diffs, uid)
                # Getting a set ranked makes it yours even when every difficulty is a guest's,
                # exactly as the crawl credits it and as osu! counts it on the profile.
                if mine is None and host:
                    mine = set_mode(diffs)
                if mine is not None:
                    modes[mine] += 1
                else:
                    collabs.append(diffs)

        ids = [b['id'] for diffs in collabs for b in diffs if b.get('id')]
        owners = {}
        for i in range(0, len(ids), BEATMAP_IDS_PER_CALL):
            r = get(session, API_BEATMAPS_URL, headers, {'ids[]': ids[i:i + BEATMAP_IDS_PER_CALL]},
                    deadline=deadline)
            if r is None:
                return None
            for bmap in r.json().get('beatmaps') or []:
                owners[bmap['id']] = [o['id'] for o in (bmap.get('owners') or [])]
        for diffs in collabs:
            mine = set_mode([b for b in diffs if uid in owners.get(b.get('id'), [])])
            if mine is not None:
                modes[mine] += 1
        return dict(modes)

    def one(uid):
        # Checked before the request so the mappers still queued behind a throttle return at
        # once, instead of each sitting through its own backoff chain long past the deadline.
        if time.monotonic() > deadline:
            return uid, None, None
        r = get(session, f'{API_USER_URL}/{uid}', headers, {'key': 'id'}, deadline=deadline)
        if r is None:
            return uid, None, None
        data = r.json()
        row = {}
        # Filled by the worker, merged by the collector below: three threads sharing one
        # harvest dict would be one more thing to get right for nothing.
        found = {} if harvest is not None and seen is not None else None
        for kind, (count_key, src, mode_key) in PROFILE_KINDS.items():
            count = data.get(src) or 0
            row[count_key] = count
            row[mode_key] = (own_modes(uid, kind, count, kind != 'guest', found) or {}) if count else {}
        return uid, row, found

    with concurrent.futures.ThreadPoolExecutor(max_workers=PROFILE_WORKERS) as pool:
        futures = [pool.submit(one, uid) for uid in uids]
        for done, fut in enumerate(concurrent.futures.as_completed(futures), 1):
            try:
                uid, row, found = fut.result()
            except (AuthRejected, requests.exceptions.RequestException):
                continue
            if row:
                counts[uid] = row
            if found:
                # By set id, so a set two mappers both point at is folded once.
                harvest.update(found)
            if progress and done % 500 == 0:
                progress(f"Read {done}/{len(uids)} profiles...")
    return counts


def fold_unlisted(session, unlisted, token, owners_mode, stats, names, progress):
    """Fold in the mapsets the search index does not list, and say how many there were.

    The crawl pages the search index, so a set the index drops is invisible to it and its
    plays are missing from the ladder - even though the set is still ranked, still playable,
    and still counted on its mapper's profile. DMCA takedowns are what does that.

    The profile pass already read those sets in full while it was reading the listings for
    the mode split, so recovering them costs one owners lookup and nothing else.
    """
    if not unlisted:
        return 0
    sets = list(unlisted.values())
    owners = resolve_owners(session, sets, token, owners_mode)
    if owners is None:
        # Falling back to the stored author would hand one mapper a collaborator's plays.
        progress(f"{len(sets)} mapsets the search index does not list were found, but their "
                 f"authors could not be read; left out rather than credited to the wrong mapper.")
        return 0
    # Usernames were refreshed against the API before this runs, and a mapset carries the
    # name its host had when it ranked, so the listing's `creator` may be several renames
    # out of date. It is the fallback here, never the answer.
    stored = {}
    aggregate_page(sets, stats, stored, owners)
    for uid, name in stored.items():
        names.setdefault(uid, name)
    progress(f"Folded in {len(sets)} mapsets the search index does not list "
             f"(a DMCA takedown keeps a set ranked but drops it from search).")
    return len(sets)


# ---- Aggregation ----

def aggregate_page(beatmapsets, stats, names, owners_by_diff=None):
    """Folds one search page into the running per-mapper totals.

    Every mapset reaches this function once, so a mapper's set count is incremented here
    rather than tracked through a growing set of ids.
    """
    for bset in beatmapsets:
        if bset.get('creator'):
            names[bset['user_id']] = bset['creator']
        # (uid, role) -> how many difficulties the mapper made on this set, per mode. Counted
        # rather than collected, because the set is filed under the one mode they worked in
        # most, exactly as set_mode() files it from a profile listing.
        set_modes = defaultdict(Counter)
        set_modes_all = Counter()     # every mode present on the set, whoever mapped it
        for bmap in bset.get('beatmaps', []):
            # ponytail: a loved set can hold non-loved diffs; those land in the ranked bucket. ~0.1% of diffs.
            status = bmap.get('status') or bset.get('status')
            state = 'loved' if status == 'loved' else 'ranked'
            mode = 'catch' if bmap.get('mode') == 'fruits' else bmap.get('mode', 'osu')
            set_modes_all[mode] += 1
            pc = bmap.get('playcount') or 0
            # A collab counts in full for every author, so each co-mapper shows its plays.
            for uid in diff_owners(bmap, owners_by_diff):
                # Hosting the set makes it your own map; anyone else on it is a guest mapper.
                role = 'own' if uid == bset.get('user_id') else 'guest'
                b = stats[(state, role)]
                b['pc'][uid] += pc
                b['maps'][uid] += 1
                b['mode_pc'][uid][mode] += pc
                b['mode_maps'][uid][mode] += 1
                set_modes[(uid, role)][mode] += 1

        # Getting a set ranked makes it yours even when every difficulty on it is a guest's:
        # osu! counts it on your profile, so the mapset ladder counts it too. Playcount and
        # difficulty totals stay untouched — this credits the set, not work nobody did.
        host = bset.get('user_id')
        if host and set_modes_all and not set_modes[(host, 'own')]:
            set_modes[(host, 'own')] = Counter(set_modes_all)

        for (uid, role), modes in set_modes.items():
            stats['sets'][role]['count'][uid] += 1
            # One set, one mode. Adding it to every mode the mapper touched is what made
            # sets_by_mode overshoot the total for the 4% of mappers who work in more than
            # one mode on a single set - a hybrid mapper ranked one set, not one of each.
            stats['sets'][role]['modes'][uid][dominant_mode(modes)] += 1


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


# The search endpoint will not report a `total` above this, whatever the real figure is.
SEARCH_TOTAL_CAP = 10000


def corpus_total(session, token):
    """How many mapsets a full crawl should expect, or None if that cannot be established.

    A page's own `total` is capped at SEARCH_TOTAL_CAP, which made the progress line read
    "60000/10000" - and, far worse, quietly disabled the completeness guard, since a crawl
    that died at 15000 sets is not below 10000 * 0.99 either. Asking per status gets the real
    figures back, because each one is counted separately and comes in under the cap.

    Returns None rather than a guess: the caller treats an unknown total as "no guard", which
    is what it already did, instead of refusing to publish a scan that is actually complete.
    """
    headers = {**HEADERS, 'Authorization': f'Bearer {token}'} if token else dict(HEADERS)
    url = API_SEARCH_URL if token else SEARCH_URL
    total = 0
    for status in ('ranked', 'loved'):
        try:
            r = get(session, url, headers, {'s': status, 'nsfw': 'true'})
        except AuthRejected:
            return None
        if r is None:
            return None
        n = (r.json() or {}).get('total')
        # A missing total is unusable; a zero one is an answer. Exactly the cap means the
        # figure is the cap rather than the count - a per-status total is otherwise reported
        # in full, well past it (ranked alone is ~57k), so only the exact value is suspect.
        # A corpus of precisely 10000 reads as unknown, which disables the guard rather than
        # misfiring it.
        if n is None or n == SEARCH_TOTAL_CAP:
            return None
        total += n
    return total or None


def fetch_page(session, cursor, token):
    """One page of search results.

    Returns (data, token). The token comes back None once the API has refused it, so the
    caller stops paying the auth round trip on every remaining page.
    """
    # Oldest first, deliberately. Newest-first puts every freshly ranked set at the head of
    # the ordering, so a set that ranks mid-scan shifts everything below it down by one and
    # the cursor steps straight over a set it has not read yet. A scan that checkpoints and
    # resumes over days loses a handful of sets that way. Ascending appends new sets at the
    # end, where the cursor has not been, so nothing is ever skipped.
    params = {'sort': 'ranked_asc', 'nsfw': 'true'}
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

    token = authenticate()
    session = requests.Session()

    state = load_state() if resume else None
    if state:
        progress(f"Resuming from checkpoint: {len(state['seen'])} mapsets already scanned.")
    else:
        state = {'cursor': None, 'pages': 0, 'seen': set(), 'reported_total': None,
                 'stats': new_stats(), 'names': {}, 'owners_mode': None}

    stats, names = state['stats'], state['names']
    truncated = None

    cache = load_cache() if resume else new_cache()
    run_no = cache['runs']
    # Every difficulty this crawl actually saw. Used only to prune the cache at the end, and
    # only on a complete pass - pruning against a partial one would throw away good entries
    # for every set the crawl never reached.
    seen_diffs = set()
    owners_from_cache = owners_read = 0

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
            # Deliberately not data['total'], which is capped and would read as the corpus.
            state['reported_total'] = corpus_total(session, token)
            if state['reported_total']:
                progress(f"Expecting about {state['reported_total']} mapsets.")
            else:
                progress("Could not establish the corpus size; this pass cannot check itself "
                         "for completeness and will publish whatever it reaches.")

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

        # Owners are the expensive half of the crawl and the half that does not change, so
        # they come from the cache wherever possible. Playcounts, which do change, were read
        # fresh in the search page above and are never cached at all.
        use_owners_cache = state['owners_mode'] in ('bulk', 'set')
        known = cache['owners'] if use_owners_cache else {}
        page_diffs = [b['id'] for bset in fresh for b in bset.get('beatmaps') or []
                      if b.get('id') is not None]
        seen_diffs.update(page_diffs)
        # The rotation re-checks a whole page at a time, not scattered difficulty ids.
        # Scattering costs one lookup call on every page - a handful of ids never fills a
        # 50-id batch - so it would spend ~1200 calls a run to re-check ~1/30 of the cache.
        # By page, 29 runs in 30 cost nothing at all and the due page fills its batches.
        # Search order is ranked_asc, so page boundaries are stable between runs and every
        # page comes up exactly once per rotation.
        page_due = due_for_recheck(state['pages'], run_no, OWNERS_ROTATION)
        need = {d for d in page_diffs if page_due or d not in known}

        looked_up = resolve_owners(session, fresh, token, state['owners_mode'], need=need)
        if looked_up is None:
            truncated = f"owners lookup stopped responding after {state['pages']} pages"
            break

        owners = {d: known[d] for d in page_diffs if d in known and d not in need}
        owners_from_cache += len(owners)
        owners.update(looked_up)
        owners_read += len(looked_up)
        if use_owners_cache:
            cache['owners'].update(looked_up)

        aggregate_page(fresh, stats, names, owners)
        state['seen'].update(b['id'] for b in page_sets)
        state['pages'] += 1
        state['cursor'] = data.get('cursor_string')

        if state['pages'] % CHECKPOINT_EVERY == 0:
            save_state(state)
            # Alongside the checkpoint, so owners paid for before a crash are not paid twice.
            save_cache(cache)
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
        # Kept, not pruned: the owners read before the run died are still correct, and the
        # next run resuming from the checkpoint should not pay for them a second time.
        save_cache(cache)
        msg = (f"Incomplete scan ({truncated}). Previous leaderboard kept; "
               f"{total_sets} mapsets checkpointed, re-run to continue.")
        progress(msg)
        return {'error': msg, 'total_sets_scanned': total_sets, 'reported_total': state['reported_total']}

    if owners_from_cache or owners_read:
        progress(f"Owners: {owners_read} read from the API, {owners_from_cache} from cache "
                 f"(re-checking 1/{OWNERS_ROTATION or 1} of the cache each run).")

    all_ids = set()
    for bucket in BUCKETS:
        all_ids |= set(stats[bucket]['pc'])
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

    profiles = {}
    unlisted_sets = 0
    if token:
        # A mapper's official counts only move when their set count moves - which the crawl
        # has just measured for every mapper - so most of them need no request at all. The
        # crawled count is kept beside the cached row as the basis it was read against; when
        # the two disagree the row is stale by definition. The rotation slice is re-read
        # regardless, to catch the movements the crawl cannot see.
        crawled_sets = {uid: sum(stats['sets'][r]['count'][uid] for r in ROLES)
                        for uid in top_ids}
        cached_rows = cache['profiles']
        basis = cache['profile_basis']
        stale = [uid for uid in top_ids
                 if uid not in cached_rows
                 or basis.get(uid) != crawled_sets.get(uid)
                 or due_for_recheck(uid, run_no, PROFILE_ROTATION)]

        progress(f"Profiles: {len(top_ids) - len(stale)} unchanged since the last scan, "
                 f"reading {len(stale)} (up to {PROFILE_BUDGET / 60:.0f} min)...")
        session = requests.Session()
        # The listings this pass reads are also the only place a set the search index drops
        # can be recovered from, so it collects them on the way past.
        unlisted = {}
        readings = fetch_profile_counts(session, stale, token, progress,
                                        seen=state['seen'], harvest=unlisted)
        unlisted_sets = fold_unlisted(session, unlisted, token, state['owners_mode'],
                                      stats, names, progress)
        # A mapper whose only ranked set is one of those is new to the ladder here, after
        # top_ids was built. Appended, not re-sorted: the rows are ordered by playcount below.
        for uid in sorted({u for b in BUCKETS for u in stats[b]['pc']} - all_ids):
            all_ids.add(uid)
            top_ids.append(uid)
        session.close()

        for uid, row_data in readings.items():
            cached_rows[uid] = row_data
            basis[uid] = crawled_sets.get(uid)
        # A mapper whose read failed keeps its cached basis untouched, so the next run sees
        # the mismatch again and retries instead of treating the stale row as confirmed.
        for uid in stale:
            if uid not in readings:
                basis.pop(uid, None)

        profiles = {uid: cached_rows[uid] for uid in top_ids if uid in cached_rows}
        progress(f"Official counts now known for {len(profiles)}/{len(top_ids)} mappers "
                 f"({len(readings)} read this run).")
        if len(readings) < len(stale):
            progress("The profile pass ran out of budget or hit throttling. Publishing the "
                     "ladder anyway: playcounts come from the crawl, which is complete, and "
                     "the mappers it missed are retried next run.")
    else:
        progress("No API token: mapset counts stay as crawled, not as osu! shows them.")

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
            'loved_maps': pick(loved_b, 'maps'),
            'loved_maps_by_mode': modes(loved_b, 'mode_maps'),
            'own_playcount': pick(own, 'pc'),
            'own_maps': pick(own, 'maps'),
            'own_by_mode': modes(own, 'mode_pc'),
            'own_maps_by_mode': modes(own, 'mode_maps'),
            'own_loved_playcount': stats[('loved', 'own')]['pc'][uid],
            'own_loved_by_mode': dict(stats[('loved', 'own')]['mode_pc'][uid]),
            'own_loved_maps': stats[('loved', 'own')]['maps'][uid],
            'own_loved_maps_by_mode': dict(stats[('loved', 'own')]['mode_maps'][uid]),
            'sets': sum(stats['sets'][r]['count'][uid] for r in ROLES),
            'own_sets': stats['sets']['own']['count'][uid],
            'sets_by_mode': merge_modes(*(stats['sets'][r]['modes'][uid] for r in ROLES)),
            'own_sets_by_mode': dict(stats['sets']['own']['modes'][uid]),
            **profiles.get(uid, {}),
        }

    # top_ids was ordered before the fold above, which can add plays to any mapper.
    mappers = sorted((row(uid) for uid in top_ids), key=lambda m: -m['playcount'])

    result = {
        'last_scan': datetime.utcnow().isoformat(),
        # The crawl's corpus, plus whatever the fold recovered from outside the index.
        'total_sets_scanned': total_sets + unlisted_sets,
        'reported_total': state['reported_total'],
        'total_mappers': len(all_ids),
        'collab_credit': state['owners_mode'] != 'none',
        'names_current': names_current,
        # Firebase stores an empty object as nothing, so a mapper with an empty category
        # loses its split. One flag for the scan says the split was read, per row or not.
        'profile_modes': bool(profiles),
        # What this pass actually re-read. Playcounts are always 100% fresh - they ride in
        # on the search pages - so these say how much of the *static* half was trusted from
        # cache, which is the only thing that can go stale.
        'owners_read': owners_read,
        'owners_cached': owners_from_cache,
        'owners_rotation': OWNERS_ROTATION,
        'mappers': mappers,
    }

    progress("Saving mapper leaderboard to Firebase...")
    save_to_firebase(result, path='mappers')

    # Only a complete pass may prune. Dropping entries the crawl simply never reached would
    # quietly bill the next run for work already paid for, and on a run that died early it
    # would throw away most of the cache.
    cache['owners'] = {d: o for d, o in cache['owners'].items() if d in seen_diffs}
    live = set(top_ids)
    cache['profiles'] = {u: r for u, r in cache['profiles'].items() if u in live}
    cache['profile_basis'] = {u: b for u, b in cache['profile_basis'].items() if u in live}
    cache['runs'] = run_no + 1
    save_cache(cache)

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
        {'user_id': 7, 'creator': 'GhostHost', 'status': 'ranked', 'beatmaps': [
            {'id': 20, 'user_id': 8, 'mode': 'taiko', 'playcount': 9, 'status': 'ranked'},
        ]},
    ], stats, names)
    own, guest = stats[('ranked', 'own')], stats[('ranked', 'guest')]
    # Host 1 mapped one diff on their own set; user 2 guest-mapped two diffs on it.
    assert dict(own['pc']) == {1: 10, 3: 7, 4: 50}, dict(own['pc'])
    assert dict(guest['pc']) == {2: 8, 5: 50, 6: 50, 8: 9}, dict(guest['pc'])
    # The collab's 50 plays land in full on all three authors, not split between them,
    # and only the host counts them as his own map.
    assert own['maps'][4] == 1 and guest['maps'][5] == 1 and guest['maps'][6] == 1
    assert diff_owners({'id': 99, 'user_id': 4}, {99: [4, 5]}) == [4, 5]
    assert diff_owners({'id': 7, 'user_id': 4}) == [4], "no owners info falls back to the stored author"
    assert diff_owners({'id': 8, 'user_id': 4}, {8: [4, 5, 4]}) == [4, 5], "a repeated owner must count once"
    assert diff_owners({'id': 9, 'user_id': 4, 'owners': [{'id': 5}, {'id': 5}]}) == [5]
    assert dict(stats[('loved', 'guest')]['pc']) == {2: 100}
    # A non-loved diff inside a loved set falls in the ranked bucket, credited to its host.
    assert dict(stats[('loved', 'own')]['pc']) == {}
    assert own['pc'][3] == 7

    # Mapset counts: user 2 guest-mapped two diffs on set 1, which is still one mapset.
    sets_own, sets_gd = stats['sets']['own']['count'], stats['sets']['guest']['count']
    assert dict(sets_own) == {1: 1, 3: 1, 4: 1, 7: 1}, dict(sets_own)
    assert dict(sets_gd) == {2: 2, 5: 1, 6: 1, 8: 1}, dict(sets_gd)
    # Host 7 mapped nothing on their own set: the mapset counts, the difficulty does not.
    assert own['maps'][7] == 0 and own['pc'][7] == 0
    assert dict(stats['sets']['own']['modes'][7]) == {'taiko': 1}
    # The loved set holds one diff of user 2 and one of its host: one mapset each, not two.
    assert sets_gd[2] == 2 and sets_own[3] == 1
    # User 2 mapped one osu and one catch diff on set 1, and one osu diff on the loved set.
    # That is two mapsets, so two is what the columns must add to - the catch diff moves the
    # set's column only if it outnumbers the others, and here it ties and loses on MODE_ORDER.
    assert dict(stats['sets']['guest']['modes'][2]) == {'osu': 2}, dict(stats['sets']['guest']['modes'][2])

    # The invariant the whole per-mode split exists to keep: a set is filed under exactly one
    # mode, so every mapper's columns add back up to the figure shown on "All".
    for role in ROLES:
        counts, modes = stats['sets'][role]['count'], stats['sets'][role]['modes']
        for uid, total in counts.items():
            assert sum(modes[uid].values()) == total, (
                f"{role} sets for {uid}: columns {dict(modes[uid])} do not sum to {total}")

    assert dominant_mode(Counter({'osu': 1, 'taiko': 3})) == 'taiko', "the majority mode wins"
    assert dominant_mode(Counter({'taiko': 2, 'osu': 2})) == 'osu', "ties go by MODE_ORDER"
    assert dominant_mode(Counter()) is None
    assert dict(guest['mode_pc'][2]) == {'osu': 5, 'catch': 3}
    assert merge_modes(guest['mode_pc'][2], stats[('loved', 'guest')]['mode_pc'][2]) == {'osu': 105, 'catch': 3}
    assert names == {1: 'Host', 3: 'LovedHost', 4: 'CollabHost', 7: 'GhostHost'}

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

    # The whole point of the profile pass: osu!'s own counts, split by the mode the mapper
    # worked in, each set landing in exactly one column so the columns add up to the count.
    ME = 33599
    class Profiles:
        status_code = 200
        def get(self, url, params=None, **k):
            self.url, self.status_code = url, 200
            return self
        def json(self):
            if self.url.endswith('/beatmaps'):     # the collab's real authors
                return {'beatmaps': [{'id': 9, 'owners': [{'id': ME}, {'id': 7}]}]}
            if self.url.endswith('/ranked'):
                return [
                    # Hers: a hybrid set she mapped the osu half of. One set, one column.
                    {'beatmaps': [{'id': 1, 'mode': 'osu', 'user_id': ME},
                                  {'id': 2, 'mode': 'taiko', 'user_id': 7}]},
                    # Hosted, every difficulty a guest's: still hers, and still one set.
                    {'beatmaps': [{'id': 3, 'mode': 'mania', 'user_id': 7}]},
                    # Both halves hers: one set, filed where she did the most work.
                    {'beatmaps': [{'id': 10, 'mode': 'taiko', 'user_id': ME},
                                  {'id': 11, 'mode': 'mania', 'user_id': ME},
                                  {'id': 12, 'mode': 'mania', 'user_id': ME}]},
                ]
            if self.url.endswith('/loved'):
                return [{'beatmaps': [{'id': 4, 'mode': 'fruits', 'user_id': ME}]}]
            if self.url.endswith('/guest'):
                return [
                    {'beatmaps': [{'id': 5, 'mode': 'taiko', 'user_id': ME},
                                  {'id': 6, 'mode': 'osu', 'user_id': 7}]},
                    # A collab: the author on record is someone else, owners knows better.
                    {'beatmaps': [{'id': 9, 'mode': 'osu', 'user_id': 7}]},
                ]
            return {'ranked_beatmapset_count': 3, 'loved_beatmapset_count': 1,
                    'guest_beatmapset_count': 2}
    got = fetch_profile_counts(Profiles(), [ME], 'token')
    assert got == {ME: {
        'profile_ranked_sets': 3, 'profile_ranked_by_mode': {'osu': 1, 'mania': 2},
        'profile_loved_sets': 1, 'profile_loved_by_mode': {'catch': 1},
        'profile_guest_sets': 2, 'profile_guest_by_mode': {'taiko': 1, 'osu': 1},
    }}, got
    for count_key, mode_key in [('profile_ranked_sets', 'profile_ranked_by_mode'),
                                ('profile_guest_sets', 'profile_guest_by_mode')]:
        assert sum(got[ME][mode_key].values()) == got[ME][count_key], \
            f"{mode_key} must add up to {count_key}: {got[ME]}"
    assert fetch_profile_counts(Profiles(), [ME], None) == {}, "no token means no counts"

    # A category listing that fails must cost the split, never the official count.
    class HalfProfile(Profiles):
        def get(self, url, params=None, **k):
            self.url = url
            self.status_code = 404 if url.endswith('/ranked') else 200
            return self
    got = fetch_profile_counts(HalfProfile(), [ME], 'token')
    assert got[ME]['profile_ranked_sets'] == 3, got
    assert got[ME]['profile_ranked_by_mode'] == {}, got
    print("profile pass OK: counts official, modes add up, collabs credited")

    # A DMCA takedown keeps a set ranked and on the profile but drops it from search, so the
    # crawl never sees it. The listings the pass just read are where it comes back from.
    class Unlisted(Profiles):
        def json(self):
            if self.url.endswith('/ranked'):
                return [
                    {'id': 100, 'user_id': ME, 'creator': 'osuplayer111', 'status': 'ranked',
                     'beatmaps': [{'id': 500, 'mode': 'osu', 'user_id': ME,
                                   'status': 'ranked', 'playcount': 7}]},
                    # Qualified is not missing from the ladder, it was never meant to be on it.
                    {'id': 101, 'user_id': ME, 'creator': 'Andrea', 'status': 'qualified',
                     'beatmaps': [{'id': 501, 'mode': 'osu', 'user_id': ME,
                                   'status': 'qualified', 'playcount': 9}]},
                    # Already crawled: folding it in again would double its plays.
                    {'id': 102, 'user_id': ME, 'creator': 'Andrea', 'status': 'ranked',
                     'beatmaps': [{'id': 502, 'mode': 'osu', 'user_id': ME,
                                   'status': 'ranked', 'playcount': 3}]},
                ]
            return Profiles.json(self)

    harvest = {}
    counts = fetch_profile_counts(Unlisted(), [ME], 'token', seen={102}, harvest=harvest)
    assert set(harvest) == {100}, f"only unseen, leaderboarded sets are worth folding: {harvest}"
    assert counts[ME]['profile_ranked_sets'] == 3, "harvesting must not disturb the counts"
    assert fetch_profile_counts(Unlisted(), [ME], 'token') is not None, "harvest stays optional"

    folded_stats, folded_names = new_stats(), {}
    assert fold_unlisted(Profiles(), harvest, 'token', 'bulk',
                         folded_stats, folded_names, lambda _m: None) == 1
    assert folded_stats[('ranked', 'own')]['pc'][ME] == 7, "the recovered plays must land"
    assert folded_stats['sets']['own']['count'][ME] == 1, "and so must the set"
    assert folded_names[ME] == 'osuplayer111', "a mapper new to the ladder still gets a name"
    # Usernames are refreshed before the fold runs, and a mapset carries the name its host
    # had when it ranked - so the fold must not drag a rename back.
    refreshed = {ME: 'Andrea'}
    fold_unlisted(Profiles(), harvest, 'token', 'bulk', new_stats(), refreshed, lambda _m: None)
    assert refreshed[ME] == 'Andrea', "the fold must not overwrite a refreshed username"

    # No authors, no credit: guessing would hand one mapper a collaborator's plays.
    class NoOwners(Profiles):
        def get(self, url, params=None, **k):
            self.url = url
            self.status_code = 404 if url.endswith('/beatmaps') else 200
            return self
    empty = new_stats()
    assert fold_unlisted(NoOwners(), harvest, 'token', 'bulk', empty, {}, lambda _m: None) == 0
    assert not empty[('ranked', 'own')]['pc'], "a failed owners lookup must fold nothing"
    assert fold_unlisted(Profiles(), {}, 'token', 'bulk', empty, {}, lambda _m: None) == 0
    print("unlisted sets OK: recovered from the profile listings, qualified and seen ones left out")

    # Configured credentials must never degrade to the public search in silence.
    _real_token = scan_logic.get_token
    scan_logic.get_token = lambda: None
    try:
        _id, _sec = scan_logic.CLIENT_ID, scan_logic.CLIENT_SECRET
        scan_logic.CLIENT_ID = scan_logic.CLIENT_SECRET = None
        assert authenticate() is None, "no credentials means the public search is fine"
        scan_logic.CLIENT_ID, scan_logic.CLIENT_SECRET = 'id', 'secret'
        globals()['TOKEN_WAITS'] = (0,)
        try:
            authenticate()
            raise AssertionError("configured credentials that fail must raise")
        except RuntimeError:
            pass
        print("auth policy OK: silent degradation only when nothing is configured")
    finally:
        scan_logic.get_token = _real_token
        scan_logic.CLIENT_ID, scan_logic.CLIENT_SECRET = _id, _sec
        globals()['TOKEN_WAITS'] = (0, 30, 90, 300, 600)

    # Live check of the network path; a full run is what run_mapper_scan is for.
    page, _ = fetch_page(requests.Session(), None, scan_logic.get_token())
    assert page and page['beatmapsets'], "search returned nothing"
    print(f"live fetch OK: {len(page['beatmapsets'])} sets of {page.get('total')} reported")
