import json
import os
import tempfile
import unittest
from contextlib import ExitStack
from unittest import mock

os.environ.setdefault('OSU_CLIENT_ID', 'test-client')
os.environ.setdefault('OSU_CLIENT_SECRET', 'test-secret')

import app as web_app
import mapper_scan
import scan_logic
from mapper_scan import BUCKETS, HG_LOVED_MAPS, HG_LOVED_PC, HG_MAPS, HG_PC, HG_SETS


def sets():
    return [
        # Host 1 maps one diff; guest 2 maps an osu and a catch diff - one set, filed osu.
        {'id': 100, 'user_id': 1, 'creator': 'Host', 'status': 'ranked', 'beatmaps': [
            {'id': 11, 'user_id': 1, 'mode': 'osu', 'playcount': 10, 'status': 'ranked'},
            {'id': 12, 'user_id': 2, 'mode': 'osu', 'playcount': 5, 'status': 'ranked'},
            {'id': 13, 'user_id': 2, 'mode': 'fruits', 'playcount': 3, 'status': 'ranked'},
        ]},
        # A loved set of host 3's with a guest diff by 2 and one ranked-status diff by 2.
        {'id': 101, 'user_id': 3, 'creator': 'LovedHost', 'status': 'loved', 'beatmaps': [
            {'id': 14, 'user_id': 2, 'mode': 'osu', 'playcount': 100, 'status': 'loved'},
            {'id': 15, 'user_id': 2, 'mode': 'taiko', 'playcount': 7, 'status': 'graveyard'},
        ]},
        # A collab of host 1 with 4 and 5: the host's share is theirs, the rest are guests.
        {'id': 102, 'user_id': 1, 'creator': 'Host', 'status': 'ranked', 'beatmaps': [
            {'id': 99, 'user_id': 1, 'mode': 'mania', 'playcount': 50, 'status': 'ranked',
             'owners': [{'id': 1}, {'id': 4}, {'id': 5}]},
        ]},
        # Guest 2 again for host 1, on a second set.
        {'id': 103, 'user_id': 1, 'creator': 'Host', 'status': 'ranked', 'beatmaps': [
            {'id': 20, 'user_id': 2, 'mode': 'osu', 'playcount': 9, 'status': 'ranked'},
        ]},
    ]


class AggregateByHostTests(unittest.TestCase):
    def setUp(self):
        self.stats = mapper_scan.new_stats()
        mapper_scan.aggregate_page(sets(), self.stats, {})
        self.by_host = self.stats['by_host']

    def test_records_each_guest_under_the_host_of_the_set(self):
        self.assertEqual(set(self.by_host), {1, 3})
        self.assertEqual(set(self.by_host[1]), {2, 4, 5}, 'the host is never their own guest')
        # Two sets for host 1, both filed osu; the catch diff counts its plays under catch.
        self.assertEqual(self.by_host[1][2], {'osu': [14, 2, 2, 0, 0], 'catch': [3, 1, 0, 0, 0]})
        self.assertEqual(self.by_host[1][4], {'mania': [50, 1, 1, 0, 0]})
        # The loved diff is loved; the graveyard one inside the loved set is not, as on the board.
        self.assertEqual(self.by_host[3][2], {'osu': [100, 1, 1, 100, 1], 'taiko': [7, 1, 0, 0, 0]})

    def test_leaving_out_every_host_takes_each_guest_figure_to_zero(self):
        """The invariant the board relies on: per-host figures add up to the guest totals."""
        guest_buckets = [b for b in BUCKETS if b[1] == 'guest']
        loved_guest = ('loved', 'guest')
        guests = {g for hosts in self.by_host.values() for g in hosts}
        for guest in guests:
            per_mode = {}
            for hosts in self.by_host.values():
                for mode, entry in hosts.get(guest, {}).items():
                    acc = per_mode.setdefault(mode, [0] * 5)
                    for i, v in enumerate(entry):
                        acc[i] += v
            total = lambda i: sum(e[i] for e in per_mode.values())
            with self.subTest(guest=guest):
                self.assertEqual(total(HG_PC), sum(self.stats[b]['pc'][guest] for b in guest_buckets))
                self.assertEqual(total(HG_MAPS), sum(self.stats[b]['maps'][guest] for b in guest_buckets))
                self.assertEqual(total(HG_LOVED_PC), self.stats[loved_guest]['pc'][guest])
                self.assertEqual(total(HG_LOVED_MAPS), self.stats[loved_guest]['maps'][guest])
                self.assertEqual(total(HG_SETS), self.stats['sets']['guest']['count'][guest])
                for mode, entry in per_mode.items():
                    self.assertEqual(entry[HG_PC], sum(self.stats[b]['mode_pc'][guest][mode]
                                                       for b in guest_buckets))
                    self.assertEqual(entry[HG_SETS], self.stats['sets']['guest']['modes'][guest][mode])

    def test_set_without_a_host_counts_for_guests_but_files_under_nobody(self):
        stats = mapper_scan.new_stats()
        mapper_scan.aggregate_page([{'id': 1, 'status': 'ranked', 'beatmaps': [
            {'id': 1, 'user_id': 2, 'mode': 'osu', 'playcount': 4, 'status': 'ranked'}]}], stats, {})
        self.assertEqual(stats[('ranked', 'guest')]['pc'][2], 4)
        self.assertEqual(stats['by_host'], {})

    def test_published_shards_are_per_host_and_survive_json(self):
        published = mapper_scan.build_host_guests(self.stats, 'stamp')
        self.assertEqual(published['version'], 1)
        self.assertEqual(set(published['hosts']), {'1', '3'})
        shard = json.loads(json.dumps(published))['hosts']['1']
        self.assertEqual(shard['last_scan'], 'stamp')
        self.assertEqual(shard['guests']['4'], {'mania': [50, 1, 1, 0, 0]})

    def test_checkpoint_keeps_by_host_and_discards_the_layout_before_it(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, 'state.pickle')
            mapper_scan.save_state({'stats': self.stats, 'seen': {100}}, path)
            self.assertEqual(mapper_scan.load_state(path)['stats']['by_host'], self.by_host)
            import pickle
            with open(path, 'wb') as f:
                pickle.dump({'version': 7, 'stats': {}}, f)
            self.assertIsNone(mapper_scan.load_state(path))


class PublishTests(unittest.TestCase):
    def test_shards_land_before_the_board_and_a_failure_keeps_the_old_board(self):
        order = []
        def save(data, path):
            order.append(path)
            return True
        with mock.patch.object(mapper_scan, 'save_to_firebase', side_effect=save):
            self.assertTrue(mapper_scan.publish_mapper_results({'mappers': []}, {'hosts': {}}, {'hosts': {}}))
        self.assertEqual(order, ['gd_author_index', 'gd_host_guests', 'mappers'])

        order.clear()
        def fail_shards(data, path):
            order.append(path)
            return path != 'gd_host_guests'
        with mock.patch.object(mapper_scan, 'save_to_firebase', side_effect=fail_shards):
            self.assertFalse(mapper_scan.publish_mapper_results({'mappers': []}, None, {'hosts': {}}))
        self.assertEqual(order, ['gd_host_guests'])

    def test_nightly_scan_publishes_shards_and_flags_the_board(self):
        stack = ExitStack()
        self.addCleanup(stack.close)
        page = {'beatmapsets': sets(), 'cursor_string': None}
        for name, value in [('authenticate', 'token'), ('load_state', None),
                            ('load_cache', mapper_scan.new_cache()), ('fetch_page', (page, 'token')),
                            ('corpus_total', 4), ('detect_owners_mode', 'bulk'), ('resolve_owners', {}),
                            ('load_from_firebase', {}), ('fetch_profile_counts', {}), ('fold_unlisted', 0)]:
            stack.enter_context(mock.patch.object(mapper_scan, name, return_value=value))
        for name in ['save_cache', 'save_state', 'clear_state']:
            stack.enter_context(mock.patch.object(mapper_scan, name))
        stack.enter_context(mock.patch.object(scan_logic, 'resolve_users_parallel', return_value={}))
        publish = stack.enter_context(mock.patch.object(mapper_scan, 'publish_mapper_results',
                                                        return_value=True))
        result = mapper_scan.run_mapper_scan()
        self.assertNotIn('error', result)
        self.assertIs(result['host_guests'], True)
        shards = publish.call_args.args[2]
        self.assertEqual(shards['last_scan'], result['last_scan'])
        self.assertEqual(shards['hosts']['1']['guests']['4'], {'mania': [50, 1, 1, 0, 0]})


class EndpointTests(unittest.TestCase):
    def setUp(self):
        self.client = web_app.app.test_client()
        web_app.LADDER_IDS.update(version=None, ids=frozenset())
        self.addCleanup(web_app.LADDER_IDS.update, version=None, ids=frozenset())
        self.shard = {'last_scan': 'stamp', 'guests': {'2': {'osu': [14, 2, 2, 0, 0]}}}
        def load(path):
            if path == web_app.next_path('mappers'):
                return {'last_scan': 'stamp', 'mappers': [{'osu_id': 1}, {'osu_id': 7}]}
            if path == web_app.next_path('gd_host_guests/hosts/1'):
                return self.shard
            return None
        patch = mock.patch.object(web_app, 'get_leaderboard_data', side_effect=load)
        self.load = patch.start()
        self.addCleanup(patch.stop)

    def test_serves_one_hosts_shard(self):
        r = self.client.get('/api/host_guests/1')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.get_json(), {'host_id': 1, 'last_scan': 'stamp', 'guests': self.shard['guests']})
        self.assertIn('max-age', r.headers['Cache-Control'])

    def test_host_nobody_guested_for_has_nothing_to_leave_out(self):
        r = self.client.get('/api/host_guests/7')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.get_json()['guests'], {})

    def test_id_off_the_ladder_is_refused_without_reading_a_shard(self):
        r = self.client.get('/api/host_guests/999')
        self.assertEqual(r.status_code, 404)
        self.assertNotIn(mock.call(web_app.next_path('gd_host_guests/hosts/999')), self.load.call_args_list)


if __name__ == '__main__':
    unittest.main()
