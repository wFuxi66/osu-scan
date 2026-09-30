import copy
import json
import os
import pickle
import tempfile
import threading
import time
import unittest
from contextlib import ExitStack
from datetime import datetime, timezone
from unittest import mock

os.environ.setdefault('OSU_CLIENT_ID', 'test-client')
os.environ.setdefault('OSU_CLIENT_SECRET', 'test-secret')

import app as web_app
import global_scan
import mapper_scan
import scan_logic


class AuthorIndexTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(mock.patch.object(scan_logic, 'AUTHOR_CACHE', {}))
        self.stack.enter_context(mock.patch.object(scan_logic, '_save_json_atomic'))
        self.bset = {'id': 100, 'user_id': 1, 'status': 'ranked', 'creator': 'Host',
                     'beatmaps': [{'id': 10, 'user_id': 2, 'last_updated': '2026-09-01T00:00:00'},
                                  {'id': 11, 'user_id': 1, 'last_updated': '2026-09-02T00:00:00'}]}
        self.owners = {10: [2, 3], 11: [1, 3]}
        self.cache = mapper_scan.new_cache()
        mapper_scan.record_author_sets(self.cache, [self.bset], self.owners)
        self.index = mapper_scan.build_author_index(self.cache, {100}, {1: 'Host', 2: 'Primary', 3: 'Collab'},
                                                    datetime.now(timezone.utc).isoformat())
        self.shard = self.index['hosts']['1']

    def test_first_manual_scan_uses_published_authors_with_no_local_cache_or_api_reads(self):
        with mock.patch.object(scan_logic, 'get_set_with_retry') as get:
            credits, unread = scan_logic.analyze_sets([copy.deepcopy(self.bset)], 1, 'token',
                                                     author_index=self.shard)
        get.assert_not_called()
        self.assertEqual(unread, 0)
        self.assertEqual({c['mapper_id']: c['mapper_name'] for c in credits}, {2: 'Primary', 3: 'Collab'})
        self.assertEqual(sum(c['mapper_id'] == 3 for c in credits), 1)

    def test_changed_difficulty_fetches_only_missing_authors(self):
        bset = copy.deepcopy(self.bset)
        bset['beatmaps'][0]['last_updated'] = '2026-09-03T00:00:00'
        response = mock.Mock()
        response.json.return_value = {'beatmaps': [{'id': 10, 'owners': [{'id': 4, 'username': 'New'}]}]}
        with mock.patch.object(scan_logic, 'get_set_with_retry', return_value=response) as get:
            credits, unread = scan_logic.analyze_sets([bset], 1, 'token', author_index=self.shard)
        self.assertEqual(get.call_count, 1)
        self.assertIn('ids%5B%5D=10', get.call_args.args[0])
        self.assertNotIn('ids%5B%5D=11', get.call_args.args[0])
        self.assertEqual({c['mapper_id'] for c in credits}, {3, 4})
        self.assertEqual(unread, 0)

    def test_stale_qualified_missing_and_malformed_snapshots_fall_back(self):
        stale = copy.deepcopy(self.shard)
        stale['last_scan'] = datetime.fromtimestamp(time.time() - scan_logic.AUTHOR_INDEX_TTL - 1,
                                                   timezone.utc).isoformat()
        malformed = copy.deepcopy(self.shard)
        malformed['sets']['100']['beatmaps']['10'][2] = [3, '4']
        qualified = copy.deepcopy(self.bset)
        qualified['status'] = 'qualified'
        for bset, shard in [(self.bset, stale), (self.bset, None), (qualified, self.shard),
                            (self.bset, malformed), (self.bset, {'version': 1, 'last_scan': 'bad'})]:
            with self.subTest(shard=shard), mock.patch.object(scan_logic, 'get_set_with_retry', return_value=None) as get:
                credits, unread = scan_logic.analyze_sets([copy.deepcopy(bset)], 1, 'token', author_index=shard)
                self.assertTrue(get.called)
                self.assertEqual((credits, unread), ([], 1))

    def test_snapshot_excludes_sets_not_seen_and_incomplete_authors(self):
        mapper_scan.record_author_sets(self.cache, [dict(self.bset, id=200)], self.owners)
        index = mapper_scan.build_author_index(self.cache, {100}, {}, 'stamp')
        self.assertEqual(set(index['hosts']['1']['sets']), {'100'})
        mapper_scan.record_author_sets(self.cache, [self.bset], {10: [3]})
        self.assertNotIn(100, self.cache['author_sets'])

    def test_cache_upgrade_and_restart_preserve_previous_owners_and_recorded_sets(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, 'owners.pickle')
            old_cache = {'version': mapper_scan.CACHE_VERSION, 'runs': 3,
                         'owners': self.owners, 'profiles': {}, 'profile_basis': {}}
            with open(path, 'wb') as f:
                pickle.dump(old_cache, f)
            upgraded = mapper_scan.load_cache(path)
            self.assertEqual(upgraded['owners'], self.owners)
            self.assertEqual(upgraded['runs'], 3)
            mapper_scan.record_author_sets(upgraded, [self.bset], self.owners)
            mapper_scan.save_cache(upgraded, path)
            restored = mapper_scan.load_cache(path)
            index = mapper_scan.build_author_index(restored, {100}, {}, 'stamp')
            self.assertEqual(index['hosts']['1']['sets'], self.shard['sets'])

    def test_incomplete_nightly_scan_keeps_published_data_and_saves_author_checkpoint(self):
        for name, value in [('authenticate', 'token'), ('load_state', None), ('load_cache', self.cache),
                            ('fetch_page', ({'beatmapsets': [self.bset], 'cursor_string': 'more'}, 'token')),
                            ('corpus_total', 2), ('detect_owners_mode', 'bulk'), ('resolve_owners', self.owners)]:
            self.stack.enter_context(mock.patch.object(mapper_scan, name, return_value=value))
        save_cache = self.stack.enter_context(mock.patch.object(mapper_scan, 'save_cache'))
        self.stack.enter_context(mock.patch.object(mapper_scan, 'save_state'))
        publish = self.stack.enter_context(mock.patch.object(mapper_scan, 'publish_mapper_results'))
        clear = self.stack.enter_context(mock.patch.object(mapper_scan, 'clear_state'))
        result = mapper_scan.run_mapper_scan(max_pages=1)
        self.assertIn('error', result)
        publish.assert_not_called()
        clear.assert_not_called()
        self.assertIn(100, save_cache.call_args.args[0]['author_sets'])

    def test_publish_keeps_index_separate_and_does_not_replace_board_after_index_failure(self):
        stored = {}
        def save(data, path):
            stored[path] = data
            return True
        with mock.patch.object(mapper_scan, 'save_to_firebase', side_effect=save):
            self.assertTrue(mapper_scan.publish_mapper_results({'mappers': []}, self.index))
        self.assertEqual(set(stored), {'gd_author_index', 'mappers'})
        self.assertNotIn('hosts', stored['mappers'])
        with mock.patch.object(mapper_scan, 'save_to_firebase', return_value=False) as save:
            self.assertFalse(mapper_scan.publish_mapper_results({}, self.index))
        self.assertEqual(save.call_args.kwargs['path'], 'gd_author_index')
        self.assertEqual(save.call_count, 1)

    def test_offline_namespace_and_host_reads_use_same_published_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, 'gd_author_index_cache.json')
            with open(path, 'w') as f:
                json.dump(self.index, f)
            with mock.patch.object(global_scan, 'FIREBASE_URL', ''), mock.patch.object(
                    global_scan, 'local_path', side_effect=lambda name: os.path.join(directory, name.split('/')[-1] + '_cache.json')):
                self.assertEqual(global_scan.load_from_firebase('preprod/gd_author_index/hosts/1'), self.shard)
                self.assertIsNone(global_scan.load_from_firebase('preprod/gd_author_index/hosts/999'))

    def test_web_scan_loads_only_host_shard_in_the_site_namespace(self):
        web_app.JOBS['author-job'] = {'status': 'running'}
        web_app.SCANS_RUNNING = 1
        self.addCleanup(web_app.JOBS.pop, 'author-job', None)
        with mock.patch.object(scan_logic, 'get_token', return_value='token'), \
                mock.patch.object(scan_logic, 'get_user_id', return_value=(1, 'Host')), \
                mock.patch.object(scan_logic, 'get_beatmapsets', return_value=([copy.deepcopy(self.bset)], False)), \
                mock.patch.object(scan_logic, 'get_set_with_retry') as get, \
                mock.patch.object(web_app, 'get_leaderboard_data', return_value=self.shard) as load:
            web_app.run_scan_job('author-job', 'Host', 'gd', threading.Event())
        get.assert_not_called()
        load.assert_called_once_with(web_app.next_path('gd_author_index/hosts/1'))
        self.assertEqual(web_app.JOBS['author-job']['status'], 'done')

    def test_unavailable_index_loader_does_not_fail_manual_scan(self):
        response = mock.Mock()
        response.json.return_value = {'beatmaps': [{'id': bid, 'owners': [{'id': 3, 'username': 'Collab'}]}
                                                  for bid in [10, 11]]}
        with mock.patch.object(scan_logic, 'get_token', return_value='token'), \
                mock.patch.object(scan_logic, 'get_user_id', return_value=(1, 'Host')), \
                mock.patch.object(scan_logic, 'get_beatmapsets', return_value=([copy.deepcopy(self.bset)], False)), \
                mock.patch.object(scan_logic, 'get_set_with_retry', return_value=response):
            result = scan_logic.generate_leaderboard_for_user('Host', author_index_loader=mock.Mock(side_effect=OSError('offline')))
        self.assertEqual(result['leaderboard'][0]['total_gds'], 1)
        self.assertEqual(result['unread_sets'], 0)

    def test_nightly_scan_publishes_its_existing_reads_and_changed_metadata_invalidates_cache(self):
        cache = mapper_scan.new_cache()
        cache['runs'] = 1  # page zero is not due for its scheduled ownership rotation.
        cache['owners'] = dict(self.owners)
        cache['owners_basis'] = {10: ['old-date', 2], 11: scan_logic.author_basis(self.bset['beatmaps'][1])}
        for name, value in [('authenticate', 'token'), ('load_state', None), ('load_cache', cache),
                            ('fetch_page', ({'beatmapsets': [self.bset], 'cursor_string': None}, 'token')),
                            ('corpus_total', 1), ('detect_owners_mode', 'bulk'), ('load_from_firebase', {}),
                            ('fetch_profile_counts', {}), ('fold_unlisted', 0)]:
            self.stack.enter_context(mock.patch.object(mapper_scan, name, return_value=value))
        for name in ['save_cache', 'save_state', 'clear_state']:
            self.stack.enter_context(mock.patch.object(mapper_scan, name))
        self.stack.enter_context(mock.patch.object(scan_logic, 'resolve_users_parallel', return_value={1: 'Host', 2: 'Primary', 3: 'Collab'}))
        resolve = self.stack.enter_context(mock.patch.object(mapper_scan, 'resolve_owners', return_value={10: [2, 3]}))
        publish = self.stack.enter_context(mock.patch.object(mapper_scan, 'publish_mapper_results', return_value=True))
        result = mapper_scan.run_mapper_scan()
        self.assertNotIn('error', result)
        self.assertEqual(resolve.call_args.kwargs['need'], {10})
        self.assertEqual(publish.call_args.args[1]['hosts']['1']['sets'], self.shard['sets'])
