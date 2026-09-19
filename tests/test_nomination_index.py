import os
import threading
import unittest
from unittest import mock

os.environ.setdefault('OSU_CLIENT_ID', 'test-client')
os.environ.setdefault('OSU_CLIENT_SECRET', 'test-secret')

import app as web_app
import global_scan
import scan_logic


class NominationIndexProducerTests(unittest.TestCase):
    def test_build_nomination_index_deduplicates_and_sorts_nominators(self):
        nominations_by_bn = {
            20: [{'id': 100}, {'id': 101}],
            10: [{'id': 100}, {'id': 100}],
        }

        index = global_scan.build_nomination_index(nominations_by_bn, '2026-09-19T12:00:00')

        self.assertEqual(index, {
            'last_scan': '2026-09-19T12:00:00',
            'sets': {
                '100': [10, 20],
                '101': [20],
            },
        })

    def test_publish_scan_results_stores_index_separately_from_leaderboard(self):
        stored = {}

        def save(data, path='leaderboard'):
            stored[path] = data
            return True

        result = {'last_scan': 'stamp', 'top_bns': []}
        index = {'last_scan': 'stamp', 'sets': {'100': [10, 20]}}

        with mock.patch.object(global_scan, 'save_to_firebase', side_effect=save):
            published = global_scan.publish_scan_results(result, index)

        self.assertTrue(published)
        self.assertEqual(stored, {
            'nomination_index': index,
            'leaderboard': result,
        })

    def test_publish_scan_results_does_not_replace_leaderboard_when_index_write_fails(self):
        attempted_paths = []

        def save(data, path='leaderboard'):
            attempted_paths.append(path)
            return path != 'nomination_index'

        with mock.patch.object(global_scan, 'save_to_firebase', side_effect=save):
            published = global_scan.publish_scan_results(
                {'last_scan': 'stamp'},
                {'last_scan': 'stamp', 'sets': {}},
            )

        self.assertFalse(published)
        self.assertEqual(attempted_paths, ['nomination_index'])

    def test_global_scan_publishes_index_from_completed_bn_reads(self):
        class FakeSession:
            def mount(self, *args):
                pass

            def close(self):
                pass

        bset = {
            'id': 100,
            'nominations_summary': {'eligible_main_rulesets': ['osu']},
            'beatmaps': [],
        }
        bns = [
            {'osu_id': 10, 'username': 'One', 'modes': ['osu'], 'is_current': False},
            {'osu_id': 20, 'username': 'Two', 'modes': ['osu'], 'is_current': True},
        ]
        stored = {}

        def nominations_for(bn_id, token, cancel_event=None, session=None):
            return [bset] if bn_id in (10, 20) else []

        def save(data, path='leaderboard'):
            stored[path] = data
            return True

        with mock.patch.object(scan_logic, 'get_token', return_value='token'), \
                mock.patch.object(global_scan.bn_data, 'get_all_bns', return_value=bns), \
                mock.patch.object(global_scan, 'load_from_firebase', return_value={}), \
                mock.patch.object(global_scan.requests, 'Session', return_value=FakeSession()), \
                mock.patch.object(global_scan, 'fetch_bn_nominations', side_effect=nominations_for), \
                mock.patch.object(global_scan, 'save_to_firebase', side_effect=save), \
                mock.patch.object(global_scan.time, 'sleep'):
            result = global_scan.run_global_scan()

        self.assertNotIn('error', result)
        self.assertEqual(stored['nomination_index']['sets'], {'100': [10, 20]})
        self.assertEqual(stored['nomination_index']['last_scan'], result['last_scan'])


class NominationIndexConsumerTests(unittest.TestCase):
    def test_ranked_set_uses_index_when_nomination_count_matches(self):
        bset = {
            'id': 100,
            'status': 'ranked',
            'nominations_summary': {'current': 2},
        }
        index = {'sets': {'100': [10, 20]}}

        self.assertEqual(scan_logic.indexed_nominator_ids(bset, index), [10, 20])

    def test_qualified_set_ignores_index(self):
        bset = {
            'id': 100,
            'status': 'qualified',
            'nominations_summary': {'current': 2},
        }
        index = {'sets': {'100': [10, 20]}}

        self.assertIsNone(scan_logic.indexed_nominator_ids(bset, index))

    def test_ranked_set_ignores_index_when_nomination_count_differs(self):
        bset = {
            'id': 100,
            'status': 'ranked',
            'nominations_summary': {'current': 2},
        }
        index = {'sets': {'100': [10]}}

        self.assertIsNone(scan_logic.indexed_nominator_ids(bset, index))

    def test_ranked_set_ignores_missing_or_malformed_index_entries(self):
        bset = {
            'id': 100,
            'status': 'ranked',
            'nominations_summary': {'current': 2},
        }
        bad_indexes = [
            None,
            {},
            {'sets': {}},
            {'sets': {'100': '10,20'}},
            {'sets': {'100': [10, 10]}},
            {'sets': {'100': [10, '20']}},
        ]

        for index in bad_indexes:
            with self.subTest(index=index):
                self.assertIsNone(scan_logic.indexed_nominator_ids(bset, index))

    def test_process_ranked_set_returns_indexed_nominators_without_network(self):
        class NoNetworkSession:
            def get(self, *args, **kwargs):
                raise AssertionError('validated index hit must not make an API request')

        bset = {
            'id': 100,
            'status': 'ranked',
            'artist': 'Artist',
            'title': 'Title',
            'ranked_date': '2026-09-19T12:00:00Z',
            'nominations_summary': {'current': 2},
        }

        entries = scan_logic.process_nominator_set(
            bset,
            'token',
            session=NoNetworkSession(),
            nomination_index={'sets': {'100': [10, 20]}},
        )

        self.assertEqual(entries, [
            {'nominator_id': 10, 'set_title': 'Artist - Title', 'date': '2026-09-19'},
            {'nominator_id': 20, 'set_title': 'Artist - Title', 'date': '2026-09-19'},
        ])

    def test_process_qualified_set_ignores_legacy_cache_and_reads_live(self):
        class LiveSession:
            calls = 0

            def get(self, *args, **kwargs):
                self.calls += 1
                response = mock.Mock(status_code=200)
                response.json.return_value = {'current_nominations': [{'user_id': 20}]}
                return response

        bset = {
            'id': 100,
            'status': 'qualified',
            'artist': 'Artist',
            'title': 'Title',
            'last_updated': '2026-09-19T12:00:00Z',
            'nominations_summary': {'current': 1},
        }
        session = LiveSession()

        with mock.patch.dict(scan_logic.NOM_CACHE, {100: [10]}, clear=True):
            entries = scan_logic.process_nominator_set(
                bset,
                'token',
                session=session,
                nomination_index={'sets': {'100': [10]}},
            )

        self.assertEqual(entries, [
            {'nominator_id': 20, 'set_title': 'Artist - Title', 'date': '2026-09-19'},
        ])
        self.assertEqual(session.calls, 1)

    def test_process_set_reads_live_when_index_ids_are_unhashable(self):
        class LiveSession:
            calls = 0

            def get(self, *args, **kwargs):
                self.calls += 1
                response = mock.Mock(status_code=200)
                response.json.return_value = {'current_nominations': [{'user_id': 20}]}
                return response

        bset = {
            'id': 100,
            'status': 'ranked',
            'artist': 'Artist',
            'title': 'Title',
            'ranked_date': '2026-09-19T12:00:00Z',
            'nominations_summary': {'current': 1},
        }
        session = LiveSession()

        entries = scan_logic.process_nominator_set(
            bset,
            'token',
            session=session,
            nomination_index={'sets': {'100': [[]]}},
        )

        self.assertEqual(entries, [
            {'nominator_id': 20, 'set_title': 'Artist - Title', 'date': '2026-09-19'},
        ])
        self.assertEqual(session.calls, 1)

    def test_analyze_nominators_passes_index_to_each_set(self):
        bset = {
            'id': 100,
            'status': 'ranked',
            'artist': 'Artist',
            'title': 'Title',
            'ranked_date': '2026-09-19T12:00:00Z',
            'nominations_summary': {'current': 1},
        }

        with mock.patch.dict(scan_logic.NOM_CACHE, {}, clear=True):
            nominations, unread = scan_logic.analyze_nominators(
                [bset],
                'token',
                nomination_index={'sets': {'100': [10]}},
            )

        self.assertEqual(nominations, [
            {'nominator_id': 10, 'set_title': 'Artist - Title', 'date': '2026-09-19'},
        ])
        self.assertEqual(unread, 0)

    def test_user_nominator_scan_uses_supplied_index(self):
        bset = {
            'id': 100,
            'status': 'ranked',
            'artist': 'Artist',
            'title': 'Title',
            'ranked_date': '2026-09-19T12:00:00Z',
            'nominations_summary': {'current': 1},
        }
        index = {'sets': {'100': [10]}}

        with mock.patch.object(scan_logic, 'get_token', return_value='token'), \
                mock.patch.object(scan_logic, 'get_user_id', return_value=(50, 'Mapper')), \
                mock.patch.object(scan_logic, 'get_beatmapsets', return_value=([bset], False)), \
                mock.patch.dict(scan_logic.NOM_CACHE, {}, clear=True), \
                mock.patch.dict(scan_logic.USER_CACHE, {10: 'Nominator'}, clear=True):
            result = scan_logic.generate_nominator_leaderboard_for_user(
                'Mapper',
                nomination_index=index,
            )

        self.assertEqual(result['leaderboard'], [{
            'mapper_id': 10,
            'mapper_name': 'Nominator',
            'total_gds': 1,
            'last_gd_date': '2026-09-19',
        }])
        self.assertEqual(result['unread_sets'], 0)

    def test_web_nominator_job_loads_published_index(self):
        bset = {
            'id': 100,
            'status': 'ranked',
            'artist': 'Artist',
            'title': 'Title',
            'ranked_date': '2026-09-19T12:00:00Z',
            'nominations_summary': {'current': 1},
        }
        index = {'sets': {'100': [10]}}

        def load_index(path):
            return index if path.endswith('nomination_index') else None

        web_app.JOBS.clear()
        web_app.RESULTS_CACHE.clear()
        web_app.SCAN_CACHE.clear()
        web_app.JOBS['job'] = {'status': 'running'}
        web_app.SCANS_RUNNING = 1

        with mock.patch.object(scan_logic, 'get_token', return_value='token'), \
                mock.patch.object(scan_logic, 'get_user_id', return_value=(50, 'Mapper')), \
                mock.patch.object(scan_logic, 'get_beatmapsets', return_value=([bset], False)), \
                mock.patch.object(scan_logic, 'get_set_with_retry', return_value=None), \
                mock.patch.object(web_app, 'get_leaderboard_data', side_effect=load_index), \
                mock.patch.dict(scan_logic.NOM_CACHE, {}, clear=True), \
                mock.patch.dict(scan_logic.USER_CACHE, {10: 'Nominator'}, clear=True):
            web_app.run_scan_job('job', 'Mapper', 'nominators', threading.Event())

        result_id = web_app.JOBS['job']['result_id']
        self.assertEqual(web_app.RESULTS_CACHE[result_id]['leaderboard'], [{
            'mapper_id': 10,
            'mapper_name': 'Nominator',
            'total_gds': 1,
            'last_gd_date': '2026-09-19',
        }])

if __name__ == '__main__':
    unittest.main()
