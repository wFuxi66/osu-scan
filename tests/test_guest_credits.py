import os
import threading
import time
import unittest
from unittest import mock

os.environ.setdefault('OSU_CLIENT_ID', 'test-client')
os.environ.setdefault('OSU_CLIENT_SECRET', 'test-secret')

import scan_logic


class GuestCreditTests(unittest.TestCase):
    def setUp(self):
        cache = mock.patch.object(scan_logic, 'AUTHOR_CACHE', {})
        cache.start()
        self.addCleanup(cache.stop)
        save = mock.patch.object(scan_logic, '_save_json_atomic')
        self.save = save.start()
        self.addCleanup(save.stop)

    def listing(self):
        return {'id': 100, 'status': 'ranked', 'beatmaps': [
            {'id': 10, 'user_id': 2, 'last_updated': '2026-09-01T00:00:00'},
            {'id': 11, 'user_id': 1, 'last_updated': '2026-09-02T00:00:00'},
        ]}

    def response(self, payload):
        response = mock.Mock()
        response.json.return_value = payload
        return response

    def test_profile_omits_collaborator_but_bulk_authors_restore_one_credit(self):
        bset = self.listing()
        authors = [{'id': 1, 'username': 'host'}, {'id': 2, 'username': 'primary'},
                   {'id': 3, 'username': 'collaborator'}]
        response = self.response({'beatmaps': [
            {'id': 10, 'owners': authors}, {'id': 11, 'owners': authors},
        ]})
        with mock.patch.object(scan_logic, 'get_set_with_retry', return_value=response) as get:
            credits, unread = scan_logic.analyze_sets([bset, bset], 1, 'token')
        self.assertEqual(unread, 0)
        self.assertEqual([g['mapper_id'] for g in credits], [2, 3])
        self.assertEqual(get.call_count, 1)
        self.assertIn('ids%5B%5D=10', get.call_args.args[0])

    def test_missing_bulk_author_is_recovered_from_full_set(self):
        bset = self.listing()
        complete = self.listing()
        for beatmap in complete['beatmaps']:
            beatmap['owners'] = [{'id': 3, 'username': 'collaborator'}]
        with mock.patch.object(scan_logic, 'get_set_with_retry', side_effect=[
                self.response({'beatmaps': []}), self.response(complete)]):
            credits, unread = scan_logic.analyze_sets([bset], 1, 'token')
        self.assertEqual(unread, 0)
        self.assertEqual([g['mapper_id'] for g in credits], [3])

    def test_failed_authors_are_reported_instead_of_counting_partial_credits(self):
        with mock.patch.object(scan_logic, 'get_set_with_retry', return_value=None):
            credits, unread = scan_logic.analyze_sets([self.listing()], 1, 'token')
        self.assertEqual((credits, unread), ([], 1))

    def test_complete_owner_data_needs_no_requests_and_preserves_loved(self):
        bset = self.listing()
        bset['status'] = 'loved'
        for beatmap in bset['beatmaps']:
            beatmap['owners'] = [{'id': 3, 'username': 'collaborator'}]
        with mock.patch.object(scan_logic, 'get_set_with_retry') as get:
            credits, unread = scan_logic.analyze_sets([bset], 1, 'token')
        get.assert_not_called()
        self.assertEqual(unread, 0)
        self.assertEqual(len(credits), 1)
        self.assertTrue(credits[0]['loved'])

    def test_repeat_scan_reuses_authors_and_changed_difficulty_is_refetched(self):
        def fetch(batch, token, cancel):
            return [{'id': bid, 'owners': [{'id': 3, 'username': 'collaborator'}]}
                    for bid in batch]

        with mock.patch.object(scan_logic, 'fetch_author_batch', side_effect=fetch) as get:
            first, _ = scan_logic.analyze_sets([self.listing()], 1, 'token')
            second, _ = scan_logic.analyze_sets([self.listing()], 1, 'token')
            self.assertEqual(first, second)
            self.assertEqual(get.call_count, 1)
            changed = self.listing()
            changed['beatmaps'][0]['last_updated'] = '2026-09-03T00:00:00'
            scan_logic.analyze_sets([changed], 1, 'token')
            self.assertEqual(get.call_args.args[0], [10])
        self.assertEqual(self.save.call_args.args[0], scan_logic.AUTHOR_CACHE_FILE)

    def test_expired_authors_are_refetched(self):
        beatmap = self.listing()['beatmaps'][0]
        scan_logic.AUTHOR_CACHE['10'] = {
            'basis': scan_logic.author_basis(beatmap),
            'owners': [{'id': 99}], 'expires_at': time.time() - 1,
        }
        with mock.patch.object(scan_logic, 'get_set_with_retry', return_value=None) as get:
            credits, unread = scan_logic.analyze_sets([self.listing()], 1, 'token')
        self.assertEqual((credits, unread), ([], 1))
        self.assertIn('ids%5B%5D=10', get.call_args_list[0].args[0])

    def test_settled_authors_last_a_week_and_qualified_authors_expire_after_five_minutes(self):
        beatmap = self.listing()['beatmaps'][0]
        beatmap['owners'] = [{'id': 3}]
        with mock.patch.object(scan_logic.time, 'time', return_value=1000):
            for status in scan_logic.SETTLED_STATUSES:
                scan_logic.remember_authors(beatmap, status)
                self.assertEqual(scan_logic.AUTHOR_CACHE['10']['expires_at'], 1000 + 7 * 86400)
            scan_logic.remember_authors(beatmap, 'qualified')
            self.assertEqual(scan_logic.AUTHOR_CACHE['10']['expires_at'], 1300)

    def test_status_change_invalidates_authors_even_before_expiry(self):
        beatmap = self.listing()['beatmaps'][0]
        beatmap['owners'] = [{'id': 99}]
        scan_logic.remember_authors(beatmap, 'ranked')
        qualified = self.listing()
        qualified['status'] = 'qualified'
        with mock.patch.object(scan_logic, 'get_set_with_retry', return_value=None) as get:
            credits, unread = scan_logic.analyze_sets([qualified], 1, 'token')
        self.assertEqual((credits, unread), ([], 1))
        self.assertIn('ids%5B%5D=10', get.call_args_list[0].args[0])

    def test_batches_overlap_instead_of_waiting_for_previous_response(self):
        barrier = threading.Barrier(scan_logic.AUTHOR_WORKERS)
        bset = {'id': 100, 'status': 'ranked', 'beatmaps': [
            {'id': bid, 'user_id': 2} for bid in range(50 * scan_logic.AUTHOR_WORKERS)]}

        def fetch(batch, token, cancel):
            barrier.wait(timeout=3)
            return [{'id': bid, 'owners': [{'id': 3, 'username': 'collaborator'}]}
                    for bid in batch]

        with mock.patch.object(scan_logic, 'fetch_author_batch', side_effect=fetch), \
                mock.patch.object(scan_logic, 'get_set_with_retry') as fallback:
            credits, unread = scan_logic.analyze_sets([bset], 1, 'token')
        fallback.assert_not_called()
        self.assertEqual(unread, 0)
        self.assertEqual([g['mapper_id'] for g in credits], [3])
