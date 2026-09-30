import os
import unittest
from unittest import mock

os.environ.setdefault('OSU_CLIENT_ID', 'test-client')
os.environ.setdefault('OSU_CLIENT_SECRET', 'test-secret')

import scan_logic


class GuestCreditTests(unittest.TestCase):
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
