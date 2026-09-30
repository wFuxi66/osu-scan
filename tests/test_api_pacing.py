import os
import threading
import unittest
from unittest import mock

os.environ.setdefault('OSU_CLIENT_ID', 'test-client')
os.environ.setdefault('OSU_CLIENT_SECRET', 'test-secret')

import scan_logic


class APIPacingTests(unittest.TestCase):
    def setUp(self):
        self.now = 0.0
        self.pacer = scan_logic.APIPacer()
        patches = [
            mock.patch.object(scan_logic, 'API_PACER', self.pacer),
            mock.patch.object(scan_logic.time, 'monotonic', side_effect=lambda: self.now),
            mock.patch.object(scan_logic.time, 'sleep', side_effect=self.advance),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)

    def advance(self, seconds):
        self.now += seconds

    def response(self, status=200, retry_after=None):
        response = mock.Mock()
        response.status_code = status
        response.headers = {} if retry_after is None else {'Retry-After': retry_after}
        return response

    def test_sessions_and_direct_requests_share_the_same_pacing(self):
        starts = []

        def get(*args, **kwargs):
            starts.append(self.now)
            return self.response()

        session = mock.Mock()
        session.get.side_effect = get
        with mock.patch.object(scan_logic.requests, 'get', side_effect=get):
            scan_logic.api_get('url', session=session)
            scan_logic.api_get('url')
            scan_logic.api_get('url', session=session)
        self.assertEqual(starts, [0, 1, 2])

    def test_429_waits_full_retry_after_and_delays_other_sessions(self):
        starts = []
        responses = [self.response(429, '120'), self.response(), self.response()]

        def get(*args, **kwargs):
            starts.append(self.now)
            return responses.pop(0)

        session = mock.Mock()
        session.get.side_effect = get
        with mock.patch.object(scan_logic.requests, 'get', side_effect=get):
            scan_logic.api_get('url', session=session)
            scan_logic.api_get('url')
        self.assertEqual(starts, [0, 120, 121])

    def test_waiting_request_rechecks_cooldown_extended_by_another_worker(self):
        self.pacer.wait()
        extended = False

        def sleep(seconds):
            nonlocal extended
            if not extended:
                self.pacer.cooldown(120)
                extended = True
            self.advance(seconds)

        with mock.patch.object(scan_logic.time, 'sleep', side_effect=sleep):
            self.pacer.wait()
        self.assertEqual(self.now, 120)

    def test_exhausted_429_retries_leave_cooldown_for_next_scan(self):
        session = mock.Mock()
        session.get.return_value = self.response(429, '120')
        response = scan_logic.api_get('url', session=session)
        self.assertEqual(response.status_code, 429)
        self.assertEqual(session.get.call_count, 3)
        self.assertEqual(self.now, 240)
        session.get.return_value = self.response()
        scan_logic.api_get('another scan', session=session)
        self.assertEqual(self.now, 360)

    def test_retry_after_supports_dates_and_defaults_for_missing_or_invalid_headers(self):
        with mock.patch.object(scan_logic.time, 'time', return_value=0):
            self.assertEqual(scan_logic.retry_after_seconds('Thu, 01 Jan 1970 00:02:00 GMT'), 120)
        for value in [None, 'invalid']:
            self.assertEqual(scan_logic.retry_after_seconds(value), 60)

    def test_simultaneous_workers_cannot_reserve_the_same_request_start(self):
        reservations = []

        class RecordingPacer(scan_logic.APIPacer):
            @property
            def next_request(self):
                return self._next_request

            @next_request.setter
            def next_request(self, value):
                self._next_request = value
                if value:
                    reservations.append(value)

        pacer = RecordingPacer()

        def worker():
            pacer.wait()

        workers = [threading.Thread(target=worker) for _ in range(6)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=3)
        self.assertEqual(sorted(reservations), [1, 2, 3, 4, 5, 6])
