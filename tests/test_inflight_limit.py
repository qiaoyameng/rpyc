import threading
import time
import unittest

import rpyc
from rpyc.core import consts
from rpyc.core.protocol import RequestLimitExceeded
from rpyc.core.stream import PipeStream
from rpyc.lib import spawn
from rpyc.utils.factory import connect_stream


class EchoService(rpyc.Service):
    def exposed_echo(self, obj):
        return obj

    def exposed_sleep(self, seconds):
        time.sleep(seconds)
        return seconds

    def exposed_call_back(self, cb):
        # reverse call into the client while its request is in flight
        return cb()


class TestInFlightLimit(unittest.TestCase):
    def setUp(self):
        client_stream, server_stream = PipeStream.create_pair()
        self.server_conn = connect_stream(server_stream, service=EchoService)
        self.server_thread = spawn(self.server_conn.serve_all)
        self.conn = connect_stream(
            client_stream, service=rpyc.VoidService,
            config={"max_inflight_requests": 1})
        # pre-warm the root netref (HANDLE_GETROOT/HANDLE_INSPECT need permits)
        self.root = self.conn.root

    def tearDown(self):
        self.conn.close()
        self.server_thread.join(5)

    def _echo(self, obj):
        return self.conn.async_request(consts.HANDLE_CALLATTR, self.root, "echo", (obj,), ())

    def _echo_wait(self, obj, wait_timeout):
        return self.conn.async_request_wait(
            consts.HANDLE_CALLATTR, self.root, "echo", (obj,), (), wait_timeout=wait_timeout)

    def _sleep(self, seconds):
        return self.conn.async_request(consts.HANDLE_CALLATTR, self.root, "sleep", (seconds,), ())

    def test_immediate_rejection_and_table_peak(self):
        res_a = self._sleep(0.4)
        self.assertEqual(len(self.conn._request_callbacks), 1)
        self.assertEqual(len(self.conn._inflight), 1)
        # immediate mode: rejected at once, and never enters the callback table
        self.assertRaises(RequestLimitExceeded, self._echo, "x")
        self.assertEqual(len(self.conn._request_callbacks), 1)
        self.assertEqual(res_a.value, 0.4)
        # permit and table entry released on result
        self.assertEqual(len(self.conn._request_callbacks), 0)
        self.assertEqual(len(self.conn._inflight), 0)
        # limit is enforced again after recovery
        res_b = self._sleep(0.1)
        self.assertRaises(RequestLimitExceeded, self._echo, "y")
        self.assertEqual(res_b.value, 0.1)
        self.assertEqual(len(self.conn._inflight), 0)

    def test_bounded_wait_succeeds_after_permit_release(self):
        peak = [0]
        stop = threading.Event()

        def sampler():
            while not stop.is_set():
                peak[0] = max(peak[0], len(self.conn._request_callbacks))
                time.sleep(0.002)

        thread = threading.Thread(target=sampler)
        thread.start()
        try:
            res_a = self._sleep(0.3)
            t0 = time.time()
            res_b = self._echo_wait("queued", wait_timeout=5)
            self.assertGreaterEqual(time.time() - t0, 0.2)  # actually waited
            self.assertEqual(res_b.value, "queued")
            self.assertEqual(res_a.value, 0.3)
        finally:
            stop.set()
            thread.join()
        # backpressure: at most one request was ever registered
        self.assertLessEqual(peak[0], 1)
        self.assertEqual(len(self.conn._inflight), 0)
        self.assertEqual(len(self.conn._request_callbacks), 0)

    def test_bounded_wait_timeout(self):
        res_a = self._sleep(1.0)
        t0 = time.time()
        self.assertRaises(RequestLimitExceeded, self._echo_wait, "x", 0.2)
        dt = time.time() - t0
        self.assertGreaterEqual(dt, 0.15)
        self.assertLess(dt, 0.9)
        # the timed-out submission never consumed a permit or table entry
        self.assertEqual(len(self.conn._request_callbacks), 1)
        self.assertEqual(len(self.conn._inflight), 1)
        self.assertEqual(res_a.value, 1.0)
        self.assertEqual(len(self.conn._inflight), 0)

    def test_reentrant_reverse_call_makes_bounded_progress(self):
        # limit=1: the outer request holds the only permit while the remote
        # side calls back into us; the nested submission must wait in a
        # bounded way and unwind instead of deadlocking both parties.
        states = []

        def cb():
            states.append(len(self.conn._request_callbacks))
            try:
                nested = self._echo_wait("nested", wait_timeout=0.5)
                return nested.value
            except RequestLimitExceeded:
                return "limit-hit"

        t0 = time.time()
        res = self.conn.async_request(consts.HANDLE_CALLATTR, self.root, "call_back", (cb,), ())
        self.assertEqual(res.value, "limit-hit")
        self.assertLess(time.time() - t0, 5)
        # the nested request was never queued while waiting for a permit
        self.assertEqual(states, [1])
        # the connection unwound and recovered instead of deadlocking
        self.assertEqual(len(self.conn._inflight), 0)
        self.assertEqual(len(self.conn._request_callbacks), 0)
        self.assertEqual(self._echo("alive").value, "alive")

    def test_local_cancel_releases_permit(self):
        # expiry-driven cancel
        res_a = self._sleep(2)
        res_a.set_expiry(0.2)
        self.assertRaises(rpyc.AsyncResultTimeout, res_a.wait)
        self.assertEqual(len(self.conn._inflight), 0)
        self.assertEqual(len(self.conn._request_callbacks), 0)
        # explicit cancel
        res_b = self._sleep(2)
        self.assertEqual(len(self.conn._inflight), 1)
        self.assertTrue(res_b.cancel())
        self.assertFalse(res_b.cancel())
        self.assertEqual(len(self.conn._inflight), 0)
        self.assertEqual(len(self.conn._request_callbacks), 0)
        # permits were really recovered: new submissions are accepted
        self.assertEqual(self._echo("after-cancel").value, "after-cancel")

    def test_disconnect_releases_permits_and_wakes_waiter(self):
        self._sleep(5)
        errors = []

        def waiter():
            try:
                self._echo_wait("x", wait_timeout=30)
            except (EOFError, OSError, RequestLimitExceeded) as exc:
                errors.append(exc)

        thread = threading.Thread(target=waiter)
        thread.start()
        time.sleep(0.3)
        self.assertEqual(len(self.conn._inflight), 1)
        self.conn.close()
        thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertEqual(len(self.conn._inflight), 0)
        self.assertEqual(len(self.conn._request_callbacks), 0)

    def test_config_change_and_duplicate_completion_do_not_over_release(self):
        fake_seq = 10 ** 9
        self.conn._acquire_inflight_permit(fake_seq)
        self.assertEqual(len(self.conn._inflight), 1)
        # disabling the limit at runtime must not break release bookkeeping
        self.conn._config["max_inflight_requests"] = None
        self.conn._release_inflight_permit(fake_seq)
        self.conn._release_inflight_permit(fake_seq)  # duplicate completion
        self.assertEqual(len(self.conn._inflight), 0)
        # re-enabling the limit: it is still enforced exactly
        self.conn._config["max_inflight_requests"] = 1
        self.conn._acquire_inflight_permit(fake_seq)
        self.assertRaises(RequestLimitExceeded, self.conn._acquire_inflight_permit, fake_seq + 1)
        self.conn._release_inflight_permit(fake_seq)
        self.assertEqual(len(self.conn._inflight), 0)


if __name__ == "__main__":
    unittest.main()
