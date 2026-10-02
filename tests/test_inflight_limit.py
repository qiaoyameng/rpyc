import threading
import time
import unittest

import rpyc
from rpyc.core import consts
from rpyc.core.protocol import Connection, InflightLimitExceeded
from rpyc.core.channel import Channel
from rpyc.core.stream import PipeStream
from rpyc.core.service import VoidService


class CallbackService(rpyc.Service):
    """Service used to exercise the in-flight request limit over a local
    pipe connection."""

    def __init__(self):
        self._gate = threading.Event()

    def exposed_block(self):
        self._gate.wait(10)
        return "unblocked"

    def exposed_ping(self):
        return "pong"

    def exposed_call_cb(self, cb):
        return cb()

    def release(self):
        self._gate.set()


class TestInflightLimit(unittest.TestCase):
    def setUp(self):
        client_stream, server_stream = PipeStream.create_pair()
        self.service = CallbackService()
        self.server = Connection(self.service, Channel(server_stream),
                                 config={"connid": "server"})
        self.server_thread = threading.Thread(target=self.server.serve_all, daemon=True)
        self.server_thread.start()
        self.client = Connection(VoidService(), Channel(client_stream),
                                 config={"connid": "client", "max_inflight_requests": 1})
        self.ping = self.client.root.exposed_ping
        self.a_block = rpyc.async_(self.client.root.exposed_block)

    def tearDown(self):
        self.service.release()
        try:
            self.client.close()
        except Exception:
            pass
        self.server_thread.join(timeout=5)

    def _submit_waiter(self, results, slot_timeout):
        try:
            results["res"] = self.client.async_request(
                consts.HANDLE_CALL, self.ping, (), (), slot_timeout=slot_timeout)
        except Exception as exc:
            results["err"] = exc

    def test_immediate_reject_and_quota_recovery(self):
        res1 = self.a_block()
        time.sleep(0.2)
        # slot held from registration; table cannot exceed the limit
        self.assertEqual(len(self.client._inflight_seqs), 1)
        self.assertEqual(len(self.client._request_callbacks), 1)
        with self.assertRaises(InflightLimitExceeded):
            self.a_block()
        # rejected request was not buffered anywhere
        self.assertEqual(len(self.client._request_callbacks), 1)
        self.assertEqual(len(self.client._inflight_seqs), 1)
        self.service.release()
        self.assertEqual(res1.value, "unblocked")
        # quota and table fully recovered once the result arrived
        self.assertEqual(len(self.client._inflight_seqs), 0)
        self.assertEqual(len(self.client._request_callbacks), 0)
        # connection still usable afterwards
        self.assertEqual(self.client.root.exposed_ping(), "pong")

    def test_reentrant_callback_chain_immediate_reject(self):
        # remote calls back into us while our request holds the only slot;
        # the nested local->remote request must be rejected, not deadlock
        client = self.client
        observed = []

        def local_cb():
            observed.append(len(client._request_callbacks))
            try:
                client.async_request(consts.HANDLE_CALL, self.ping, (), ())
            except InflightLimitExceeded:
                return "limit-hit"
            return "unexpected"

        a_call_cb = rpyc.async_(self.client.root.exposed_call_cb)
        res = a_call_cb(local_cb)
        self.assertEqual(res.value, "limit-hit")
        # while the reverse callback ran, our request was the only one in flight
        self.assertEqual(observed, [1])
        self.assertEqual(len(client._inflight_seqs), 0)
        self.assertEqual(len(client._request_callbacks), 0)

    def test_reentrant_callback_chain_bounded_wait(self):
        # same chain, but the nested request uses a bounded wait: it must make
        # bounded progress attempts and then fail in finite time instead of
        # locking up both peers
        client = self.client

        def local_cb():
            t0 = time.time()
            try:
                client.async_request(consts.HANDLE_CALL, self.ping, (), (),
                                     slot_timeout=0.5)
            except InflightLimitExceeded:
                return time.time() - t0
            return -1

        a_call_cb = rpyc.async_(self.client.root.exposed_call_cb)
        res = a_call_cb(local_cb)
        dt = res.value
        self.assertGreaterEqual(dt, 0.4)
        self.assertLess(dt, 5)
        self.assertEqual(len(client._inflight_seqs), 0)
        self.assertEqual(len(client._request_callbacks), 0)

    def test_bounded_wait_queues_until_slot_frees(self):
        res1 = self.a_block()
        time.sleep(0.2)
        results = {}
        t = threading.Thread(target=self._submit_waiter, args=(results, 5))
        t.start()
        time.sleep(0.5)
        # waiter is still parked and has not been buffered into the table
        self.assertNotIn("res", results)
        self.assertNotIn("err", results)
        self.assertEqual(len(self.client._request_callbacks), 1)
        self.service.release()
        t.join(5)
        self.assertFalse(t.is_alive())
        res2 = results["res"]
        self.assertEqual(res1.value, "unblocked")
        self.assertEqual(res2.value, "pong")
        self.assertEqual(len(self.client._inflight_seqs), 0)
        self.assertEqual(len(self.client._request_callbacks), 0)

    def test_bounded_wait_timeout(self):
        res1 = self.a_block()
        time.sleep(0.2)
        t0 = time.time()
        with self.assertRaises(InflightLimitExceeded):
            self.client.async_request(consts.HANDLE_CALL, self.ping, (), (),
                                      slot_timeout=0.4)
        dt = time.time() - t0
        self.assertGreaterEqual(dt, 0.35)
        self.assertLess(dt, 5)
        # the timed-out request never entered the table
        self.assertEqual(len(self.client._request_callbacks), 1)
        self.assertEqual(len(self.client._inflight_seqs), 1)
        self.service.release()
        self.assertEqual(res1.value, "unblocked")
        self.assertEqual(len(self.client._inflight_seqs), 0)
        self.assertEqual(len(self.client._request_callbacks), 0)

    def test_disconnect_releases_waiters_and_quota(self):
        self.a_block()
        time.sleep(0.2)
        results = {}
        t = threading.Thread(target=self._submit_waiter, args=(results, 30))
        t.start()
        time.sleep(0.5)
        # abrupt disconnect: the peer's stream dies while a request is in
        # flight and another thread is waiting for a slot
        self.server._channel.close()
        t.join(5)
        self.assertFalse(t.is_alive())
        # the waiter woke up with an error instead of hanging forever
        self.assertIn("err", results)
        self.assertIsInstance(results["err"], (EOFError, OSError, InflightLimitExceeded))
        # connection close released the table and all slots
        deadline = time.time() + 5
        while self.client._request_callbacks and time.time() < deadline:
            time.sleep(0.05)
        self.assertEqual(len(self.client._request_callbacks), 0)
        self.assertEqual(len(self.client._inflight_seqs), 0)

    def test_release_idempotent_and_config_change_safe(self):
        conn = self.client
        conn._config["max_inflight_requests"] = 3
        conn._acquire_inflight_slot(100, 0)
        conn._acquire_inflight_slot(101, 0)
        self.assertEqual(len(conn._inflight_seqs), 2)
        conn._release_inflight_slot(100)
        conn._release_inflight_slot(100)  # duplicate completion
        conn._release_inflight_slot(999)  # never held a slot
        self.assertEqual(conn._inflight_seqs, {101})
        # switching to unlimited must not strand or over-release slots
        conn._config["max_inflight_requests"] = None
        conn._acquire_inflight_slot(102, 0)  # untracked while unlimited
        conn._release_inflight_slot(101)
        self.assertEqual(len(conn._inflight_seqs), 0)


if __name__ == "__main__":
    unittest.main()
