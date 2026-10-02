import time
import unittest

import rpyc
from rpyc.core import consts
from rpyc.core.stream import PipeStream


class ControllableClock(object):
    def __init__(self, elapsed, offset=0.0):
        self.elapsed = elapsed
        self.offset = offset

    def __call__(self):
        return self.elapsed[0] + self.offset


class EntryCallback(object):
    def __init__(self, observed, calls):
        self.observed = observed
        self.calls = calls

    def exposed_reenter(self, peer):
        self.calls.append("reenter")
        if self.observed is not None:
            self.observed.append(rpyc.current_deadline_budget())
        return peer.pong()


class CallbackRelay(object):
    def __init__(self, callback, reentry_peer):
        self.callback = callback
        self.reentry_peer = reentry_peer

    def exposed_reenter(self, peer):
        return self.callback.reenter(self.reentry_peer)


class ReentryPeer(object):
    def __init__(self, service=None, observed=None):
        self.service = service
        self.observed = observed

    def exposed_pong(self):
        if self.observed is not None:
            self.observed.append(rpyc.current_deadline_budget())
        if self.service is None:
            return "pong"
        self.service.calls.append("pong")
        self.service.observed.append(rpyc.current_deadline_budget())
        return self.service.exposed_pong()


class ChainService(rpyc.Service):
    def __init__(self, name, elapsed):
        self.name = name
        self.elapsed = elapsed
        self.observed = []
        self.calls = []
        self.outbound_root = None
        self.reentry_peer = None
        self.callback_observed = None

    def _advance_and_observe(self, amount, label):
        self.elapsed[0] += amount
        self.calls.append(label)
        self.observed.append(rpyc.current_deadline_budget())

    def exposed_chain(self, depth, callback=None):
        self._advance_and_observe(1.0, "chain")
        if self.name == "C":
            return callback.reenter(self.reentry_peer)
        if depth > 1:
            if self.name == "B":
                callback = CallbackRelay(callback, ReentryPeer(self, self.callback_observed))
                return self.outbound_root.chain(depth - 1, callback)
        return callback.reenter(self.reentry_peer)

    def exposed_reenter(self, peer):
        self._advance_and_observe(1.0, "reenter")
        return peer.pong()

    def exposed_pong(self):
        return "pong"


class SlowService(rpyc.Service):
    def __init__(self):
        self.finished = False

    def exposed_sleep(self, seconds):
        time.sleep(seconds)
        self.finished = True
        return "slept"

    def exposed_sleep_proxy(self):
        return self.exposed_sleep


def connect_pair(local_service, remote_service, local_config=None, remote_config=None):
    local_stream, remote_stream = PipeStream.create_pair()
    local_conn = rpyc.connect_stream(local_stream, local_service, local_config or {})
    remote_conn = rpyc.connect_stream(remote_stream, remote_service, remote_config or {})
    thread = rpyc.spawn(remote_conn.serve_all)
    return local_conn, remote_conn, thread


class TestDeadlineBudget(unittest.TestCase):
    def test_three_layer_chain_and_reentrant_callback_inherit_budget(self):
        elapsed = [100.0]
        clocks = [ControllableClock(elapsed, offset) for offset in (0.0, 1000.0, -500.0)]
        configs = [{"monotonic_clock": clock, "sync_request_timeout": 30} for clock in clocks]
        service_b = ChainService("B", elapsed)
        service_c = ChainService("C", elapsed)

        # A is the entry client. B calls C, C calls B's relay, B calls A,
        # and A re-enters B.
        conn_ab, server_ab, thread_ab = connect_pair(rpyc.VoidService(), service_b, configs[0], configs[1])
        conn_bc, server_bc, thread_bc = connect_pair(service_b, service_c, configs[1], configs[2])
        service_b.outbound_root = conn_bc.root
        service_b.reentry_peer = ReentryPeer(service_b)
        callback_observed = []
        callback_calls = []
        service_b.callback_observed = callback_observed

        try:
            with conn_ab.deadline_budget(10.0):
                result = conn_ab.root.chain(3, EntryCallback(callback_observed, callback_calls))
            self.assertEqual(result, "pong")

            self.assertEqual(service_b.observed, [9.0, 8.0])
            self.assertEqual(service_c.observed, [8.0])
            self.assertEqual(callback_observed, [8.0, 8.0])
            self.assertEqual(callback_calls, ["reenter"])
            self.assertEqual(
                [service_b.calls, service_c.calls],
                [["chain", "pong"], ["chain"]],
            )
        finally:
            conn_ab.close()
            conn_bc.close()
            thread_ab.join(0.2)
            thread_bc.join(0.2)
            server_ab.close()
            server_bc.close()

    def test_explicit_child_timeout_only_tightens_budget(self):
        service = SlowService()
        conn, _, _ = connect_pair(rpyc.VoidService(), service, {"sync_request_timeout": 5})
        try:
            with conn.deadline_budget(0.2):
                started = time.monotonic()
                with self.assertRaises(rpyc.AsyncResultTimeout):
                    conn.sync_request(consts.HANDLE_CALL, conn.root.sleep, (0.2,), timeout=0.05)
                self.assertLess(time.monotonic() - started, 0.15)
        finally:
            conn.close()
            time.sleep(0.2)
            self.assertTrue(service.finished)

    def test_expired_before_send(self):
        service = SlowService()
        conn, _, _ = connect_pair(rpyc.VoidService(), service, {"sync_request_timeout": 5})
        try:
            with conn.deadline_budget(0.0):
                with self.assertRaises(TimeoutError):
                    conn.root.sleep(0)
                self.assertFalse(conn._request_callbacks)
            self.assertEqual(conn.root.get_service_name(), "SLOW")
        finally:
            conn.close()

    def test_late_reply_is_cleaned_up_and_connection_still_works(self):
        service = SlowService()
        conn, _, _ = connect_pair(rpyc.VoidService(), service, {"sync_request_timeout": 5})
        try:
            sleep = conn.root.sleep
            self.assertEqual(sleep(0), "slept")
            res = conn.async_request(consts.HANDLE_CALL, sleep, (0.05,))
            seq = res._seq
            res.set_expiry(0.01)
            with self.assertRaises(rpyc.AsyncResultTimeout):
                res.value
            self.assertNotIn(seq, conn._request_callbacks)
            time.sleep(0.08)
            self.assertEqual(conn.ping("after"), "after")
        finally:
            conn.close()

    def test_expired_request_is_rejected_before_dispatch(self):
        service = SlowService()
        elapsed = [1000.0]
        client_config = {
            "monotonic_clock": ControllableClock(elapsed, 0.0),
            "sync_request_timeout": 5,
        }
        server_config = {
            "monotonic_clock": ControllableClock(elapsed, 10.0),
            "sync_request_timeout": 5,
        }
        conn, _, _ = connect_pair(rpyc.VoidService(), service, client_config, server_config)
        try:
            conn._negotiate_capabilities()
            async_result = rpyc.AsyncResult(conn)
            seq = conn._get_seq_id()
            conn._request_callbacks[seq] = async_result
            async_result._seq = seq
            sleep_proxy = conn.root.sleep_proxy()
            conn._send(
                consts.MSG_REQUEST,
                seq,
                (consts.HANDLE_CALL, conn._box((sleep_proxy, (0.0,), ()))),
                deadline_budget=-5.0,
            )
            with self.assertRaises(TimeoutError):
                async_result.value
            self.assertFalse(service.finished)
        finally:
            conn.close()

    def test_legacy_peer_rejects_budget_without_claiming_end_to_end_timeout(self):
        class LegacyConnection(rpyc.Connection):
            def _handle_handshake(self, remote_capabilities=None):
                raise KeyError(consts.HANDLE_HANDSHAKE)

        class LegacyService(rpyc.Service):
            _protocol = LegacyConnection

        conn, server, thread = connect_pair(rpyc.VoidService(), LegacyService(), {"sync_request_timeout": 2})
        try:
            self.assertFalse(conn.deadline_budget_supported)
            self.assertEqual(conn.root.get_service_name(), "LEGACY")
            with self.assertRaises(rpyc.DeadlinePropagationNotSupportedError):
                with conn.deadline_budget(1.0):
                    conn.root.get_service_name()
        finally:
            conn.close()
            thread.join(0.2)
            server.close()


if __name__ == "__main__":
    unittest.main()
