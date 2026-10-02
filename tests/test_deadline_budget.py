"""Tests for the optional call-chain deadline budget.

A deadline budget bounds a whole chain of nested/reentrant requests (as
opposed to ``sync_request_timeout`` which bounds each hop individually).
The remaining budget travels on the wire as relative seconds and each peer
adopts it as a local deadline against its own monotonic clock, so peers with
different clock origins interoperate.

These tests use in-process pipe connections and a controllable clock
(``rpyc.core.deadline.monotonic`` is substituted) to build three-layer
chains, reentrant callbacks and expired requests deterministically.
"""
import logging
import threading
import time
import unittest

import rpyc
from rpyc.core import consts, deadline
from rpyc.core.async_ import AsyncResult
from rpyc.core.channel import Channel
from rpyc.core.protocol import Connection
from rpyc.core.stream import PipeStream
from rpyc.lib import spawn


def make_pair(client_service=rpyc.VoidService, client_config=None,
              server_service=rpyc.VoidService, server_config=None):
    """Builds a connected (client, server) pair over anonymous pipes."""
    stream1, stream2 = PipeStream.create_pair()
    server = server_service._connect(Channel(stream2), server_config or {})
    spawn(server.serve_all)
    client = client_service._connect(Channel(stream1), client_config or {})
    return client, server


def pending_replies(conn):
    """Serves any already-sent replies (e.g. of fire-and-forget HANDLE_DEL
    requests) so the callback registry reflects only genuine leaks."""
    conn.poll_all(0.1)
    return conn._request_callbacks


class OffsetClock(object):
    """Controllable monotonic clock. Every thread gets its own origin to
    simulate machines whose monotonic clocks are not aligned; advancing the
    clock advances all of them at the same rate."""

    def __init__(self, start=1000.0):
        self._now = start
        self._origins = {}

    def __call__(self):
        tid = threading.get_ident()
        origin = self._origins.setdefault(tid, len(self._origins) * 100000.0)
        return self._now + origin

    def advance(self, seconds):
        self._now += seconds


class ClockFixture(unittest.TestCase):
    def setUp(self):
        self.clock = OffsetClock()
        self._real_monotonic = deadline.monotonic
        deadline.monotonic = self.clock
        self.addCleanup(self._restore_clock)

    def _restore_clock(self):
        deadline.monotonic = self._real_monotonic


class Worker(rpyc.Service):
    """Leaf service: simulates work by advancing the controlled clock and
    records the budget it inherited."""

    def __init__(self, clock, seen_budgets):
        self._clock = clock
        self._seen = seen_budgets

    def exposed_work(self, cost):
        self._seen.append(deadline.remaining())
        self._clock.advance(cost)
        return deadline.remaining()


class Relay(rpyc.Service):
    """Middle layer: does some work, calls the next hop twice."""

    def __init__(self, clock, next_root, seen_budgets):
        self._clock = clock
        self._next = next_root
        self._seen = seen_budgets

    def exposed_layer(self, cost1, cost2):
        self._seen.append(deadline.remaining())
        self._clock.advance(cost1)
        result = self._next.work(cost2)
        self._next.work(0)  # second hop: must hit the exhausted budget
        return result


class TestThreeLayerChain(ClockFixture):
    """A -> B -> C where the layers together would exceed the entry budget."""

    def setUp(self):
        super().setUp()
        self.c_seen = []
        self.b_seen = []
        self.conn_bc, self.server_c = make_pair(
            server_service=Worker(self.clock, self.c_seen))
        next_root = self.conn_bc.root  # B's handle to C
        self.conn_ab, self.server_b = make_pair(
            client_config={"call_chain_budget": 10, "sync_request_timeout": 30},
            server_service=Relay(self.clock, next_root, self.b_seen))
        self.addCleanup(self.conn_ab.close)
        self.addCleanup(self.conn_bc.close)

    def test_entry_budget_bounds_whole_chain(self):
        # each layer could individually take up to sync_request_timeout (30s),
        # but the chain only has 10 (simulated) seconds in total
        with self.assertRaises(TimeoutError):
            self.conn_ab.root.layer(4, 8)
        # B saw the full budget, C saw what remained after B's 4s of work
        self.assertEqual(self.b_seen, [10])
        self.assertEqual(self.c_seen, [6])
        # C was never asked for the second round of work: the budget was
        # exhausted (4 + 8 > 10) and the request was rejected pre-dispatch
        self.assertEqual(len(self.c_seen), 1)
        # no reply callbacks leaked on either hop
        self.assertEqual(pending_replies(self.conn_ab), {})
        self.assertEqual(pending_replies(self.conn_bc), {})

    def test_budget_propagation_was_negotiated(self):
        with self.assertRaises(TimeoutError):
            self.conn_ab.root.layer(4, 8)
        self.assertIs(self.conn_ab.deadline_propagation, True)
        self.assertIs(self.conn_bc.deadline_propagation, True)

    def test_chain_within_budget_completes(self):
        result = self.conn_ab.root.layer(1, 1)
        # C returns its remaining budget; the second work(0) call also fits
        self.assertIsNotNone(result)
        self.assertEqual(self.b_seen, [10])
        self.assertEqual(self.c_seen, [9, 8])
        self.assertEqual(pending_replies(self.conn_ab), {})


class Callbacker(rpyc.Service):
    def __init__(self, clock, seen_budgets):
        self._clock = clock
        self._seen = seen_budgets

    def exposed_invoke(self, callback, cost):
        self._clock.advance(cost)
        return callback()

    def exposed_echo(self, value):
        self._seen.append(deadline.remaining())
        return value


class TestReentrantCallback(ClockFixture):
    """A calls B; B calls back into A; the callback calls B again. Every hop
    must inherit the entry budget."""

    def setUp(self):
        super().setUp()
        self.server_seen = []
        self.conn, self.server = make_pair(
            server_service=Callbacker(self.clock, self.server_seen))
        self.addCleanup(self.conn.close)
        self.conn.root  # warm up the root netref outside any budget

    def test_callback_chain_inherits_budget(self):
        callback_seen = []

        def callback():
            callback_seen.append(deadline.remaining())
            self.clock.advance(3)
            return self.conn.root.echo("ok")  # reentrant call back to B

        with self.conn.deadline(10):
            result = self.conn.root.invoke(callback, 4)
            still_left = deadline.remaining()

        self.assertEqual(result, "ok")
        # B spent 4s before calling back: the callback saw 6 of 10 left
        self.assertEqual(callback_seen, [6])
        # the callback spent 3s: the reentrant call carried 3 to B
        self.assertEqual(self.server_seen, [3])
        # the whole chain fits the entry budget
        self.assertAlmostEqual(still_left, 3)
        self.assertEqual(pending_replies(self.conn), {})
        self.assertIs(self.conn.deadline_propagation, True)

    def test_budget_exhaustion_inside_callback(self):
        def callback():
            self.clock.advance(8)  # 4 (by B) + 8 > 10
            return self.conn.root.echo("too late")

        with self.conn.deadline(10):
            with self.assertRaises(TimeoutError):
                self.conn.root.invoke(callback, 4)
        # the reentrant request was rejected before dispatch; nothing leaked
        self.assertEqual(self.server_seen, [])
        self.assertEqual(pending_replies(self.conn), {})


class Echo(rpyc.Service):
    def __init__(self):
        self.calls = []

    def exposed_echo(self, value):
        self.calls.append(value)
        return value


class TestExpiredRequest(ClockFixture):
    def setUp(self):
        super().setUp()
        self.service = Echo()
        self.conn, self.server = make_pair(server_service=self.service)
        self.addCleanup(self.conn.close)
        self.conn.root  # warm up outside any budget

    def test_expired_budget_rejected_before_dispatch(self):
        with self.conn.deadline(5):
            self.clock.advance(6)  # budget is now in the past
            with self.assertRaises(TimeoutError):
                self.conn.root.echo("nope")
        # the request never left the client and nothing was registered
        self.assertEqual(self.service.calls, [])
        self.assertEqual(pending_replies(self.conn), {})

    def test_connection_usable_after_expiry(self):
        with self.conn.deadline(5):
            self.clock.advance(6)
            with self.assertRaises(TimeoutError):
                self.conn.root.echo("nope")
        # a subsequent normal RPC works, inside and outside a fresh budget
        self.assertEqual(self.conn.root.echo("plain"), "plain")
        with self.conn.deadline(5):
            self.assertEqual(self.conn.root.echo("budgeted"), "budgeted")
        self.assertEqual(self.service.calls, ["plain", "budgeted"])
        self.assertIs(self.conn.deadline_propagation, True)
        self.assertEqual(pending_replies(self.conn), {})

    def test_server_rejects_request_with_spent_budget(self):
        # simulate a budget that was still positive when sent but is already
        # exhausted when the server dispatches it
        ares = AsyncResult(self.conn)
        seq = self.conn._get_seq_id()
        self.conn._request_callbacks[seq] = ares
        self.conn._send(consts.MSG_REQUEST, seq,
                        (consts.HANDLE_PING, self.conn._box(("x",)), -1.0))
        with self.assertRaises(TimeoutError):
            ares.value
        self.assertEqual(pending_replies(self.conn), {})


class Sleeper(rpyc.Service):
    def exposed_sleep(self, seconds):
        time.sleep(seconds)
        return "slept"

    def exposed_echo(self, value):
        return value


class TestLateReplyCleanup(unittest.TestCase):
    """Uses the real clock with short durations: a timeout must not kill the
    remote thread, the expired request must be cleaned up, and the late reply
    must be discarded without disturbing subsequent RPCs."""

    def setUp(self):
        self.conn, self.server = make_pair(server_service=Sleeper())
        self.addCleanup(self.conn.close)
        self.conn.root  # warm up

    def test_late_reply_is_discarded(self):
        start = time.time()
        with self.conn.deadline(0.2):
            with self.assertRaises(TimeoutError):
                self.conn.root.sleep(1.0)
        self.assertLess(time.time() - start, 1.0)
        # the expired request was removed from the callback registry
        self.assertEqual(pending_replies(self.conn), {})
        # the remote thread keeps running and replies eventually; the next
        # RPC must consume (and ignore) that late reply transparently
        time.sleep(1.2)
        self.assertEqual(self.conn.root.echo("alive"), "alive")
        self.assertEqual(pending_replies(self.conn), {})

    def test_explicit_timeout_only_tightens_budget(self):
        # generous budget, tight per-call timeout: the per-call timeout wins
        start = time.time()
        with self.conn.deadline(30):
            ares = self.conn.async_request(consts.HANDLE_CALLATTR,
                                           self.conn.root, "sleep", (0.4,), (),
                                           timeout=0.1)
            with self.assertRaises(TimeoutError):
                ares.value
        self.assertLess(time.time() - start, 0.4)
        self.assertEqual(pending_replies(self.conn), {})
        time.sleep(0.5)  # let the late reply arrive
        self.assertEqual(self.conn.root.echo("alive"), "alive")


class LegacyConnection(Connection):
    """Simulates an old endpoint: it does not know HANDLE_GETCAPS and answers
    the capability query with an exception, like any pre-budget RPyC."""

    @classmethod
    def _request_handlers(cls):
        handlers = super()._request_handlers()
        handlers.pop(consts.HANDLE_GETCAPS, None)
        return handlers


class LegacyService(rpyc.Service):
    _protocol = LegacyConnection

    def exposed_echo(self, value):
        return value

    def exposed_sleep(self, seconds):
        time.sleep(seconds)
        return "slept"


class TestLegacyEndpoint(unittest.TestCase):
    """Compatibility rules for peers that did not negotiate the capability:
    requests keep the legacy wire format, the budget is enforced locally
    only, and the connection must not claim end-to-end enforcement."""

    def setUp(self):
        self.warnings = []
        logger = logging.getLogger("test.deadline_budget.legacy")
        handler = logging.Handler()
        handler.emit = self.warnings.append
        logger.addHandler(handler)
        logger.setLevel(logging.WARNING)
        self.addCleanup(logger.removeHandler, handler)
        config = {"call_chain_budget": 0.2, "logger": logger}
        self.conn, self.server = make_pair(client_config=config,
                                           server_service=LegacyService())
        self.addCleanup(self.conn.close)
        # observe the exact wire format of outgoing requests
        self.sent = []
        real_send = self.conn._send

        def spy_send(msg, seq, args):
            if msg == consts.MSG_REQUEST:
                self.sent.append((seq, args))
            return real_send(msg, seq, args)
        self.conn._send = spy_send

    def test_legacy_peer_gets_legacy_format_and_local_enforcement(self):
        self.assertEqual(self.conn.root.echo("hi"), "hi")
        # the capability query failed -> no end-to-end claim may be made
        self.assertIs(self.conn.deadline_propagation, False)
        self.assertTrue(self.warnings, "fallback must not be silent")
        # every request (including the capability query) used the 2-tuple
        # legacy format; no budget was ever attached
        self.assertTrue(self.sent)
        self.assertTrue(all(len(args) == 2 for _, args in self.sent))
        # the budget is still enforced locally
        start = time.time()
        with self.assertRaises(TimeoutError):
            self.conn.root.sleep(0.5)
        self.assertLess(time.time() - start, 0.5)
        # the expired request was cleaned from the callback registry, even
        # though the (busy) legacy peer has not replied yet
        sleep_seq = [seq for seq, args in self.sent
                     if args[0] == consts.HANDLE_CALL][-1]
        self.assertNotIn(sleep_seq, self.conn._request_callbacks)
        # connection stays usable once the late reply has arrived
        time.sleep(0.5)
        self.assertEqual(self.conn.root.echo("alive"), "alive")
        self.assertEqual(pending_replies(self.conn), {})


if __name__ == "__main__":
    unittest.main()
