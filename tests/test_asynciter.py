"""
Tests for remote asynchronous iteration (the ``async-iter/1`` protocol):
``rpyc.asynciter`` adapts a netref of a remote async generator so it can be
consumed with ``async for`` without blocking the client's event loop.

Connections are established in-process over anonymous pipes (PipeStream),
and the server-side async generators are controlled by threading events.
"""
import asyncio
import threading
import unittest

import rpyc
from rpyc.core import PipeStream, Channel, Connection, VoidService, Service, consts
from rpyc.utils.asynciter import asynciter


async def _wait_flag(flag):
    """Awaits a threading.Event from within an async generator."""
    while not flag.is_set():
        await asyncio.sleep(0.005)


class StreamControl(object):
    """Server-side control for an event-driven async generator."""

    def __init__(self):
        self.release = threading.Event()  # releases the second item
        self.closed = threading.Event()   # set by the generator's finally

    async def generator(self):
        try:
            yield 0
            await _wait_flag(self.release)
            yield 1
            await asyncio.Event().wait()  # stays open until closed
        finally:
            self.closed.set()


class AsyncIterService(Service):
    def __init__(self):
        self.ctrls = {}

    def exposed_echo(self, value):
        return value

    def exposed_counter(self, n, interval=0.01):
        async def gen():
            for i in range(n):
                yield i
                await asyncio.sleep(interval)
        return gen()

    def exposed_failing(self):
        async def gen():
            yield 0
            raise ValueError("boom")
        return gen()

    def exposed_controlled(self, name):
        ctrl = StreamControl()
        self.ctrls[name] = ctrl
        return ctrl.generator()


class LegacyConnection(Connection):
    """Simulates an endpoint that predates capability negotiation."""

    @classmethod
    def _request_handlers(cls):
        handlers = super()._request_handlers()
        for handler in (consts.HANDLE_GETCAPS, consts.HANDLE_AITER_NEW,
                        consts.HANDLE_AITER_ANEXT, consts.HANDLE_AITER_ACLOSE,
                        consts.HANDLE_AITER_ATHROW):
            handlers.pop(handler, None)
        return handlers


class PipePair(object):
    """An in-process RPyC connection pair over anonymous pipes."""

    def __init__(self, service, conn_class=Connection):
        s1, s2 = PipeStream.create_pair()
        self.server_conn = conn_class(service, Channel(s1))
        self.client_conn = Connection(VoidService(), Channel(s2))
        self.thread = threading.Thread(target=self.server_conn.serve_all, daemon=True)
        self.thread.start()

    def close(self):
        for conn in (self.client_conn, self.server_conn):
            try:
                conn.close()
            except Exception:
                pass


class Heartbeat(object):
    def __init__(self):
        self.count = 0

    async def run(self):
        while True:
            self.count += 1
            await asyncio.sleep(0.005)


class TestAsyncIter(unittest.TestCase):
    def setUp(self):
        self.service = AsyncIterService()
        self.pair = PipePair(self.service)
        self.conn = self.pair.client_conn

    def tearDown(self):
        self.pair.close()

    def test_capability_negotiated(self):
        caps = self.conn.get_remote_capabilities()
        self.assertIn(consts.CAP_ASYNC_ITER_V1, caps)

    def test_full_iteration(self):
        async def main():
            it = asynciter(self.conn.root.counter(3))
            collected = []
            async for item in it:
                collected.append(item)
            self.assertEqual(collected, [0, 1, 2])
            # iteration past the end keeps raising StopAsyncIteration
            with self.assertRaises(StopAsyncIteration):
                await it.__anext__()
            await it.aclose()
        asyncio.run(main())

    def test_first_item_then_exception(self):
        async def main():
            it = asynciter(self.conn.root.failing())
            self.assertEqual(await it.__anext__(), 0)
            with self.assertRaises(ValueError) as ctx:
                await it.__anext__()
            self.assertIn("boom", str(ctx.exception))
            await it.aclose()
        asyncio.run(main())

    def test_concurrent_anext_serialized(self):
        async def main():
            it = asynciter(self.conn.root.counter(3))
            # concurrent pulls are serialized: order is determined by the
            # pulls, not by reply arrival
            results = await asyncio.gather(it.__anext__(), it.__anext__())
            self.assertEqual(results, [0, 1])
            self.assertEqual(await it.__anext__(), 2)
            await it.aclose()
        asyncio.run(main())

    def test_server_rejects_concurrent_pull(self):
        async def main():
            it = asynciter(self.conn.root.controlled("c"))
            ctrl = self.service.ctrls["c"]
            await it._ensure_started()
            self.assertEqual(await it.__anext__(), 0)
            # bypass the client-side serializer: the server must deterministically
            # reject a second concurrent pull instead of reordering elements
            f1 = it._call_remote(consts.HANDLE_AITER_ANEXT, it._remote_it)
            f2 = it._call_remote(consts.HANDLE_AITER_ANEXT, it._remote_it)
            with self.assertRaises(RuntimeError):
                await f2
            ctrl.release.set()
            self.assertEqual(await f1, 1)
            await it.aclose()
        asyncio.run(main())

    def test_athrow_explicitly_rejected(self):
        async def main():
            it = asynciter(self.conn.root.counter(1))
            with self.assertRaises(NotImplementedError):
                it.athrow(ValueError())
            # the server side rejects the reserved operation as well
            await it._ensure_started()
            with self.assertRaises(NotImplementedError):
                await it._call_remote(consts.HANDLE_AITER_ATHROW, it._remote_it)
            await it.aclose()
        asyncio.run(main())

    def test_cancel_wait_late_reply_and_close(self):
        async def main():
            ctrl_name = "cancel"
            it = asynciter(self.conn.root.controlled(ctrl_name))
            ctrl = self.service.ctrls[ctrl_name]
            hb = Heartbeat()
            hb_task = asyncio.create_task(hb.run())
            try:
                self.assertEqual(await it.__anext__(), 0)
                task = asyncio.create_task(it.__anext__())
                await asyncio.sleep(0.2)  # let the pull reach the server
                beats = hb.count
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                # the remote pull is still in flight; it completes late and
                # its element must be discarded without disturbing the loop
                ctrl.release.set()
                await asyncio.sleep(0.5)
                self.assertGreater(hb.count, beats)  # loop never blocked
                # explicit close cooperatively runs the remote finally
                await it.aclose()
                self.assertTrue(ctrl.closed.is_set())
            finally:
                hb_task.cancel()
        asyncio.run(main())

    def test_early_close_runs_remote_finally(self):
        async def main():
            ctrl_name = "early"
            async with asynciter(self.conn.root.controlled(ctrl_name)) as it:
                ctrl = self.service.ctrls[ctrl_name]
                async for item in it:
                    self.assertEqual(item, 0)
                    break
            # leaving the async with block closed the remote iterator
            self.assertTrue(ctrl.closed.is_set())
            with self.assertRaises(StopAsyncIteration):
                await it.__anext__()
            await it.aclose()  # idempotent
        asyncio.run(main())

    def test_close_while_pull_in_flight(self):
        async def main():
            ctrl_name = "inflight"
            it = asynciter(self.conn.root.controlled(ctrl_name))
            ctrl = self.service.ctrls[ctrl_name]
            self.assertEqual(await it.__anext__(), 0)
            task = asyncio.create_task(it.__anext__())  # pends remotely
            await asyncio.sleep(0.2)
            await it.aclose()  # server cancels the in-flight pull, runs finally
            self.assertTrue(ctrl.closed.is_set())
            with self.assertRaises(StopAsyncIteration):
                await task
        asyncio.run(main())

    def test_heartbeat_not_blocked_by_pull(self):
        async def main():
            ctrl_name = "heartbeat"
            it = asynciter(self.conn.root.controlled(ctrl_name))
            ctrl = self.service.ctrls[ctrl_name]
            hb = Heartbeat()
            hb_task = asyncio.create_task(hb.run())
            try:
                self.assertEqual(await it.__anext__(), 0)

                async def release_later():
                    await asyncio.sleep(0.4)
                    ctrl.release.set()

                asyncio.create_task(release_later())
                beats = hb.count
                # this pull waits ~0.4s for the remote item; the local event
                # loop must keep pumping heartbeats meanwhile
                self.assertEqual(await it.__anext__(), 1)
                self.assertGreaterEqual(hb.count - beats, 20)
                await it.aclose()
            finally:
                hb_task.cancel()
        asyncio.run(main())

    def test_close_does_not_affect_other_rpc(self):
        async def main():
            it1 = asynciter(self.conn.root.counter(2))
            collected = []
            async for item in it1:
                collected.append(item)
            await it1.aclose()
            # a plain RPC over the same connection is unaffected
            res = rpyc.async_(self.conn.root.echo)("hello")
            while not res.ready:
                await asyncio.sleep(0.005)
            self.assertEqual(res.value, "hello")
            # and so is a second iterator over the same connection
            it2 = asynciter(self.conn.root.counter(2))
            collected2 = []
            async for item in it2:
                collected2.append(item)
            self.assertEqual(collected2, [0, 1])
            await it2.aclose()
        asyncio.run(main())

    def test_disconnect_fails_pull_and_runs_remote_finally(self):
        ctrl_name = "disconnect"
        outcome = {}

        async def main():
            it = asynciter(self.conn.root.controlled(ctrl_name))
            self.assertEqual(await it.__anext__(), 0)
            task = asyncio.create_task(it.__anext__())  # pends remotely
            await asyncio.sleep(0.2)
            closer = threading.Thread(target=self.pair.server_conn.close, daemon=True)
            closer.start()
            try:
                await task
            except (EOFError, OSError) as exc:
                outcome["exc"] = exc
            closer.join(5)
            outcome["closed_late"] = self.service.ctrls[ctrl_name].closed.is_set()

        asyncio.run(main())
        self.assertIsInstance(outcome.get("exc"), (EOFError, OSError))
        # server-side cleanup closed the generator: its finally ran
        self.assertTrue(self.service.ctrls[ctrl_name].closed.wait(5))


class TestLegacyEndpoint(unittest.TestCase):
    """An endpoint that did not negotiate capabilities must not be mistaken
    for supporting remote async iteration."""

    def setUp(self):
        self.service = AsyncIterService()
        self.pair = PipePair(self.service, conn_class=LegacyConnection)
        self.conn = self.pair.client_conn

    def tearDown(self):
        self.pair.close()

    def test_legacy_endpoint_rejected(self):
        self.assertEqual(self.conn.get_remote_capabilities(), frozenset())

        async def main():
            it = asynciter(self.conn.root.counter(1))
            with self.assertRaises(NotImplementedError) as ctx:
                await it.__anext__()
            self.assertIn("async-iter", str(ctx.exception))
            # ordinary RPC on the same legacy endpoint is unaffected
            res = rpyc.async_(self.conn.root.echo)("ok")
            while not res.ready:
                await asyncio.sleep(0.005)
            self.assertEqual(res.value, "ok")

        asyncio.run(main())


if __name__ == "__main__":
    unittest.main()
