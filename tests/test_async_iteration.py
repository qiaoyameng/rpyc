import asyncio
import threading
import unittest

import rpyc
from rpyc.core import consts
from rpyc.core.stream import PipeStream


@rpyc.service
class AsyncIterationService(rpyc.Service):
    def __init__(self):
        self.closed = threading.Event()
        self.cancel_release = None
        self.cancel_release_loop = None
        self.disconnect_release = None
        self.disconnect_release_loop = None
        self.disconnect_conn = None

    def on_connect(self, conn):
        self.closed.clear()
        self.cancel_release = asyncio.Event()
        self.disconnect_release = asyncio.Event()
        manager = conn._async_iteration()
        self.cancel_release_loop = manager.loop
        self.disconnect_release_loop = manager.loop
        self.disconnect_conn = conn

    async def exposed_values(self):
        yield 1
        yield 2

    async def exposed_error(self):
        yield 1
        raise ValueError("boom")

    async def exposed_cancel_then_close(self):
        try:
            yield 1
            await self.cancel_release.wait()
        finally:
            self.closed.set()

    async def exposed_disconnecting(self):
        try:
            yield 1
            await self.disconnect_release.wait()
        finally:
            self.closed.set()

    def exposed_ping(self):
        self.ping_count += 1
        return "pong"

    def exposed_close_from_server(self):
        self.disconnect_conn.close()

    def exposed_release_cancel(self):
        self.cancel_release_loop.call_soon_threadsafe(self.cancel_release.set)


@rpyc.service
class OldAsyncEndpoint(rpyc.Service):
    async def exposed_values(self):
        yield 1
        yield 2


class TestRemoteAsyncIteration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.service = AsyncIterationService()
        self.conn = rpyc.connect_thread(
            remote_service=self.service,
            remote_config={"import_custom_exceptions": True},
            config={"import_custom_exceptions": True},
        )

    async def asyncTearDown(self):
        if not self.conn.closed:
            self.conn.close()

    async def heartbeat(self, stop_event, ticks=10):
        count = 0
        while count < ticks and not stop_event.is_set():
            await asyncio.sleep(0.005)
            count += 1
        return count

    async def test_first_items_and_stop(self):
        values = []
        async for value in rpyc.async_iter(self.conn.root.values):
            values.append(value)
        self.assertEqual(values, [1, 2])

    async def test_exception_from_remote_generator(self):
        values = []
        with self.assertRaises(ValueError):
            async for value in rpyc.async_iter(self.conn.root.error):
                values.append(value)
        self.assertEqual(values, [1])

    async def test_heartbeat_while_waiting_and_rpc_after_close(self):
        stop = threading.Event()
        heartbeat = asyncio.ensure_future(self.heartbeat(stop))
        iterator = rpyc.async_iter(self.conn.root.cancel_then_close)
        try:
            async for value in iterator:
                self.assertEqual(value, 1)
                break
        finally:
            stop.set()
        ticks = await heartbeat
        self.assertGreater(ticks, 1)
        self.assertTrue(self.service.closed.wait(1))
        self.assertEqual(self.conn.root.ping(), "pong")

    async def test_cancel_then_explicit_close_runs_remote_finally(self):
        iterator = rpyc.async_iter(self.conn.root.cancel_then_close)
        self.assertEqual(await iterator.__anext__(), 1)
        pending = asyncio.ensure_future(iterator.__anext__())
        await asyncio.sleep(0.05)
        pending.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await pending
        await iterator.aclose()
        self.assertTrue(self.service.closed.wait(1))
        self.assertEqual(self.conn.root.ping(), "pong")

    async def test_disconnect_while_waiting(self):
        iterator = rpyc.async_iter(self.conn.root.disconnecting)
        self.assertEqual(await iterator.__anext__(), 1)
        pending = asyncio.ensure_future(iterator.__anext__())
        await asyncio.sleep(0.05)
        try:
            self.conn.root.close_from_server()
        except EOFError:
            pass
        with self.assertRaises(EOFError):
            await pending

    async def test_athrow_and_asend_are_explicitly_unsupported(self):
        iterator = rpyc.async_iter(self.conn.root.values)
        with self.assertRaises(TypeError):
            await iterator.athrow(ValueError())
        with self.assertRaises(TypeError):
            await iterator.asend(None)

    async def test_concurrent_pull_is_stable(self):
        wrapper = rpyc.async_iter(self.conn.root.cancel_then_close)
        iterator = wrapper._iterator
        await iterator._start()
        self.assertEqual(await iterator._pull_one(), 1)
        first = asyncio.ensure_future(iterator._pull_one())
        await asyncio.sleep(0.02)
        with self.assertRaises(rpyc.RemoteAsyncIterationBusyError):
            await iterator._pull_one()
        first.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await first
        await wrapper.aclose()
        self.assertTrue(self.service.closed.wait(1))


class TestOldAsyncIterationEndpoint(unittest.IsolatedAsyncioTestCase):
    async def test_handshake_required(self):
        class OldServer(rpyc.Connection):
            @classmethod
            def _request_handlers(cls):
                handlers = super()._request_handlers()
                for handler in (
                    consts.HANDLE_AITER_HELLO,
                    consts.HANDLE_AITER_CREATE,
                    consts.HANDLE_AITER_ANEXT,
                    consts.HANDLE_AITER_ATHROW,
                    consts.HANDLE_AITER_ACLOSE,
                    consts.HANDLE_AITER_CANCEL,
                ):
                    handlers.pop(handler, None)
                return handlers

        service = OldAsyncEndpoint()
        server_side, client_side = PipeStream.create_pair()
        threading.Thread(
            target=lambda: OldServer(service, rpyc.Channel(server_side)).serve_all(),
            daemon=True,
        ).start()
        conn = rpyc.connect_stream(client_side, rpyc.VoidService)
        try:
            with self.assertRaises(rpyc.RemoteAsyncIterationNotSupportedError):
                await rpyc.async_iter(conn.root.values).__anext__()
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
