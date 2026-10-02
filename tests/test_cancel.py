import gc
import threading
import unittest

import rpyc
from rpyc.core.channel import Channel
from rpyc.core import consts
from rpyc.core.protocol import Connection
from rpyc.core.stream import PipeStream
from rpyc.utils.factory import connect_channel


class CancelService(rpyc.Service):
    started = ready_for_finish = allow_finish = late_started = late_release = late_object_released = None
    skip_started = None
    delay_spawn = False
    delayed_requests = []

    def on_connect(self, conn):
        for name in ("started", "ready_for_finish", "allow_finish", "late_started",
                     "late_release", "late_object_released", "skip_started"):
            setattr(type(self), name, threading.Event())
        type(self).delay_spawn = False
        type(self).delayed_requests = []

    def exposed_loop(self):
        type(self).started.set()
        token = rpyc.current_cancellation_token()
        while not token.wait(0.001):
            pass
        return "completed"

    def exposed_finish(self):
        type(self).ready_for_finish.set()
        type(self).allow_finish.wait(5)
        return "completed"

    def exposed_late_reference(self):
        type(self).late_started.set()
        type(self).late_release.wait(5)
        return LateObject()

    def exposed_skip_marker(self):
        type(self).skip_started.set()
        return "ran"

    def exposed_other(self):
        return "other"


class LateObject(object):
    def __del__(self):
        CancelService.late_object_released.set()


class OldProtocolConnection(Connection):
    @classmethod
    def _request_handlers(cls):
        handlers = super()._request_handlers()
        handlers.pop(consts.HANDLE_PROTOCOL)
        return handlers


class OldProtocolService(rpyc.Service):
    _protocol = OldProtocolConnection

    def exposed_ping(self):
        return "old-peer"


def connect_pipes(service):
    stream1, stream2 = PipeStream.create_pair()
    client = connect_channel(Channel(stream1), rpyc.VoidService)
    server = service._connect(Channel(stream2), {})
    original_spawn = rpyc.core.protocol.spawn

    def controlled_spawn(func, *args):
        if service.delay_spawn:
            service.delayed_requests.append((func, args))
        else:
            return original_spawn(func, *args)

    server._spawn_request = controlled_spawn
    thread = threading.Thread(target=server.serve_all)
    thread.start()
    return client, server, thread


class TestCancellation(unittest.TestCase):
    def setUp(self):
        self.service = CancelService()
        self.conn, self.server, self.thread = connect_pipes(self.service)
        self.loop = self.conn.root.loop
        self.finish = self.conn.root.finish
        self.late_reference = self.conn.root.late_reference
        self.skip_marker = self.conn.root.skip_marker
        self.other = self.conn.root.other

    def tearDown(self):
        self.conn.close()
        self.server.close()
        self.thread.join(1)

    def test_running_request_is_cooperatively_cancelled(self):
        result = rpyc.async_(self.loop)()
        self.assertTrue(self.service.started.wait(1))
        self.assertTrue(self.conn.cancel_supported(1))
        self.assertEqual(self.other(), "other")

        status = result.cancel(1)

        self.assertEqual(status, rpyc.CancelResult.CANCELLED)
        self.assertTrue(result.cancelled)
        self.assertRaises(rpyc.AsyncResultCancelled, result.wait)
        self.assertRaises(rpyc.AsyncResultCancelled, lambda: result.value)
        self.assertEqual(result.cancel(1), rpyc.CancelResult.CANCELLED)

    def test_completion_wins_race(self):
        result = rpyc.async_(self.finish)()
        self.assertTrue(self.service.ready_for_finish.wait(1))

        self.service.allow_finish.set()
        self.assertEqual(result.cancel(1), rpyc.CancelResult.COMPLETED)
        self.assertFalse(result.cancelled)
        self.assertEqual(result.value, "completed")
        self.assertFalse(result.error)

    def test_unstarted_request_can_be_skipped(self):
        self.service.delay_spawn = True
        result = rpyc.async_(self.skip_marker)()
        while not self.service.delayed_requests:
            self.conn.poll(0.01)
        self.assertEqual(result.cancel(1), rpyc.CancelResult.CANCELLED)
        self.service.delay_spawn = False

        func, args = self.service.delayed_requests[0]
        func(*args)
        self.conn.poll_all(0.2)

        self.assertFalse(self.service.skip_started.wait(0.1))
        self.assertRaises(rpyc.AsyncResultCancelled, result.wait)
        self.service.delayed_requests.clear()

    def test_unsupported_peer_returns_unsupported(self):
        old_service = OldProtocolService()
        old_client, old_server, old_thread = connect_pipes(old_service)
        try:
            self.assertEqual(old_client.root.ping(), "old-peer")
            ping = old_client.root.ping
            result = rpyc.async_(ping)()
            self.assertFalse(old_client.cancel_supported(1))
            self.assertEqual(result.cancel(1), rpyc.CancelResult.UNSUPPORTED)
            self.assertEqual(result.value, "old-peer")
        finally:
            old_client.close()
            old_server.close()
            old_thread.join(1)

    def test_callback_runs_at_most_once_for_interleaved_replies(self):
        calls = []
        result = rpyc.async_(self.late_reference)()
        result.add_callback(calls.append)
        self.assertTrue(self.service.late_started.wait(1))

        self.assertEqual(result.cancel(1), rpyc.CancelResult.CANCELLED)
        self.service.late_release.set()
        self.conn.poll_all(1)
        gc.collect()
        self.conn.poll_all(0.2)

        self.assertEqual(calls, [result])
        self.assertTrue(self.service.late_object_released.wait(1))
        self.assertRaises(rpyc.AsyncResultCancelled, lambda: result.value)
        self.assertEqual(self.other(), "other")


if __name__ == "__main__":
    unittest.main()
