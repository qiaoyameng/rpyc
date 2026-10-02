import threading
import time
import unittest

import rpyc
from rpyc.core import brine, consts
from rpyc.core.channel import Channel
from rpyc.core.cancellation import current_cancellation_token
from rpyc.core.protocol import Connection
from rpyc.core.service import Service, VoidService
from rpyc.core.stream import PipeStream
from rpyc.lib import get_id_pack


class FakeDelayedChannel:
    __slots__ = ["channel", "frame"]

    def __init__(self, channel, frame):
        self.channel = channel
        self.frame = frame

    def poll(self, timeout):
        return True

    def recv(self):
        frame, self.frame = self.frame, None
        if frame is None:
            return self.channel.recv()
        return frame

    def send(self, data):
        self.channel.send(data)

    def close(self):
        self.channel.close()

    @property
    def closed(self):
        return self.channel.closed

    def fileno(self):
        return self.channel.fileno()


class CancelService(Service):
    def __init__(self):
        self.cancel_seen = threading.Event()

    def exposed_block(self, started, finish):
        token = current_cancellation_token()
        started.set()
        try:
            while not finish.is_set():
                token.raise_if_cancelled()
                time.sleep(0.01)
        except rpyc.CancelledError:
            self.cancel_seen.set()
            raise
        return "finished"

    def exposed_identity(self, value):
        return value

    def exposed_instant(self):
        return "ok"


class LegacyCancelConnection(Connection):
    def _handle_handshake(self, capabilities):
        raise KeyError(consts.HANDLE_HANDSHAKE)


class LegacyCancelService(Service):
    _protocol = LegacyCancelConnection

    def exposed_block(self, started, finish):
        started.set()
        while not finish.is_set():
            time.sleep(0.01)
        return "finished"


class CancelTestMixin:
    def make_connections(self, service=CancelService):
        stream_a, stream_b = PipeStream.create_pair()
        client = VoidService._connect(Channel(stream_a), {"allow_all_attrs": True})
        server = service._connect(Channel(stream_b))
        threads = [
            threading.Thread(target=client.serve_all, daemon=True),
            threading.Thread(target=server.serve_all, daemon=True),
        ]
        for thread in threads:
            thread.start()
        self.addCleanup(client.close)
        self.addCleanup(server.close)
        return client, server, threads

    def wait_until(self, predicate, timeout=2):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return
            time.sleep(0.005)
        self.assertTrue(predicate())


class TestCancellation(CancelTestMixin, unittest.TestCase):
    def test_running_request_is_cancelled_cooperatively(self):
        client, server, _ = self.make_connections()
        started = threading.Event()
        finish = threading.Event()
        block = rpyc.async_(client.root.block)
        result = block(started, finish)

        started.wait(2)
        status = result.cancel()

        self.assertIs(status, rpyc.CancelStatus.CANCELLED)
        self.wait_until(lambda: result.ready)
        self.assertTrue(result.cancelled)
        server._local_root.cancel_seen.wait(2)
        with self.assertRaises(rpyc.CancelledError):
            result.value
        with self.assertRaises(rpyc.CancelledError):
            result.value
        self.assertEqual(client.root.instant(), "ok")

    def test_completion_wins_race(self):
        client, server, _ = self.make_connections()
        started = threading.Event()
        finish = threading.Event()
        completed = threading.Event()
        block = rpyc.async_(client.root.block)
        result = block(started, finish)

        started.wait(2)
        result.add_callback(lambda value: completed.set())
        finish.set()

        self.wait_until(lambda: result.ready)
        completed.wait(2)
        status = result.cancel()

        self.assertIs(status, rpyc.CancelStatus.COMPLETED)
        self.assertFalse(result.cancelled)
        self.assertEqual(result.value, "finished")
        self.assertTrue(completed.is_set())

    def test_queued_request_is_skipped_and_other_request_finishes(self):
        client, server, _ = self.make_connections()
        first_started = threading.Event()
        first_finish = threading.Event()
        skipped_event = threading.Event()
        callback_count = []

        block_method = client.root.block
        identity_method = client.root.identity
        block = rpyc.async_(block_method)
        identity = rpyc.async_(identity_method)
        first = block(first_started, first_finish)
        first_started.wait(2)
        second = identity(skipped_event)
        self.wait_until(
            lambda: server._server_requests.get(second._seq) is not None
            and server._server_requests[second._seq].state == "pending"
        )
        second.add_callback(lambda result: callback_count.append(result.cancelled))

        status = second.cancel()
        self.assertIs(status, rpyc.CancelStatus.CANCELLED)
        with self.assertRaises(rpyc.CancelledError):
            second.value
        self.assertEqual(callback_count, [True])

        first_finish.set()
        self.assertEqual(first.value, "finished")
        self.assertEqual(client.root.instant(), "ok")

        skipped_id = get_id_pack(skipped_event)
        self.wait_until(lambda: skipped_id not in client._local_objects._dict)

    def test_late_remote_reference_reply_is_released(self):
        client, server, _ = self.make_connections()
        started = threading.Event()
        finish = threading.Event()
        skipped_event = threading.Event()
        callback_count = []

        block_method = client.root.block
        identity_method = client.root.identity
        block = rpyc.async_(block_method)
        identity = rpyc.async_(identity_method)
        first = block(started, finish)
        started.wait(2)
        late_result = identity(skipped_event)
        self.wait_until(
            lambda: server._server_requests.get(late_result._seq) is not None
            and server._server_requests[late_result._seq].state == "pending"
        )
        late_result.add_callback(lambda result: callback_count.append(result.cancelled))
        self.assertIs(late_result.cancel(), rpyc.CancelStatus.CANCELLED)

        marker = object()
        marker_package = server._box(marker)
        marker_id = get_id_pack(marker)
        raw_reply = brine.I1.pack(consts.MSG_REPLY) + brine.dump(
            (late_result._seq, marker_package)
        )

        client._channel = FakeDelayedChannel(client._channel, raw_reply)
        self.wait_until(lambda: marker_id not in server._local_objects._dict)

        self.assertEqual(callback_count, [True])
        with self.assertRaises(rpyc.CancelledError):
            late_result.value

        finish.set()
        self.assertEqual(first.value, "finished")

    def test_unsupported_peer_returns_unsupported(self):
        client, server, _ = self.make_connections(LegacyCancelService)
        started = threading.Event()
        finish = threading.Event()
        block = rpyc.async_(client.root.block)
        result = block(started, finish)

        started.wait(2)
        status = result.cancel()

        self.assertIs(status, rpyc.CancelStatus.UNSUPPORTED)
        self.assertFalse(result.cancelled)
        self.assertFalse(result.ready)

        finish.set()
        self.assertEqual(result.value, "finished")


if __name__ == "__main__":
    unittest.main()
