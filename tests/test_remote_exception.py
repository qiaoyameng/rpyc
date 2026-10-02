from __future__ import with_statement
import builtins
import queue
import rpyc
import unittest

from rpyc.core import vinegar
from rpyc.core.channel import Channel
from rpyc.lib import spawn


class MyService(rpyc.Service):

    def exposed_set_version(self):
        rpyc.version.__version__ = '1.0.0'

    def exposed_remote_assert(self, val):
        assert val


class ForbiddenRemoteException(Exception):
    pass


class StructuredExceptionService(rpyc.Service):
    def exposed_exception_group(self):
        leaf = ValueError("leaf value error")
        leaf.add_note("leaf note")
        forbidden = ForbiddenRemoteException("custom type denied")
        forbidden.add_note("custom note")

        inner = ExceptionGroup("inner group", (forbidden,))
        group = ExceptionGroup("outer group", (leaf, inner))
        group.add_note("group note")

        try:
            try:
                raise RuntimeError("original failure")
            except RuntimeError as cause:
                raise group from cause
        except Exception as exc:
            exc.__context__ = leaf
            leaf.__cause__ = exc
            raise

    def exposed_second_rpc_after_bad_exception(self):
        return "ok"


class InMemoryStream(object):
    MAX_IO_CHUNK = 64000

    def __init__(self, incoming, outgoing):
        self.incoming = incoming
        self.outgoing = outgoing
        self.buffer = b""
        self.closed = False
        self.peer_closed = False

    def poll(self, timeout=0):
        if self.buffer:
            return True
        try:
            if hasattr(timeout, "timeleft"):
                timeout = timeout.timeleft()
            if timeout is None:
                item = self.incoming.get()
                self.incoming.queue.appendleft(item)
            else:
                item = self.incoming.get(timeout=timeout)
                self.incoming.queue.appendleft(item)
            return True
        except queue.Empty:
            return False

    def read(self, count):
        while len(self.buffer) < count:
            item = self.incoming.get()
            if item == b"":
                raise EOFError("closed by test peer")
            self.buffer += item
        data = self.buffer[:count]
        self.buffer = self.buffer[count:]
        return data

    def write(self, data):
        self.outgoing.put(bytes(data))

    def close(self):
        self.closed = True
        if not self.peer_closed:
            self.peer_closed = True
            self.outgoing.put(b"")

    def fileno(self):
        return -1


def make_connection_pair(service, negotiate_client=True, config=None):
    client_to_server = queue.Queue()
    server_to_client = queue.Queue()
    client_stream = InMemoryStream(server_to_client, client_to_server)
    server_stream = InMemoryStream(client_to_server, server_to_client)

    server_conn = service._connect(Channel(server_stream), config or {})

    def serve_forever():
        while not server_conn.closed:
            try:
                server_conn.serve(None)
            except (EOFError, OSError):
                break

    server_thread = spawn(serve_forever)

    client_conn = rpyc.connect_channel(
        Channel(client_stream),
        service=rpyc.VoidService,
        config={},
        negotiate_capabilities=negotiate_client,
    )
    return client_conn, server_conn, server_thread


class TestRemoteException(unittest.TestCase):
    def setUp(self):
        self.server = rpyc.utils.server.OneShotServer(MyService, port=0)
        self.server.logger.quiet = False
        self.server._start_in_thread()
        self.original_version_string = rpyc.version.__version__
        self.conn = rpyc.connect("localhost", port=self.server.port)

    def tearDown(self):
        rpyc.version.__version__ = self.original_version_string
        self.conn.close()

    def test_remote_exception(self):
        # Since the server/client share the same namespace, the version will change for both.
        # Even so, this should suffice for unit testing
        warn_msg = 'WARNING: Remote is on RPyC 1.0.0 and local is on RPyC 1.0.0.'
        try:
            self.conn.root.remote_assert(False)
        except Exception as exc:
            exc_rpyc_version = exc._remote_version
            exc_remote_tb = exc._remote_tb
        else:
            exc_rpyc_version = None
            exc_remote_tb = ''
        self.assertEqual(self.original_version_string, exc_rpyc_version)
        self.assertFalse(warn_msg in exc_remote_tb)
        try:
            self.conn.root.set_version()
            self.conn.root.remote_assert(False)
        except Exception as exc:
            exc_rpyc_version = exc._remote_version
            exc_remote_tb = exc._remote_tb
        else:
            exc_rpyc_version = None
            exc_remote_tb = ''
        self.assertEqual('1.0.0', exc_rpyc_version)
        self.assertTrue(warn_msg in exc_remote_tb)


class TestExclusionsRemoteException(unittest.TestCase):
    def setUp(self):
        config = {'include_local_traceback': False, 'include_local_version': False}
        self.server = rpyc.utils.server.OneShotServer(MyService, port=0, protocol_config=config)
        self.server.logger.quiet = False
        self.server._start_in_thread()
        self.conn = rpyc.connect("localhost", port=self.server.port)

    def tearDown(self):
        self.conn.close()

    def test_remote_exception(self):
        try:
            self.conn.root.remote_assert(False)
        except Exception as exc:
            exc_rpyc_version = exc._remote_version
            exc_remote_tb = exc._remote_tb
        else:
            exc_rpyc_version = None
            exc_remote_tb = ''
        self.assertEqual("<traceback denied>", exc_remote_tb)
        self.assertEqual("<version denied>", exc_rpyc_version)


@unittest.skipUnless(hasattr(builtins, "ExceptionGroup"), "requires ExceptionGroup")
class TestStructuredRemoteException(unittest.TestCase):
    def setUp(self):
        self.conn, self.server_conn, self.server_thread = make_connection_pair(
            StructuredExceptionService,
            config={
                "import_custom_exceptions": False,
                "instantiate_custom_exceptions": False,
            },
        )

    def tearDown(self):
        self.conn.close()
        self.server_thread.join(timeout=2)

    def test_exception_group_tree_notes_cause_context_and_unknown_type(self):
        with self.assertRaises(ExceptionGroup) as caught:
            self.conn.root.exception_group()

        group = caught.exception
        self.assertEqual("outer group", group._remote_message)
        self.assertEqual(["group note"], group.__notes__)
        self.assertIs(group._remote_exception_tree, group)

        leaf, inner = group.exceptions
        self.assertEqual("leaf value error", str(leaf).split("\n\n=========", 1)[0])
        self.assertEqual(["leaf note"], leaf.__notes__)

        self.assertIsInstance(inner, ExceptionGroup)
        self.assertEqual("inner group", inner._remote_message)
        custom, = inner.exceptions
        self.assertNotIsInstance(custom, ForbiddenRemoteException)
        self.assertIsInstance(custom, vinegar.GenericException)
        self.assertEqual(
            f"{__name__}.ForbiddenRemoteException",
            getattr(custom, "_remote_type_name", None),
        )
        self.assertEqual(["custom note"], custom.__notes__)

        self.assertIsInstance(group.__cause__, RuntimeError)
        self.assertIs(group.__context__, leaf)
        self.assertIs(leaf.__cause__, group)

    def test_bad_exception_does_not_break_next_rpc(self):
        with self.assertRaises(ExceptionGroup):
            self.conn.root.exception_group()

        self.assertEqual("ok", self.conn.root.second_rpc_after_bad_exception())


@unittest.skipUnless(hasattr(builtins, "ExceptionGroup"), "requires ExceptionGroup")
class TestStructuredExceptionSerialization(unittest.TestCase):
    def test_shared_reference_and_cycle(self):
        leaf = ValueError("shared leaf")
        group = ExceptionGroup("group", (leaf,))
        group.__cause__ = leaf
        leaf.__context__ = group

        raw = vinegar.dump_structured(type(group), group, None, False, True)
        loaded = vinegar.load_structured(raw, False, False, False)

        loaded_leaf, = loaded.exceptions
        self.assertIs(loaded.__cause__, loaded_leaf)
        self.assertIs(loaded_leaf.__context__, loaded)

    def test_text_length_is_truncated(self):
        exc = ValueError("x" * 100)
        raw = vinegar.dump_structured(type(exc), exc, None, False, True, total_text_budget=20)
        loaded = vinegar.load_structured(raw, False, False, False)

        self.assertEqual(20, len(loaded._remote_message))
        self.assertTrue(loaded._remote_message.endswith("... [truncated]"))

    def test_depth_and_node_limits_are_explicit(self):
        exc = ValueError("deep leaf")
        for _ in range(4):
            exc = ExceptionGroup("nested", (exc,))

        raw = vinegar.dump_structured(type(exc), exc, None, False, True, max_depth=3)
        loaded = vinegar.load_structured(raw, False, False, False)

        truncated = loaded.exceptions[0].exceptions[0].exceptions[0]
        self.assertIsInstance(truncated, vinegar.TruncatedRemoteException)
        self.assertIn("depth-limit", str(truncated))
        depth_parent = loaded.exceptions[0].exceptions[0]
        self.assertEqual(((0, "depth-limit"),), depth_parent._remote_truncated_children)

        children = tuple(ValueError(str(index)) for index in range(8))
        group = ExceptionGroup("wide", children)
        raw = vinegar.dump_structured(type(group), group, None, False, True, max_nodes=4)
        loaded = vinegar.load_structured(raw, False, False, False)

        self.assertEqual(3, sum(isinstance(child, ValueError)
                               and not isinstance(child, vinegar.TruncatedRemoteException)
                               for child in loaded.exceptions))
        self.assertEqual(5, sum(isinstance(child, vinegar.TruncatedRemoteException)
                                for child in loaded.exceptions))
        self.assertEqual(tuple((index, "node-limit") for index in range(3, 8)),
                         loaded._remote_truncated_children)

    def test_malformed_payload_is_loaded_not_raised(self):
        payload = (vinegar.STRUCTURED_EXCEPTION_MAGIC, 1, 0, (("invalid",),))
        loaded = vinegar.load_structured(payload, False, False, False)
        self.assertIsInstance(loaded, vinegar.GenericException)
        self.assertIn("malformed", str(loaded))

    def test_manual_hierarchy_cycle_becomes_reference(self):
        node = (
            0, ("builtins", "ExceptionGroup"), "self cycle",
            (), (), (), (("node", 0),), None, None, False, "",
        )
        payload = (vinegar.STRUCTURED_EXCEPTION_MAGIC, 1, 0, (node,))
        loaded = vinegar.load_structured(payload, False, False, False)
        marker, = loaded.exceptions
        self.assertIsInstance(marker, vinegar.RemoteExceptionReference)
        self.assertIn("node 0", str(marker))


@unittest.skipUnless(hasattr(builtins, "ExceptionGroup"), "requires ExceptionGroup")
class TestStructuredExceptionCapabilityNegotiation(unittest.TestCase):
    def test_legacy_peer_receives_legacy_exception(self):
        conn, server_conn, server_thread = make_connection_pair(
            StructuredExceptionService, negotiate_client=False,
        )
        try:
            with self.assertRaises(Exception) as caught:
                conn.root.exception_group()
            self.assertIn("outer group", str(caught.exception))
            self.assertFalse(hasattr(caught.exception, "_remote_exception_tree"))
            self.assertNotIsInstance(caught.exception, ExceptionGroup)
        finally:
            conn.close()
            server_thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
