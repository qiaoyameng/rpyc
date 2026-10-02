import os
import sys
import threading
import unittest
from unittest import mock

import rpyc
from rpyc.core import brine, vinegar

PY311 = sys.version_info >= (3, 11)


class CustomBoom(Exception):
    """Custom exception the client is not allowed to import/instantiate"""
    pass


class TreeService(rpyc.Service):
    def exposed_raise_nested_group(self):
        inner = ExceptionGroup("inner", [ValueError("leaf1"), KeyError("leaf2")])
        raise ExceptionGroup("outer", [inner, RuntimeError("leaf3")])

    def exposed_raise_with_notes(self):
        exc = ValueError("with notes")
        exc.add_note("note one")
        exc.add_note("note two")
        raise exc

    def exposed_raise_custom(self):
        raise CustomBoom("custom boom")

    def exposed_raise_cyclic_cause(self):
        first = ValueError("first")
        second = RuntimeError("second")
        first.__cause__ = second
        second.__cause__ = first
        raise first

    def exposed_raise_deep_cause(self, depth):
        exc = None
        for index in range(depth):
            nxt = ValueError(f"level{index}")
            if exc is not None:
                nxt.__cause__ = exc
            exc = nxt
        raise exc

    def exposed_raise_wide_group(self, width):
        raise ExceptionGroup("wide", [ValueError(f"leaf{i}") for i in range(width)])

    def exposed_raise_long_message(self, length):
        raise ValueError("x" * length)

    def exposed_div(self, a, b):
        return a // b


class BaseTreeTestCase(unittest.TestCase):
    server_config = None
    client_config = None

    def setUp(self):
        # full-duplex in-process pipe pair (the sandbox may forbid sockets)
        client_to_server = os.pipe()
        server_to_client = os.pipe()
        self._files = []
        self.server_conn = rpyc.connect_pipes(
            self._fdopen(client_to_server[0], "rb"),
            self._fdopen(server_to_client[1], "wb"),
            service=TreeService, config=self.server_config or {})
        self.server_thread = threading.Thread(target=self.server_conn.serve_all)
        self.server_thread.daemon = True
        self.server_thread.start()
        self.conn = rpyc.connect_pipes(
            self._fdopen(server_to_client[0], "rb"),
            self._fdopen(client_to_server[1], "wb"),
            config=self.client_config or {})

    def _fdopen(self, fd, mode):
        fileobj = os.fdopen(fd, mode)
        self._files.append(fileobj)
        return fileobj

    def tearDown(self):
        self.conn.close()
        self.server_conn.close()
        for fileobj in self._files:
            try:
                fileobj.close()
            except Exception:
                pass

    def assertRemoteRaises(self, func, *args):
        try:
            func(*args)
        except Exception as exc:
            return exc
        self.fail("expected a remote exception")


@unittest.skipUnless(PY311, "ExceptionGroup requires Python 3.11+")
class TestExceptionTree(BaseTreeTestCase):
    def test_capability_negotiated(self):
        self.assertRemoteRaises(self.conn.root.raise_with_notes)
        # the client learned the server's caps during negotiation
        self.assertIn(vinegar.EXC_TREE_CAPABILITY, self.conn._remote_caps)

    def test_nested_exception_group(self):
        exc = self.assertRemoteRaises(self.conn.root.raise_nested_group)
        self.assertIsInstance(exc, ExceptionGroup)
        self.assertEqual(exc.message, "outer")
        self.assertEqual(len(exc.exceptions), 2)
        inner, runtime = exc.exceptions
        self.assertIsInstance(inner, ExceptionGroup)
        self.assertEqual(inner.message, "inner")
        self.assertEqual(len(inner.exceptions), 2)
        self.assertIsInstance(inner.exceptions[0], ValueError)
        self.assertEqual(inner.exceptions[0].args, ("leaf1",))
        self.assertIsInstance(inner.exceptions[1], KeyError)
        self.assertIsInstance(runtime, RuntimeError)
        self.assertEqual(runtime.args, ("leaf3",))
        # every node carries its own remote traceback attribute
        self.assertTrue(hasattr(exc, "_remote_tb"))
        self.assertTrue(hasattr(inner, "_remote_tb"))
        self.assertTrue(hasattr(runtime, "_remote_tb"))

    def test_notes_preserved(self):
        exc = self.assertRemoteRaises(self.conn.root.raise_with_notes)
        self.assertIsInstance(exc, ValueError)
        self.assertEqual(exc.__notes__, ["note one", "note two"])

    def test_custom_exception_degrades_to_generic(self):
        exc = self.assertRemoteRaises(self.conn.root.raise_custom)
        self.assertIsInstance(exc, vinegar.GenericException)
        self.assertNotIsInstance(exc, CustomBoom)
        # the remote type name is carried by the generic class
        self.assertIn("CustomBoom", type(exc).__name__)
        self.assertEqual(exc.args, ("custom boom",))

    def test_cause_cycle_uses_references(self):
        exc = self.assertRemoteRaises(self.conn.root.raise_cyclic_cause)
        self.assertIsInstance(exc, ValueError)
        self.assertIsInstance(exc.__cause__, RuntimeError)
        # the cycle is decoded as object references, not by unbounded recursion
        self.assertIs(exc.__cause__.__cause__, exc)

    def test_rpc_still_works_after_bad_exception(self):
        self.assertRemoteRaises(self.conn.root.raise_cyclic_cause)
        self.assertRemoteRaises(self.conn.root.raise_nested_group)
        self.assertEqual(self.conn.root.div(6, 2), 3)

    def test_malformed_tree_payload_does_not_raise(self):
        for raw in [(vinegar.EXC_TREE_TAG, ("garbage",), 0),
                    (vinegar.EXC_TREE_TAG, (), 0),
                    (vinegar.EXC_TREE_TAG, (), 7),
                    (vinegar.EXC_TREE_TAG, ((1, 2, 3),), 0)]:
            exc = vinegar.load(raw, False, False, False)
            self.assertIsInstance(exc, vinegar.GenericException)
            self.assertIn("failed to decode", exc.args[0])


@unittest.skipUnless(PY311, "ExceptionGroup requires Python 3.11+")
class TestExceptionTreeTruncation(BaseTreeTestCase):
    server_config = {"exception_tree_max_depth": 4,
                     "exception_tree_max_nodes": 8,
                     "exception_tree_max_text": 100}

    def test_depth_limit_marks_truncation(self):
        exc = self.assertRemoteRaises(self.conn.root.raise_deep_cause, 20)
        chain = []
        while exc is not None:
            chain.append(exc)
            exc = exc.__cause__
        # depth limit + root => chain stops at a truncation marker
        self.assertLessEqual(len(chain), 5)
        marker = chain[-1]
        self.assertIsInstance(marker, vinegar.GenericException)
        self.assertIn("truncated", marker.args[0])
        self.assertIn("depth", marker.args[0])

    def test_node_limit_marks_truncation(self):
        exc = self.assertRemoteRaises(self.conn.root.raise_wide_group, 50)
        self.assertIsInstance(exc, ExceptionGroup)
        children = exc.exceptions
        # the group's arity is preserved, but excess children decode to a
        # single shared truncation marker object
        distinct = {id(child) for child in children}
        self.assertLessEqual(len(distinct), 9)
        markers = [c for c in children if isinstance(c, vinegar.GenericException)]
        self.assertTrue(markers)
        self.assertTrue(all(m is markers[0] for m in markers))
        self.assertIn("truncated", markers[0].args[0])

    def test_text_limit_marks_truncation(self):
        exc = self.assertRemoteRaises(self.conn.root.raise_long_message, 10000)
        self.assertIsInstance(exc, ValueError)
        self.assertLessEqual(len(exc.args[0]), 120)
        self.assertTrue(exc.args[0].endswith("... <truncated>"))


@unittest.skipUnless(PY311, "ExceptionGroup requires Python 3.11+")
class TestLegacyCompatibility(BaseTreeTestCase):
    """A peer that does not advertise the tree capability keeps receiving
    the legacy format"""
    client_config = {"enable_exception_trees": False}

    def test_legacy_format_used(self):
        with mock.patch.object(vinegar, "dump_tree",
                               side_effect=AssertionError("tree format used")):
            exc = self.assertRemoteRaises(self.conn.root.raise_with_notes)
        # legacy format: plain exception with a formatted remote traceback
        self.assertIsInstance(exc, ValueError)
        self.assertIn("ValueError", exc._remote_tb)
        self.assertFalse(hasattr(exc, "__notes__"))

    def test_rpc_still_works(self):
        self.assertRemoteRaises(self.conn.root.raise_with_notes)
        self.assertEqual(self.conn.root.div(6, 2), 3)


class TestVinegarTreeUnit(unittest.TestCase):
    def roundtrip(self, exc_instance, **dump_kwargs):
        try:
            raise exc_instance
        except BaseException:
            typ, val, tb = sys.exc_info()
        raw = vinegar.dump_tree(typ, val, tb, True, True, **dump_kwargs)
        # the wire format must only contain safe, brine-dumpable values
        self.assertTrue(brine.dumpable(raw))
        return vinegar.load(brine.load(brine.dump(raw)), False, False, False)

    @unittest.skipUnless(PY311, "ExceptionGroup requires Python 3.11+")
    def test_shared_reference_preserved(self):
        shared = ValueError("shared")
        group = ExceptionGroup("g", [shared, ExceptionGroup("h", [shared])])
        exc = self.roundtrip(group)
        self.assertIs(exc.exceptions[0], exc.exceptions[1].exceptions[0])

    def test_cause_context_and_suppress_flag(self):
        cause = KeyError("the cause")
        exc = ValueError("top")
        exc.__cause__ = cause
        loaded = self.roundtrip(exc)
        self.assertIsInstance(loaded.__cause__, KeyError)
        self.assertTrue(loaded.__suppress_context__)
        self.assertIsNone(loaded.__context__)

    def test_stop_iteration_optimization_kept(self):
        raw = vinegar.dump(StopIteration, StopIteration(), None, True, True)
        self.assertIs(vinegar.load(raw, False, False, False), StopIteration)


if __name__ == "__main__":
    unittest.main()
