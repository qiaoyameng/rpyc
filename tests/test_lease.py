import gc
import threading
import time
import unittest

import rpyc
from rpyc.core.channel import Channel
from rpyc.core.stream import PipeStream
from rpyc.lib import spawn
from rpyc.utils.lease import LeaseManager


REPR_MARKER = "SESSION-REPR-MARKER-0307e62f"


class LeasedChild(object):
    def __init__(self):
        self.name = "child"

    def greet(self):
        return f"hello {self.name}"


class LeasedSession(object):
    def __init__(self, started_event=None):
        self.value = 41
        self.child = LeasedChild()
        self.items = [10, 20, 30]
        self.started_event = started_event

    def get_value(self):
        return self.value

    def get_child(self):
        return self.child

    def slow_value(self, delay):
        if self.started_event is not None:
            self.started_event.set()
        time.sleep(delay)
        return self.value

    def __repr__(self):
        return f"<{REPR_MARKER}>"


class LeaseService(rpyc.Service):
    def __init__(self):
        self.manager = LeaseManager()
        self.leases = []
        self.call_started = threading.Event()

    def exposed_lease_session(self):
        lease = self.manager.issue(LeasedSession(started_event=self.call_started))
        self.leases.append(lease)
        return lease.proxy

    def exposed_lease_count(self):
        return len(self.manager)

    def exposed_plain_object(self):
        return PlainObject()


class PlainObject(object):
    def exposed_greet(self):
        return "hello plain"


class TestLease(unittest.TestCase):
    def setUp(self):
        self.service = LeaseService()
        client_stream, server_stream = PipeStream.create_pair()
        self.server_conn = self.service._connect(Channel(server_stream), {})
        self.server_thread = spawn(self.server_conn.serve_all)
        self.conn = rpyc.connect_stream(client_stream)

    def tearDown(self):
        self.conn.close()
        self.server_thread.join(3)

    def assert_revoked(self, thunk):
        with self.assertRaises(Exception) as ctx:
            thunk()
        self.assertIn("revoked", str(ctx.exception))
        self.assertNotIn(REPR_MARKER, str(ctx.exception))

    def test_revoked_references_stay_dead(self):
        root = self.conn.root.lease_session()
        # keep three kinds of references: the object, a bound method, a sub-object
        obj_ref = root
        method_ref = root.get_value
        child_ref = root.get_child()
        iterator_ref = iter(root.items)

        # everything works while the lease is valid
        self.assertEqual(obj_ref.value, 41)
        obj_ref.value = 42
        self.assertEqual(obj_ref.value, 42)
        self.assertEqual(method_ref(), 42)
        self.assertEqual(child_ref.greet(), "hello child")
        self.assertEqual(next(iterator_ref), 10)

        # revocation is idempotent
        lease = self.service.leases[0]
        self.assertTrue(lease.valid)
        lease.revoke()
        lease.revoke()
        self.assertFalse(lease.valid)
        self.assertEqual(self.conn.root.lease_count(), 0)

        # every saved reference is now dead: attribute read/write, call,
        # call through an old bound method, sub-object access, iteration
        self.assert_revoked(lambda: obj_ref.value)
        self.assert_revoked(lambda: setattr(obj_ref, "value", 1))
        self.assert_revoked(lambda: method_ref())
        self.assert_revoked(lambda: child_ref.greet())
        self.assert_revoked(lambda: next(iterator_ref))

        # a new lease is a new generation: it works, and the old references
        # do not regain permission
        root2 = self.conn.root.lease_session()
        self.assertEqual(root2.get_value(), 41)
        self.assertNotEqual(self.service.leases[1].generation, lease.generation)
        self.assert_revoked(lambda: obj_ref.value)
        self.assert_revoked(lambda: method_ref())
        self.assert_revoked(lambda: child_ref.greet())
        self.assert_revoked(lambda: next(iterator_ref))

    def test_independent_leases_and_plain_objects(self):
        root_a = self.conn.root.lease_session()
        root_b = self.conn.root.lease_session()
        plain = self.conn.root.plain_object()

        self.service.leases[0].revoke()

        self.assert_revoked(lambda: root_a.get_value())
        # an independent lease is unaffected
        self.assertEqual(root_b.get_value(), 41)
        # plain (unwrapped) objects keep working with the normal rules
        self.assertEqual(plain.greet(), "hello plain")
        with self.assertRaises(AttributeError):
            plain.name = "nope"  # allow_setattr is False by default

    def test_error_and_repr_do_not_leak_object(self):
        root = self.conn.root.lease_session()
        self.assertNotIn(REPR_MARKER, repr(root))
        self.service.leases[0].revoke()
        self.assertNotIn(REPR_MARKER, repr(root))
        self.assert_revoked(lambda: root.get_value())

    def test_in_flight_call_completes_after_revoke(self):
        root = self.conn.root.lease_session()
        slow = rpyc.async_(root.slow_value)
        result = slow(0.5)
        # wait until the server is actually executing the call
        self.assertTrue(self.service.call_started.wait(3))
        self.service.leases[0].revoke()
        # the call was already executing when the lease was revoked
        self.assertEqual(result.value, 41)
        # but new calls are rejected
        self.assert_revoked(lambda: root.get_value())

    def test_lease_records_released(self):
        # revocation releases the lease record
        root = self.conn.root.lease_session()
        self.assertEqual(self.conn.root.lease_count(), 1)
        lease = self.service.leases.pop()
        lease.revoke()
        self.assertEqual(self.conn.root.lease_count(), 0)

        # garbage collection of every leased proxy releases the record, too
        root2 = self.conn.root.lease_session()
        self.assertEqual(self.conn.root.lease_count(), 1)
        self.service.leases.clear()
        del root2
        del root
        gc.collect()
        self.conn.ping()  # round-trip: lets the server process HANDLE_DEL
        gc.collect()      # collects the server-side lease <-> proxy cycle
        self.assertEqual(self.conn.root.lease_count(), 0)

    def test_cannot_lease_by_value_objects(self):
        with self.assertRaises(TypeError):
            self.service.manager.issue(42)


if __name__ == "__main__":
    unittest.main()
