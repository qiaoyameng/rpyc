import gc
import time
import unittest

import rpyc
from rpyc.core import Channel, Connection, PipeStream
from rpyc.lib import spawn
from rpyc.utils.lease import Lease, LeaseRevoked


SECRET_TOKEN = "s3cr3t-token"


class Child(object):
    def __init__(self):
        self.values = [10, 20, 30]

    def describe(self):
        return "child"


class Secret(object):
    def __init__(self):
        self.token = SECRET_TOKEN
        self.child = Child()

    def __repr__(self):
        return f"<Secret {self.token}>"

    def get_token(self):
        return self.token

    def get_child(self):
        return self.child

    def get_iterator(self):
        return iter(self.child.values)


class LeaseService(rpyc.Service):
    def __init__(self):
        self.secret = Secret()
        self.other = Secret()
        self.plain = Secret()
        self.lease = Lease()
        self.other_lease = Lease()

    def exposed_get_leased(self):
        return self.lease.wrap(self.secret)

    def exposed_get_other_leased(self):
        return self.other_lease.wrap(self.other)

    def exposed_get_plain(self):
        return self.plain


class TestLeaseLocal(unittest.TestCase):
    """server-side behavior of leases, without a connection"""

    def test_local_use_and_revoke(self):
        lease = Lease()
        proxy = lease.wrap(Secret())
        self.assertEqual(proxy.get_token(), SECRET_TOKEN)
        self.assertEqual(proxy.token, SECRET_TOKEN)
        lease.revoke()
        lease.revoke()  # repeated revocation is idempotent
        with self.assertRaises(LeaseRevoked):
            proxy.get_token()
        with self.assertRaises(LeaseRevoked):
            proxy.token

    def test_context_manager_revokes(self):
        with Lease() as lease:
            proxy = lease.wrap(Secret())
            self.assertEqual(proxy.get_token(), SECRET_TOKEN)
        with self.assertRaises(LeaseRevoked):
            proxy.get_token()

    def test_revoke_releases_records_and_gc_too(self):
        lease = Lease()
        proxy = lease.wrap(Secret())
        derived = proxy.get_child()
        self.assertEqual(len(lease), 2)
        del derived
        gc.collect()
        self.assertEqual(len(lease), 1)
        lease.revoke()
        self.assertEqual(len(lease), 0)
        del proxy
        gc.collect()
        self.assertEqual(len(lease), 0)


class TestLeaseOverConnection(unittest.TestCase):
    """leases enforced over a local (in-process) connection"""

    def setUp(self):
        self.service = LeaseService()
        client_stream, server_stream = PipeStream.create_pair()
        self.server_conn = Connection(self.service, Channel(server_stream),
                                      config={"allow_all_attrs": True})
        self.server_thread = spawn(self.server_conn.serve_all)
        self.conn = rpyc.connect_stream(client_stream,
                                        config={"instantiate_custom_exceptions": True})

    def tearDown(self):
        self.conn.close()
        self.server_conn.close()
        self.server_thread.join(5)

    def test_wrapped_object_and_derived_references_work(self):
        obj = self.conn.root.get_leased()
        self.assertEqual(obj.get_token(), SECRET_TOKEN)      # call
        self.assertEqual(obj.token, SECRET_TOKEN)            # attribute read
        obj.token = "updated"                                # attribute write
        self.assertEqual(self.service.secret.token, "updated")
        child = obj.get_child()                              # derived sub-object
        self.assertEqual(child.describe(), "child")
        it = obj.get_iterator()                              # derived iterator
        self.assertEqual(next(it), 10)
        self.assertEqual(next(it), 20)

    def test_revoke_rejects_object_method_and_subobject(self):
        obj = self.conn.root.get_leased()
        method = obj.get_token        # bound-method netref
        child = obj.get_child()       # sub-object netref
        it = obj.get_iterator()
        self.assertEqual(next(it), 10)
        self.assertGreaterEqual(len(self.service.lease), 3)

        self.service.lease.revoke()
        self.service.lease.revoke()   # repeated revocation is idempotent
        self.assertEqual(len(self.service.lease), 0)  # records released

        with self.assertRaises(LeaseRevoked):         # new call via the object
            obj.get_token()
        with self.assertRaises(LeaseRevoked):         # call via old bound method
            method()
        with self.assertRaises(LeaseRevoked):         # call via sub-object
            child.describe()
        with self.assertRaises(LeaseRevoked):         # attribute read
            obj.token
        with self.assertRaises(LeaseRevoked):         # attribute write
            obj.token = "nope"
        with self.assertRaises(LeaseRevoked):         # iteration continuation
            next(it)
        # the protected object was never touched after revocation
        self.assertEqual(self.service.secret.token, SECRET_TOKEN)

    def test_new_generation_does_not_rehabilitate_old_references(self):
        old_obj = self.conn.root.get_leased()
        old_method = old_obj.get_token
        old_child = old_obj.get_child()
        self.service.lease.revoke()

        # a new generation of the same lease works again
        new_obj = self.conn.root.get_leased()
        self.assertEqual(new_obj.get_token(), SECRET_TOKEN)
        self.assertEqual(new_obj.get_child().describe(), "child")

        # references from the revoked generation stay rejected
        with self.assertRaises(LeaseRevoked):
            old_obj.get_token()
        with self.assertRaises(LeaseRevoked):
            old_method()
        with self.assertRaises(LeaseRevoked):
            old_child.describe()
        with self.assertRaises(LeaseRevoked):
            old_obj.token

    def test_independent_lease_and_plain_object_unaffected(self):
        leased = self.conn.root.get_leased()
        other = self.conn.root.get_other_leased()
        plain = self.conn.root.get_plain()

        self.service.lease.revoke()

        with self.assertRaises(LeaseRevoked):
            leased.get_token()
        # an independent lease keeps working
        self.assertEqual(other.get_token(), SECRET_TOKEN)
        # an unwrapped object keeps the regular rules and keeps working
        self.assertEqual(plain.get_token(), SECRET_TOKEN)
        self.assertEqual(plain.token, SECRET_TOKEN)

    def test_error_does_not_leak_protected_repr(self):
        obj = self.conn.root.get_leased()
        self.service.lease.revoke()

        for attempt in (lambda: obj.get_token(), lambda: obj.token, lambda: repr(obj)):
            try:
                result = attempt()
            except LeaseRevoked as exc:
                text = str(exc)  # includes the remote traceback
            else:
                text = str(result)
            self.assertNotIn(SECRET_TOKEN, text)
            self.assertNotIn("<Secret", text)

    def test_records_released_on_garbage_collection(self):
        obj = self.conn.root.get_leased()
        method = obj.get_token
        child = obj.get_child()
        self.assertGreaterEqual(len(self.service.lease), 3)

        del obj
        del method
        del child
        gc.collect()
        deadline = time.time() + 10
        while len(self.service.lease) > 0 and time.time() < deadline:
            gc.collect()
            time.sleep(0.02)
        self.assertEqual(len(self.service.lease), 0)

    def test_garbage_collection_after_revoke(self):
        root = self.conn.root
        baseline = len(self.server_conn._local_objects._dict)
        obj = root.get_leased()
        self.assertGreater(len(self.server_conn._local_objects._dict), baseline)

        self.service.lease.revoke()
        del obj
        gc.collect()
        # HANDLE_DEL bookkeeping must keep working for revoked references,
        # so the server-side objects are still released
        deadline = time.time() + 10
        while len(self.server_conn._local_objects._dict) > baseline and time.time() < deadline:
            gc.collect()
            time.sleep(0.02)
        self.assertEqual(len(self.server_conn._local_objects._dict), baseline)


if __name__ == "__main__":
    unittest.main()
