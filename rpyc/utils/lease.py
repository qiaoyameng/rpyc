"""
Explicit server-side *leases* for session-scoped remote objects.

Some exposed objects should only be usable for the duration of a single
session operation, yet clients may keep their netrefs around for much longer.
A :class:`LeaseManager` lets the server hand out *leased proxies* for such
objects and later revoke the whole group of remote references at once.

Usage::

    class MyService(rpyc.Service):
        def on_connect(self, conn):
            self.leases = LeaseManager()

        def exposed_begin_session(self):
            self.session_lease = self.leases.issue(SessionState())
            return self.session_lease.proxy

        def exposed_end_session(self):
            self.session_lease.revoke()

Semantics:

* The lease identity propagates with the leased proxy and every remote
  reference derived from it (attributes, method results, iterators, ...),
  because every derived object is itself leased under the same lease.
* The server re-validates the lease against the registry's *current
  generation* on every dispatched operation. Permission is never decided by
  client-side caches; revoking a lease and issuing a new one (a new
  generation) never resurrects references leased under the old generation.
* Calls already executing when :meth:`Lease.revoke` is invoked are allowed
  to run to completion (documented contract); any *new* call, attribute
  read/write, iteration step, or call through a previously obtained bound
  method is rejected with :class:`LeaseRevokedError`.
* Lease records are released both by revocation and by garbage collection
  (once the last leased proxy of the lease is gone). Revocation is
  idempotent.
* Objects that were never leased are unaffected and keep the normal RPyC
  attribute-access rules.
* Error messages and the proxy's ``repr`` never include the ``repr`` of the
  protected object.
"""
import itertools
import threading
import weakref

from rpyc.core import brine
from rpyc.core import DEFAULT_CONFIG


class LeaseRevokedError(Exception):
    """Raised on the server (and propagated to the client) when an operation
    is attempted through a lease that has been revoked. The message never
    contains the ``repr`` of the protected object."""
    pass


def _protect(lease, value):
    """wrap *value* so that remote references to it are governed by *lease*;
    by-value (brine-dumpable) objects are returned as-is"""
    if brine.dumpable(value):
        return value
    if type(value) is tuple:
        # tuples are boxed item-wise by the protocol, so protect each item
        return tuple(_protect(lease, item) for item in value)
    if isinstance(value, _LeasedProxy):
        return value
    return _LeasedProxy(lease, value)


class _LeasedProxy(object):
    """The server-side wrapper handed to the client (as a netref) in place of
    a leased object. Every operation dispatched by the protocol re-validates
    the lease before touching the wrapped object."""
    __slots__ = ("_lease", "_obj")

    def __init__(self, lease, obj):
        self._lease = lease
        self._obj = obj

    # -- internal helpers (not reachable through the protocol: names starting
    #    with an underscore are rejected by _check_name) --
    def _check(self):
        if not self._lease.valid:
            raise LeaseRevokedError(f"lease #{self._lease.lease_id} has been revoked")

    def _protect(self, value):
        return _protect(self._lease, value)

    def _check_name(self, name):
        if name.startswith("_") and name not in self._lease._manager._safe_attrs:
            raise AttributeError(f"cannot access {name!r}")

    # -- rpyc attribute-access hooks (see Connection._access_attr) --
    def _rpyc_getattr(self, name):
        self._check()
        self._check_name(name)
        return self._protect(getattr(self._obj, name))

    def _rpyc_setattr(self, name, value):
        self._check()
        self._check_name(name)
        setattr(self._obj, name, value)

    def _rpyc_delattr(self, name):
        self._check()
        self._check_name(name)
        delattr(self._obj, name)

    # -- dunders invoked directly by protocol request handlers or needed so
    #    that the client-side netref class exposes the operation at all --
    def __call__(self, *args, **kwargs):
        self._check()
        return self._protect(self._obj(*args, **kwargs))

    def __iter__(self):
        self._check()
        return self._protect(iter(self._obj))

    def __next__(self):
        self._check()
        return self._protect(next(self._obj))

    def __len__(self):
        self._check()
        return len(self._obj)

    def __getitem__(self, item):
        self._check()
        return self._protect(self._obj[item])

    def __setitem__(self, item, value):
        self._check()
        self._obj[item] = value

    def __delitem__(self, item):
        self._check()
        del self._obj[item]

    def __contains__(self, item):
        self._check()
        return item in self._obj

    def __enter__(self):
        self._check()
        return self._protect(self._obj.__enter__())

    def __exit__(self, exc, typ, tb):
        self._check()
        return self._obj.__exit__(exc, typ, tb)

    def __repr__(self):
        # deliberately does not include the repr of the protected object
        state = "valid" if self._lease.valid else "revoked"
        return f"<leased proxy #{self._lease.lease_id} ({state})>"


class Lease(object):
    """A handle for a single issued lease. The server keeps this object to
    revoke the lease later; the client only ever sees ``lease.proxy``.

    .. note::
       ``Lease`` objects are kept alive by their leased proxies. Once every
       leased proxy of the lease has been garbage collected (and the server
       dropped its own handle), the lease record is released automatically.
    """
    __slots__ = ("_manager", "_lease_id", "_generation", "_proxy", "__weakref__")

    def __init__(self, manager, lease_id, generation, obj):
        self._manager = manager
        self._lease_id = lease_id
        self._generation = generation
        self._proxy = _LeasedProxy(self, obj)

    @property
    def lease_id(self):
        """the id of this lease (unique within its manager)"""
        return self._lease_id

    @property
    def generation(self):
        """the generation this lease belongs to"""
        return self._generation

    @property
    def proxy(self):
        """the leased proxy to hand out to the client"""
        return self._proxy

    @property
    def valid(self):
        """whether the lease is still valid, i.e. the registry's current
        record for this lease id still matches this lease's generation"""
        return self._manager._is_current(self)

    def revoke(self):
        """revokes the lease, rejecting any further operations made through
        its leased proxies. Idempotent: revoking an already-revoked lease is
        a no-op. Calls already in flight run to completion."""
        self._manager._release(self)

    def __repr__(self):
        state = "valid" if self.valid else "revoked"
        return f"<Lease #{self._lease_id} generation={self._generation} ({state})>"


class LeaseManager(object):
    """Issues and tracks leases for session-scoped exposed objects.

    :param safe_attrs: the set of ``_``-prefixed (dunder) attribute names
                       that leased proxies are allowed to forward; defaults
                       to the protocol's ``safe_attrs`` configuration. Public
                       (non-``_``) attributes of the leased object are always
                       forwarded while the lease is valid.
    """

    def __init__(self, safe_attrs=None):
        if safe_attrs is None:
            safe_attrs = DEFAULT_CONFIG["safe_attrs"]
        self._safe_attrs = frozenset(safe_attrs)
        self._lock = threading.Lock()
        self._records = {}  # lease_id -> (generation, weakref(Lease))
        self._lease_ids = itertools.count(1)
        self._generations = itertools.count(1)

    def issue(self, obj):
        """issues a new lease (a new generation) governing *obj* and every
        remote reference derived from it

        :param obj: the local object to protect; must not be brine-dumpable
                    (by-value objects cannot be leased, since the client
                    receives a copy rather than a reference)
        :returns: the :class:`Lease` handle; hand ``lease.proxy`` to the client
        """
        if brine.dumpable(obj):
            raise TypeError(f"cannot lease a by-value object of type {type(obj).__name__!r}")
        with self._lock:
            lease_id = next(self._lease_ids)
            generation = next(self._generations)
            lease = Lease(self, lease_id, generation, obj)
            self._records[lease_id] = (generation, weakref.ref(lease, self._make_gc_callback(lease_id)))
        return lease

    def revoke(self, lease):
        """revokes *lease*; equivalent to ``lease.revoke()`` (idempotent)"""
        lease.revoke()

    def __len__(self):
        """the number of live lease records"""
        with self._lock:
            return len(self._records)

    def _make_gc_callback(self, lease_id):
        def _gc_callback(ref):
            with self._lock:
                entry = self._records.get(lease_id)
                if entry is not None and entry[1] is ref:
                    del self._records[lease_id]
        return _gc_callback

    def _is_current(self, lease):
        with self._lock:
            entry = self._records.get(lease._lease_id)
        return entry is not None and entry[0] == lease._generation and entry[1]() is lease

    def _release(self, lease):
        with self._lock:
            entry = self._records.get(lease._lease_id)
            if entry is not None and entry[1]() is lease:
                del self._records[lease._lease_id]
