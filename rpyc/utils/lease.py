"""
Explicit *lease* wrappers: a server-side mechanism that lets a service hand out
a group of remote references whose validity is bound to a revocable *generation*.

Some exposed objects should only be usable for the duration of a single session
operation, yet nothing stops a client from keeping its netrefs around forever.
Wrapping such objects in a :class:`Lease` gives the server an explicit,
server-enforced kill switch for the whole group of references derived from the
wrapped object:

* The lease identity propagates from the wrapped object to every remote
  reference *derived* from it (attributes that are read, results of method
  calls, iterators, items, etc.), because every derived by-reference object is
  itself wrapped in the same lease generation before it crosses the wire.
* Every operation dispatched by the protocol on a leased proxy (calls,
  attribute reads/writes/deletes, iteration, item access, context-manager
  exit, ...) re-validates the generation **on the server side**. Authorization
  never depends on any client-side cache.
* Calls that are already executing when :meth:`Lease.revoke` is invoked are
  allowed to run to completion; only operations that *begin* after revocation
  are rejected with :class:`LeaseRevoked`.
* Revocation is idempotent and immediately releases the lease's bookkeeping
  records; records of garbage-collected proxies are released automatically.
* Objects that were never wrapped are completely unaffected and keep the
  regular attribute-access rules.

Typical usage::

    class MyService(rpyc.Service):
        def exposed_begin_operation(self):
            self._lease = Lease()
            return self._lease.wrap(self._session)

        def exposed_end_operation(self):
            self._lease.revoke()

After ``revoke()``, the same :class:`Lease` may be reused: wrapping an object
again starts a *new generation*. Stale references from older generations never
regain access.
"""
import threading
import weakref

from rpyc.core import brine


class LeaseRevoked(Exception):
    """Raised (on the server, propagated to the client) when an operation is
    attempted through a reference whose lease generation has been revoked.

    The message is deliberately generic: it must not leak any details (such as
    the ``repr``) of the protected object.
    """

    def __init__(self, message="the lease governing this object has been revoked"):
        super().__init__(message)


#: names of attributes that are local to the leased proxy itself
_LEASED_ATTRS = frozenset(["_leased_lease", "_leased_generation", "_leased_target"])


class _LeasedProxy(object):
    """A server-side wrapper that governs access to a single leased object.

    Every operation re-validates the lease generation before touching the
    wrapped (protected) object, and every by-reference value derived from the
    wrapped object is itself wrapped in the same lease generation, so the
    lease propagates along the whole graph of remote references.
    """
    __slots__ = ("_leased_lease", "_leased_generation", "_leased_target", "__weakref__")

    def __init__(self, lease, generation, target):
        object.__setattr__(self, "_leased_lease", lease)
        object.__setattr__(self, "_leased_generation", generation)
        object.__setattr__(self, "_leased_target", target)

    def _leased_check(self):
        self._leased_lease._check(self._leased_generation)

    def __getattr__(self, name):
        # dunder lookups (including the protocol's bookkeeping probes such as
        # ____id_pack__) must never consult the protected object, so that
        # framework bookkeeping (e.g. HANDLE_DEL after revocation) keeps
        # working; protocol dunders are served by the methods defined above.
        if name in _LEASED_ATTRS or (name.startswith("__") and name.endswith("__")):
            raise AttributeError(name)
        self._leased_check()
        return self._leased_lease._derive(getattr(self._leased_target, name))

    def __setattr__(self, name, value):
        if name in _LEASED_ATTRS:
            object.__setattr__(self, name, value)
            return
        self._leased_check()
        setattr(self._leased_target, name, value)

    def __delattr__(self, name):
        if name in _LEASED_ATTRS:
            object.__delattr__(self, name)
            return
        self._leased_check()
        delattr(self._leased_target, name)

    # attribute-access overriders used by Connection._access_attr; the lease
    # wrapper is the access-control boundary for the wrapped object, so the
    # regular attribute rules (_check_attr) do not apply to it.
    def _rpyc_getattr(self, name):
        self._leased_check()
        return self._leased_lease._derive(getattr(self._leased_target, name))

    def _rpyc_setattr(self, name, value):
        self._leased_check()
        setattr(self._leased_target, name, value)

    def _rpyc_delattr(self, name):
        self._leased_check()
        delattr(self._leased_target, name)

    def __call__(self, *args, **kwargs):
        self._leased_check()
        return self._leased_lease._derive(self._leased_target(*args, **kwargs))

    def __iter__(self):
        self._leased_check()
        return self._leased_lease._derive(iter(self._leased_target))

    def __next__(self):
        self._leased_check()
        return self._leased_lease._derive(next(self._leased_target))

    def __getitem__(self, key):
        self._leased_check()
        return self._leased_lease._derive(self._leased_target[key])

    def __setitem__(self, key, value):
        self._leased_check()
        self._leased_target[key] = value

    def __delitem__(self, key):
        self._leased_check()
        del self._leased_target[key]

    def __len__(self):
        self._leased_check()
        return len(self._leased_target)

    def __contains__(self, item):
        self._leased_check()
        return item in self._leased_target

    def __enter__(self):
        self._leased_check()
        return self._leased_lease._derive(self._leased_target.__enter__())

    def __exit__(self, exc, typ, tb):
        self._leased_check()
        return self._leased_target.__exit__(exc, typ, tb)

    def __dir__(self):
        self._leased_check()
        return dir(self._leased_target)

    def __repr__(self):
        # never leak the protected object's repr once the lease is revoked
        lease = self._leased_lease
        if lease._is_current(self._leased_generation):
            try:
                return repr(self._leased_target)
            except Exception:
                return "<leased object (repr failed)>"
        return "<leased object (lease revoked)>"

    def __str__(self):
        lease = self._leased_lease
        if lease._is_current(self._leased_generation):
            try:
                return str(self._leased_target)
            except Exception:
                return "<leased object (str failed)>"
        return "<leased object (lease revoked)>"


class Lease(object):
    """A revocable, generation-scoped lease for groups of remote references.

    * :meth:`wrap` wraps an object so that every remote reference derived from
      it (attributes, call results, iterators, ...) belongs to the lease's
      current generation.
    * :meth:`revoke` invalidates the current generation: all references handed
      out under it stop working, while objects that were never wrapped are
      unaffected. Revoking more than once is harmless (idempotent).
    * After revocation the lease may be reused: the next :meth:`wrap` starts a
      new generation. References from older generations never regain access.
    * Operations that already started executing when :meth:`revoke` is called
      run to completion; only newly dispatched operations are rejected.
    * Bookkeeping records are released both on :meth:`revoke` and when leased
      proxies are garbage collected (e.g. when the client drops its netrefs).

    A lease may also be used as a context manager; leaving the ``with`` block
    revokes the current generation::

        with Lease() as lease:
            return lease.wrap(self._session)
    """
    __slots__ = ("_lock", "_generation", "_records", "__weakref__")

    def __init__(self):
        self._lock = threading.Lock()
        self._generation = 0
        # live proxies of the current generation; entries vanish automatically
        # when the proxies are garbage collected
        self._records = weakref.WeakSet()

    def __len__(self):
        """the number of live references governed by the current generation"""
        with self._lock:
            return len(self._records)

    @property
    def generation(self):
        """the current generation number (incremented by every revocation)"""
        return self._generation

    def wrap(self, obj):
        """Wraps *obj* in a leased proxy belonging to the current generation.

        By-value (brine-dumpable) objects are returned as-is, since they are
        copied to the client rather than referenced.

        :param obj: the object to protect
        :returns: a leased proxy for *obj* to hand out to the client
        """
        with self._lock:
            return self._derive(obj)

    def revoke(self):
        """Revokes the current generation and releases its lease records.

        Idempotent: revoking an already-revoked generation has no additional
        effect. References of the revoked generation are rejected lazily, the
        next time they are used; operations already in flight complete.
        """
        with self._lock:
            self._generation += 1
            self._records.clear()

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        self.revoke()
        return False

    def _is_current(self, generation):
        return generation == self._generation

    def _check(self, generation):
        if generation != self._generation:
            raise LeaseRevoked()

    def _derive(self, value):
        """propagates the lease to a value derived from a leased object"""
        if brine.dumpable(value):
            return value
        if type(value) is tuple:
            return tuple(self._derive(item) for item in value)
        if isinstance(value, _LeasedProxy) and value._leased_lease is self:
            return value
        proxy = _LeasedProxy(self, self._generation, value)
        self._records.add(proxy)
        return proxy
