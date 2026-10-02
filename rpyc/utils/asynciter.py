"""
Client-side adapter for iterating *remote* asynchronous iterators with
``async for`` (the ``async-iter/1`` protocol).

Given a netref to a remote async iterable (e.g. an async generator returned
by an exposed method), :func:`asynciter` builds an
:class:`AsyncRemoteIterator` that can be consumed with ``async for``::

    async with rpyc.asynciter(conn.root.event_stream()) as stream:
        async for item in stream:
            print(item)

The adapter never performs synchronous netref calls on the client's event
loop: requests are issued asynchronously and replies are awaited by a
background serving thread (see :class:`~rpyc.utils.helpers.BgServingThread`)
and delivered into the loop through a pipe bridge, so the loop stays
responsive (heartbeats, timers and other tasks keep running) while a pull
is in flight.

Protocol notes (see :mod:`rpyc.core.aio`):

* capability negotiation happens lazily on first use; endpoints that did
  not negotiate ``async-iter/1`` are rejected with ``NotImplementedError``
  instead of being mistaken for supporting it
* every pull is an independent request (matched by sequence id), and pulls
  on one iterator are serialized, so element order never depends on reply
  arrival order
* cancelling the local wait for a pull abandons that request; when the late
  reply arrives it is discarded (any netref it carries is released by the
  usual netref garbage collection)
* leaving the ``async with`` block (or calling :meth:`aclose`) closes the
  remote iterator, cooperatively running the remote ``finally`` blocks
* ``athrow()`` is explicitly rejected: the first protocol version only
  supports ``__anext__`` and ``aclose``
"""
import asyncio
import threading
import weakref

from rpyc.core import consts
from rpyc.core.aio import _PipeBridge
from rpyc.utils.helpers import BgServingThread

__all__ = ["asynciter", "AsyncRemoteIterator"]


class _ConnState(object):
    """Per-connection state shared by all async remote iterators."""

    def __init__(self, conn, bg_serving):
        self.conn = conn
        self.caps = None
        self.dead = None
        self.adapters = weakref.WeakSet()
        self.lock = threading.Lock()
        if bg_serving is None:
            bg_serving = BgServingThread(conn, callback=self._bg_died)
        self.bg = bg_serving
        self._install_close_hook()

    def _install_close_hook(self):
        previous = self.conn._config.get("before_closed")

        def _before_closed(root):
            try:
                self.notify_dead(EOFError("connection was closed"))
            finally:
                if previous is not None:
                    previous(root)

        self.conn._config["before_closed"] = _before_closed

    def _bg_died(self):
        self.notify_dead(EOFError("connection serving thread terminated"))

    def notify_dead(self, exc):
        with self.lock:
            if self.dead is not None:
                return
            self.dead = exc
            adapters = list(self.adapters)
        for adapter in adapters:
            adapter._notify_conn_dead(exc)


_conn_states = weakref.WeakKeyDictionary()
_conn_states_lock = threading.Lock()


def _get_conn_state(conn, bg_serving):
    with _conn_states_lock:
        state = _conn_states.get(conn)
        if state is None:
            state = _ConnState(conn, bg_serving)
            _conn_states[conn] = state
        return state


class AsyncRemoteIterator(object):
    """An asynchronous iterator over a remote asynchronous iterator.

    Do not instantiate directly; use :func:`asynciter`. The adapter is also
    an asynchronous context manager: leaving the ``async with`` block calls
    :meth:`aclose`, which cooperatively runs the remote ``finally`` blocks.
    """

    def __init__(self, proxy, bg_serving=None):
        if not hasattr(proxy, "____conn__") or not hasattr(proxy, "____id_pack__"):
            raise TypeError(f"'proxy' must be a netref of a remote async iterable, got {proxy!r}")
        self._proxy = proxy
        self._conn = proxy.____conn__
        self._bg_serving = bg_serving
        self._loop = None
        self._bridge = None
        self._state = None
        self._start_lock = None
        self._req_lock = None
        self._pending = set()
        self._remote_it = None
        self._closed = False

    def __aiter__(self):
        return self

    async def __aenter__(self):
        await self._ensure_started()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.aclose()
        return False

    # -- event loop / connection state ---------------------------------------

    def _ensure_loop(self):
        loop = asyncio.get_running_loop()
        if self._loop is loop:
            return
        if self._loop is not None:
            raise RuntimeError("an AsyncRemoteIterator is bound to the event loop it was first used on")
        self._loop = loop
        self._bridge = _PipeBridge(loop)
        self._start_lock = asyncio.Lock()
        self._req_lock = asyncio.Lock()
        self._state = _get_conn_state(self._conn, self._bg_serving)
        self._state.adapters.add(self)

    def _raise_if_dead(self):
        if self._state.dead is not None:
            raise self._state.dead
        if self._conn.closed:
            raise EOFError("connection is closed")

    async def _ensure_started(self):
        if self._closed:
            raise RuntimeError("the async remote iterator is closed")
        self._ensure_loop()
        if self._remote_it is not None:
            return
        async with self._start_lock:
            if self._remote_it is not None:
                return
            self._raise_if_dead()
            caps = await self._get_remote_caps()
            if consts.CAP_ASYNC_ITER_V1 not in caps:
                raise NotImplementedError(
                    f"the remote peer did not negotiate the {consts.CAP_ASYNC_ITER_V1!r} "
                    "capability; remote async iteration is not supported by this endpoint")
            self._remote_it = await self._call_remote(consts.HANDLE_AITER_NEW, self._proxy)

    async def _get_remote_caps(self):
        state = self._state
        if state.caps is None:
            if self._conn._remote_caps is not None:
                state.caps = self._conn._remote_caps
            else:
                try:
                    caps = await self._call_remote(consts.HANDLE_GETCAPS)
                except Exception:
                    # endpoints predating capability negotiation reject the
                    # unknown handler; they must not be mistaken as supporting
                    if state.dead is not None:
                        raise state.dead
                    state.caps = frozenset()
                else:
                    state.caps = frozenset(caps)
                self._conn._remote_caps = state.caps
        return state.caps

    # -- request plumbing ------------------------------------------------------

    def _call_remote(self, handler, *args):
        """Issues an asynchronous request and returns an asyncio.Future.

        Never blocks the event loop: the reply is awaited by the
        connection's background serving thread and delivered into the loop
        through the pipe bridge.
        """
        fut = self._loop.create_future()
        self._pending.add(fut)
        try:
            ares = self._conn.async_request(handler, *args)
        except Exception:
            self._pending.discard(fut)
            raise

        def _on_reply(_ares):
            try:
                value = _ares.value
            except BaseException as exc:
                self._deliver(fut, exc, True)
            else:
                self._deliver(fut, value, False)

        ares.add_callback(_on_reply)
        return fut

    def _deliver(self, fut, value, is_exc):
        # called from the serving thread; hop into the event loop
        try:
            self._bridge.submit(lambda: self._resolve(fut, value, is_exc))
        except RuntimeError:
            pass  # bridge already closed (adapter torn down)

    def _resolve(self, fut, value, is_exc):
        # runs in the event loop thread
        self._pending.discard(fut)
        try:
            self._deliver_resolved(fut, value, is_exc)
        finally:
            self._close_bridge_if_idle()

    def _deliver_resolved(self, fut, value, is_exc):
        if fut.done():
            # the waiter was cancelled: the late reply (and any netref it
            # carries) is dropped; netref garbage collection releases the
            # remote reference as usual
            return
        if is_exc:
            fut.set_exception(value)
        else:
            fut.set_result(value)

    def _close_bridge_if_idle(self):
        # the bridge must outlive every in-flight request: late replies of
        # abandoned pulls are still delivered (and discarded) through it
        if self._closed and not self._pending and self._bridge is not None:
            self._bridge.close()

    def _notify_conn_dead(self, exc):
        # called from any thread when the connection goes away
        if self._bridge is None:
            return
        try:
            self._bridge.submit(lambda: self._fail_pending(exc))
        except RuntimeError:
            pass  # bridge already closed

    def _fail_pending(self, exc):
        # runs in the event loop thread
        pending = list(self._pending)
        self._pending.clear()
        for fut in pending:
            if not fut.done():
                fut.set_exception(exc)
        if self._bridge is not None:
            self._bridge.close()

    # -- async iterator protocol ------------------------------------------------

    async def __anext__(self):
        if self._closed:
            raise StopAsyncIteration
        await self._ensure_started()
        async with self._req_lock:
            if self._closed:
                raise StopAsyncIteration
            return await self._call_remote(consts.HANDLE_AITER_ANEXT, self._remote_it)

    async def aclose(self):
        """Closes the remote iterator, cooperatively running the remote
        ``finally`` blocks. Idempotent; safe to call after the connection
        dropped (remote cleanup then happens on the server's side).
        """
        if self._closed:
            return
        self._closed = True
        if self._loop is None:
            return  # never started; nothing to do
        remote = self._remote_it
        self._remote_it = None
        if remote is not None and self._state.dead is None and not self._conn.closed:
            await self._call_remote(consts.HANDLE_AITER_ACLOSE, remote)
        self._close_bridge_if_idle()
        # dropping our reference lets netref gc release the remote object

    def athrow(self, *args):
        """Not supported: the ``async-iter/1`` protocol only defines
        ``__anext__`` and ``aclose``. Always raises ``NotImplementedError``.
        """
        raise NotImplementedError(
            "athrow() is not supported by the async-iter/1 protocol; "
            "only __anext__() and aclose() are available")


def asynciter(proxy, bg_serving=None):
    """Creates an asynchronous iterator over a remote asynchronous iterator.

    :param proxy: a netref of a remote async iterable (e.g. an async
                  generator returned by an exposed method)
    :param bg_serving: an optional :class:`~rpyc.utils.helpers.BgServingThread`
                       for the connection; one is created automatically if
                       not given
    :returns: an :class:`AsyncRemoteIterator` usable with ``async for``

    Example::

        async with rpyc.asynciter(conn.root.event_stream()) as stream:
            async for item in stream:
                print(item)
    """
    return AsyncRemoteIterator(proxy, bg_serving=bg_serving)
