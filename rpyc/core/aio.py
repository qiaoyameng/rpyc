"""
Server-side machinery for the ``async-iter/1`` protocol: driving remote
asynchronous iterators (e.g. async generators) on behalf of a client that
consumes them with ``async for``.

The client-side adapter lives in :mod:`rpyc.utils.asynciter`.

Protocol overview (all operations are regular RPyC requests, so every pull
carries its own sequence id and replies are matched by identity, never by
arrival order):

``HANDLE_AITER_NEW``
    Creates a :class:`RemoteAsyncIterator` for a remote async iterable and
    returns it by reference.
``HANDLE_AITER_ANEXT``
    Pulls the next item. Replies with the item, or raises
    ``StopAsyncIteration`` (normal end of iteration) or any exception the
    iterator itself raised. Concurrent pulls on the same iterator are
    rejected deterministically.
``HANDLE_AITER_ACLOSE``
    Closes the iterator, cooperatively running the remote ``finally``
    blocks (via ``aclose()``) before replying.
``HANDLE_AITER_ATHROW``
    Reserved. The first protocol version explicitly rejects it with
    ``NotImplementedError``.
"""
import asyncio
import collections
import concurrent.futures
import os
import threading


class _PipeBridge(object):
    """Thread-safe callback submission into an event loop.

    Uses an ``os.pipe()`` registered with ``loop.add_reader`` so that
    submissions reliably wake the loop even in environments where the
    asyncio self-pipe (``socketpair``) is restricted. Falls back to
    ``loop.call_soon_threadsafe`` when a pipe cannot be registered.
    """

    def __init__(self, loop):
        self._loop = loop
        self._queue = collections.deque()
        self._closed = False
        self._rfd = None
        self._wfd = None
        try:
            rfd, wfd = os.pipe()
            os.set_blocking(rfd, False)
            os.set_blocking(wfd, False)
            loop.add_reader(rfd, self._drain)
        except (AttributeError, OSError, NotImplementedError, RuntimeError):
            pass
        else:
            self._rfd = rfd
            self._wfd = wfd

    def submit(self, callback):
        """Schedules *callback* to run in the loop's thread. Thread-safe."""
        if self._closed:
            raise RuntimeError("bridge is closed")
        self._queue.append(callback)
        if self._wfd is not None:
            try:
                os.write(self._wfd, b"\0")
            except OSError:
                pass
        else:
            self._loop.call_soon_threadsafe(self._drain)

    def _drain(self):
        # runs in the loop's thread
        if self._rfd is not None:
            try:
                os.read(self._rfd, 4096)
            except OSError:
                pass
        while self._queue:
            callback = self._queue.popleft()
            callback()

    def close(self):
        """Deregisters the pipe. Must be called from the loop's thread."""
        if self._closed:
            return
        self._closed = True
        if self._rfd is not None:
            try:
                self._loop.remove_reader(self._rfd)
            except Exception:
                pass
            for fd in (self._rfd, self._wfd):
                try:
                    os.close(fd)
                except OSError:
                    pass
            self._rfd = self._wfd = None


class RemoteAsyncIterator(object):
    """Server-side endpoint of the ``async-iter/1`` protocol.

    Wraps a single asynchronous iterator and drives it on a dedicated event
    loop running in a dedicated thread, so the connection's serving threads
    never need to own an event loop. Instances are created by the
    ``HANDLE_AITER_NEW`` request handler and are normally referenced by
    exactly one client-side
    :class:`~rpyc.utils.asynciter.AsyncRemoteIterator`; when the last
    reference goes away (or the connection drops), the iterator is closed
    and the remote ``finally`` blocks run.
    """

    PUMP_INTERVAL = 0.05  # serve() slice used while waiting for a pull

    def __init__(self, async_iterable):
        aiter_method = getattr(async_iterable, "__aiter__", None)
        if aiter_method is None:
            raise TypeError(f"object of type {type(async_iterable).__name__!r} is not async-iterable")
        aiter = aiter_method()
        if not hasattr(aiter, "__anext__"):
            raise TypeError(f"object of type {type(async_iterable).__name__!r} is not an asynchronous iterator")
        self._aiter = aiter
        self._state_lock = threading.Lock()
        self._closed = False
        self._pending = None  # (concurrent.futures.Future, cancel) of the in-flight pull
        self._loop = asyncio.new_event_loop()
        self._bridge = _PipeBridge(self._loop)
        self._thread = threading.Thread(target=self._run_loop, name="rpyc-remote-aiter", daemon=True)
        self._thread.start()

    def __del__(self):
        try:
            self.close(timeout=5)
        except Exception:
            pass

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    # -- loop submission machinery -------------------------------------------

    def _submit(self, coro_factory):
        """Submits a coroutine to the loop thread.

        :returns: a ``(future, cancel)`` pair where *future* is a
                  :class:`concurrent.futures.Future` for the coroutine's
                  result and *cancel* asks the loop to cancel it.
        """
        cf = concurrent.futures.Future()
        box = {}

        def _start():
            if not cf.set_running_or_notify_cancel():
                return
            task = self._loop.create_task(coro_factory())
            box["task"] = task
            task.add_done_callback(lambda done: self._resolve(cf, done))

        self._bridge.submit(_start)

        def _cancel():
            cf.cancel()
            task = box.get("task")
            if task is not None:
                self._bridge.submit(task.cancel)

        return cf, _cancel

    @staticmethod
    def _resolve(cf, task):
        # runs in the loop thread; forwards the task outcome to cf
        try:
            result = task.result()
        except BaseException as exc:
            try:
                cf.set_exception(exc)
            except concurrent.futures.InvalidStateError:
                pass  # cancelled concurrently
        else:
            try:
                cf.set_result(result)
            except concurrent.futures.InvalidStateError:
                pass  # cancelled concurrently

    # -- protocol operations ---------------------------------------------------

    async def _drive_anext(self):
        return await self._aiter.__anext__()

    def submit_anext(self):
        """Starts one pull and returns its :class:`concurrent.futures.Future`.

        Pulls are serialized: a concurrent pull is rejected deterministically
        with :class:`RuntimeError` rather than being reordered by reply
        arrival. Pulling a closed iterator raises ``StopAsyncIteration``.
        """
        with self._state_lock:
            if self._closed:
                raise StopAsyncIteration()
            if self._pending is not None:
                raise RuntimeError(
                    "concurrent __anext__ on the same remote async iterator "
                    "is not supported by async-iter/1 (pulls are serialized)")
            cf, cancel = self._submit(self._drive_anext)
            self._pending = (cf, cancel)
        cf.add_done_callback(self._clear_pending)
        return cf

    def _clear_pending(self, fut):
        with self._state_lock:
            if self._pending is not None and self._pending[0] is fut:
                self._pending = None

    def wait_anext(self, pump):
        """Performs one pull, pumping ``pump(timeout)`` while waiting so the
        calling thread can keep serving the connection (this is what allows
        an interleaved ``HANDLE_AITER_ACLOSE`` to be dispatched reentrantly
        while a pull is in flight).
        """
        cf = self.submit_anext()
        while not cf.done():
            pump(self.PUMP_INTERVAL)
        try:
            return cf.result()
        except (asyncio.CancelledError, concurrent.futures.CancelledError):
            # the iterator was closed (aclose) while this pull was in flight
            raise StopAsyncIteration()

    async def _finalize(self):
        try:
            aclose = getattr(self._aiter, "aclose", None)
            if aclose is not None:
                await aclose()
        finally:
            self._loop.call_soon(self._shutdown_in_loop)

    def _shutdown_in_loop(self):
        self._bridge.close()
        self._loop.stop()

    def close(self, timeout=None):
        """Closes the remote iterator: cancels any in-flight pull, runs the
        iterator's ``aclose()`` (so remote ``finally`` blocks execute
        cooperatively) and shuts the dedicated loop down. Idempotent.
        """
        with self._state_lock:
            if self._closed:
                return
            self._closed = True
            pending = self._pending
        if pending is not None:
            cf, cancel = pending
            cancel()
            try:
                cf.result(timeout)  # wait for the generator to unwind
            except BaseException:
                pass
        try:
            fin, _ = self._submit(self._finalize)
            fin.result(timeout)
        finally:
            self._shutdown_loop()

    def _shutdown_loop(self):
        if threading.current_thread() is not self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        try:
            self._loop.close()
        except Exception:
            pass
