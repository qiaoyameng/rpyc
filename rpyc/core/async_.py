import time  # noqa: F401
from enum import Enum
from threading import RLock
from rpyc.lib import Timeout
from rpyc.lib.compat import TimeoutError as AsyncResultTimeout


class CancelledError(Exception):
    """Raised when an asynchronous result was explicitly cancelled."""


class CancelStatus(str, Enum):
    """Result of requesting cancellation of an asynchronous operation."""

    CANCELLED = "cancelled"
    COMPLETED = "completed"
    UNSUPPORTED = "unsupported"


class AsyncResult(object):
    """*AsyncResult* represents a computation that occurs in the background and
    will eventually have a result. Use the :attr:`value` property to access the
    result (which will block if the result has not yet arrived).
    """
    __slots__ = ["_conn", "_seq", "_is_ready", "_is_exc", "_is_cancelled",
                 "_callbacks", "_obj", "_ttl", "_lock"]

    def __init__(self, conn, seq=None):
        self._conn = conn
        self._seq = seq
        self._is_ready = False
        self._is_exc = None
        self._is_cancelled = False
        self._obj = None
        self._callbacks = []
        self._ttl = Timeout(None)
        self._lock = RLock()

    def __repr__(self):
        if self._is_cancelled:
            state = "cancelled"
        elif self._is_ready:
            state = "ready"
        elif self._is_exc:
            state = "error"
        elif self.expired:
            state = "expired"
        else:
            state = "pending"
        return f"<AsyncResult object ({state}) at 0x{id(self):08x}>"

    def __call__(self, is_exc, obj):
        with self._lock:
            if self.expired:
                return
            if self._is_ready:
                return
            self._is_exc = is_exc
            self._obj = obj
            self._is_ready = True
            callbacks = self._callbacks[:]
            del self._callbacks[:]
        for cb in callbacks:
            cb(self)

    def cancel(self):
        """Requests cooperative cancellation of the remote operation.

        This does not interrupt the server thread. A cancellation message is
        acknowledged independently, and the running function can observe its
        :class:`~rpyc.core.cancellation.CancellationToken`.
        """
        return self._conn._cancel_async_result(self)

    def _claim_result(self):
        with self._lock:
            return not self._is_ready and not self.expired

    def _set_cancelled(self):
        with self._lock:
            if self._is_ready:
                return self._is_cancelled
            exc = CancelledError("request cancelled")
            self._is_cancelled = True
            self._is_exc = True
            self._obj = exc
            self._is_ready = True
            callbacks = self._callbacks[:]
            del self._callbacks[:]
        for cb in callbacks:
            cb(self)
        return True

    def wait(self):
        """Waits for the result to arrive. If the AsyncResult object has an
        expiry set, and the result did not arrive within that timeout,
        an :class:`AsyncResultTimeout` exception is raised"""
        while self._waiting():
            # Serve the connection since we are not ready. Suppose
            # the reply for our seq is served. The callback is this class
            # so __call__ sets our obj and _is_ready to true.
            self._conn.serve(self._ttl, waiting=self._waiting)

        # Check if we timed out before result was ready
        if not self._is_ready:
            raise AsyncResultTimeout("result expired")

    @property
    def cancelled(self):
        """Indicates whether the result was explicitly cancelled."""
        return self._is_cancelled

    def _waiting(self):
        return not (self._is_ready or self.expired)

    def add_callback(self, func):
        """Adds a callback to be invoked when the result arrives. The callback
        function takes a single argument, which is the current AsyncResult
        (``self``). If the result has already arrived, the function is invoked
        immediately.

        :param func: the callback function to add
        """
        with self._lock:
            ready = self._is_ready
            if not ready:
                self._callbacks.append(func)
        if ready:
            func(self)

    def set_expiry(self, timeout):
        """Sets the expiry time (in seconds, relative to now) or ``None`` for
        unlimited time

        :param timeout: the expiry time in seconds or ``None``
        """
        self._ttl = Timeout(timeout)

    @property
    def ready(self):
        """Indicates whether the result has arrived"""
        if self._is_ready:
            return True
        if self.expired:
            return False
        self._conn.poll_all()
        return self._is_ready

    @property
    def error(self):
        """Indicates whether the returned result is an exception"""
        return self.ready and self._is_exc

    @property
    def expired(self):
        """Indicates whether the AsyncResult has expired"""
        return not self._is_ready and self._ttl.expired()

    @property
    def value(self):
        """Returns the result of the operation. If the result has not yet
        arrived, accessing this property will wait for it. If the result does
        not arrive before the expiry time elapses, :class:`AsyncResultTimeout`
        is raised. If the returned result is an exception, it will be raised
        here. Otherwise, the result is returned directly.
        """
        self.wait()
        if self._is_exc:
            raise self._obj
        else:
            return self._obj
