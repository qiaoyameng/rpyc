import contextvars
from enum import IntEnum
from threading import Event, RLock

from rpyc.core import consts


class CancelResult(IntEnum):
    CANCELLED = consts.CANCEL_CANCELLED
    COMPLETED = consts.CANCEL_COMPLETED
    UNSUPPORTED = consts.CANCEL_UNSUPPORTED


class AsyncResultCancelled(Exception):
    """Raised when an AsyncResult was cancelled before completing."""


class CancellationToken(object):
    """Cooperative cancellation state associated with one remote request."""

    __slots__ = ("_event", "_cancelled", "_finished", "_lock")

    def __init__(self):
        self._event = Event()
        self._cancelled = False
        self._finished = False
        self._lock = RLock()

    @property
    def cancelled(self):
        return self._cancelled

    @property
    def is_cancelled(self):
        return self._cancelled

    @property
    def finished(self):
        return self._finished

    def cancel(self):
        with self._lock:
            if self._finished:
                return False
            if self._cancelled:
                return True
            self._cancelled = True
            self._event.set()
            return True

    def complete(self):
        with self._lock:
            if self._cancelled:
                return False
            if self._finished:
                return True
            self._finished = True
            return True

    def wait(self, timeout=None):
        return self._event.wait(timeout)

    def raise_if_cancelled(self):
        if self._cancelled:
            raise AsyncResultCancelled("request cancelled")


_current_token = contextvars.ContextVar("rpyc_cancellation_token", default=None)


def current_cancellation_token():
    """Return the token for the request handled by the current thread."""
    return _current_token.get()


get_cancellation_token = current_cancellation_token
