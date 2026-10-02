import contextvars
import threading

from rpyc.core.async_ import CancelledError


_current_token = contextvars.ContextVar("rpyc_cancellation_token", default=None)
_executing_server_request = contextvars.ContextVar("rpyc_server_request", default=False)


class CancellationToken:
    """A cooperative cancellation token for a remote request."""

    __slots__ = ["_seq", "_event", "_cancelled", "_lock"]

    def __init__(self, seq):
        self._seq = seq
        self._event = threading.Event()
        self._cancelled = False
        self._lock = threading.Lock()

    @property
    def seq(self):
        """The sequence number of the request associated with this token."""
        return self._seq

    @property
    def cancelled(self):
        """Whether cancellation has been requested."""
        return self._cancelled

    def wait(self, timeout=None):
        """Waits until cancellation is requested."""
        self._event.wait(timeout)

    def raise_if_cancelled(self):
        """Raises :class:`CancelledError` when cancellation was requested."""
        if self.cancelled:
            raise CancelledError("request cancelled")

    def _cancel(self):
        with self._lock:
            if self._cancelled:
                return False
            self._cancelled = True
            self._event.set()
            return True


def current_cancellation_token():
    """Returns the token for the request currently executing on this thread."""
    return _current_token.get()


def _set_current_cancellation_token(token):
    return _current_token.set(token)


def _reset_current_cancellation_token(token):
    _current_token.reset(token)
