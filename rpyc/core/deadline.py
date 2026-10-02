import time
import threading
from contextlib import contextmanager


class DeadlinePropagationNotSupportedError(Exception):
    """The peer does not support propagation of a call-chain deadline."""


_state = threading.local()


def _deadlines():
    stack = getattr(_state, "deadlines", None)
    if stack is None:
        stack = []
        _state.deadlines = stack
    return stack


class Deadline(object):
    """A monotonic deadline that can be reconstructed from a remaining budget."""

    __slots__ = ["finite", "deadline", "clock"]

    def __init__(self, budget=None, clock=time.monotonic, deadline=None):
        self.clock = clock
        if deadline is None:
            self.finite = budget is not None
            self.deadline = clock() + budget if self.finite else None
        else:
            self.finite = True
            self.deadline = deadline

    def expired(self):
        return self.finite and self.clock() >= self.deadline

    def timeleft(self):
        if not self.finite:
            return None
        return max(0.0, self.deadline - self.clock())


def current_deadline():
    stack = _deadlines()
    return stack[-1] if stack else None


def current_deadline_budget():
    deadline = current_deadline()
    return None if deadline is None else deadline.timeleft()


def current_clock():
    deadline = current_deadline()
    return deadline.clock if deadline is not None else time.monotonic


def push_deadline(deadline):
    _deadlines().append(deadline)
    return deadline


def push_none_deadline():
    _deadlines().append(None)
    return None


def pop_deadline(token=None):
    stack = _deadlines()
    if stack:
        stack.pop()
    return current_deadline()


@contextmanager
def deadline_budget(budget, clock=time.monotonic):
    deadline = Deadline(budget, clock)
    _deadlines().append(deadline)
    try:
        yield deadline
    finally:
        _deadlines().pop()
