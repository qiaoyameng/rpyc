"""Optional call-chain deadline budget propagation.

A *deadline budget* bounds the total time spent by an entire chain of
(possibly nested and reentrant) RPyC requests, as opposed to
``sync_request_timeout`` which bounds each individual request/response
exchange. The entry point of a chain establishes a budget; every outgoing
request made while the budget is active carries the *remaining* budget to the
peer, which adopts it as a local deadline against its own monotonic clock.
Nested requests and reverse callbacks made while serving such a request
inherit the budget the same way.

Budgets travel on the wire as relative seconds, never as absolute timestamps,
so peers whose monotonic clocks have different origins interoperate correctly.
"""
import threading
import time
from contextlib import contextmanager

# The clock used to establish local deadlines. It is referenced through this
# module at call time so tests may substitute a controlled clock.
monotonic = time.monotonic

_state = threading.local()


def get_deadline():
    """Returns the absolute deadline (in :data:`monotonic` terms) of the call
    chain the current thread is part of, or ``None`` if no budget is active."""
    return getattr(_state, "deadline", None)


def remaining():
    """Returns the seconds left in the current thread's call-chain budget, or
    ``None`` when no budget is active. The result may be negative when the
    budget has just been exceeded."""
    dl = get_deadline()
    if dl is None:
        return None
    return dl - monotonic()


@contextmanager
def scope(seconds):
    """Establishes a call-chain deadline ``seconds`` from now for the current
    thread. If a budget is already active, the new deadline may only tighten
    it (the earlier deadline wins). The previous deadline is restored on exit.
    """
    with scope_at(monotonic() + seconds) as ts:
        yield ts


@contextmanager
def scope_at(ts):
    """Adopts an absolute local deadline for the current thread. Used by the
    serving side to adopt a budget received over the wire. Like :func:`scope`,
    an already active budget can only be tightened."""
    prev = get_deadline()
    if prev is not None and prev <= ts:
        yield prev
        return
    _state.deadline = ts
    try:
        yield ts
    finally:
        _state.deadline = prev
