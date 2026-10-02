import asyncio
import inspect
import os
import threading

from rpyc.core import consts
from rpyc.core.netref import BaseNetref

class RemoteAsyncIterationNotSupportedError(Exception):
    """Raised when a peer did not negotiate remote asynchronous iteration."""


class RemoteAsyncIterationBusyError(Exception):
    """Raised when the same remote iterator already has an in-flight pull."""


_DEFERRED_REPLY = object()
class _Stop:
    __slots__ = ()



def _is_remote_reference(obj):
    return isinstance(obj, BaseNetref)


def _release_netrefs(conn, obj):
    if isinstance(obj, BaseNetref) and obj.____conn__ is conn:
        count = obj.____refcount__
        obj.____refcount__ = 0
        if count:
            try:
                conn.async_request(consts.HANDLE_DEL, obj, count)
            except Exception:
                pass
    elif isinstance(obj, tuple):
        for item in obj:
            _release_netrefs(conn, item)


def async_iter(obj, *args, **kwargs):
    """Create an ``async for`` adapter for a remote async iterator or factory."""
    if not isinstance(obj, BaseNetref):
        raise TypeError(f"expected a remote object, got {type(obj).__name__!r}")
    return _RemoteAsyncGenerator(_RemoteAsyncIterator(obj, args, kwargs))


class _RemoteAsyncGenerator(object):
    __slots__ = ("_iterator", "_generator", "_closed", "_stopped")

    def __init__(self, iterator):
        self._iterator = iterator
        self._generator = None
        self._closed = False
        self._stopped = False

    def __aiter__(self):
        self._generator = self._iterate()
        return self._generator

    def __anext__(self):
        if self._generator is None:
            self.__aiter__()
        return self._generator.__anext__()

    async def _iterate(self):
        try:
            while True:
                value = await self._iterator._pull_one()
                if isinstance(value, _Stop):
                    self._stopped = True
                    return
                yield value
        finally:
            if not self._stopped:
                await self._iterator.aclose()

    async def aclose(self):
        if self._generator is None:
            try:
                return await self._iterator.aclose()
            finally:
                await self._iterator.close_transport()
        result = await self._generator.aclose()
        self._closed = True
        await self._iterator.close_transport()
        return result

    async def athrow(self, *args, **kwargs):
        return await self._iterator.athrow(*args, **kwargs)

    async def asend(self, *args, **kwargs):
        return await self._iterator.asend(*args, **kwargs)

    def fail_all(self):
        self._iterator.fail_all()


class _AsyncIterationManager(object):
    def __init__(self, conn):
        self.conn = conn
        self.loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, name="RPyCAsyncIteration")
        self._thread.daemon = True
        self._lock = threading.Lock()
        self._entries = {}
        self._next_id = 1
        self._queued_wakeups = 0
        self._read_fd, self._write_fd = os.pipe()
        os.set_blocking(self._read_fd, False)
        os.set_blocking(self._write_fd, False)
        self._thread.start()

    def _run(self):
        asyncio.set_event_loop(self.loop)
        self.loop.add_reader(self._read_fd, self._drain_wakeups)
        try:
            self.loop.run_forever()
            pending = asyncio.all_tasks(self.loop)
            if pending:
                self.loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())
        finally:
            self.loop.close()
            asyncio.set_event_loop(None)
            try:
                os.close(self._read_fd)
                os.close(self._write_fd)
            except OSError:
                pass

    def _submit(self, coro):
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        self._wake()
        return future

    def _call_soon(self, callback, *args):
        self.loop.call_soon_threadsafe(callback, *args)
        self._wake()

    def _wake(self):
        self._queued_wakeups += 1
        try:
            os.write(self._write_fd, b"\0")
        except BlockingIOError:
            pass
        except OSError:
            pass

    def _drain_wakeups(self):
        consumed = 0
        try:
            while True:
                os.read(self._read_fd, 1)
                consumed += 1
        except BlockingIOError:
            pass
        except OSError:
            pass
        self._queued_wakeups -= consumed
        if self._queued_wakeups > 0:
            try:
                os.write(self._write_fd, b"\0")
            except BlockingIOError:
                pass
            except OSError:
                pass

    def _send_reply(self, seq, value):
        self.conn._send(consts.MSG_REPLY, seq, self.conn._box(value))

    def _send_exception(self, seq, exc):
        self.conn._send(
            consts.MSG_EXCEPTION,
            seq,
            self.conn._box_exc(type(exc), exc, exc.__traceback__),
        )

    def hello(self):
        return consts.AITER_PROTOCOL, (
            consts.AITER_OP_ANEXT,
            consts.AITER_OP_ACLOSE,
        )

    def create(self, seq, source, args, kwargs):
        with self._lock:
            iterator_id = self._next_id
            self._next_id += 1
            entry = _AsyncIteratorEntry(iterator_id)
            entry.starting = True
            self._entries[iterator_id] = entry
        future = self._submit(self._create(entry, source, args, dict(kwargs)))
        entry.create_future = future
        future.add_done_callback(lambda future: self._create_done(future, entry, seq))
        return _DEFERRED_REPLY

    def _create_done(self, future, entry, seq):
        close_seq = None
        try:
            future.result()
        except Exception as exc:
            with self._lock:
                self._entries.pop(entry.iterator_id, None)
                close_seq = entry.close_seq if entry.close_requested else None
            self._send_exception(seq, exc)
            if close_seq is not None:
                self._send_reply(close_seq, None)
            return
        entry.starting = False
        if entry.close_requested:
            self._call_soon(self._close_entry, entry)
        self._send_reply(seq, entry.iterator_id)

    async def _create(self, entry, source, args, kwargs):
        obj = source
        if callable(source):
            def call_source():
                return source(*args, **kwargs)

            obj = await self.loop.run_in_executor(None, call_source)
        obj = await self._resolve(obj)

        if _is_remote_reference(obj):
            def get_aiter():
                return obj.__aiter__()

            ag = await self.loop.run_in_executor(None, get_aiter)
        else:
            ag = obj.__aiter__()
        ag = await self._resolve(ag)
        is_async_iterable = hasattr(ag, "__anext__") and hasattr(ag, "__aclose__")
        if not inspect.isasyncgen(ag) and not is_async_iterable:
            raise TypeError("remote object is not an asynchronous iterator")
        entry.ag = ag

    def _anext(self, ag):
        return ag.__anext__()

    def _aclose(self, ag):
        return ag.aclose()

    async def _resolve(self, obj):
        while inspect.isawaitable(obj):
            obj = await obj
        return obj

    def anext(self, seq, iterator_id, pull_id):
        with self._lock:
            entry = self._entries.get(iterator_id)
            if entry is None or entry.closed:
                raise StopAsyncIteration
            if entry.task is not None or entry.starting:
                raise RemoteAsyncIterationBusyError("remote async iterator is already pulling")
            entry.task = True
        self._call_soon(self._start_pull, entry, seq, pull_id)
        return _DEFERRED_REPLY

    def _start_pull(self, entry, seq, pull_id):
        entry.pull_id = pull_id
        entry.task = asyncio.ensure_future(self._pull(entry, seq, pull_id))

    async def _pull(self, entry, seq, pull_id):
        try:
            value = await self._anext(entry.ag)
            self._send_reply(seq, (consts.AITER_VALUE, pull_id, value))
        except StopAsyncIteration:
            with self._lock:
                entry.closed = True
                self._entries.pop(entry.iterator_id, None)
            self._send_reply(seq, (consts.AITER_STOPPED, pull_id, None))
        except asyncio.CancelledError:
            self._send_reply(seq, (consts.AITER_CANCELED, pull_id, None))
        except BaseException as exc:
            self._send_exception(seq, exc)
        finally:
            entry.task = None
            entry.pull_id = None

    def athrow(self, iterator_id, args, kwargs):
        raise TypeError("remote athrow is not supported")

    def cancel(self, iterator_id, pull_id):
        with self._lock:
            entry = self._entries.get(iterator_id)
            if entry is None or entry.closed or entry.pull_id != pull_id:
                return None
        self._call_soon(self._cancel_pull, entry)
        return _DEFERRED_REPLY

    def _cancel_pull(self, entry):
        task = entry.task
        if task is not True and task is not None and not task.done():
            task.cancel()

    def aclose(self, seq, iterator_id):
        with self._lock:
            entry = self._entries.get(iterator_id)
            if entry is None or entry.closed:
                return None
            entry.close_requested = True
            entry.close_seq = seq
            if entry.starting:
                callback = lambda future: self._create_close_ready(future, entry)
                entry.create_future.add_done_callback(callback)
            elif entry.task is True:
                self._submit(self._wait_then_close(entry))
            else:
                self._submit(self._close_entry(entry))
        return _DEFERRED_REPLY


    async def _wait_then_close(self, entry):
        while entry.task is True:
            await asyncio.sleep(0)
        await self._close_entry(entry)

    def _create_close_ready(self, future, entry):
        if future.exception() is None:
            self._submit(self._close_entry(entry))

    async def _close_entry(self, entry):
        task = entry.task
        if task is True:
            await asyncio.sleep(0)
            task = entry.task
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        try:
            await self._aclose(entry.ag)
        finally:
            entry.closed = True
            with self._lock:
                self._entries.pop(entry.iterator_id, None)
            self._send_reply(entry.close_seq, None)

    def shutdown(self, on_closed=None):
        if not self.loop.is_running():
            if on_closed is not None:
                on_closed()
            return
        self._submit(self._shutdown(on_closed))

    async def _shutdown(self, on_closed=None):
        try:
            await self._close_all()
        finally:
            self.loop.stop()
            if on_closed is not None:
                on_closed()

    async def _close_all(self):
        with self._lock:
            entries = list(self._entries.values())
            self._entries.clear()
        for entry in entries:
            entry.close_requested = True
            try:
                await self._close_entry_without_reply(entry)
            except Exception:
                pass

    async def _close_entry_without_reply(self, entry):
        if entry.starting:
            await asyncio.wrap_future(entry.create_future)
        task = entry.task
        if task is True:
            await asyncio.sleep(0)
            task = entry.task
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if entry.ag is not None:
            await self._aclose(entry.ag)
        entry.closed = True


class _AsyncIteratorEntry(object):
    __slots__ = ("iterator_id", "ag", "task", "pull_id", "closed", "starting",
                 "close_requested", "close_seq", "create_future")

    def __init__(self, iterator_id):
        self.iterator_id = iterator_id
        self.ag = None
        self.task = None
        self.pull_id = None
        self.closed = False
        self.starting = False
        self.close_requested = False
        self.close_seq = None
        self.create_future = None


class _RemoteAsyncIterator(object):
    __slots__ = (
        "_conn", "_source", "_args", "_kwargs", "_iterator_id", "_loop",
        "_pull_seq", "_busy", "_continue_read_fd", "_continue_write_fd",
        "_closed", "_capabilities", "_waiting", "_started",
        "_start_failed",
    )

    def __init__(self, source, args, kwargs):
        self._conn = source.____conn__
        self._source = source
        self._args = args
        self._kwargs = kwargs
        self._iterator_id = None
        self._loop = None
        self._pull_seq = itertools_count()
        self._busy = False
        self._closed = False
        self._capabilities = ()
        self._waiting = {}
        self._started = False
        self._start_failed = False
        self._continue_read_fd, self._continue_write_fd = os.pipe()
        os.set_blocking(self._continue_read_fd, False)
        os.set_blocking(self._continue_write_fd, False)
        self._conn._async_iterators.add(self)

    def __aiter__(self):
        return self

    async def _start(self):
        if self._started:
            return
        loop = asyncio.get_running_loop()
        self._loop = loop
        if not getattr(self._conn, "_async_event_loop", False):
            from rpyc.utils.helpers import BgServingThread
            self._conn._async_event_loop = loop
            self._conn._async_serving = BgServingThread(self._conn, sleep_interval=0)
        loop.add_reader(self._continue_read_fd, self._continue_ready)
        try:
            try:
                protocol, operations = await self._await_request(
                    *self._request(consts.HANDLE_AITER_HELLO)
                )
            except KeyError as exc:
                raise RemoteAsyncIterationNotSupportedError(
                    "remote endpoint does not support async iteration"
                ) from exc
            except (TypeError, ValueError) as exc:
                raise RemoteAsyncIterationNotSupportedError(
                    "invalid async iteration handshake"
                ) from exc
            if protocol != consts.AITER_PROTOCOL or not isinstance(operations, tuple):
                raise RemoteAsyncIterationNotSupportedError("unsupported async iteration protocol")
            operations = set(operations)
            required = (consts.AITER_OP_ANEXT, consts.AITER_OP_ACLOSE)
            if any(operation not in operations for operation in required):
                raise RemoteAsyncIterationNotSupportedError(
                    "remote endpoint lacks required async iteration operations"
                )
            self._capabilities = operations
            args = (self._source, self._args, tuple(self._kwargs.items()))
            create_request = self._request(consts.HANDLE_AITER_CREATE, *args)
            self._iterator_id = await self._await_request(*create_request)
            self._started = True
        except BaseException:
            self._started = False
            self._start_failed = True
            if self._conn._async_serving is not None:
                self._conn._async_serving._active = False
            raise

    async def _pull_one(self):
        await self._start()
        if self._closed or self._start_failed:
            return _Stop()
        if self._busy:
            raise RemoteAsyncIterationBusyError("async iterator is already pulling")
        self._busy = True
        try:
            pull_id = next(self._pull_seq)
            status, reply_id, value = await self._await_request(
                *self._request(consts.HANDLE_AITER_ANEXT, self._iterator_id, pull_id),
                (self._iterator_id, pull_id)
            )
            if reply_id != pull_id:
                raise RuntimeError("async iteration pull identity mismatch")
            if status == consts.AITER_VALUE:
                return value
            if status == consts.AITER_STOPPED:
                self._closed = True
                return _Stop()
            if status == consts.AITER_CANCELED:
                raise EOFError("remote async iteration was canceled")
            raise RuntimeError(f"unknown async iteration status {status!r}")
        finally:
            self._busy = False

    async def athrow(self, *args, **kwargs):
        await self._start()
        if consts.AITER_OP_ATHROW not in self._capabilities:
            raise TypeError("remote athrow is not supported")
        request = self._request(
            consts.HANDLE_AITER_ATHROW, self._iterator_id, args, tuple(kwargs.items())
        )
        return await self._await_request(*request)

    async def asend(self, *args, **kwargs):
        raise TypeError("remote asend is not supported")

    async def aclose(self):
        if self._closed or self._start_failed:
            return
        await self._start()
        if self._closed:
            return
        self._busy = True
        try:
            try:
                request = self._request(
                    consts.HANDLE_AITER_ACLOSE, self._iterator_id
                )
                await self._await_request(*request)
            except EOFError:
                pass
            finally:
                self._closed = True
        finally:
            self._busy = False

    async def close_transport(self):
        loop = self._loop
        if loop is not None:
            loop.remove_reader(self._continue_read_fd)
        for fd in (self._continue_read_fd, self._continue_write_fd):
            try:
                os.close(fd)
            except OSError:
                pass

    def _request(self, handler, *args):
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        completed = loop.create_future()
        read_fd, write_fd = os.pipe()
        os.set_blocking(read_fd, False)
        os.set_blocking(write_fd, False)
        seq = self._conn._get_seq_id()
        self._waiting[seq] = (future, completed)

        def reader():
            loop.remove_reader(read_fd)
            try:
                os.read(read_fd, 1)
            except OSError:
                pass
            self._complete_request(seq, None, None, completed)

        def callback(is_exc, obj):
            if self._loop.is_closed():
                if not is_exc:
                    _release_netrefs(self._conn, obj)
                self._close_request_fds((read_fd, write_fd))
                if not completed.done():
                    completed.set_result(None)
            else:
                self._waiting[seq] = (future, completed, is_exc, obj)
                try:
                    os.write(write_fd, b"\0")
                except OSError:
                    self._complete_request(seq, is_exc, obj, completed)

        loop.add_reader(read_fd, reader)
        try:
            self._conn._request_callbacks[seq] = callback
            self._conn._send(consts.MSG_REQUEST, seq, (handler, self._conn._box(args)))
        except BaseException:
            loop.remove_reader(read_fd)
            self._close_request_fds((read_fd, write_fd))
            self._waiting.pop(seq, None)
            if not future.done():
                future.set_exception(EOFError("connection closed"))
            raise
        return future, completed, (read_fd, write_fd), seq

    async def _await_request(self, future, completed, fds, seq, cancel_args=None):
        try:
            is_exc, obj = await future
            self._continue()
        except asyncio.CancelledError:
            if cancel_args is not None:
                try:
                    self._conn.async_request(consts.HANDLE_AITER_CANCEL, *cancel_args)
                except Exception:
                    pass
            await completed
            self._waiting.pop(seq, None)
            self._close_request_fds(fds)
            raise
        finally:
            if not future.cancelled():
                self._waiting.pop(seq, None)
                self._close_request_fds(fds)
        if is_exc:
            raise obj
        return obj

    def _complete_request(self, seq, is_exc, obj, completed):
        if is_exc is None and obj is None:
            waiting = self._waiting.get(seq)
            if waiting is None or len(waiting) < 4:
                return
            is_exc, obj = waiting[2], waiting[3]
        try:
            waiting = self._waiting.get(seq)
            if waiting is None or waiting[0].done():
                if not is_exc:
                    _release_netrefs(self._conn, obj)
            else:
                waiting[0].set_result((is_exc, obj))
        finally:
            if not completed.done():
                completed.set_result(None)

    def _close_request_fds(self, fds):
        for fd in fds:
            try:
                os.close(fd)
            except OSError:
                pass

    def _continue(self):
        try:
            os.write(self._continue_write_fd, b"\0")
        except OSError:
            pass

    def _continue_ready(self):
        try:
            os.read(self._continue_read_fd, 1)
        except OSError:
            pass

    def fail_all(self):
        for seq in list(self._waiting.keys()):
            waiting = self._waiting.get(seq)
            if waiting is not None and not waiting[0].done():
                self._request_callbacks_failure(seq, EOFError("connection closed"))

    def _request_callbacks_failure(self, seq, error):
        callback = self._conn._request_callbacks.get(seq)
        if callback is not None:
            callback(True, error)


def itertools_count():
    import itertools
    return itertools.count(1)
