"""Operation-scoped defaults and deadlines shared by all transports."""

from __future__ import annotations

import contextvars
import inspect
import math
import time
from contextlib import contextmanager
from functools import wraps

from hurricache.grpc.exceptions import CancelledError, DeadlineExceededError

deadline: contextvars.ContextVar[float | None] = contextvars.ContextVar("hurricache_deadline", default=None)
threshold: contextvars.ContextVar[int] = contextvars.ContextVar("hurricache_compression", default=1024)
started_millis: contextvars.ContextVar[int | None] = contextvars.ContextVar("hurricache_started_millis", default=None)


def absolute_ttl(ttl):
    started = started_millis.get()
    return (int(time.time() * 1000) if started is None else started) + ttl


def remaining(timeout=None):
    end = deadline.get()
    if end is None:
        return timeout
    left = end - time.monotonic()
    if left <= 0:
        raise DeadlineExceededError("HurriCache operation deadline exceeded")
    return left if timeout is None else min(left, timeout)


@contextmanager
def operation(client, timeout):
    if getattr(client, "_closed", False):
        raise CancelledError("client is closed")
    if timeout is not None and (not math.isfinite(timeout) or timeout < 0):
        raise ValueError("timeout must be finite and nonnegative")
    end = deadline.get()
    if end is None and timeout is not None:
        end = time.monotonic() + timeout
    token = deadline.set(end)
    started_token = started_millis.set(started_millis.get() if started_millis.get() is not None else int(time.time() * 1000))
    compression_token = threshold.set(client._default_compression_threshold)
    try:
        remaining()
        yield
    finally:
        threshold.reset(compression_token)
        started_millis.reset(started_token)
        deadline.reset(token)


class Defaults:
    _closed: bool
    _default_ttl: int
    _default_compression_threshold: int
    @property
    def default_client_id(self):
        return self._default_client_id

    @property
    def default_timeout(self):
        return self._default_timeout

    @property
    def default_ttl(self):
        return self._default_ttl

    @property
    def default_compression_threshold(self):
        return self._default_compression_threshold

    @property
    def target(self):
        if hasattr(self, "_host"):
            return f"{self._host}:{self._port}"
        return self._coordinators[max(0, self._coordinator_index - 1) % len(self._coordinators)]


def configure(client, ttl, compression_threshold):
    if not isinstance(ttl, int) or ttl < 0:
        raise ValueError("default_ttl must be a nonnegative integer")
    if not isinstance(compression_threshold, int) or compression_threshold < 0:
        raise ValueError("default_compression_threshold must be a nonnegative integer")
    client._default_ttl = ttl
    client._default_compression_threshold = compression_threshold
    client._closed = False


def _wrap(method):
    signature = inspect.signature(method)

    def arguments(client, args, kwargs):
        bound = signature.bind(client, *args, **kwargs)
        if "ttl" in signature.parameters:
            ttl = bound.arguments.get("ttl", signature.parameters["ttl"].default)
            if ttl is None:
                ttl = client._default_ttl
            if not isinstance(ttl, int) or ttl < 0:
                raise ValueError("ttl must be a nonnegative integer")
            bound.arguments["ttl"] = ttl
        return bound

    if inspect.iscoroutinefunction(method):
        @wraps(method)
        async def asynchronous(client, *args, **kwargs):
            with operation(client, kwargs.get("timeout", client._default_timeout)):
                bound = arguments(client, args, kwargs)
                return await method(*bound.args, **bound.kwargs)
        return asynchronous

    @wraps(method)
    def synchronous(client, *args, **kwargs):
        with operation(client, kwargs.get("timeout", client._default_timeout)):
            bound = arguments(client, args, kwargs)
            return method(*bound.args, **bound.kwargs)
    return synchronous


def operations(cls):
    for name, method in list(vars(cls).items()):
        if not name.startswith("_") and name != "close" and inspect.isfunction(method):
            setattr(cls, name, _wrap(method))
    return cls


def lock_expiration(duration):
    if not math.isfinite(duration) or duration < 0:
        raise ValueError("lock duration must be finite and nonnegative")
    if duration == 0:
        return None
    # Java adds millisecond durations to currentTimeMillis before truncating.
    seconds = (int(time.time() * 1000) + int(duration * 1000)) // 1000
    if not 0 <= seconds <= 0xFFFFFFFF:
        raise ValueError("lock expiration exceeds uint32")
    return seconds
