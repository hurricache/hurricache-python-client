"""Coordinator-aware HurriCache clients."""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import random
import threading
from collections.abc import Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import grpc
from tenacity import AsyncRetrying, Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from hurricache.grpc import coordinator_pb2, coordinator_pb2_grpc
from hurricache.grpc.async_client import AsyncHurriCacheClient
from hurricache.grpc.client import HurriCacheClient
from hurricache.grpc.exceptions import FailedPreconditionError, UnavailableError
from hurricache.grpc.models import KeyHintData, Mode


@dataclass(frozen=True, slots=True)
class _Topology:
    max_shards: int
    routes: dict[tuple[int, int], str]
    targets: frozenset[str]


_EMPTY_TOPOLOGY = _Topology(0, {}, frozenset())
_WRITE_PREFIXES = ("create_", "add_", "remove", "update_", "set_", "lock_", "unlock_", "get_and_")


def _coordinator_addresses(value: str | Sequence[str], port: int | None) -> tuple[str, ...]:
    addresses = (value,) if isinstance(value, str) else tuple(value)
    if port is not None:
        if len(addresses) != 1 or ":" in addresses[0]:
            raise ValueError("coordinator_port can only be used with one hostname")
        addresses = (f"{addresses[0]}:{port}",)
    if not addresses or any(not item for item in addresses):
        raise ValueError("at least one coordinator address is required")
    return addresses


def _split_target(target: str) -> tuple[str, int]:
    normalized = target.removeprefix("dns:///")
    host, separator, port = normalized.rpartition(":")
    if not separator or not host:
        raise ValueError(f"cache target must be host:port, got {target!r}")
    return host, int(port)


def _topology_from(response: coordinator_pb2.RoutingInfoData) -> _Topology:
    if response.max_shards <= 0:
        raise ValueError("coordinator returned max_shards=0")
    routes: dict[tuple[int, int], str] = {}
    targets: set[str] = set()
    for peer in response.peerRouting:
        targets.add(peer.target)
        for shard in peer.partitionIds:
            routes[(peer.role, shard)] = peer.target
    return _Topology(response.max_shards, routes, frozenset(targets))


def _hint_from_call(method: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> KeyHintData | None:
    try:
        bound = inspect.signature(method).bind_partial(*args, **kwargs)
    except (TypeError, ValueError):
        return kwargs.get("hint")
    value = bound.arguments.get("hint")
    return value if isinstance(value, KeyHintData) else None


def _is_write(name: str) -> bool:
    if name.startswith("atomic_"):
        return name not in {"atomic_load", "atomic_load_and_delete"}
    return name.startswith(_WRITE_PREFIXES)


class HurriCacheSmartClient:
    """Synchronous smart client with coordinator failover and background refresh."""

    def __init__(
        self,
        coordinators: str | Sequence[str] = "localhost:50051",
        coordinator_port: int | None = None,
        *,
        default_client_id: int = 0,
        default_timeout: float = 1.0,
        readiness_timeout: float = 60.0,
        refresh_interval: float = 30.0,
        mode: Mode = Mode.MASTER_THEN_BACKUP,
        credentials: grpc.ChannelCredentials | None = None,
    ) -> None:
        self._coordinators = _coordinator_addresses(coordinators, coordinator_port)
        self._default_client_id = default_client_id
        self._default_timeout = default_timeout
        self._readiness_timeout = readiness_timeout
        self._refresh_interval = refresh_interval
        self._configured_mode = Mode(mode)
        self._credentials = credentials
        self._mode_override: contextvars.ContextVar[Mode | None] = contextvars.ContextVar(
            f"hurricache_sync_mode_{id(self)}", default=None
        )
        self._topology = _EMPTY_TOPOLOGY
        self._clients: dict[str, HurriCacheClient] = {}
        self._coordinator_channel: grpc.Channel | None = None
        self._coordinator_index = 0
        self._random_shard = 0
        self._ready = threading.Event()
        self._stop = threading.Event()
        self._lock = threading.RLock()
        self._thread = threading.Thread(target=self._refresh_loop, name="hurricache-topology", daemon=True)
        self._thread.start()

    @property
    def ready(self) -> bool:
        return self._ready.is_set()

    @property
    def topology(self) -> _Topology:
        with self._lock:
            return self._topology

    def set_mode(self, mode: Mode) -> "HurriCacheSmartClient":
        self._mode_override.set(Mode(mode))
        return self

    def clear_mode(self) -> "HurriCacheSmartClient":
        self._mode_override.set(None)
        return self

    @contextmanager
    def mode(self, mode: Mode):
        token = self._mode_override.set(Mode(mode))
        try:
            yield self
        finally:
            self._mode_override.reset(token)

    def wait_until_ready(self, timeout: float | None = None) -> None:
        if not self._ready.wait(self._readiness_timeout if timeout is None else timeout):
            raise TimeoutError("HurriCache topology was not ready before the readiness timeout")

    def _coordinator(self, address: str) -> tuple[grpc.Channel, Any]:
        if self._credentials is None:
            channel = grpc.insecure_channel(address)
        else:
            channel = grpc.secure_channel(address, self._credentials)
        return channel, coordinator_pb2_grpc.CoordinatorServiceStub(channel)

    def _refresh_once(self) -> None:
        attempts = len(self._coordinators)
        retrying = Retrying(
            stop=stop_after_attempt(attempts),
            wait=wait_exponential(multiplier=0.1, max=1.0),
            retry=retry_if_exception_type(Exception),
            reraise=True,
        )
        for attempt in retrying:
            with attempt:
                address = self._coordinators[self._coordinator_index % attempts]
                self._coordinator_index += 1
                channel, stub = self._coordinator(address)
                old_channel = self._coordinator_channel
                self._coordinator_channel = channel
                if old_channel is not None:
                    old_channel.close()
                stream = stub.provideGlobalRoutingInfo(
                    coordinator_pb2.Void(), timeout=max(self._default_timeout, 0.1)
                )
                response = next(iter(stream))
                stream.cancel()
                self._apply_topology(_topology_from(response))

    def _refresh_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._refresh_once()
            except Exception:
                pass
            self._stop.wait(self._refresh_interval)

    def _apply_topology(self, topology: _Topology) -> None:
        with self._lock:
            removed = set(self._clients) - topology.targets
            for target in removed:
                self._clients.pop(target).close()
            for target in topology.targets:
                if target not in self._clients:
                    host, port = _split_target(target)
                    self._clients[target] = HurriCacheClient(
                        host,
                        port,
                        default_client_id=self._default_client_id,
                        default_timeout=self._default_timeout,
                        credentials=self._credentials,
                    )
            self._topology = topology
            self._ready.set()

    def _shard(self, topology: _Topology, hint: KeyHintData | None) -> int:
        if hint is None or hint.week_hash is None:
            with self._lock:
                self._random_shard = (self._random_shard + 1) & 0x7FFFFFFF
                return self._random_shard % topology.max_shards
        return (hint.week_hash & 0xFFFFFFFF) % topology.max_shards

    def _endpoints(self, topology: _Topology, shard: int, mode: Mode) -> list[HurriCacheClient]:
        master = self._clients.get(topology.routes.get((coordinator_pb2.MASTER, shard), ""))
        backup = self._clients.get(topology.routes.get((coordinator_pb2.BACKUP, shard), ""))
        if mode is Mode.MASTER:
            ordered = [master]
        elif mode is Mode.BACKUP:
            ordered = [backup]
        elif mode is Mode.LB_SMART:
            ordered = [master, backup] if random.getrandbits(1) else [backup, master]
        else:
            ordered = [master, backup]
        return [client for client in ordered if client is not None]

    def _execute(self, name: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        self.wait_until_ready()
        topology = self.topology
        method_type = getattr(HurriCacheClient, name)
        hint = _hint_from_call(method_type, (None, *args), kwargs)
        mode = self._mode_override.get() or (Mode.MASTER_THEN_BACKUP if _is_write(name) else self._configured_mode)
        endpoints = self._endpoints(topology, self._shard(topology, hint), mode)
        if not endpoints:
            raise UnavailableError("no healthy endpoints are available for the selected shard")
        last_error: Exception | None = None
        for endpoint in endpoints:
            try:
                return getattr(endpoint, name)(*args, **kwargs)
            except FailedPreconditionError as error:
                if error.route and error.route in self._clients:
                    return getattr(self._clients[error.route], name)(*args, **kwargs)
                raise
            except UnavailableError as error:
                last_error = error
                continue
        assert last_error is not None
        raise last_error

    def __getattr__(self, name: str):
        if not hasattr(HurriCacheClient, name) or name.startswith("_"):
            raise AttributeError(name)

        def routed(*args: Any, **kwargs: Any) -> Any:
            return self._execute(name, args, kwargs)

        return routed

    def close(self) -> None:
        self._stop.set()
        if self._thread is not threading.current_thread():
            self._thread.join(timeout=min(3.0, self._refresh_interval + 0.1))
        with self._lock:
            if self._coordinator_channel is not None:
                self._coordinator_channel.close()
                self._coordinator_channel = None
            for client in self._clients.values():
                client.close()
            self._clients.clear()

    def __enter__(self) -> "HurriCacheSmartClient":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()


class AsyncHurriCacheSmartClient:
    """Asynchronous smart client with task-local mode overrides."""

    def __init__(
        self,
        coordinators: str | Sequence[str] = "localhost:50051",
        coordinator_port: int | None = None,
        *,
        default_client_id: int = 0,
        default_timeout: float = 1.0,
        readiness_timeout: float = 60.0,
        refresh_interval: float = 30.0,
        mode: Mode = Mode.MASTER_THEN_BACKUP,
        credentials: grpc.ChannelCredentials | None = None,
    ) -> None:
        self._coordinators = _coordinator_addresses(coordinators, coordinator_port)
        self._default_client_id = default_client_id
        self._default_timeout = default_timeout
        self._readiness_timeout = readiness_timeout
        self._refresh_interval = refresh_interval
        self._configured_mode = Mode(mode)
        self._credentials = credentials
        self._mode_override: contextvars.ContextVar[Mode | None] = contextvars.ContextVar(
            f"hurricache_async_mode_{id(self)}", default=None
        )
        self._topology = _EMPTY_TOPOLOGY
        self._clients: dict[str, AsyncHurriCacheClient] = {}
        self._coordinator_channel: grpc.aio.Channel | None = None
        self._coordinator_index = 0
        self._random_shard = 0
        self._ready = asyncio.Event()
        self._closed = False
        self._task: asyncio.Task[None] | None = None

    @property
    def ready(self) -> bool:
        return self._ready.is_set()

    def set_mode(self, mode: Mode) -> "AsyncHurriCacheSmartClient":
        self._mode_override.set(Mode(mode))
        return self

    def clear_mode(self) -> "AsyncHurriCacheSmartClient":
        self._mode_override.set(None)
        return self

    @contextmanager
    def mode(self, mode: Mode):
        token = self._mode_override.set(Mode(mode))
        try:
            yield self
        finally:
            self._mode_override.reset(token)

    async def start(self) -> "AsyncHurriCacheSmartClient":
        if self._task is None:
            self._task = asyncio.create_task(self._refresh_loop(), name="hurricache-async-topology")
        return self

    async def wait_until_ready(self, timeout: float | None = None) -> None:
        await self.start()
        try:
            await asyncio.wait_for(self._ready.wait(), self._readiness_timeout if timeout is None else timeout)
        except TimeoutError as error:
            raise TimeoutError("HurriCache topology was not ready before the readiness timeout") from error

    async def _refresh_once(self) -> None:
        attempts = len(self._coordinators)
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(attempts),
            wait=wait_exponential(multiplier=0.1, max=1.0),
            retry=retry_if_exception_type(Exception),
            reraise=True,
        ):
            with attempt:
                address = self._coordinators[self._coordinator_index % attempts]
                self._coordinator_index += 1
                if self._credentials is None:
                    channel = grpc.aio.insecure_channel(address)
                else:
                    channel = grpc.aio.secure_channel(address, self._credentials)
                old_channel = self._coordinator_channel
                self._coordinator_channel = channel
                if old_channel is not None:
                    await old_channel.close()
                stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
                call = stub.provideGlobalRoutingInfo(
                    coordinator_pb2.Void(), timeout=max(self._default_timeout, 0.1)
                )
                response = await call.read()
                if response is grpc.aio.EOF:
                    raise UnavailableError("coordinator returned no topology")
                call.cancel()
                await self._apply_topology(_topology_from(response))

    async def _refresh_loop(self) -> None:
        while not self._closed:
            try:
                await self._refresh_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            try:
                await asyncio.sleep(self._refresh_interval)
            except asyncio.CancelledError:
                raise

    async def _apply_topology(self, topology: _Topology) -> None:
        removed = set(self._clients) - topology.targets
        for target in removed:
            await self._clients.pop(target).close()
        for target in topology.targets:
            if target not in self._clients:
                host, port = _split_target(target)
                self._clients[target] = AsyncHurriCacheClient(
                    host,
                    port,
                    default_client_id=self._default_client_id,
                    default_timeout=self._default_timeout,
                    credentials=self._credentials,
                )
        self._topology = topology
        self._ready.set()

    def _shard(self, hint: KeyHintData | None) -> int:
        if hint is None or hint.week_hash is None:
            self._random_shard = (self._random_shard + 1) & 0x7FFFFFFF
            return self._random_shard % self._topology.max_shards
        return (hint.week_hash & 0xFFFFFFFF) % self._topology.max_shards

    def _endpoints(self, shard: int, mode: Mode) -> list[AsyncHurriCacheClient]:
        master = self._clients.get(self._topology.routes.get((coordinator_pb2.MASTER, shard), ""))
        backup = self._clients.get(self._topology.routes.get((coordinator_pb2.BACKUP, shard), ""))
        if mode is Mode.MASTER:
            ordered = [master]
        elif mode is Mode.BACKUP:
            ordered = [backup]
        elif mode is Mode.LB_SMART:
            ordered = [master, backup] if random.getrandbits(1) else [backup, master]
        else:
            ordered = [master, backup]
        return [client for client in ordered if client is not None]

    async def _execute(self, name: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        await self.wait_until_ready()
        method_type = getattr(AsyncHurriCacheClient, name)
        hint = _hint_from_call(method_type, (None, *args), kwargs)
        mode = self._mode_override.get() or (Mode.MASTER_THEN_BACKUP if _is_write(name) else self._configured_mode)
        endpoints = self._endpoints(self._shard(hint), mode)
        if not endpoints:
            raise UnavailableError("no healthy endpoints are available for the selected shard")
        last_error: Exception | None = None
        for endpoint in endpoints:
            try:
                return await getattr(endpoint, name)(*args, **kwargs)
            except FailedPreconditionError as error:
                if error.route and error.route in self._clients:
                    return await getattr(self._clients[error.route], name)(*args, **kwargs)
                raise
            except UnavailableError as error:
                last_error = error
                continue
        assert last_error is not None
        raise last_error

    def __getattr__(self, name: str):
        if not hasattr(AsyncHurriCacheClient, name) or name.startswith("_"):
            raise AttributeError(name)

        async def routed(*args: Any, **kwargs: Any) -> Any:
            return await self._execute(name, args, kwargs)

        return routed

    async def close(self) -> None:
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        if self._coordinator_channel is not None:
            await self._coordinator_channel.close()
            self._coordinator_channel = None
        await asyncio.gather(*(client.close() for client in self._clients.values()), return_exceptions=True)
        self._clients.clear()

    async def __aenter__(self) -> "AsyncHurriCacheSmartClient":
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()
