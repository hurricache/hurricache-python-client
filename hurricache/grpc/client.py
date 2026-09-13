"""HurriCache gRPC client implementation.

API follows the Java FastCacheAsyncSimpleClient pattern:
  - First argument is always a raw ``bytes`` key.
  - Second argument is an optional ``KeyHintData`` wrapper (may be ``None``).
  - Additional arguments (clientId, timeout, etc.) follow.

Value-returning methods return ``bytes`` and raise:
  - ``KeyNotFoundError`` if the key does not exist
  - ``PermissionDeniedError`` if access is denied
  - ``HurriCacheRpcError`` for other gRPC errors
"""

from __future__ import annotations

import time
from typing import Any

import grpc

from hurricache.grpc import cache_pb2, cache_pb2_grpc
from hurricache.grpc.batching import check, continuation, plan
from hurricache.grpc.exceptions import HurriCacheError, KeyNotFoundError, PartialOperationError, mapped_rpc_error
from hurricache.grpc.models import CasResult, KeyHintData, LockStatus, LockType, OrderedPayload, Payload
from hurricache.grpc.operation import Defaults, absolute_ttl, configure, lock_expiration, operations, remaining
from hurricache.grpc.utils import (
    build_get_request,
    create_key,
    create_ordered_key,
    create_ordered_value,
    create_value,
    decode_batch,
    decode_hint,
    decode_key,
    decode_value,
)


def _handle_rpc_error(method: str, error: grpc.RpcError, key: bytes = b"") -> None:
    """Convert gRPC error to appropriate Python exception."""
    raise mapped_rpc_error(method, error, key)


def _extract_value(response: cache_pb2.ValueResponse) -> bytes:
    """Extract bytes from a ValueResponse, raising KeyNotFoundError if empty."""
    if response is None or (not response.HasField("value_unordered") and not response.HasField("value_ordered")):
        raise KeyNotFoundError(b"", "Value response is empty")
    if response.HasField("value_unordered") and response.value_unordered is not None:
        return decode_value(response.value_unordered)
    if response.HasField("value_ordered") and response.value_ordered is not None:
        return decode_value(response.value_ordered)
    raise KeyNotFoundError(b"", "Value response has no value field")


@operations
class HurriCacheClient(Defaults):
    """Client for HurriCacheGrpcService.

    Args:
        host: gRPC server hostname or IP address.
        port: gRPC server port number.
        default_client_id: Default client ID used when not specified per-call.
        default_timeout: Default RPC timeout (seconds).
        credentials: Optional gRPC channel credentials.
        compression: Optional compression algorithm.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 50000,
        default_client_id: int = 0,
        default_timeout: float = 1.0,
        credentials: grpc.ChannelCredentials | None = None,
        compression: grpc.Compression | None = None,
        *,
        default_ttl: int = 0,
        default_compression_threshold: int = 1024,
    ):
        configure(self, default_ttl, default_compression_threshold)
        self._host = host
        self._port = port
        self._default_client_id = default_client_id
        self._default_timeout = default_timeout
        self._credentials = credentials
        self._compression = compression
        self._channel: grpc.Channel | None = None
        self._stub: cache_pb2_grpc.HurriCacheGrpcServiceStub | None = None

    # ------------------------------------------------------------------
    # Channel / stub management
    # ------------------------------------------------------------------

    @property
    def channel(self) -> grpc.Channel:
        if self._channel is None:
            options: list[tuple[str, object]] = []
            if self._credentials is not None:
                self._channel = grpc.secure_channel(
                    f"{self._host}:{self._port}",
                    self._credentials,
                    options=options,
                    compression=self._compression,
                )
            else:
                self._channel = grpc.insecure_channel(
                    f"{self._host}:{self._port}",
                    options=options,
                    compression=self._compression,
                )
            self._stub = cache_pb2_grpc.HurriCacheGrpcServiceStub(self._channel)
        return self._channel

    @property
    def stub(self) -> cache_pb2_grpc.HurriCacheGrpcServiceStub:
        if self._stub is None:
            _ = self.channel
        assert self._stub is not None
        return self._stub

    def close(self) -> None:
        self._closed = True
        if self._channel is not None:
            self._channel.close()
            self._channel = None
            self._stub = None

    def __enter__(self) -> "HurriCacheClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_client_id(self, client_id: int | None) -> int:
        return client_id if client_id is not None and client_id != 0 else self._default_client_id

    def _build_key(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        client_id: int | None = None,
    ) -> cache_pb2.Key:
        cid = self._resolve_client_id(client_id)
        return create_key(key, hint, cid)

    def _build_get_req(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        client_id: int | None = None,
    ) -> cache_pb2.GetRequest:
        return build_get_request(key, hint, self._resolve_client_id(client_id))

    def _call(self, method_name: str, rpc_callable, *args, **kwargs) -> grpc.Call:
        """Execute an RPC call and handle errors."""
        kwargs["timeout"] = remaining(kwargs.get("timeout", self._default_timeout))
        try:
            if args:
                check(args[0])
            return rpc_callable(*args, **kwargs)
        except grpc.RpcError as e:
            request = args[0] if args else None
            key = decode_key(request.key) if request is not None and hasattr(request, "key") else b""
            _handle_rpc_error(method_name, e, key)
            raise  # pragma: no cover

    def _collect_stream(self, method_name: str, stream, key: bytes | str):
        result: Any = None
        completed_chunks = 0
        completed_items = 0
        try:
            for batch in stream:
                remaining()
                decoded = decode_batch(batch)
                completed_chunks += 1
                completed_items += len(decoded)
                if result is None:
                    result = {} if isinstance(decoded, dict) else []
                if isinstance(result, dict):
                    result.update(decoded)
                else:
                    result.extend(decoded)
        except grpc.RpcError as error:
            mapped = mapped_rpc_error(method_name, error, key.encode() if isinstance(key, str) else key)
            if completed_chunks:
                raise PartialOperationError(completed_chunks, completed_items, result) from mapped
            raise mapped from error
        except Exception as error:
            if completed_chunks:
                raise PartialOperationError(completed_chunks, completed_items, result) from error
            raise
        return [] if result is None else result

    def _create_container_chunked(self, request, **kwargs):
        if request.HasField("ttl"):
            for value in list(request.value_unordered) + list(request.value_ordered):
                if request.type in (cache_pb2.MAP, cache_pb2.ORDERED_MAP, cache_pb2.ORDERED_SET):
                    value.ttl = request.ttl
        chunks = plan(request, creation=True)
        # Preflight continuation headers as well as the create headers.
        for chunk, _ in chunks[1:]:
            reserved = cache_pb2.KeyHint(week_hash=0xFFFFFFFF, strong_hash=0xFFFFFFFF)
            check(continuation(chunk, reserved))
        response = self._call("create_container", self.stub.createContainer, chunks[0][0], **kwargs)
        hint = decode_hint(response) or KeyHintData.unspecified()
        done, acknowledged = chunks[0][1], 1
        rpc = self.stub.addElementToTail if request.type in (cache_pb2.LIST, cache_pb2.VECTOR, cache_pb2.QUEUE) else self.stub.addElement
        for chunk, count in chunks[1:]:
            try:
                wire_hint = response.keyHint if response.HasField("keyHint") else None
                result = self._call("create_container", rpc, continuation(chunk, wire_hint), **kwargs)
                if request.type in (cache_pb2.LIST, cache_pb2.VECTOR, cache_pb2.QUEUE) and result.size == 0:
                    raise HurriCacheError("server rejected continuation chunk")
            except Exception as error:
                raise PartialOperationError(acknowledged, done, hint) from error
            acknowledged += 1
            done += count
        return hint

    def _call_add_chunked(self, name, rpc, request, **kwargs):
        boolean = kwargs.pop("_boolean", False) or name in {
            "add_element_to_head", "add_element_to_tail", "add_element_to_position_by_value"
        }
        chunks = plan(request)
        total, done, acknowledged = 0, 0, 0
        for chunk, count in chunks:
            if count == 0:
                continue
            try:
                response = self._call(name, rpc, chunk, **kwargs)
            except Exception as error:
                if acknowledged:
                    raise PartialOperationError(acknowledged, done, True if boolean else total) from error
                raise
            if total + response.size > 0xFFFFFFFF:
                raise PartialOperationError(acknowledged + 1, done + count, True if boolean else total) from OverflowError("count exceeds uint32")
            total += response.size
            if boolean and response.size == 0:
                if acknowledged:
                    raise PartialOperationError(acknowledged, done, False) from HurriCacheError("server rejected chunk")
                return False
            acknowledged += 1
            done += count
        return True if boolean else total

    # ------------------------------------------------------------------
    # Lock Management
    # ------------------------------------------------------------------

    def lock_object(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        lock_type: LockType = LockType.NO_LOCK,
        client_id: int | None = None,
        lock_duration: float = 0,
        **kwargs,
    ) -> LockStatus:
        """Acquire a lock on a key.

        Args:
            key: Key to lock (must exist).
            hint: Optional KeyHintData for routing.
            lock_type: LockType (NO_LOCK, WRITE_LOCK, READ_LOCK, GLOBAL).
            client_id: Client ID holding the lock. Only this client can unlock.
            lock_duration: Lock duration in seconds. Server auto-releases after this.

        Returns:
            Complete LockStatus returned by the server.

        Raises:
            PermissionDeniedError: If key is already locked by another client.
        """
        request = cache_pb2.LockRequest(
            key=self._build_key(key, hint, client_id),
            lockType=cache_pb2.LockType.Value(LockType(lock_type).name),
            clientId=self._resolve_client_id(client_id),
            lockDuration=lock_expiration(lock_duration),
        )
        response = self._call("lock_object", self.stub.lockObject, request, **kwargs)
        return LockStatus(response.result)

    def unlock_object(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> LockStatus:
        """Release a lock on a key.

        Args:
            key: Key to unlock.
            hint: Optional KeyHintData for routing.
            client_id: Must match the client_id that acquired the lock.

        Returns:
            Complete LockStatus returned by the server.
        """
        request = cache_pb2.UnLockRequest(
            key=self._build_key(key, hint, client_id),
            clientId=self._resolve_client_id(client_id),
        )
        response = self._call("unlock_object", self.stub.unlockObject, request, **kwargs)
        return LockStatus(response.result)

    # ------------------------------------------------------------------
    # TTL Management
    # ------------------------------------------------------------------

    def set_ttl(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        ttl: int = 0,
        client_id: int | None = None,
        **kwargs,
    ) -> bool:
        """Set TTL for a key.

        Args:
            key: Key to set TTL on.
            hint: Optional KeyHintData for routing.
            ttl: TTL in milliseconds (relative). Converted to absolute time.
            client_id: Client ID.

        Returns:
            True if TTL was set successfully.
        """
        request = cache_pb2.TtlRequest(
            key=self._build_key(key, hint, client_id),
        )
        request.ttl = absolute_ttl(ttl)
        response = self._call("set_ttl", self.stub.setTtl, request, **kwargs)
        return response.value

    def get_ttl(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> int:
        """Get remaining TTL for a key.

        Args:
            key: Key to query.
            hint: Optional KeyHintData for routing.
            client_id: Client ID.

        Returns:
            Remaining TTL in milliseconds, or 0 if no TTL set.
        """
        request = self._build_get_req(key, hint, client_id)
        response = self._call("get_ttl", self.stub.getTtl, request, **kwargs)
        return response.ttl - int(time.time() * 1000) if response.HasField("ttl") else -1

    # ------------------------------------------------------------------
    # Key-Value Operations
    # ------------------------------------------------------------------

    def create_key_value(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        value: bytes = b"",
        ttl: int | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> KeyHintData:
        """Create a scalar key-value entry.

        Args:
            key: Key bytes.
            hint: Optional KeyHintData for routing.
            value: Value bytes to store.
            ttl: TTL in milliseconds (relative). Converted to absolute time.
            client_id: Client ID. If != 0, lock_info is created with type=NO_LOCK.

        Returns:
            KeyHintData with week_hash and strong_hash for subsequent operations.
        """
        ttl = self._default_ttl if ttl is None else ttl
        request = cache_pb2.CreateRequest(
            key=self._build_key(key, hint, client_id),
            value=create_value(value, ttl, self._resolve_client_id(client_id), compress=True),
        )
        response = self._call("create_key_value", self.stub.createKeyValue, request, **kwargs)
        return decode_hint(response) or KeyHintData.unspecified()

    def get_value(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> bytes:
        """Get value by key.

        Args:
            key: Key to read.
            hint: Optional KeyHintData for routing.
            client_id: Client ID.

        Returns:
            Value bytes.

        Raises:
            KeyNotFoundError: If key does not exist.
        """
        request = self._build_get_req(key, hint, client_id)
        response = self._call("get_value", self.stub.getValue, request, **kwargs)
        return _extract_value(response)

    def get_and_delete_value(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> bytes:
        """Atomically read and delete a key.

        Args:
            key: Key to read and delete.
            hint: Optional KeyHintData for routing.
            client_id: Client ID.

        Returns:
            Value bytes.

        Raises:
            KeyNotFoundError: If key does not exist.
        """
        request = self._build_get_req(key, hint, client_id)
        response = self._call("get_and_delete_value", self.stub.getAndDeleteValue, request, **kwargs)
        return _extract_value(response)

    def exist_key(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> bool:
        """Check if a key exists.

        Args:
            key: Key to check.
            hint: Optional KeyHintData for routing.
            client_id: Client ID.

        Returns:
            True if key exists, False otherwise.
        """
        request = self._build_get_req(key, hint, client_id)
        response = self._call("exist_key", self.stub.existKey, request, **kwargs)
        return response.value

    def update_value(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        value: bytes = b"",
        ttl: int | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> bytes:
        """Update an existing value. Returns old value.

        Args:
            key: Key to update.
            hint: Optional KeyHintData for routing.
            value: New value bytes.
            ttl: TTL in milliseconds (relative). Converted to absolute time.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            Old value bytes.
        """
        ttl = self._default_ttl if ttl is None else ttl
        request = cache_pb2.UpdateRequest(
            key=self._build_key(key, hint, client_id),
            value=create_value(value, ttl, self._resolve_client_id(client_id), compress=True),
        )
        response = self._call("update_value", self.stub.updateValue, request, **kwargs)
        if response.HasField("value") and response.value is not None:
            return decode_value(response.value)
        return b""

    def remove(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> bool:
        """Remove a key.

        Args:
            key: Key to remove.
            hint: Optional KeyHintData for routing.
            client_id: Client ID.

        Returns:
            True if removed, False otherwise.
        """
        request = self._build_get_req(key, hint, client_id)
        response = self._call("remove", self.stub.remove, request, **kwargs)
        return response.value

    # ------------------------------------------------------------------
    # Container Management & Streaming
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Container Creation (Unordered)
    # ------------------------------------------------------------------

    def create_vector(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        values: list[bytes] | None = None,
        ttl: int | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> KeyHintData:
        """Create a VECTOR container (dynamic array).

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            values: Initial list of value bytes.
            ttl: TTL in milliseconds (relative). Converted to absolute time.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            KeyHintData for subsequent operations.
        """
        ttl = self._default_ttl if ttl is None else ttl
        return self._create_unordered_container(
            key, hint, cache_pb2.ContainerType.VECTOR, values, ttl, client_id, **kwargs
        )

    def create_list(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        values: list[bytes] | None = None,
        ttl: int | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> KeyHintData:
        """Create a LIST container (doubly-linked list).

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            values: Initial list of value bytes.
            ttl: TTL in milliseconds (relative). Converted to absolute time.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            KeyHintData for subsequent operations.
        """
        ttl = self._default_ttl if ttl is None else ttl
        return self._create_unordered_container(
            key, hint, cache_pb2.ContainerType.LIST, values, ttl, client_id, **kwargs
        )

    def create_queue(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        values: list[bytes] | None = None,
        ttl: int | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> KeyHintData:
        """Create a QUEUE container (FIFO).

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            values: Initial list of value bytes.
            ttl: TTL in milliseconds (relative). Converted to absolute time.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            KeyHintData for subsequent operations.
        """
        ttl = self._default_ttl if ttl is None else ttl
        return self._create_unordered_container(
            key, hint, cache_pb2.ContainerType.QUEUE, values, ttl, client_id, **kwargs
        )

    def create_set(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        values: list[bytes] | None = None,
        ttl: int | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> KeyHintData:
        """Create a SET container (unordered unique elements).

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            values: Initial list of value bytes.
            ttl: TTL in milliseconds (relative). Converted to absolute time.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            KeyHintData for subsequent operations.
        """
        ttl = self._default_ttl if ttl is None else ttl
        return self._create_unordered_container(
            key, hint, cache_pb2.ContainerType.SET, values, ttl, client_id, **kwargs
        )

    def _create_unordered_container(
        self,
        key: bytes,
        hint: KeyHintData | None,
        type: cache_pb2.ContainerType,
        values: list[bytes] | None,
        ttl: int,
        client_id: int | None,
        **kwargs,
    ) -> KeyHintData:
        cid = self._resolve_client_id(client_id)
        proto_key = self._build_key(key, hint, cid)
        values_proto = [create_value(v, 0, cid) for v in (values or [])]

        builder = cache_pb2.CreateContainerRequest(
            key=proto_key,
            type=type,
            value_unordered=values_proto,
        )
        if ttl > 0:
            builder.ttl = absolute_ttl(ttl)
        return self._create_container_chunked(builder, **kwargs)

    # ------------------------------------------------------------------
    # Container Creation (Map)
    # ------------------------------------------------------------------

    def create_map(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        keys: list[bytes] | None = None,
        values: list[bytes] | None = None,
        ttl: int | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> KeyHintData:
        """Create a MAP container (unordered hash map).

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            keys: List of key bytes.
            values: List of value bytes.
            ttl: TTL in milliseconds (relative). Converted to absolute time.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            KeyHintData for subsequent operations.
        """
        ttl = self._default_ttl if ttl is None else ttl
        cid = self._resolve_client_id(client_id)
        if len(keys or []) != len(values or []):
            raise ValueError("map keys and values must have equal lengths")
        proto_key = self._build_key(key, hint, cid)
        if len(keys or ()) != len(values or ()):
            raise ValueError("map keys and values must have equal lengths")
        if keys is not None and len(keys) != len(values or ()):
            raise ValueError("map keys and values must have equal lengths")
        proto_keys = [create_key(k, None, cid, compress=False) for k in (keys or [])]
        proto_values = [create_value(v, ttl, cid, compress=False) for v in (values or [])]

        builder = cache_pb2.CreateContainerRequest(
            key=proto_key,
            type=cache_pb2.ContainerType.MAP,
            key_unordered=proto_keys,
            value_unordered=proto_values,
        )
        if ttl > 0:
            builder.ttl = absolute_ttl(ttl)
        return self._create_container_chunked(builder, **kwargs)

    # ------------------------------------------------------------------
    # Container Creation (OrderedSet)
    # ------------------------------------------------------------------

    def create_ordered_set(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        values: list[OrderedPayload] | None = None,
        ttl: int | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> KeyHintData:
        """Create an ORDERED_SET container (weight/score-ranked elements).

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            values: List of OrderedPayload (value + order/weight).
            ttl: TTL in milliseconds (relative). Converted to absolute time.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            KeyHintData for subsequent operations.
        """
        ttl = self._default_ttl if ttl is None else ttl
        cid = self._resolve_client_id(client_id)
        proto_key = self._build_key(key, hint, cid)
        values_proto = [create_ordered_value(v.value, v.order, ttl, cid) for v in (values or [])]

        builder = cache_pb2.CreateContainerRequest(
            key=proto_key,
            type=cache_pb2.ContainerType.ORDERED_SET,
            value_ordered=values_proto,
        )
        if ttl > 0:
            builder.ttl = absolute_ttl(ttl)
        return self._create_container_chunked(builder, **kwargs)

    # ------------------------------------------------------------------
    # Container Creation (OrderedMap)
    # ------------------------------------------------------------------

    def create_ordered_map(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        keys: list[OrderedPayload] | None = None,
        values: list[bytes] | None = None,
        ttl: int | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> KeyHintData:
        """Create an ORDERED_MAP container (keys with weight/score, values are bytes).

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            keys: List of OrderedPayload (key + order/weight).
            values: List of value bytes.
            ttl: TTL in milliseconds (relative). Converted to absolute time.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            KeyHintData for subsequent operations.
        """
        ttl = self._default_ttl if ttl is None else ttl
        cid = self._resolve_client_id(client_id)
        if len(keys or []) != len(values or []):
            raise ValueError("ordered-map keys and values must have equal lengths")
        proto_key = self._build_key(key, hint, cid)
        proto_keys = [create_ordered_key(k.value, k.order, 0, cid) for k in (keys or [])]
        proto_values = [create_value(v, ttl, cid, compress=True) for v in (values or [])]

        builder = cache_pb2.CreateContainerRequest(
            key=proto_key,
            type=cache_pb2.ContainerType.ORDERED_MAP,
            key_ordered=proto_keys,
            value_unordered=proto_values,
        )
        if ttl > 0:
            builder.ttl = absolute_ttl(ttl)
        return self._create_container_chunked(builder, **kwargs)

    def get_container(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        client_id: int | None = None,
        **kwargs,
    ):
        """Stream all elements from a container.

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            client_id: Client ID.

        Returns:
            Stream of BatchValueResponse.
        """
        request = self._build_get_req(key, hint, client_id)
        stream = self._call("get_container", self.stub.getContainer, request, **kwargs)
        return self._collect_stream("get_container", stream, key)

    def get_size(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> int:
        """Get container size or value length.

        For containers: returns element count (except QUEUE returns 0).
        For scalar keys: returns value byte length.

        Args:
            key: Key or container key.
            hint: Optional KeyHintData for routing.
            client_id: Client ID.

        Returns:
            Size as integer.
        """
        request = self._build_get_req(key, hint, client_id)
        response = self._call("get_size", self.stub.getSize, request, **kwargs)
        return response.size

    # ------------------------------------------------------------------
    # Boundary Reads
    # ------------------------------------------------------------------

    def get_tail(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> bytes:
        """Get last element (tail/back) of a container.

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            client_id: Client ID.

        Returns:
            Last value bytes.

        Raises:
            KeyNotFoundError: If container does not exist.
        """
        request = self._build_get_req(key, hint, client_id)
        response = self._call("get_tail", self.stub.getTail, request, **kwargs)
        return _extract_value(response)

    def get_front(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> bytes:
        """Alias for get_head. Get first element (front) of a container.

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            client_id: Client ID.

        Returns:
            First value bytes.
        """
        return self.get_head(key, hint, client_id, **kwargs)

    def get_head(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> bytes:
        """Get first element (head/front) of a container.

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            client_id: Client ID.

        Returns:
            First value bytes.

        Raises:
            KeyNotFoundError: If container does not exist.
        """
        request = self._build_get_req(key, hint, client_id)
        response = self._call("get_head", self.stub.getHead, request, **kwargs)
        return _extract_value(response)

    def get_value_in_container(
        self,
        key: bytes,
        element_key: bytes,
        hint: KeyHintData | None = None,
        element_hint: KeyHintData | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> bytes:
        """Get value by element key inside a container (MAP/ORDERED_MAP).

        Args:
            key: Container key.
            element_key: Element key inside the container.
            hint: Optional KeyHintData for container routing.
            element_hint: Optional KeyHintData for element routing.
            client_id: Client ID.

        Returns:
            Value bytes.

        Raises:
            KeyNotFoundError: If element does not exist.
        """
        request = cache_pb2.ContainerGetRequest(
            key=self._build_key(key, hint, client_id),
            element_key=create_key(element_key, element_hint, self._resolve_client_id(client_id), compress=False),
        )
        response = self._call("get_value_in_container", self.stub.getValueInContainer, request, **kwargs)
        return _extract_value(response)

    def exist_key_in_container(
        self,
        key: bytes,
        element_key: bytes,
        hint: KeyHintData | None = None,
        element_hint: KeyHintData | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> bool:
        """Check if an element key exists inside a container.

        Args:
            key: Container key.
            element_key: Element key to check.
            hint: Optional KeyHintData for container routing.
            element_hint: Optional KeyHintData for element routing.
            client_id: Client ID.

        Returns:
            True if element exists, False otherwise.
        """
        request = cache_pb2.ContainerGetRequest(
            key=self._build_key(key, hint, client_id),
            element_key=create_key(element_key, element_hint, self._resolve_client_id(client_id), compress=False),
        )
        response = self._call("exist_key_in_container", self.stub.existKeyInContainer, request, **kwargs)
        return response.value

    def contains_container_key(
        self,
        key: bytes,
        element_key: bytes,
        hint: KeyHintData | None = None,
        element_hint: KeyHintData | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> bool:
        """Alias for exist_key_in_container.

        Check if an element key exists inside a container.
        """
        return self.exist_key_in_container(key, element_key, hint, element_hint, client_id, **kwargs)

    # ------------------------------------------------------------------
    # Pop Operations
    # ------------------------------------------------------------------

    def get_and_remove_front(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> bytes:
        """Pop first element from container (FIFO queue, LIST front).

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            client_id: Client ID.

        Returns:
            Removed value bytes.

        Raises:
            KeyNotFoundError: If container is empty or does not exist.
        """
        request = self._build_get_req(key, hint, client_id)
        response = self._call("get_and_remove_front", self.stub.getAndRemoveFront, request, **kwargs)
        return _extract_value(response)

    def get_and_remove_tail(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> bytes:
        """Pop last element from container (LIST/VECTOR back).

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            client_id: Client ID.

        Returns:
            Removed value bytes.

        Raises:
            KeyNotFoundError: If container is empty or does not exist.
        """
        request = self._build_get_req(key, hint, client_id)
        response = self._call("get_and_remove_tail", self.stub.getAndRemoveTail, request, **kwargs)
        return _extract_value(response)

    def get_and_remove_element_at_position(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        pos: int = 0,
        type: cache_pb2.ContainerType = cache_pb2.ContainerType.UNDEFINED,
        client_id: int | None = None,
        **kwargs,
    ) -> bytes:
        """Remove element at position from container.

        For LIST/VECTOR: 0-based index.
        For ORDERED_SET: removes elements matching exact weight.

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            pos: Position index (LIST/VECTOR) or weight (ORDERED_SET).
            type: ContainerType.
            client_id: Client ID.

        Returns:
            Removed value bytes.
        """
        request = cache_pb2.KeyPositionRequest(
            key=self._build_key(key, hint, client_id),
            type=type,
            pos=pos,
        )
        response = self._call("get_and_remove_element_at_position", self.stub.getAndRemoveElementAtPosition, request, **kwargs)
        return _extract_value(response)

    def get_and_delete_value_in_container(
        self,
        key: bytes,
        element_key: bytes,
        hint: KeyHintData | None = None,
        element_hint: KeyHintData | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> bytes:
        """Atomically read and delete an element from a container.

        Args:
            key: Container key.
            element_key: Element key to delete.
            hint: Optional KeyHintData for container routing.
            element_hint: Optional KeyHintData for element routing.
            client_id: Client ID.

        Returns:
            Deleted value bytes.

        Raises:
            KeyNotFoundError: If element does not exist.
        """
        request = cache_pb2.ContainerGetRequest(
            key=self._build_key(key, hint, client_id),
            element_key=create_key(element_key, element_hint, self._resolve_client_id(client_id), compress=False),
        )
        response = self._call("get_and_delete_value_in_container", self.stub.getAndDeleteValueInContainer, request, **kwargs)
        return _extract_value(response)

    def update_value_in_container(
        self,
        key: bytes,
        element_key: bytes,
        value: bytes,
        hint: KeyHintData | None = None,
        element_hint: KeyHintData | None = None,
        ttl: int | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> bytes:
        """Update value of an element in a container. Returns old value.

        Args:
            key: Container key.
            element_key: Element key to update.
            value: New value bytes.
            hint: Optional KeyHintData for container routing.
            element_hint: Optional KeyHintData for element routing.
            ttl: TTL in milliseconds (relative). Converted to absolute time.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            Old value bytes.
        """
        ttl = self._default_ttl if ttl is None else ttl
        request = cache_pb2.UpdateContainerRequest(
            key=self._build_key(key, hint, client_id),
            element_key=create_key(element_key, element_hint, self._resolve_client_id(client_id), compress=False),
            value=create_value(value, ttl, self._resolve_client_id(client_id)),
        )
        response = self._call("update_value_in_container", self.stub.updateValueInContainer, request, **kwargs)
        if response.HasField("value") and response.value is not None:
            return decode_value(response.value)
        return b""

    # ------------------------------------------------------------------
    # Positional & Range Reads
    # ------------------------------------------------------------------

    def get_element_at_position(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        pos: int = 0,
        type: cache_pb2.ContainerType = cache_pb2.ContainerType.UNDEFINED,
        client_id: int | None = None,
        **kwargs,
    ) -> bytes:
        """Get element at position from container.

        For LIST/VECTOR: 0-based index.
        For ORDERED_SET: access by exact uint64 weight.

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            pos: Position index or weight.
            type: ContainerType.
            client_id: Client ID.

        Returns:
            Value bytes.
        """
        request = cache_pb2.KeyPositionRequest(
            key=self._build_key(key, hint, client_id),
            type=type,
            pos=pos,
        )
        response = self._call("get_element_at_position", self.stub.getElementAtPosition, request, **kwargs)
        return _extract_value(response)

    def get_element_in_range(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        pos: int = 0,
        end: int = 0,
        type: cache_pb2.ContainerType = cache_pb2.ContainerType.UNDEFINED,
        reverse: bool = False,
        client_id: int | None = None,
        **kwargs,
    ):
        """Stream elements in range from container.

        For LIST/VECTOR: index range [pos..end].
        For ORDERED_SET: weight range [min_weight..max_weight].

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            pos: Start position or weight.
            end: End position or weight.
            type: ContainerType.
            reverse: Reverse traversal flag.
            client_id: Client ID.

        Returns:
            Stream of BatchValueResponse.
        """
        request = cache_pb2.KeyPositionRequest(
            key=self._build_key(key, hint, client_id),
            type=type,
            pos=pos,
            end=end,
            reverse=reverse,
        )
        stream = self._call("get_element_in_range", self.stub.getElementInRange, request, **kwargs)
        return self._collect_stream("get_element_in_range", stream, key)

    # ------------------------------------------------------------------
    # Deletion Operations
    # ------------------------------------------------------------------

    def remove_head(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> bool:
        """Remove first element from container (pop front).

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            client_id: Client ID.

        Returns:
            True if removed, False otherwise.
        """
        request = self._build_get_req(key, hint, client_id)
        response = self._call("remove_head", self.stub.removeHead, request, **kwargs)
        return response.value

    def remove_tail(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> bool:
        """Remove last element from container (pop back).

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            client_id: Client ID.

        Returns:
            True if removed, False otherwise.
        """
        request = self._build_get_req(key, hint, client_id)
        response = self._call("remove_tail", self.stub.removeTail, request, **kwargs)
        return response.value

    def remove_element_at_position(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        pos: int = 0,
        type: cache_pb2.ContainerType = cache_pb2.ContainerType.UNDEFINED,
        client_id: int | None = None,
        **kwargs,
    ) -> bool:
        """Remove element at position from container.

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            pos: Position index (LIST/VECTOR) or weight (ORDERED_SET).
            type: ContainerType.
            client_id: Client ID.

        Returns:
            True if removed, False otherwise.
        """
        request = cache_pb2.KeyPositionRequest(
            key=self._build_key(key, hint, client_id),
            type=type,
            pos=pos,
        )
        response = self._call("remove_element_at_position", self.stub.removeElementAtPosition, request, **kwargs)
        return response.value

    def remove_from_container_by_key_value(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        type: cache_pb2.ContainerType = cache_pb2.ContainerType.UNDEFINED,
        values: list[bytes] | None = None,
        keys: list[bytes] | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> int:
        """Remove elements from container by key or value.

        For MAP/ORDERED_MAP: use keys parameter.
        For SET/ORDERED_SET/LIST/VECTOR: use values parameter.

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            type: ContainerType.
            values: Values to remove.
            keys: Keys to remove.
            client_id: Client ID.

        Returns:
            Number of elements removed.
        """
        cid = self._resolve_client_id(client_id)
        proto_values = [create_value(v, 0, cid) for v in (values or [])]
        proto_keys = [create_key(k, None, cid) for k in (keys or [])]
        request = cache_pb2.RemoveFromContainerRequest(
            key=self._build_key(key, hint, client_id),
            type=type,
            values=proto_values,
            keys=proto_keys,
        )
        return self._call_add_chunked("remove_from_container_by_key_value", self.stub.removeFromContainerByKeyValue, request, **kwargs)

    def remove_in_container(
        self,
        key: bytes,
        element_key: bytes,
        hint: KeyHintData | None = None,
        element_hint: KeyHintData | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> int:
        """Remove element by key from container.

        Args:
            key: Container key.
            element_key: Element key to remove.
            hint: Optional KeyHintData for container routing.
            element_hint: Optional KeyHintData for element routing.
            client_id: Client ID.

        Returns:
            Number of elements removed.
        """
        request = cache_pb2.ContainerGetRequest(
            key=self._build_key(key, hint, client_id),
            element_key=create_key(element_key, element_hint, self._resolve_client_id(client_id), compress=False),
        )
        response = self._call("remove_in_container", self.stub.removeInContainer, request, **kwargs)
        return response.size

    # ------------------------------------------------------------------
    # Insertion Operations
    # ------------------------------------------------------------------

    def add_element_to_tail(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        values: list[bytes] | None = None,
        ttl: int = 0,
        client_id: int | None = None,
        **kwargs,
    ) -> bool:
        """Add elements to tail (end) of container.

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            values: List of value bytes to add.
            ttl: TTL in milliseconds (relative). Applied to each value.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            True if successful.
        """
        cid = self._resolve_client_id(client_id)
        request = cache_pb2.AddToRequest(
            key=self._build_key(key, hint, client_id),
            value_unordered=[create_value(v, ttl, cid) for v in (values or [])],
        )
        return self._call_add_chunked("add_element_to_tail", self.stub.addElementToTail, request, **kwargs)

    def add_element_to_head(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        values: list[bytes] | None = None,
        ttl: int = 0,
        client_id: int | None = None,
        **kwargs,
    ) -> bool:
        """Add elements to head (beginning) of container.

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            values: List of value bytes to add.
            ttl: TTL in milliseconds (relative). Applied to each value.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            True if successful.
        """
        cid = self._resolve_client_id(client_id)
        request = cache_pb2.AddToRequest(
            key=self._build_key(key, hint, client_id),
            value_unordered=[create_value(v, ttl, cid) for v in (values or [])],
        )
        return self._call_add_chunked("add_element_to_head", self.stub.addElementToHead, request, **kwargs)

    def add_element(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        values: list[bytes] | None = None,
        keys: list[bytes] | None = None,
        ttl: int = 0,
        client_id: int | None = None,
        **kwargs,
    ) -> int:
        """Add elements to container (unordered).

        For MAP: use both keys and values.
        For SET/LIST/VECTOR: use values only.

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            values: List of value bytes.
            keys: List of key bytes (for MAP).
            ttl: TTL in milliseconds (relative). Applied to each value.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            Number of elements added.
        """
        cid = self._resolve_client_id(client_id)
        if keys is not None and len(keys) != len(values or []):
            raise ValueError("map keys and values must have equal lengths")
        proto_values = [create_value(v, ttl, cid) for v in (values or [])]
        proto_keys = [create_key(k, None, cid, compress=False) for k in (keys or [])]
        request = cache_pb2.AddToRequest(
            key=self._build_key(key, hint, client_id),
            value_unordered=proto_values,
            key_unordered=proto_keys,
        )
        if keys is not None:
            request.type = cache_pb2.MAP
        return self._call_add_chunked("add_element", self.stub.addElement, request, **kwargs)

    def add_element_hash_map(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        values: list[bytes] | None = None,
        keys: list[bytes] | None = None,
        ttl: int = 0,
        client_id: int | None = None,
        **kwargs,
    ) -> int:
        """Alias for add_element with keys+values for MAP containers."""
        return self.add_element(key, hint, values, keys, ttl, client_id, **kwargs)

    def add_element_to_position_by_value(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        pos: bytes = b"",
        is_before: bool = True,
        values: list[bytes] | None = None,
        ttl: int = 0,
        client_id: int | None = None,
        **kwargs,
    ) -> bool:
        """Insert elements before or after a pivot value.

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            pos: Pivot value bytes.
            is_before: True = insert before pivot, False = after.
            values: List of value bytes to insert.
            ttl: TTL in milliseconds (relative). Applied to each value.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            True if successful.
        """
        cid = self._resolve_client_id(client_id)
        request = cache_pb2.AddToValRequest(
            key=self._build_key(key, hint, client_id),
            ttl=ttl,
            isBefore=is_before,
            pos=create_value(pos, 0, cid),
            value=[create_value(v, 0, cid) for v in (values or [])],
        )
        return self._call_add_chunked("add_element_to_position_by_value", self.stub.addElementToPositionByValue, request, **kwargs)

    def add_element_to_position_before(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        pivot: bytes = b"",
        values: list[bytes] | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> bool:
        """Add elements before a pivot value.

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            pivot: Pivot value bytes.
            values: List of value bytes to insert.
            client_id: Client ID.

        Returns:
            True if successful.
        """
        return self.add_element_to_position_by_value(
            key, hint, pos=pivot, is_before=True, values=values, client_id=client_id, **kwargs
        )

    def add_element_to_position_after(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        pivot: bytes = b"",
        values: list[bytes] | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> bool:
        """Add elements after a pivot value.

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            pivot: Pivot value bytes.
            values: List of value bytes to insert.
            client_id: Client ID.

        Returns:
            True if successful.
        """
        return self.add_element_to_position_by_value(
            key, hint, pos=pivot, is_before=False, values=values, client_id=client_id, **kwargs
        )

    def add_element_unordered(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        values: list[bytes] | None = None,
        keys: list[bytes] | None = None,
        pos: int = -1,
        ttl: int = 0,
        client_id: int | None = None,
        **kwargs,
    ) -> int:
        """Add elements to unordered containers (VECTOR, LIST, SET).

        For VECTOR/LIST: pos is the absolute index to insert before.
        pos=-1 means append to end.

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            values: List of value bytes.
            keys: List of key bytes (for MAP).
            pos: Insert position index (-1 = end).
            ttl: TTL in milliseconds (relative). Applied to each value.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            Number of elements added.
        """
        cid = self._resolve_client_id(client_id)
        if keys is not None and len(keys) != len(values or ()):
            raise ValueError("map keys and values must have equal lengths")
        proto_keys = [create_key(k, None, cid, compress=False) for k in (keys or [])]
        proto_values = [create_value(v, ttl, cid) for v in (values or [])]

        request = cache_pb2.AddToRequest(
            key=self._build_key(key, hint, client_id),
            key_unordered=proto_keys,
            value_unordered=proto_values,
            pos=pos if pos >= 0 else 0xFFFFFFFF,
        )
        return self._call_add_chunked("add_element_unordered", self.stub.addElement, request, **kwargs)

    def add_element_ordered(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        values: list[OrderedPayload] | None = None,
        keys: list[OrderedPayload] | None = None,
        pos: int = 0,
        ttl: int = 0,
        client_id: int | None = None,
        **kwargs,
    ) -> int:
        """Add elements to ordered containers (ORDERED_SET, ORDERED_MAP).

        For ORDERED_SET: values contain (value, order), pos is ignored.
        For ORDERED_MAP: keys contain (key, order), values are bytes.

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            values: List of OrderedPayload (for ORDERED_SET).
            keys: List of OrderedPayload (for ORDERED_MAP).
            pos: Position (ignored for ORDERED_SET).
            ttl: TTL in milliseconds (relative). Applied to each value.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            Number of elements added.
        """
        cid = self._resolve_client_id(client_id)
        request = cache_pb2.AddToRequest(key=self._build_key(key, hint, client_id), pos=pos)
        if keys is not None:
            request.type = cache_pb2.ORDERED_MAP
            if len(keys) != len(values or []):
                raise ValueError("ordered-map keys and values must have equal lengths")
            request.key_ordered.extend(create_ordered_key(k, client_id=cid) for k in keys)
            request.value_unordered.extend(create_value(v, ttl, cid, compress=True) for v in (values or []))
        else:
            request.value_ordered.extend(create_ordered_value(v, ttl=ttl, client_id=cid) for v in (values or []))
        return self._call_add_chunked("add_element_ordered", self.stub.addElement, request, **kwargs)

    # ------------------------------------------------------------------
    # Atomic Primitives
    # ------------------------------------------------------------------

    def atomic_load(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> int:
        """Atomic load of counter value.

        Args:
            key: Atomic key.
            hint: Optional KeyHintData for routing.
            client_id: Client ID.

        Returns:
            Current int64 value.

        Raises:
            KeyNotFoundError: If key does not exist.
        """
        request = self._build_get_req(key, hint, client_id)
        response = self._call("atomic_load", self.stub.atomicLoad, request, **kwargs)
        return response.val

    def atomic_load_and_delete(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> int:
        """Atomic load and delete counter.

        Args:
            key: Atomic key.
            hint: Optional KeyHintData for routing.
            client_id: Client ID.

        Returns:
            Current int64 value before deletion.
        """
        request = self._build_get_req(key, hint, client_id)
        response = self._call("atomic_load_and_delete", self.stub.atomicLoadAndDelete, request, **kwargs)
        return response.val

    def atomic_create(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        value: int = 0,
        ttl: int | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> KeyHintData:
        """Create an atomic counter.

        Args:
            key: Atomic key.
            hint: Optional KeyHintData for routing.
            value: Initial int64 value (0 if not specified).
            ttl: TTL in milliseconds (relative). Converted to absolute time.
            client_id: Client ID. If != 0, lock_info is created with type=NO_LOCK.

        Returns:
            KeyHintData for subsequent operations.
        """
        ttl = self._default_ttl if ttl is None else ttl
        cid = self._resolve_client_id(client_id)
        val = cache_pb2.AtomicValue(val=value)
        builder = cache_pb2.AtomicCreate(
            key=self._build_key(key, hint, cid),
            val=val,
        )
        if ttl > 0:
            builder.ttl = absolute_ttl(ttl)
        if cid != 0:
            builder.lock_info.type = cache_pb2.LockType.NO_LOCK
            builder.lock_info.lockedBy = cid
        response = self._call("atomic_create", self.stub.atomicCreate, builder, **kwargs)
        return decode_hint(response) or KeyHintData.unspecified()

    def atomic_store(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        value: int = 0,
        ttl: int | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> KeyHintData:
        """Atomic store (write) to counter.

        Args:
            key: Atomic key.
            hint: Optional KeyHintData for routing.
            value: New int64 value.
            ttl: TTL in milliseconds (relative). Converted to absolute time.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            KeyHintData for subsequent operations.
        """
        ttl = self._default_ttl if ttl is None else ttl
        cid = self._resolve_client_id(client_id)
        val = cache_pb2.AtomicValue(val=value)
        builder = cache_pb2.AtomicCreate(
            key=self._build_key(key, hint, cid),
            val=val,
        )
        if ttl > 0:
            builder.ttl = absolute_ttl(ttl)
        if cid != 0:
            builder.lock_info.type = cache_pb2.LockType.NO_LOCK
            builder.lock_info.lockedBy = cid
        response = self._call("atomic_store", self.stub.atomicStore, builder, **kwargs)
        return decode_hint(response) or KeyHintData.unspecified()

    def atomic_exchange(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        value: int = 0,
        ttl: int | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> int:
        """Atomic exchange (swap) вЂ” writes new value, returns old.

        Args:
            key: Atomic key.
            hint: Optional KeyHintData for routing.
            value: New int64 value.
            ttl: TTL in milliseconds (relative). Converted to absolute time.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            Old int64 value before exchange.
        """
        ttl = self._default_ttl if ttl is None else ttl
        cid = self._resolve_client_id(client_id)
        val = cache_pb2.AtomicValue(val=value)
        builder = cache_pb2.AtomicCreate(
            key=self._build_key(key, hint, cid),
            val=val,
        )
        if ttl > 0:
            builder.ttl = absolute_ttl(ttl)
        if cid != 0:
            builder.lock_info.type = cache_pb2.LockType.NO_LOCK
            builder.lock_info.lockedBy = cid
        response = self._call("atomic_exchange", self.stub.atomicExchange, builder, **kwargs)
        return response.val

    def atomic_add(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        delta: int = 0,
        ttl: int | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> int:
        """Atomic add (fetch-and-add).

        Args:
            key: Atomic key.
            hint: Optional KeyHintData for routing.
            delta: int64 value to add.
            ttl: TTL in milliseconds (relative). Converted to absolute time.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            New int64 value after addition.
        """
        ttl = self._default_ttl if ttl is None else ttl
        cid = self._resolve_client_id(client_id)
        val = cache_pb2.AtomicValue(val=delta)
        builder = cache_pb2.AtomicCreate(
            key=self._build_key(key, hint, cid),
            val=val,
        )
        if ttl > 0:
            builder.ttl = absolute_ttl(ttl)
        if cid != 0:
            builder.lock_info.type = cache_pb2.LockType.NO_LOCK
            builder.lock_info.lockedBy = cid
        response = self._call("atomic_add", self.stub.atomicAdd, builder, **kwargs)
        return response.val

    def atomic_sub(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        delta: int = 0,
        ttl: int | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> int:
        """Atomic subtract (fetch-and-sub).

        Args:
            key: Atomic key.
            hint: Optional KeyHintData for routing.
            delta: int64 value to subtract.
            ttl: TTL in milliseconds (relative). Converted to absolute time.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            New int64 value after subtraction.
        """
        ttl = self._default_ttl if ttl is None else ttl
        cid = self._resolve_client_id(client_id)
        val = cache_pb2.AtomicValue(val=delta)
        builder = cache_pb2.AtomicCreate(
            key=self._build_key(key, hint, cid),
            val=val,
        )
        if ttl > 0:
            builder.ttl = absolute_ttl(ttl)
        if cid != 0:
            builder.lock_info.type = cache_pb2.LockType.NO_LOCK
            builder.lock_info.lockedBy = cid
        response = self._call("atomic_sub", self.stub.atomicSub, builder, **kwargs)
        return response.val

    def atomic_or(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        mask: int = 0,
        ttl: int | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> int:
        """Atomic bitwise OR.

        Args:
            key: Atomic key.
            hint: Optional KeyHintData for routing.
            mask: int64 mask for OR operation.
            ttl: TTL in milliseconds (relative). Converted to absolute time.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            New int64 value after OR.
        """
        ttl = self._default_ttl if ttl is None else ttl
        cid = self._resolve_client_id(client_id)
        val = cache_pb2.AtomicValue(val=mask)
        builder = cache_pb2.AtomicCreate(
            key=self._build_key(key, hint, cid),
            val=val,
        )
        if ttl > 0:
            builder.ttl = absolute_ttl(ttl)
        if cid != 0:
            builder.lock_info.type = cache_pb2.LockType.NO_LOCK
            builder.lock_info.lockedBy = cid
        response = self._call("atomic_or", self.stub.atomicOr, builder, **kwargs)
        return response.val

    def atomic_and(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        mask: int = 0,
        ttl: int | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> int:
        """Atomic bitwise AND.

        Args:
            key: Atomic key.
            hint: Optional KeyHintData for routing.
            mask: int64 mask for AND operation.
            ttl: TTL in milliseconds (relative). Converted to absolute time.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            New int64 value after AND.
        """
        ttl = self._default_ttl if ttl is None else ttl
        cid = self._resolve_client_id(client_id)
        val = cache_pb2.AtomicValue(val=mask)
        builder = cache_pb2.AtomicCreate(
            key=self._build_key(key, hint, cid),
            val=val,
        )
        if ttl > 0:
            builder.ttl = absolute_ttl(ttl)
        if cid != 0:
            builder.lock_info.type = cache_pb2.LockType.NO_LOCK
            builder.lock_info.lockedBy = cid
        response = self._call("atomic_and", self.stub.atomicAnd, builder, **kwargs)
        return response.val

    def atomic_xor(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        mask: int = 0,
        ttl: int | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> int:
        """Atomic bitwise XOR.

        Args:
            key: Atomic key.
            hint: Optional KeyHintData for routing.
            mask: int64 mask for XOR operation.
            ttl: TTL in milliseconds (relative). Converted to absolute time.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            New int64 value after XOR.
        """
        ttl = self._default_ttl if ttl is None else ttl
        cid = self._resolve_client_id(client_id)
        val = cache_pb2.AtomicValue(val=mask)
        builder = cache_pb2.AtomicCreate(
            key=self._build_key(key, hint, cid),
            val=val,
        )
        if ttl > 0:
            builder.ttl = absolute_ttl(ttl)
        if cid != 0:
            builder.lock_info.type = cache_pb2.LockType.NO_LOCK
            builder.lock_info.lockedBy = cid
        response = self._call("atomic_xor", self.stub.atomicXor, builder, **kwargs)
        return response.val

    def atomic_compare_and_set(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        expected_value: int = 0,
        new_value: int = 0,
        ttl: int | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> CasResult:
        """Atomic Compare-And-Swap (CAS).

        If current value == expected_value, set to new_value.
        Returns CasResult with success flag and actual value.

        Args:
            key: Atomic key.
            hint: Optional KeyHintData for routing.
            expected_value: Expected current value.
            new_value: New value to set if CAS succeeds.
            ttl: TTL in milliseconds (relative). Converted to absolute time.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            CasResult(success=bool, expected_value=int).
        """
        ttl = self._default_ttl if ttl is None else ttl
        cid = self._resolve_client_id(client_id)
        builder = cache_pb2.AtomicCas(
            key=self._build_key(key, hint, cid),
            expected=cache_pb2.AtomicValue(val=expected_value),
            toSet=cache_pb2.AtomicValue(val=new_value),
        )
        if ttl > 0:
            builder.ttl = absolute_ttl(ttl)
        if cid != 0:
            builder.lock_info.type = cache_pb2.LockType.NO_LOCK
            builder.lock_info.lockedBy = cid
        response = self._call("atomic_compare_and_set", self.stub.atomicCompareAndSet, builder, **kwargs)
        actual_val = response.expected.val if response.HasField("expected") else None
        return CasResult(success=response.result, expected_value=actual_val, hint=decode_hint(response))

    def add_element_to_position(self, key, hint=None, values=None, pos=0, client_id=None, **kwargs):
        return self.add_element_unordered(key, hint, values, pos=pos, client_id=client_id, **kwargs)

    def add_element_with_weight(self, key, hint=None, values=None, client_id=None, **kwargs):
        return self.add_element_ordered(key, hint, values, client_id=client_id, **kwargs)

    def add_element_ordered_set(self, key, hint=None, values=None, client_id=None, **kwargs):
        return self.add_element_ordered(key, hint, values, client_id=client_id, _boolean=True, **kwargs)

    def add_element_ordered_map(self, key, hint=None, keys=None, values=None, client_id=None, **kwargs):
        if len(keys or ()) != len(values or ()):
            raise ValueError("map keys and values must have equal lengths")
        request = cache_pb2.AddToRequest(key=self._build_key(key, hint, client_id), type=cache_pb2.ORDERED_MAP,
            key_ordered=[create_ordered_key(k, client_id=self._resolve_client_id(client_id)) for k in (keys or ())],
            value_unordered=[create_value(v, client_id=self._resolve_client_id(client_id), compress=True) for v in (values or ())])
        return self._call_add_chunked("add_element_ordered_map", self.stub.addElement, request, **kwargs)

    def stream_element_in_range_unordered(self, key, hint=None, type=cache_pb2.LIST, pos=0, end=0, client_id=None, **kwargs):
        return self.get_element_in_range(key, hint, pos=pos, end=end, type=type, client_id=client_id, **kwargs)

    def stream_element_in_range_ordered_set(self, key, hint=None, pos=0, end=0, reverse=False, client_id=None, **kwargs):
        return self.get_element_in_range(key, hint, pos=pos, end=end, type=cache_pb2.ORDERED_SET, reverse=reverse, client_id=client_id, **kwargs)

    def stream_element_in_range_ordered(self, key, hint=None, pos=0, end=0, client_id=None, **kwargs):
        return self.stream_element_in_range_ordered_set(key, hint, pos, end, client_id=client_id, **kwargs)

    update_key_value = update_value
    get_container_value = get_value_in_container
    get_and_remove_container_value = get_and_delete_value_in_container
    update_container_value = update_value_in_container
    remove_container_key = remove_in_container
    remove_from_container = remove_from_container_by_key_value

    def stream_list(self, key, hint=None, client_id=None, **kwargs):
        result = self.get_container(key, hint, client_id, **kwargs)
        if not result:
            return []
        if not isinstance(result, list) or not all(type(item) is Payload for item in result):
            raise ValueError("unexpected collection representation")
        return result

    def stream_vector(self, key, hint=None, client_id=None, **kwargs):
        result = self.get_container(key, hint, client_id, **kwargs)
        if not result:
            return []
        if not isinstance(result, list) or not all(type(item) is Payload for item in result):
            raise ValueError("unexpected collection representation")
        return result

    def stream_queue(self, key, hint=None, client_id=None, **kwargs):
        result = self.get_container(key, hint, client_id, **kwargs)
        if not result:
            return []
        if not isinstance(result, list) or not all(type(item) is Payload for item in result):
            raise ValueError("unexpected collection representation")
        return result

    def stream_set(self, key, hint=None, client_id=None, **kwargs):
        result = self.get_container(key, hint, client_id, **kwargs)
        if not result:
            return []
        if not isinstance(result, list) or not all(type(item) is Payload for item in result):
            raise ValueError("unexpected collection representation")
        return result

    def stream_map(self, key, hint=None, client_id=None, **kwargs):
        result = self.get_container(key, hint, client_id, **kwargs)
        if not result:
            return {}
        if not isinstance(result, dict) or not all(type(item) is Payload for item in result):
            raise ValueError("unexpected collection representation")
        return result

    def stream_ordered_set(self, key, hint=None, client_id=None, **kwargs):
        result = self.get_container(key, hint, client_id, **kwargs)
        if not result:
            return []
        if not isinstance(result, list) or not all(type(item) is OrderedPayload for item in result):
            raise ValueError("unexpected collection representation")
        return result

    def stream_ordered_map(self, key, hint=None, client_id=None, **kwargs):
        result = self.get_container(key, hint, client_id, **kwargs)
        if not result:
            return {}
        if not isinstance(result, dict) or not all(type(item) is OrderedPayload for item in result):
            raise ValueError("unexpected collection representation")
        return result

    def add_element_to_tail_count(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        values: list[bytes] | None = None,
        ttl: int = 0,
        client_id: int | None = None,
        **kwargs,
    ) -> int:
        """Add elements to tail (end) of container.

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            values: List of value bytes to add.
            ttl: TTL in milliseconds (relative). Applied to each value.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            Sum of acknowledged server counts.
        """
        cid = self._resolve_client_id(client_id)
        request = cache_pb2.AddToRequest(
            key=self._build_key(key, hint, client_id),
            value_unordered=[create_value(v, ttl, cid) for v in (values or [])],
        )
        return self._call_add_chunked("add_element_to_tail_count", self.stub.addElementToTail, request, **kwargs)


    def add_element_to_head_count(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        values: list[bytes] | None = None,
        ttl: int = 0,
        client_id: int | None = None,
        **kwargs,
    ) -> int:
        """Add elements to head (beginning) of container.

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            values: List of value bytes to add.
            ttl: TTL in milliseconds (relative). Applied to each value.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            Sum of acknowledged server counts.
        """
        cid = self._resolve_client_id(client_id)
        request = cache_pb2.AddToRequest(
            key=self._build_key(key, hint, client_id),
            value_unordered=[create_value(v, ttl, cid) for v in (values or [])],
        )
        return self._call_add_chunked("add_element_to_head_count", self.stub.addElementToHead, request, **kwargs)


    def add_element_to_position_by_value_count(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        pos: bytes = b"",
        is_before: bool = True,
        values: list[bytes] | None = None,
        ttl: int = 0,
        client_id: int | None = None,
        **kwargs,
    ) -> int:
        """Insert elements before or after a pivot value.

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            pos: Pivot value bytes.
            is_before: True = insert before pivot, False = after.
            values: List of value bytes to insert.
            ttl: TTL in milliseconds (relative). Applied to each value.
            client_id: Client ID. If != 0, lock_info is created.

        Returns:
            Sum of acknowledged server counts.
        """
        cid = self._resolve_client_id(client_id)
        request = cache_pb2.AddToValRequest(
            key=self._build_key(key, hint, client_id),
            ttl=ttl,
            isBefore=is_before,
            pos=create_value(pos, 0, cid),
            value=[create_value(v, 0, cid) for v in (values or [])],
        )
        return self._call_add_chunked("add_element_to_position_by_value_count", self.stub.addElementToPositionByValue, request, **kwargs)


    def add_element_to_position_before_count(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        pivot: bytes = b"",
        values: list[bytes] | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> int:
        """Add elements before a pivot value.

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            pivot: Pivot value bytes.
            values: List of value bytes to insert.
            client_id: Client ID.

        Returns:
            Sum of acknowledged server counts.
        """
        return self.add_element_to_position_by_value_count(
            key, hint, pos=pivot, is_before=True, values=values, client_id=client_id, **kwargs
        )


    def add_element_to_position_after_count(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        pivot: bytes = b"",
        values: list[bytes] | None = None,
        client_id: int | None = None,
        **kwargs,
    ) -> int:
        """Add elements after a pivot value.

        Args:
            key: Container key.
            hint: Optional KeyHintData for routing.
            pivot: Pivot value bytes.
            values: List of value bytes to insert.
            client_id: Client ID.

        Returns:
            Sum of acknowledged server counts.
        """
        return self.add_element_to_position_by_value_count(
            key, hint, pos=pivot, is_before=False, values=values, client_id=client_id, **kwargs
        )


    def get_element_with_weight(self, key, hint=None, pos=0, client_id=None, **kwargs):
        request = cache_pb2.KeyPositionRequest(key=self._build_key(key, hint, client_id), pos=pos)
        return _extract_value(self._call("get_element_with_weight", self.stub.getElementAtPosition, request, **kwargs))

    def get_and_remove_element_with_weight(self, key, hint=None, pos=0, client_id=None, **kwargs):
        request = cache_pb2.KeyPositionRequest(key=self._build_key(key, hint, client_id), pos=pos)
        return _extract_value(self._call("get_and_remove_element_with_weight", self.stub.getAndRemoveElementAtPosition, request, **kwargs))

    def stream_element_in_range_ordered_map(self, key, hint=None, pos=0, end=0, reverse=False, client_id=None, **kwargs):
        result = self.get_element_in_range(key, hint, pos, end, cache_pb2.ORDERED_MAP, reverse, client_id, **kwargs)
        if not result:
            return {}
        if not isinstance(result, dict) or not all(type(item) is OrderedPayload for item in result):
            raise ValueError("unexpected range representation")
        return result
