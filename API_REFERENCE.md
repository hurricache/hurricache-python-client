# HurriCache Python API reference

This reference describes package `hurricache` version `0.1.0`. For installation, architecture, and complete examples, see the [README](README.md).

All operation names are snake_case. Unless stated otherwise, the async classes expose the same call shape and return value as their sync equivalents, with `await` added.

## Clients

```python
HurriCacheClient(
    host: str = "localhost",
    port: int = 50000,
    default_client_id: int = 0,
    default_timeout: float = 1.0,
    credentials: grpc.ChannelCredentials | None = None,
    compression: grpc.Compression | None = None,
)

AsyncHurriCacheClient(...)  # same arguments
```

`default_timeout` and an operation's `timeout=` override are seconds. `credentials` creates a secure channel; otherwise the channel is plaintext. Sync clients support `with`; async clients support `async with`.

```python
HurriCacheSmartClient(
    coordinators: str | Sequence[str] = "localhost:50051",
    coordinator_port: int | None = None,
    *,
    default_client_id: int = 0,
    default_timeout: float = 1.0,
    readiness_timeout: float = 60.0,
    refresh_interval: float = 30.0,
    mode: Mode = Mode.MASTER_THEN_BACKUP,
    credentials: grpc.ChannelCredentials | None = None,
)

AsyncHurriCacheSmartClient(...)  # same arguments
```

Smart-client lifecycle and configuration methods:

- `wait_until_ready(timeout: float | None = None) -> None` (await on async)
- `set_mode(mode: Mode) -> Self`: set a caller-local override.
- `clear_mode() -> Self`: remove that override.
- `mode(mode: Mode)`: scoped context manager.
- `close() -> None` / `await close()`.

## Models and enums

- `Payload(value: bytes)`: immutable, hashable decoded payload.
- `OrderedPayload(value: bytes, order: int = 0)`: payload with an unsigned 64-bit score/order.
- `KeyHintData(week_hash: int | None = None, strong_hash: int | None = None)`: nullable server hashes. `is_specified` and boolean conversion indicate whether either field is present.
- `CasResult(success: bool, expected_value: int | None, hint: KeyHintData | None)`: complete CAS response.
- `LockType`: `NO_LOCK`, `WRITE_LOCK`, `READ_LOCK`, `GLOBAL`.
- `LockStatus`: `OK`, `CANT_LOCK`, `CANT_UNLOCK`, `GENERIC_ERROR`.
- `ContainerType`: `UNDEFINED`, `VECTOR`, `LIST`, `QUEUE`, `SET`, `MAP`, `ORDERED_MAP`, `ORDERED_SET`.
- `Mode`: `MASTER`, `BACKUP`, `MASTER_THEN_BACKUP`, `LB_SMART`. `MASTER_THAN_BACKUP` is a Java-source-compatible alias.

Keys accept `bytes | str`; strings use UTF-8. TTL arguments are relative milliseconds. Lock duration and RPC deadlines are seconds.

## Values and common operations

```python
create_key_value(key, hint=None, value=b"", ttl=0, client_id=None, **rpc_options) -> KeyHintData
get_value(key, hint=None, client_id=None, **rpc_options) -> bytes
get_and_delete_value(key, hint=None, client_id=None, **rpc_options) -> bytes
exist_key(key, hint=None, client_id=None, **rpc_options) -> bool
update_value(key, hint=None, value=b"", ttl=0, client_id=None, **rpc_options) -> bytes
remove(key, hint=None, client_id=None, **rpc_options) -> bool
get_size(key, hint=None, client_id=None, **rpc_options) -> int
```

`update_value` returns the previous value. `**rpc_options` are passed to gRPC and commonly include `timeout`, `metadata`, `credentials`, `wait_for_ready`, and `compression` where supported.

## TTL and locks

```python
set_ttl(key, hint=None, ttl=0, client_id=None, **rpc_options) -> bool
get_ttl(key, hint=None, client_id=None, **rpc_options) -> int
lock_object(key, hint=None, lock_type=LockType.NO_LOCK,
            client_id=None, lock_duration=0.0, **rpc_options) -> LockStatus
unlock_object(key, hint=None, client_id=None, **rpc_options) -> LockStatus
```

`get_ttl` returns remaining milliseconds or `-1` when the server returns no TTL. `lock_duration` is seconds and is converted to wire milliseconds.

## Container creation and streaming

```python
create_vector(key, hint=None, values=None, ttl=0, client_id=None, **rpc_options) -> KeyHintData
create_list(key, hint=None, values=None, ttl=0, client_id=None, **rpc_options) -> KeyHintData
create_queue(key, hint=None, values=None, ttl=0, client_id=None, **rpc_options) -> KeyHintData
create_set(key, hint=None, values=None, ttl=0, client_id=None, **rpc_options) -> KeyHintData
create_map(key, hint=None, keys=None, values=None, ttl=0, client_id=None, **rpc_options) -> KeyHintData
create_ordered_set(key, hint=None, values: list[OrderedPayload] | None = None,
                   ttl=0, client_id=None, **rpc_options) -> KeyHintData
create_ordered_map(key, hint=None, keys: list[OrderedPayload] | None = None,
                   values=None, ttl=0, client_id=None, **rpc_options) -> KeyHintData
get_container(key, hint=None, client_id=None, **rpc_options) -> list | dict
```

Large repeated inputs are chunked around 3.5 MiB. Map key/value counts must match. `get_container` consumes the server stream and returns decoded `Payload`/`OrderedPayload` lists or maps.

## Container reads and mutation

```python
get_head(key, hint=None, client_id=None, **rpc_options) -> bytes
get_front(...) -> bytes                       # alias of get_head
get_tail(key, hint=None, client_id=None, **rpc_options) -> bytes
get_and_remove_front(...) -> bytes
get_and_remove_tail(...) -> bytes
get_element_at_position(key, hint=None, pos=0, type=ContainerType.UNDEFINED,
                        client_id=None, **rpc_options) -> bytes
get_and_remove_element_at_position(...) -> bytes
get_element_in_range(key, hint=None, pos=0, end=0,
                     type=ContainerType.UNDEFINED, reverse=False,
                     client_id=None, **rpc_options) -> list | dict
remove_head(...) -> bool
remove_tail(...) -> bool
remove_element_at_position(...) -> bool
```

For list/vector, `pos`, `start`, and `end` are zero-based positions. For ordered structures, they are unsigned 64-bit weights. Range results are decoded from the full stream.

```python
get_value_in_container(key, element_key, hint=None, element_hint=None,
                       client_id=None, **rpc_options) -> bytes
exist_key_in_container(...) -> bool
contains_container_key(...) -> bool           # alias
get_and_delete_value_in_container(...) -> bytes
update_value_in_container(key, element_key, value=b"", hint=None,
                          element_hint=None, ttl=0, client_id=None,
                          **rpc_options) -> bytes
remove_in_container(...) -> int
remove_from_container_by_key_value(key, hint=None, type=ContainerType.UNDEFINED,
                                   values=None, keys=None, client_id=None,
                                   **rpc_options) -> int
```

```python
add_element_to_tail(key, hint=None, values=None, ttl=0, client_id=None, **rpc_options) -> bool
add_element_to_head(...) -> bool
add_element(key, hint=None, values=None, keys=None, ttl=0,
            client_id=None, **rpc_options) -> int
add_element_hash_map(...) -> int               # alias
add_element_unordered(key, hint=None, values=None, keys=None, pos=0,
                      ttl=0, client_id=None, **rpc_options) -> int
add_element_ordered(key, hint=None, values=None, keys=None, pos=0,
                    ttl=0, client_id=None, **rpc_options) -> int
add_element_to_position_by_value(key, hint=None, pos=b"", is_before=True,
                                 values=None, ttl=0, client_id=None,
                                 **rpc_options) -> bool
add_element_to_position_before(key, hint=None, pivot=b"", values=None,
                               client_id=None, **rpc_options) -> bool
add_element_to_position_after(...) -> bool
```

## Atomics

```python
atomic_load(key, hint=None, client_id=None, **rpc_options) -> int
atomic_load_and_delete(key, hint=None, client_id=None, **rpc_options) -> int
atomic_create(key, hint=None, value=0, ttl=0, client_id=None, **rpc_options) -> KeyHintData
atomic_store(key, hint=None, value=0, ttl=0, client_id=None, **rpc_options) -> KeyHintData
atomic_exchange(key, hint=None, value=0, ttl=0, client_id=None, **rpc_options) -> int
atomic_add(key, hint=None, delta=0, ttl=0, client_id=None, **rpc_options) -> int
atomic_sub(key, hint=None, delta=0, ttl=0, client_id=None, **rpc_options) -> int
atomic_or(key, hint=None, mask=0, ttl=0, client_id=None, **rpc_options) -> int
atomic_and(key, hint=None, mask=0, ttl=0, client_id=None, **rpc_options) -> int
atomic_xor(key, hint=None, mask=0, ttl=0, client_id=None, **rpc_options) -> int
atomic_compare_and_set(key, hint=None, expected_value=0, new_value=0,
                       ttl=0, client_id=None, **rpc_options) -> CasResult
```

## Smart routing

The smart clients maintain `(NodeRole, shard) -> target` and `target -> client` mappings from coordinator updates. A present weak hash selects the shard using unsigned modulo; a missing hint advances the Java-compatible round-robin shard counter.

`MASTER` and `BACKUP` select that role, `MASTER_THEN_BACKUP` falls back on `UNAVAILABLE`, and `LB_SMART` randomizes the first eligible role. Mutations use master-then-backup by default. A caller-local mode override has precedence. `FAILED_PRECONDITION` is retried only when the trailer names a known `x-fastcache-route` target.

## Exceptions

All client exceptions inherit `HurriCacheError`:

- `KeyNotFoundError`
- `PermissionDeniedError`
- `InvalidArgumentError`
- `DeadlineExceededError`
- `UnavailableError`
- `FailedPreconditionError` (`route` contains a reroute trailer when supplied)
- `CancelledError`
- `HurriCacheRpcError` (`method`, `code`, and `details` preserve generic gRPC context)

Synchronous streaming failures and `grpc.aio` streaming failures use the same mapping. Native task cancellation remains cancellable through `grpc.aio`; callers should not suppress `asyncio.CancelledError`.
