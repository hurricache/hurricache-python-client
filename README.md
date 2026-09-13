# HurriCache Python client

HurriCache's Python 3.10+ client provides the same cache operations and wire behavior as the Java `jdk-16` client. It includes direct and coordinator-aware clients in synchronous and native `grpc.aio` forms. The package version remains `0.1.0`.

See [API_REFERENCE.md](API_REFERENCE.md) for method signatures and return types.

## Architecture and features

- `HurriCacheClient`: synchronous connection to one standalone/cache node.
- `AsyncHurriCacheClient`: native asynchronous connection using `grpc.aio`.
- `HurriCacheSmartClient`: synchronous coordinator discovery and shard routing.
- `AsyncHurriCacheSmartClient`: asynchronous coordinator discovery and shard routing.
- Raw values, TTL, locks, vector/list/queue/set, map, ordered set/map, positions and ranges, atomics, and compare-and-swap.
- GZIP-only payload compression with a configurable threshold and per-field policy.
- Roughly 3.5 MiB batching for initial container data and additions.
- gRPC deadlines, metadata, TLS credentials, cancellation, status mapping, and decoded streaming results.

Committed protobuf modules make installed wheels self-contained. Runtime dependencies and development tools are declared only in `pyproject.toml`; this project intentionally has no `requirements.txt`.

## Installation

From a checkout:

```console
python -m pip install .
```

For development and code generation:

```console
python -m pip install -e ".[dev]"
```

The compatible dependency ranges are `grpcio>=1.60,<2`, `protobuf>=4.25,<8`, and `tenacity>=8.2,<10`.

## Standalone server

Java is pinned to `833f2dd9a581a1984129b3f709f63afe3bfdc124`; the server is pinned by digest and listens on port `50000`:

```console
docker run --rm -p 50000:50000 alexaborisov/fastcache-standalone-noavx512@sha256:e79c96e7610c10ba99c7c11f939c78154ff3a4e6d24689ffdd15f35d2d7b7b1e
```

If the image is hosted in a private registry, prefix the image name with that registry. Cluster examples below require real cache-node and coordinator endpoints; the test suite uses deterministic in-process services and does not pretend to be a live cluster.

## Direct clients

```python
from hurricache import HurriCacheClient

with HurriCacheClient("127.0.0.1", 50000, default_client_id=7,
                      default_timeout=2.0) as cache:
    hint = cache.create_key_value("greeting", value=b"hello", ttl=30_000)
    assert cache.get_value("greeting", hint) == b"hello"
    cache.update_value("greeting", hint, b"hello again")
    cache.remove("greeting", hint)
```

The asynchronous API uses the same snake-case operation names:

```python
import asyncio
from hurricache import AsyncHurriCacheClient

async def main() -> None:
    async with AsyncHurriCacheClient("127.0.0.1", 50000) as cache:
        hint = await cache.create_key_value("greeting", value=b"hello")
        print(await cache.get_value("greeting", hint))

asyncio.run(main())
```

Keys may be `bytes` or `str`; strings are encoded as UTF-8. Values remain bytes (or `Payload`/`OrderedPayload` where a collection method calls for those models).

## Smart clients

Smart clients consume the coordinator's `provideGlobalRoutingInfo` stream, refresh every 30 seconds, and wait up to 60 seconds for initial readiness. Multiple coordinator addresses are rotated with bounded exponential backoff.

```python
from hurricache import HurriCacheSmartClient, Mode

with HurriCacheSmartClient(
    ["coordinator-a:50051", "coordinator-b:50051"],
    default_timeout=2.0,
) as cache:
    cache.wait_until_ready()
    value = cache.get_value("key")
    with cache.mode(Mode.BACKUP):
        backup_value = cache.get_value("key")
```

`set_mode()` is caller-local because it is backed by `contextvars`; concurrent threads/tasks do not overwrite each other's override. `clear_mode()` restores the configured mode, and `mode(...)` provides a scoped context manager. Modes are `MASTER`, `BACKUP`, `MASTER_THEN_BACKUP` (also available through Java's misspelled alias `MASTER_THAN_BACKUP`), and `LB_SMART`.

When a hint is present, the shard is `(week_hash & 0xffffffff) % max_shards`. Without a hint, shards are selected round-robin as in the Java client. Writes default to master-then-backup. `UNAVAILABLE` falls back to the next eligible node, and `FAILED_PRECONDITION` can reroute through the server's `x-fastcache-route` trailer.

For async smart clients, await initial discovery or let the first operation do so:

```python
from hurricache import AsyncHurriCacheSmartClient

async with AsyncHurriCacheSmartClient("coordinator:50051") as cache:
    await cache.wait_until_ready()
    value = await cache.get_value("key")
```

## Hints, client IDs, TTL, deadlines, and locks

`KeyHintData(week_hash=..., strong_hash=...)` stores nullable server hashes. Pass the returned hint to later calls to avoid recomputing lookup data and to let smart clients select the shard. The client does not reproduce Java's unused custom weak-hash implementation.

`default_client_id` is used whenever a call omits `client_id` or passes zero. It participates in lock ownership metadata. TTL arguments are relative milliseconds; the client writes absolute Unix milliseconds on the wire. `get_ttl()` returns remaining milliseconds and `-1` for a non-expiring value.

`default_timeout` and per-call `timeout=` values are RPC deadlines in seconds. `lock_duration` is also expressed in seconds and is encoded as absolute Unix seconds, matching the pinned Java implementation. `lock_object()` and `unlock_object()` return the full `LockStatus` enum, not a lossy boolean.

```python
from hurricache import LockStatus, LockType

status = cache.lock_object("job", lock_type=LockType.WRITE_LOCK,
                           client_id=7, lock_duration=5.0)
if status is LockStatus.OK:
    cache.unlock_object("job", client_id=7)
```

## Collections and maps

All initial/addition lists are split before exceeding the Java-compatible request budget. Map key/value lengths are validated before the first RPC.

```python
from hurricache import OrderedPayload

vector_hint = cache.create_vector("v", values=[b"a", b"b"])
cache.add_element_to_tail("v", vector_hint, [b"c"])
assert cache.get_element_at_position("v", vector_hint, pos=1) == b"b"

map_hint = cache.create_map("m", keys=[b"a", b"b"], values=[b"1", b"2"])
assert cache.get_value_in_container("m", b"a", hint=map_hint) == b"1"

ranked = [OrderedPayload(b"low", 10), OrderedPayload(b"high", 20)]
ordered_hint = cache.create_ordered_set("scores", values=ranked)
```

Supported families are:

| Family | Creation | Typical operations |
| --- | --- | --- |
| Vector/list | `create_vector`, `create_list` | head/tail, index, range, insert, update, pop |
| Queue | `create_queue` | head/peek, enqueue, dequeue |
| Set | `create_set` | add and remove by value |
| Map | `create_map` | get/exist/update/delete by element key |
| Ordered set | `create_ordered_set` | point/range by unsigned 64-bit order |
| Ordered map | `create_ordered_map` | keyed values ordered by key weight |

`get_container()` and `get_element_in_range()` consume the gRPC stream and return decoded models: lists of `Payload`, lists of `OrderedPayload`, `dict[Payload, Payload]`, or `dict[OrderedPayload, Payload]`. No protobuf messages leak from public streaming methods.

## Atomics and CAS

```python
hint = cache.atomic_create("counter", value=10)
old = cache.atomic_add("counter", hint, delta=5)
result = cache.atomic_compare_and_set("counter", hint,
                                      expected_value=15, new_value=20)
print(result.success, result.expected_value, result.hint)
```

Atomic methods include load, load-and-delete, create, store, exchange, add, subtract, bitwise OR/AND/XOR, and compare-and-set. `CasResult` preserves the success flag, returned actual/expected value, and optional returned hint.

## Compression and errors

Payload compression uses standard-library **GZIP only**. Compression applies strictly when
`length > threshold`; the default threshold is 1,024 bytes, zero compresses every
nonempty eligible payload, and negative thresholds are rejected. Transport
compression is independent of payload compression.

| Content | Payload compression |
| --- | --- |
| Top-level keys; scalar create/update values | Configured threshold |
| List/vector/queue/unordered-set elements; positional values and pivots | None |
| Ordinary-map creation/addition keys and values | None |
| Ordered-set creation/addition values | Configured threshold |
| Ordered-map creation/addition keys and values | Configured threshold |
| Single-entry lookup/removal keys; container-update values | None |
| Batch-removal keys | Configured threshold |
| Batch-removal values | None |

Unary results, ordered values, and streamed map keys/values validate encoded size,
GZIP framing/checksum, declared raw size, and decoded-size limits. Go uses
`Config.MaxDecodedBytes`; Python bounds decoded payloads to 64 MiB.
Legacy LZ4-compressed data is incompatible: there is no format sniffing or fallback.
Recreate or migrate that data using the previous client before switching formats.

gRPC status codes map consistently in unary and streaming calls:

- `NOT_FOUND` → `KeyNotFoundError`
- `PERMISSION_DENIED` → `PermissionDeniedError`
- `INVALID_ARGUMENT` → `InvalidArgumentError`
- `DEADLINE_EXCEEDED` → `DeadlineExceededError`
- `UNAVAILABLE` → `UnavailableError`
- `FAILED_PRECONDITION` → `FailedPreconditionError`
- `CANCELLED` → `CancelledError`
- other statuses → `HurriCacheRpcError`

## Lifecycle, generation, and tests

Always use `with`/`async with`, or call `close()`/`await close()`. Smart-client shutdown stops refresh threads/tasks and closes coordinator and routed-node channels.

Regenerate committed modules after changing either `.proto` file:

```console
python tools/generate_proto.py
python tools/generate_proto.py --check
```

Local verification:

```console
python -m ruff check .
python -m mypy hurricache
python -m pytest -q --ignore=tests/test_integration.py
python -m build
```

`tests/test_integration.py` targets a real standalone node at `127.0.0.1:50000`; start the digest-pinned server before running it; historical assumptions in this legacy suite may differ from the current contract. No live coordinator cluster is required for the deterministic smart-client tests.

## Count and weighted conveniences

`add_element_to_head_count`, `add_element_to_tail_count`,
`add_element_to_position_before_count`, `add_element_to_position_after_count`, and
`add_element_to_position_by_value_count` preserve the corresponding positional
arguments and return aggregated server counts. Empty input returns zero. Existing
boolean APIs retain empty success and stop at a zero-count chunk, exposing prior
progress through `PartialOperationError`. Existing count-returning generic APIs
and the `add_element_ordered_set` boolean adaptation remain available.

`get_element_with_weight` and `get_and_remove_element_with_weight` return bytes
and accept nonnegative uint64 weights. Both use configured smart routing.
`stream_element_in_range_ordered_map` returns an ordered-key dictionary and accepts
`reverse=True`. All operations are available on sync, native async, and both smart
clients. Defaults include `default_compression_threshold`; one monotonic deadline
and one absolute creation TTL span preprocessing and all continuation requests.

Regenerate the overload inventory with `python -m tools.parity ../hurricache-java-client`.
See [API_PARITY.md](API_PARITY.md) and [FINDINGS.md](FINDINGS.md) for the pinned
contract, historical results, current validation, and known Java/server limitations.
