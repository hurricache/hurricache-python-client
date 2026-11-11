# HurriCache Python client

Version **0.2.0** targets Java commit `0a2b1d851405744e9390bcf5b5e8592177b121e4`, covering all **742 interface declarations** with synchronous, native `grpc.aio`, and coordinator-aware clients.

This release changes the public API and protobuf protocol. Upgrade the server and clients together. There is no automatic fallback to the 0.1 protocol. See [API_REFERENCE.md](API_REFERENCE.md) for signatures, [API_PARITY.md](API_PARITY.md) for the declaration inventory, and [FINDINGS.md](FINDINGS.md) for validation evidence and acceptance limits.

```console
python -m pip install .
python -m pip install ".[lz4]"
```

Python 3.10+ is supported. Committed protobuf modules make wheels self-contained. Development tools are available with `python -m pip install -e ".[dev,lz4]"`.

```python
from hurricache import HurriCacheClient, Variant

with HurriCacheClient("127.0.0.1", 50000, default_timeout=2) as cache:
    hint = cache.create("counter", value=Variant.of_long(10), ttl=30_000)
    assert cache.get("counter", hint).require_long() == 10
    previous = cache.exchange("counter", hint, value=1.5)
    assert isinstance(previous, (int, float))
    cache.remove("counter", hint)
```

`Variant.none()`, `Variant.of_bytes(b"")`, `Variant.of_long(0)`, and `Variant.of_double(0.0)` are distinct. Factories preserve numeric type. Integers must fit signed int64; bools are rejected. BYTES values are copied. Checked accessors raise `TypeError` on a different type. NaNs compare equal and signed zeros compare differently, matching Java's equality. `OrderedVariant` adds a nullable unsigned order. `Payload` and `OrderedPayload` remain byte-key models for maps.

```python
from hurricache import HurriCacheClient, OrderedVariant, Payload, Variant

with HurriCacheClient() as cache:
    hint = cache.create_list("mixed", values=[Variant.none(), b"", 0, 1.25])
    assert cache.get_list("mixed", hint)[2].require_long() == 0
    assert cache.push_tail("mixed", hint, values=[2]) == 1
    cache.add_in_container("mixed", hint, selector=2, delta=3)
    removed = cache.get_and_delete_list_elements_in_range("mixed", hint, pos=1, end=3)
    assert all(isinstance(item, Variant) for item in removed)
    cache.create_map("map", keys={Payload(b"entry"): Variant.of_double(2.5)})
    cache.exchange_in_container("map", selector=b"entry", value=3.5)
    assert cache.get_map("map")[Payload(b"entry")].require_double() == 3.5
    cache.create_ordered_set("ordered", values=[OrderedVariant.of_int(7, 10)])
    for key in ("mixed", "map", "ordered"):
        cache.remove(key)
```

Order updates for ordered maps and byte-valued ordered sets address the exact member and current order:

```python
from hurricache import HurriCacheClient, OrderedPayload, OrderedVariant

with HurriCacheClient() as cache:
    hint = cache.create_ordered_set("ranked", values=[OrderedVariant.of_bytes(b"member", 10)])
    assert cache.increment_order("ranked", hint, OrderedPayload(b"member", 10), delta=5)
    assert cache.update_order("ranked", hint, OrderedPayload(b"member", 15), new_order=0)
    # Java's update overload is also available:
    assert cache.update("ranked", hint, OrderedPayload(b"member", 0), 20)
    cache.remove("ranked", hint)
```

Both order operations return `bool` and preserve values. Missing current order is rejected before an RPC.
The new `UpdateContainerRequest` protocol uses key/value oneofs and moves the plain value field to tag 4.
Server and client must be upgraded together, including from the previous 0.2 draft targeting `8fac8f…`.

All operations have identical signatures/results on `AsyncHurriCacheClient` with `await`; use `async with` for its lifetime. Both transports retain single-operation deadlines and native async cancellation.

```python
from hurricache import HurriCacheSmartClient, Mode

with HurriCacheSmartClient(["coordinator:50051"], mode=Mode.MASTER_THEN_BACKUP) as cache:
    cache.wait_until_ready(10)
    hint = cache.create("routed", value=b"hello")
    assert cache.get("routed", hint).require_bytes() == b"hello"
    cache.remove("routed", hint)
```

`AsyncHurriCacheSmartClient` provides the async equivalent. Smart clients consume current coordinator `Endpoint` messages, bracket IPv6 targets, and follow pinned Java routing. `set_mode`, `clear_mode`/`set_mode(None)`, and `with cache.mode(Mode.BACKUP):` provide caller-local overrides.

Keys accept bytes or UTF-8 strings. Hints use `weak_hash`, `strong_hash`, and `was_relayed`; `week_hash` remains an alias. Equality and hashing use only the hashes. `client_id=None` selects the configured ID. Explicit zero also selects it by default; set `zero_client_id_uses_default=False` to send zero unchanged. Current Java field presence carries ownership in keys and lock requests.

TTL arguments use milliseconds; timeouts and lock durations use seconds. `ttl=None` uses the configured default and `ttl=0` omits creation expiration. `set_ttl(..., ttl=-1)` removes expiration. Locks encode absolute Unix seconds, following Java's implementation.

GZIP is the default payload codec. Optional `payload_compression="lz4"` uses raw blocks. Readers must select the writer's codec: the protocol has no codec identifier. Byte values and eligible keys compress only above the threshold; numeric values bypass compression. `compression=grpc.Compression.Gzip` independently controls transport compression.

Container creation and additions preflight serialized requests, preserve pairs/hints, and use one creation expiration. Head chunks are sent in input order because the server reverses each request; the result matches one request across chunk boundaries. Pivot insertions advance the anchor after each chunk, with space reserved for that full anchor during preflight. Mutation counts aggregate server counts. Streams return Python lists/dicts. Failures after progress raise `PartialOperationError` and are never replayed by smart clients. Inbound messages and individual decoded payloads are limited to 64 MiB.

## Migration from 0.1

| Previous API | 0.2 API / behavior |
| --- | --- |
| `create_key_value`, `get_value`, `update_value`, `get_and_delete_value` | `create`, `get`, `replace`, `pop`; typed reads/replacements |
| `stream_list/vector/set/map/ordered_set/ordered_map` | `get_list/vector/set/map/ordered_set/ordered_map`; variants and byte-key map models |
| `get_head/front/tail`, `get_and_remove_front/tail` | `peek_head/tail`, `pop_head/tail`; typed results |
| `remove_head/tail`, boolean mutation conveniences | `delete_head/tail`, `push_head/tail`, `insert_at/before/after`; integer counts |
| `get_element_at_position`, `get_and_remove_element_at_position`, `remove_element_at_position` | `get_at`, `pop_at`, `remove_at`; changed signatures/counts |
| `get_value_in_container`, `get_and_delete_value_in_container`, `update_value_in_container` | `get_in_container`, `pop_in_container`, `update`; hint second, then entry key |
| `add_element_hash_map`, `add_element_ordered_map` | `put_all`, `add_element_ordered_map`; typed values |
| `remove_from_container_by_key_value` | `remove_from_container_by_key_and_value`; keys before values, no type argument |
| `atomic_exchange/add/sub/and/or/xor/compare_and_set` | `exchange/add/sub/and_/or_/xor/compare_and_set`; native numbers and nested observed CAS |
| `atomic_create/store/load/load_and_delete` | Removed. Use typed `create/replace/get/pop` |
| `week_hash` | `weak_hash`; constructor/property alias retained |
| Earlier 0.2 `update` protocol (value at tag 3) | Current key/value oneofs; value at tag 4, `update_order` and `increment_order` |

Only aliases listed in [API_REFERENCE.md](API_REFERENCE.md) remain. They emit `DeprecationWarning` and use current typed/count results. Incompatible positional methods and boolean insertion conveniences were removed. There is no legacy return mode.

## Verification

```console
python -m ruff check .
python -m mypy
python -m pytest -q
python -m tools.generate_proto --check
python -m tools.parity ../hurricache-java-client --check
python -m tools.reference --check
python -m build
python -m tools.verify_wheel
python -m tools.java_interop ../hurricache-java-client --java-home "C:/Program Files/Java/jdk-17.0.3.1"
```

Live tests require `HURRICACHE_TEST_TARGET=host:port`. Pull `alexaborisov/fastcache-standalone-noavx512:latest` at live-validation start, record its digest, and use that immutable image throughout. Successful startup is required before live tests. Offline fixtures cannot complete standalone acceptance. Live cluster, real-cluster failover, and replication acceptance remain deferred. Replication stubs are provided without an application-level API. Active `Jedis`/`JedisPool` wrappers preserve shared-client lifecycle; commented Java Redis commands are outside scope.

## November 11 alignment

Container compare-and-set is included for map fields and ordered-map fields selected
by exact key and order. It uses write routing and leaves element TTL absent, matching
current Java. See API_REFERENCE.md for selectors and results.

Full queue-content getters are legacy conveniences unsupported by the server;
use queue size, peek and pop operations. The latest published server starts, but
full typed/LZ4 live acceptance still has server-side failures reproduced with Java.
See FINDINGS.md for current validation; earlier startup failures are historical.
