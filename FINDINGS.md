# Pinned GZIP validation findings

Java `jdk-16`: `833f2dd9a581a1984129b3f709f63afe3bfdc124`.
Server: `alexaborisov/fastcache-standalone-noavx512@sha256:e79c96e7610c10ba99c7c11f939c78154ff3a4e6d24689ffdd15f35d2d7b7b1e`. The mutable `latest` tag is not used for validation.

## Current validation, September 12–13, 2026

The pinned server reached gRPC readiness before every live test session.
Go scalar compression boundary/incompressible round trips, default-size batching,
map continuation chunks, and head/tail/position/before/after chunk equivalence
passed. Python sync and native-async collection creation and insertion live tests
passed for list, vector, queue, set, ordered set, map, and ordered map.

| Probe | New-digest result |
| --- | --- |
| Map update: entry `a`, previous `one`, replacement `replacement` | `Updated=true`, previous `one`, readback `replacement`; the old false-status defect is fixed. |
| Ordered-set reverse range, stored weights 1, 2, 3 | `pos=1,end=2,reverse=true` returned `[2,1]`; `pos=0,end=4` returned `[3,2,1]`. Reversed bounds `[2,1]` and `[4,0]` returned empty. |
| Queue `getContainer` | `INTERNAL: Key not found or type is not correct`; front/pop operations and queue continuation insertion passed. |
| Lock expiration | `WRITE_LOCK`, owner 77, `lockDuration=1789269692` returned `OK=0`. Owner 78 returned `CANT_LOCK=1` initially and after a 2.1-second wait, with the later request's `lockDuration=1789269694`. Both requests carried the same hint (`weekHash=3366201222`, `strongHash=2737711295`). |
| Compressed long keys | Java, Go, and Python each created, read, and removed a key containing 2,048 repeated bytes plus its prefix, with a 4,096-byte value. All GZIP round trips passed on the separate disposable instance; the historical crash was not reproduced. |

The user explicitly chose to document server limitations and preserve the agreed
contract. Clients do not rewrite lock units, queue streaming, or weighted ranges
to work around server behavior. The Python expiration probe marks the observed
server limitation as an expected failure and attempts cleanup with the lock owner.

After Docker recovered, the complete Go live suite passed on a fresh pinned
instance at `127.0.0.1:57752`. Python live parity returned **2 passed, 1 xfailed**;
the expected failure is the documented lock-expiration limitation. Both instances
were checked for gRPC readiness before testing. Long-key tests ran separately at
`127.0.0.1:61640`.

Pinned Java sources and fresh generated bindings compiled successfully with JDK 17.
The expanded bidirectional harness uses `FastCacheAsyncStandaloneClient` and
explicit `-Dhurricache.compression=gzip`; fixtures include transferred hints,
compressed ordered-set values, ordered-map keys/values, ascending/descending map
ranges, and a 4 MiB list requiring continuation chunks.

| Interoperability direction | Result |
| --- | --- |
| Java → Go | All fixtures passed, including standalone-wrapper collection continuation and ordered-map ranges in both directions. |
| Java → Python | All fixtures passed, including standalone-wrapper collection continuation and ordered-map ranges in both directions. |
| Go → Java | All fixture families passed except ordered-map keys returned by the Java reader. |
| Python → Java | Same isolated Java ordered-map reader failure. |

The pinned [Java ordered-map observer](https://github.com/hurricache/hurricache-java-client/blob/833f2dd9a581a1984129b3f709f63afe3bfdc124/src/main/java/com/hurricache/utils/StreamBatchOrderedMapObserver.java)
calls `keyOrdered.getPayload().toByteArray()`, returning the serialized
`KeyBinaryPayload` wrapper instead of its payload bytes. It also uses a `HashMap`
and has no payload-decompression logic. In these live responses the server returned
uncompressed data: values were correct, but 4,096- and 4,097-byte keys became 4,102
and 4,103 bytes. Their prefixes were `0a80202a2a2a2a2a2a2a2a2a` and
`0a81202a2a2a2a2a2a2a2a2a`. The defect reproduced for forward and reverse ranges
from both Go- and Python-written fixtures. The diagnostic harness continues through
the large collection before reporting these failures. No Java source or Go/Python
decoding workaround was introduced. Fully passing bidirectional interoperability
therefore remains blocked by this pinned Java reader defect, not by Docker.

Deterministic Python verification: **606 passed, 187 intentional skips** using
`python -m pytest -q --ignore=tests/test_integration.py`. These skips cover live
opt-ins and inapplicable direct-client cases in smart-only routing tests.
Go deterministic tests, vet, race tests (with a portable Clang compiler), binding
reproducibility, and external-consumer build passed. Python Ruff, mypy, binding
and inventory reproducibility, source/wheel distribution builds, isolated installed-wheel
smoke tests, and pip dependency consistency checks passed. The wheel environment
contains no LZ4 dependency. Docker recovered on September 13 and cleanup completed.

## Compatibility and implementation

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


The three head/tail/relative-position RPCs use regenerated `IntResponse` bindings.
Count APIs aggregate server counts with overflow checks. Boolean compatibility APIs
retain empty-input success and stop at zero-count chunks. Partial errors expose
acknowledged progress and prevent replay. Complete serialized-request preflight
includes later items, map pairs, positional offsets, headers, and continuation hints.
Creation preserves encoded elements and one absolute TTL across continuations.
Weighted point retrieval and removal both follow Java's configured routing mode.
Native async cancellation is preserved; no synchronous client is run in a worker thread.

## Cleanup and remaining reference limitations

Both September 13 task-created containers were removed after their tests.
The September 12 task-created containers were already absent when Docker recovered.
Final `docker ps -a` showed only the unrelated `redroom-dev-control-plane`, which
was left running. Disposable fixtures from failed reader probes were removed with
their dedicated server.

The remaining limitations are server queue streaming, server lock expiration,
and the pinned Java ordered-map reader. Client behavior follows the agreed contract;
these reference/server defects are documented rather than hidden by compatibility
workarounds.

Historical 26.35/26.36 interoperability results apply to the previous Java/image pair, not this digest.
