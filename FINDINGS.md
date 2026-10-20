# Java parity validation findings

## Current baseline and acceptance status — October 20, 2026

Java checkout: `b97cd2c344625411804f51e7c1034eb3a8bbd668`.
Python and Java `cache.proto` and `coordinator.proto` are byte-for-byte identical.
The inventory covers **472 interface overloads**, **227 concrete-wrapper methods
and constructors**, and **6 active Jedis methods and constructors**.

Implementation and deterministic verification are complete. **Standalone acceptance
is pending** because the newly pulled server image cannot start on this host.
Live cluster and master/replica behavior remain explicitly deferred and unverified.
No current Python-specific verification failures remain.

At live-validation start, `docker pull alexaborisov/fastcache-standalone-noavx512:latest`
resolved to:

```text
alexaborisov/fastcache-standalone-noavx512@sha256:38618a056eba897874d015f62ac730099b634839ec6c5c403492bab7b4bb52ff
```

Every subsequent server probe used that immutable digest. The default entrypoint
exited with code **132** on initial launch and on restart, before gRPC readiness,
with no application logs. An independent minimal probe confirmed the signal:

```console
docker run --rm --entrypoint /bin/sh alexaborisov/fastcache-standalone-noavx512@sha256:38618a056eba897874d015f62ac730099b634839ec6c5c403492bab7b4bb52ff -c '/app/server-fastcache -config-file /app/cache-config.yaml; result=$?; echo server-exit=$result; exit $result'
```

Output:

```text
Illegal instruction (core dumped)
server-exit=132
```

The image's shell runs successfully. `/proc/cpuinfo` identifies AMD Ryzen 5 5600U
with AVX and AVX2 and without AVX512. An unsupported instruction is a plausible
cause; the specific faulting instruction has not been identified. The task-created
container `python-parity-20261020` was removed after collecting evidence.

Consequently, current-digest standalone round trips, bidirectional **live**
Java/Python interoperability, compressed-key server support, large server batches,
and server continuation behavior could not be revalidated. Historical queue and
lock limitations are neither accepted exceptions nor confirmed fixed for this
digest. The current live lock test asserts expiration instead of automatically
marking a failure as expected.

## Completed verification

| Check | Result |
| --- | --- |
| Deterministic suite (`python -m pytest -q --ignore=tests/test_integration.py`) | **746 passed, 193 skipped**; skips are live opt-ins and inapplicable direct forms of smart-routing tests |
| Ruff; mypy | Passed |
| Protobuf generation (`python -m tools.generate_proto --check`) | Passed |
| Non-writing inventory (`python -m tools.parity ../hurricache-java-client --check`) | Passed; rejects unmapped public declarations and parameters |
| Source and wheel distribution builds | Passed |
| Isolated installed wheel, without LZ4 extra | Passed; no LZ4 package installed, GZIP works, explicit LZ4 configuration raises the documented ImportError |
| Isolated installed wheel, with LZ4 extra | Passed, including raw-block encoding and decoding |
| Installed dependency consistency (`pip check` in both environments) | Passed |
| Fresh Java bindings and current reference utility compilation, JDK 17 | Passed |
| Java → Python and Python → Java GZIP/LZ4 ordered-map wire fixtures | Passed, including compressed keys/values and iteration order |

The offline Java fixture harness compiles the unchanged pinned Java interfaces,
compression utilities, and ordered-map observer with fresh Java protobuf bindings:

```console
python -m tools.java_interop ../hurricache-java-client --java-home "C:/Program Files/Java/jdk-17.0.3.1"
```

For each codec, Java writes protobuf fixtures that Python decodes, then Python
writes fixtures that Java's `StreamBatchOrderedMapObserver` decodes. Both sides
verify original bytes, key/value pairing, and the deliberate `[2, 1]` iteration
order. This resolves the historical Java ordered-map reader blocker for the new
commit. These offline checks do not substitute for live standalone acceptance.

Run `python -m build` and `python -m tools.verify_wheel` to reproduce distribution
and isolated-install checks. Wheel smoke tests use `python -I` to prevent importing
the source checkout.

## Implemented compatibility behavior

Existing positional arguments, defaults, and successful Python result types remain.
All four clients accept keyword-only `payload_compression="gzip"` and
`zero_client_id_uses_default=True`.

With the default ownership policy, `None` and explicit `0` use the configured
default ID. With `zero_client_id_uses_default=False`, explicit zero reaches the
wire and only `None` selects the default. Scalar keys, lock requests, collection
elements, and continuation metadata are covered across all four client forms.
Python's existing atomic lock metadata adaptation is retained; Java attaches its
atomic owner through the key. Both preserve the resolved ID.

`remove_element_at_position` gains keyword-only `end=None`. It encodes `end` only
when greater than `pos`, including values above 32-bit bounds; encoded bounds use
the protobuf's uint64 validation before RPC execution. Map batch removal retains
Python's `(values, keys)` argument order and encodes the correct fields despite
Java's `(keys, values)` parameter order.

Java integer insertion results map explicitly to Python count APIs. Existing boolean
conveniences retain empty-input success and zero-count stopping behavior. Serialized
request budgeting, continuation hints, absolute creation TTL, overflow checks,
deadlines, cancellation, configured smart routing and protections against replay
after partial progress remain covered. Every operation's deterministic test now
asserts decoded results and request keys as well as RPC execution. Additional tests
cover int64 atomic values, TTL encoding, and complete CAS responses.

Payload GZIP uses the standard library. Optional `hurricache[lz4]` supplies raw LZ4
blocks with no frame or embedded size prefix; protobuf `rawSize` supplies the size.
Configuration fails clearly if the extra is absent. The selected codec is scoped
to each operation and propagated to discovered nodes and continuations. Transport
compression remains independent. No codec sniffing or fallback is performed.

Compression applies strictly when `length > threshold`, default 1,024 bytes.
Zero compresses eligible nonempty content; negative thresholds are rejected.

| Content | Payload compression |
| --- | --- |
| Top-level keys; scalar create/update values | Configured threshold and codec |
| List/vector/queue/unordered-set elements; positional values and pivots | None |
| Ordinary-map creation/addition keys and values | None |
| Ordered-set creation/addition values | Configured threshold and codec |
| Ordered-map creation/addition keys and values | Configured threshold and codec |
| Single-entry lookup/removal keys; container-update values | None |
| Batch-removal keys | Configured threshold and codec |
| Batch-removal values | None |

Decoding validates encoded size, codec validity, declared raw size, and the 64 MiB
decoded-size limit. Cache channels use a 64 MiB inbound allowance, including every
discovered smart-client node. Deterministic tests cover thresholds, empty and
incompressible values, corruption, truncation, trailing bytes, size mismatches,
limits, ordered-map pairs, and chunked operations for both codecs.

`hurricache.jedis.Jedis` and `JedisPool` wrap synchronous direct/smart clients.
`get_resource()` creates resources sharing the client; `ping()` returns `"PONG"`
locally. Closing either wrapper or leaving its context closes the shared client,
matching active Java behavior. Redis commands commented out in Java are deliberately
unimplemented. Replication protobuf messages and stubs remain unchanged; Java
exposes no application-level replication client.

## Historical evidence — previous Java/image pair only

The September 12–13 run used Java
`833f2dd9a581a1984129b3f709f63afe3bfdc124` and server digest
`sha256:e79c96e7610c10ba99c7c11f939c78154ff3a4e6d24689ffdd15f35d2d7b7b1e`.
It reported 606 deterministic Python tests passed and 187 intentional skips.
Standalone Python collection/continuation tests passed; lock expiration remained
an expected failure, and queue `getContainer` returned INTERNAL while queue
front/pop operations worked. GZIP compressed long-key round trips passed.

Java → Python live fixtures passed. Python → Java isolated an ordered-map reader
defect: Java returned serialized `KeyBinaryPayload` wrappers and discarded map
ordering. The current commit uses codec-aware key decoding and LinkedHashMap;
current bidirectional offline fixtures pass. The obsolete Java defect is no longer
listed as an active blocker. None of the earlier server outcomes are evidence of
behavior on the newly pulled October image.
