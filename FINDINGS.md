# Java parity validation findings

## Current alignment check - November 11, 2026

Python and Go 0.2.0 now match current Java `0a2b1d851405744e9390bcf5b5e8592177b121e4`:
**742 interface declarations, 319 concrete wrapper declarations, 68 typed-model
declarations, and 6 active Jedis declarations**. Java advanced from `1aa296e` with
container CAS. Both clients now expose `compare_and_set_in_container` /
`CompareAndSetInContainer` through their direct and smart transports (including
Python native async). No release, tag or client commit was published.

Container CAS uses the existing RPC and the new AtomicCas `keys` oneof:
`element_key` tag 4 for byte/string map fields, `ordered_element_key` tag 5 for
an OrderedPayload carrying an explicit exact order. It validates numeric int64
inputs/selectors before data RPCs, promotes mixed numeric operands to DOUBLE,
leaves element TTL absent as Java does, compresses byte keys only, uses write
routing, and preserves optional observed values/types/hints on failed CAS.
Positions and bools are not valid CAS selectors. Ordinary positional atomics
retain their existing selector behavior. Scalar CAS behavior remains unchanged.

Full queue-content reads are intentionally unsupported by the server contract.
The retained legacy get_queue/stream_queue and GetQueue/StreamQueue conveniences
do not expand that contract. Live tests require full queue reads to fail, then
verify queue cardinality and pop order without losing elements. Continuation
tests drain queues rather than reading their full contents. Queue size is fixed
in the fetched server; no queue-streaming server change was introduced here.

### Verification

| Check | November 11 result |
| --- | --- |
| Python deterministic suite | **1392 passed, 720 skipped**; skips are 496 explicit live opt-ins plus 224 direct-client instances of smart-only routing tests |
| Go deterministic suite, vet, race | Passed; race uses the existing Clang toolchain with bounded concurrency/memory |
| Ruff and mypy | Passed; mypy checks 17 source files |
| Current declaration inventories and API references | Passed; non-writing checks reject unmapped declarations/parameters and stale references |
| Protocol and generated bindings | Cache/coordinator descriptors match Java, Python, Go and server source, ignoring language-specific file options; regeneration is byte-for-byte reproducible |
| Offline Java/Python and Java/Go fixtures | Passed both directions under GZIP and raw LZ4 |
| Distribution checks | Python wheel/sdist build and isolated wheel installations with/without LZ4 passed; isolated archived Go consumer passed both codecs and new container CAS API |
| Live GZIP container CAS | Python **16 passed** across sync/native async/both smart forms; Go direct and smart MAP/ORDERED_MAP cases passed; independent current-Java map/ordered-map probes passed |
| Complete Python GZIP standalone suite | **180 passed, 68 failed**; no expected-failure exceptions or diagnostic filter |
| Complete Go GZIP standalone suites | Both direct and smart runs failed the remaining typed-value server checks; batching, TTL/locks, atomics, container CAS, order updates and destructive ranges passed |
| Independent current-Java API probes, GZIP | **8 passed, 5 failed**; failures reproduce NONE, set type equality, equal-weight NaN ordering, and full-width weighted lookup |
| Full live Java/Python and Java/Go fixture exchange | Attempted unfiltered; blocked at server rejection of NONE creation, so complete live bidirectional acceptance did not pass |
| Explicit live raw LZ4 | Ready server, but compressed keys/container setup fail in Java, Python and Go; setting the environment variable does not make this published image support LZ4 |

### Tested server and remaining failures

At validation start, Docker pull resolved latest to this immutable image, used
throughout both separate codec processes:

```text
alexaborisov/fastcache-standalone-noavx512@sha256:c15f06d8e4ccf3b0f5cafb5039acaaf44fe142459cb8a6fde1fdb5716374fbb2
```

Both containers reached application startup and gRPC readiness. Startup reports
`HC-26.44-5-gd9eeffa-dirty`, built November 10. The fetched server checkout is
`db0112520ee664f00272d94c2954d842d9dbb020` on `redis-connector`; the image's dirty
build label does not establish byte-for-byte correspondence with that checkout.
No patched executable was substituted and no server source was edited in this run.

Remaining failures reproduced by unchanged current Java API calls are:

- Scalar NONE creation: INVALID_ARGUMENT, `Can't insert data. Invalid type`.
- NONE in a collection: INVALID_ARGUMENT, `Incorrect type of object`.
- Integer zero and double zero in a set: one entry returned instead of two.
- Double zero and NaN in an ordered set at the same weight: one entry instead of two.
- The maximum uint64 weighted lookup: NOT_FOUND after successful creation.
- Raw-LZ4 compressed keys: DATA_LOSS, `Cant read key data protocol error`; large
  LZ4 map selectors also fail setup with INVALID_ARGUMENT. This was checked with
  `HURRICACHE_PAYLOAD_COMPRESSION=lz4` on a fresh separate process.

Java GZIP order-change lookup, ordinary vector pops, ordinary weighted read/pop,
compression, lock expiry, and both new container CAS forms pass. Several earlier
server defects are therefore corrected, but the published image still cannot
complete typed client acceptance. The clients do not hide these failures or
encode NONE as empty bytes. Protocol alignment does not imply server behavioral
acceptance. The old Docker startup/WSL outage is historical and not the current
blocker.

Smart live data RPCs use deterministic coordinator discovery pointing to the
actual standalone. Real cluster, failover and replication remain unverified.
Offline fixtures likewise do not complete live acceptance. The Java working
tree was cleanly fast-forwarded to current origin/jdk-16; Java source was not edited.

Exact commands/results, startup logs and image metadata are under
`D:/hurricache/materials/validation/clients-20261111`, including `provenance.json`,
`java-current-api-*.log`, `python-live-full-gzip.log`, and
`go-live-{,smart-}full-gzip.log`. Only task-owned containers were started/stopped;
the user's kind cluster was left running.

## Historical evidence before November 11

## Current baseline and acceptance — November 8, 2026

Python **0.2.0** targets unchanged Java commit `1aa296eda50b8363d510c2bc05fe0fab2c5dd8cf`.
Both protobuf sources are copied from that checkout. The non-writing inventory covers
**734 interface declarations**, **307 concrete-wrapper methods/constructors**,
**68 typed-model declarations/constructors**, and **6 active Jedis declarations/constructors**.

Implementation and local verification are complete. **Standalone acceptance is blocked**:
the freshly pulled latest image exits with an illegal instruction before gRPC startup.
Live standalone round trips and bidirectional live Java/Python interoperability therefore
remain unverified. Offline fixtures and deterministic gRPC services do not complete live acceptance.
Live cluster, real-cluster failover, and application-level replication acceptance remain deferred.

## Current standalone startup reproduction

Docker was already running for this validation. A fresh
`docker pull alexaborisov/fastcache-standalone-noavx512:latest` confirmed the user's updated image and resolved to:

```text
alexaborisov/fastcache-standalone-noavx512@sha256:553e630a1cc10de83235eef4486d5e0835c741abc84475f18ec3346e15b94684
```

All subsequent server probes used this immutable digest. The default entrypoint container
`python-parity-20261108` started at **2026-11-08 10:47:12 UTC** and exited at **10:47:13 UTC**
with exit code **132**, `OOMKilled=false`, and no application logs. No live tests were run
against the failed process. The task-created stopped container was removed after evidence collection.

An independent disposable shell probe reproduced the failure:

```console
docker run --rm --entrypoint /bin/sh alexaborisov/fastcache-standalone-noavx512@sha256:553e630a1cc10de83235eef4486d5e0835c741abc84475f18ec3346e15b94684 -c '/app/server-fastcache -config-file /app/cache-config.yaml; result=$?; echo server-exit=$result; exit $result'
```

```text
Illegal instruction (core dumped)
server-exit=132
```

The image's `/bin/cat` runs and reads `/app/cache-config.yaml`. The shipped configuration
binds `0.0.0.0:50000`, uses 1024 partitions and two threads. The faulting instruction
was not identified. This is a startup failure before any client request, not a Python RPC failure.

## Completed verification

| Check | Evidence |
| --- | --- |
| Full suite, `python -m pytest -q --tb=short --disable-warnings` | **1226 passed, 370 skipped**; 148 live opt-ins and 222 inapplicable direct forms of smart-only checks |
| Ruff | Passed |
| mypy | Passed, 17 package source files |
| Protobuf regeneration `python -m tools.generate_proto --check` | Passed; current cache/coordinator and replication stubs |
| Inventory `python -m tools.parity ../hurricache-java-client --check` | Passed; rejects unknown declarations and arguments |
| Reference `python -m tools.reference --check` | Passed; generated from current signatures and exact alias list |
| Source and wheel distributions | Built as hurricache 0.2.0 |
| Isolated installed wheel without LZ4 | Passed typed models, protocol, GZIP, async signatures and lifecycle; explicit LZ4 raises ImportError |
| Isolated installed wheel with LZ4 | Passed, including raw-block encode/decode |
| Installed dependency consistency | pip check passed in both environments |
| Current Java converters and clients, JDK 17 | Compiled unchanged pinned sources with fresh bindings; offline/live harnesses compile |
| Bidirectional offline Java/Python fixtures | GZIP and raw LZ4 passed typed values, NONE/empty/zeros, int64 limits, NaN/signed zero, nested CAS, compressed ordered maps, order-update oneofs and the moved plain-value field |
| Actual deterministic gRPC wire round trips | Both codecs across sync, native async and both smart clients; real coordinator Endpoint discovery |

The suite emits intentional deprecation warnings while exercising retained aliases. It contains
no expected server failures. Python runtime verification used Python 3.14.3; isolated wheels
resolved grpcio 1.85.1, protobuf 7.36.2, tenacity 9.2.1, and optional lz4 4.4.5.

## Behavioral evidence and adaptations

The November 8 Java delta adds absolute and incremental order updates on ordered maps/sets.
`update_order` maps Java's `update(OrderedPayload, long)` overload; `increment_order` maps
`incrementOrder`. `update` also dispatches that Java overload when given an OrderedPayload
and a positional order or keyword `new_order`. Existing byte-key value updates remain typed.
The new `UpdateContainerRequest` selects `element_key` (tag 2) or `ordered_element_key` (tag 3),
then `value` (tag 4), `new_order` (tag 6), or `delta_order` (tag 7). This changes compatibility
even from the earlier 0.2 draft targeting `8fac8f…`; no legacy wire fallback is added.

`test_order_updates.py` ports Java's exact current-order matching, duplicate members at different
weights, value preservation, zero order, missing-order rejection, signed deltas and uint64
underflow/overflow behavior. Both codecs run over real deterministic gRPC and coordinator
services through all four clients. Smart order updates use executeWrite routing. The order
update returns the server's bool result, including false without a previous-value message.
The atomic service oracle also follows Java's fetch-and-add semantics (return the prior value).

`test_typed_api.py` checks request fields/results through all four transports: NONE versus
empty bytes and numeric zero, int64 limits, double atomics and nested CAS, both byte oneofs,
nullable orders, hash-only hint equality, relay metadata, independent container selectors,
destructive bounds, TTL persistence, counts, chunk ordering, preflight and progress failures.

`test_wire_roundtrips.py` uses actual gRPC serialization, typed stateful services and current
coordinator discovery. `test_routing_parity.py` independently transcribes Java execute/executeWrite
policies for every operation and checks fallback, redirect, mode overrides, topology retirement
and prevention of replay. `test_current_java.py`, `test_gzip_counts.py` and `test_protocol.py`
retain codec thresholds, corrupt/truncated data, size mismatches, decoded limits, transport
independence, explicit zero IDs, locks and active Jedis lifecycle checks.

Typed builders compress bytes only. Current Java leaves Value lock_info absent for normal writes;
ownership is carried in Key.clientId and explicit lock requests. Lock duration remains absolute Unix
seconds despite the schema comment. Ordered writes normalize absent order to zero, matching Java;
decode preserves absent order as None. Successful empty value responses become Variant.none();
gRPC NOT_FOUND still maps to KeyNotFoundError. CAS returns native observed numeric types.

Scalar create/replace/remove use configured Java routing. Container mutations/atomics and destructive
ranges default to master then backup. Weighted get_and_remove_element_with_weight retains configured
routing. Explicit caller-local overrides take precedence. Container remove overloads use write routing.

Python retains serialized preflight, pairing, one creation expiration/deadline, continuation hints,
accurate count aggregation/overflow, ordered insertion across batches, native cancellation, independent
transport compression and 64 MiB inbound/decoded limits. No acknowledged operation is replayed.

## Reproduce offline and live acceptance

```console
python -m tools.java_interop ../hurricache-java-client --java-home "C:/Program Files/Java/jdk-17.0.3.1"
python -m build
python -m tools.verify_wheel
```

The Java fixture harness compiles reference interfaces, KeyValueUtils, compression utilities,
observers, simple/standalone clients, and regenerated bindings. It exchanges typed wire fixtures
and ordered-map byte fixtures in both directions, plus the current update-oneof fields under
both codecs. The live harness uses Java API writers and
the pinned Java wire converters for compressed collection/key readers; it does not claim to
validate every high-level Java collection observer.

After a matching standalone image starts successfully, set `HURRICACHE_TEST_TARGET=host:port`
and run `python -m pytest -q tests/test_integration.py tests/test_live_parity.py`. Both codecs
are selected by default; `HURRICACHE_TEST_CODECS` can select a server's configured codec.
Tests cover typed scalars/collections, atomics, selectors, destructive ranges, compressed keys,
continuations, ordering, TTL, lock expiration and order updates without automatic expected-failure exceptions.
Then run the Java harness with `--target host:port` for bidirectional live typed/byte fixtures,
atomics, long keys, ordered data, absolute/incremental order changes and large continuation lists under both codecs.

## Historical observations

The October 20 baseline was `b97cd2c344625411804f51e7c1034eb3a8bbd668`.
Its then-latest image digest `38618a056eba897874d015f62ac730099b634839ec6c5c403492bab7b4bb52ff`
also exited with code 132. The November 7 Java target was `8fac8f050a12888fac765b7f2e1e6643187d5824`;
its image digest `2c39182c691549366387907391afcb33831391095147560677c48f3c1c3df7e5` also failed startup.
Those observations are historical; the November 8 result above was independently reproduced
against a newly pulled, different digest. Prior queue/lock/server
limitations are not accepted exceptions and have not been re-established for the current image.
