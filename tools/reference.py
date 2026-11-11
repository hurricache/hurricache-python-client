"""Generate or non-destructively check the current public API reference."""

import argparse
import inspect
from pathlib import Path

from hurricache import HurriCacheClient
from hurricache.grpc.api import ALIASES, BUILDERS
from tools.parity import BASELINE

ROOT = Path(__file__).resolve().parents[1]
INTRO = '''# HurriCache 0.2.0 Python API

Java baseline: `JAVA_BASELINE`. This protocol requires a matching server.
See [README.md](README.md) for migration/examples, [API_PARITY.md](API_PARITY.md) for every Java overload,
and [FINDINGS.md](FINDINGS.md) for acceptance evidence.

## Clients and shared arguments

`HurriCacheClient` and `AsyncHurriCacheClient` accept host='localhost', port=50000, default_client_id=0,
default_timeout=1.0, credentials=None, compression=None; keyword-only default_ttl=0,
default_compression_threshold=1024, payload_compression='gzip', zero_client_id_uses_default=True.
Sync supports `with`; native async supports `async with` and awaits every operation/close.

`HurriCacheSmartClient` and `AsyncHurriCacheSmartClient` accept coordinators='localhost:50051',
coordinator_port=None; all defaults above are keyword-only, with readiness_timeout=60.0,
refresh_interval=30.0, mode=Mode.MASTER_THEN_BACKUP. Lifecycle: wait_until_ready(timeout=None),
set_mode(mode), clear_mode(), mode(mode) context manager, close(). Async readiness/close are awaited.

Keys: bytes or UTF-8 str. hint=None omits hints; client_id=None selects the configured ID.
Explicit zero selects it unless zero_client_id_uses_default=False. TTL is milliseconds; timeout= and
lock_duration are seconds. ttl=None uses default_ttl, ttl=0 omits creation TTL. Negative set_ttl persists.
`**kwargs` forwards gRPC options such as timeout= and metadata=. One deadline covers the operation.

## Results and models

Scalar reads, pop, peek, get_at and value updates return Variant. An absent successful response returns Variant.none().
Weighted single-element reads return Variant, including when the wire element carries an order.
Ordered collection elements return OrderedVariant. Server NOT_FOUND status still raises KeyNotFoundError.
Creation returns KeyHintData. Collections/ranges return list[Variant], list[OrderedVariant],
dict[Payload, Variant], or dict[OrderedPayload, Variant]. Maps preserve streamed insertion order.
Push, insert, put_all, delete and container removals return integer server counts. Whole-key remove,
contains and set_ttl return bool; lock/unlock return LockStatus; get_size returns int.
Numeric atomics return int or float, bitwise operations int. compare_and_set returns
CasResult(success, expected_value: int | float | None, hint: KeyHintData | None).

compare_and_set_in_container selects a map field with bytes/str, or an ordered-map field with
OrderedPayload carrying an explicit current order. It promotes mixed numeric operands to DOUBLE,
uses write routing, and leaves TTL absent as Java does. Positions/bools are invalid selectors.
Full queue-content reads (get_queue/stream_queue) are legacy conveniences unsupported by the
server contract. Use get_size, peek_head/peek_tail, and pop_head/pop_tail for queues.

Variant factories: none(), of_bytes(bytes), of_long(int)/of_int(int), of_double(float).
Properties: type (VariantType.NONE/BYTES/INT/DOUBLE), value, is_none/is_bytes/is_int/is_double.
Checked accessors: require_bytes(), require_long()/require_int(), require_double(), number_value().
bytes_value/long_value/int_value/double_value are checked properties. Equality is type-sensitive;
NaNs compare equal and signed zeros differ. Models are immutable/hashable and copy byte buffers.

OrderedVariant(variant, order=None)/of(variant, order=None) delegates accessors/predicates and adds has_order.
of_bytes/of_int/of_long/of_double(value, order=None) are factories. Decode preserves absent order;
Java-compatible writes encode absent order as zero. Payload(value) and OrderedPayload(value, order=0)
represent byte map keys. A nullable ordered-key order decodes as None.

KeyHintData(weak_hash=None, strong_hash=None, was_relayed=False) retains week_hash as a constructor/property alias.
Equality/hash compare only hashes; is_specified and bool indicate hash presence. Creation propagates relay metadata.
LockType: NO_LOCK, WRITE_LOCK, READ_LOCK, GLOBAL. LockStatus: OK, CANT_LOCK, CANT_UNLOCK, GENERIC_ERROR.
ContainerType: UNDEFINED, VECTOR, LIST, QUEUE, SET, MAP, ORDERED_MAP, ORDERED_SET.
Mode: MASTER, BACKUP, MASTER_THEN_BACKUP (MASTER_THAN_BACKUP alias), LB_SMART.

Container selectors accept byte/string entry keys or uint64 integer positions, including zero.
Exactly one ContainerKey oneof is encoded. bools are rejected as numeric values/selectors/positions.
Integer operands must fit signed int64 before RPC execution; bitwise operations are integer-only.
CAS promotes operands to double when either is float and preserves the observed response type.

## Operations

Signatures are identical across all four clients; use await for async. Named arguments collapse Java overloads.
create_map/create_ordered_map also accept keys={Payload: Variant} with values omitted. Other map operations
accept paired keys/values lists; mismatched lengths fail before execution. contains_key/remove optionally accept
keyword-only element_key= for Java container overloads; remove then returns an int count.
get_range supports LIST, VECTOR and SET. Ordered reads encode supplied zero bounds; None omits a bound.
Destructive ranges omit nonpositive/None bounds, following Java. remove_at sends end only when end > pos.

Ordered-map/set order updates identify exactly one member by its bytes and current order in OrderedPayload.
update_order(..., element_key=OrderedPayload(member, current_order), new_order=0) sets an unsigned uint64 order.
increment_order(..., element_key=OrderedPayload(member, current_order), delta=-5) adds a signed int64 delta.
Both return the server's bool result and preserve the member/value. Missing current orders and invalid operands
fail before RPC execution. Wrong current orders raise KeyNotFoundError; server arithmetic underflow/overflow
is exposed as HurriCacheRpcError with code OUT_OF_RANGE and leaves the member unchanged.
The Java update overload also works as update(key, hint, OrderedPayload(member, current_order), new_order),
or with keyword new_order=. Plain byte element keys continue to perform typed value updates.
Current protocol: UpdateContainerRequest selects element_key/ordered_element_key and value/new_order/delta_order.
The plain value field moved to wire tag 4; upgrade servers/clients together, including from the earlier 0.2 draft.

| Operation signature |
| --- |
'''
OUTRO = '''
## Errors, routing and bounds

KeyNotFoundError, PermissionDeniedError, InvalidArgumentError, FailedPreconditionError, UnavailableError,
DeadlineExceededError and CancelledError map server statuses. PartialOperationError reports completed_chunks,
completed_items and partial_result after progress; smart routing never replays it. Native asyncio cancellation
remains asyncio.CancelledError and cancels the active RPC.

Smart routing follows pinned Java: scalar creation/replacement/removal and reads use configured routing;
container mutations, atomics and destructive ranges default to master then backup. The weighted
get_and_remove_element_with_weight uses configured routing. Caller-local overrides take precedence.
Known-target redirect and unavailable fallback allow one final attempt. Endpoint IPv6 addresses are bracketed.
Hints preserve optional hash presence and unsigned routing bits. set_mode(None) clears the mode override.

Serialized preflight, paired map batches, one creation expiration and continuation hints apply to all clients.
Numeric values bypass compression. GZIP/default and optional raw LZ4 use strict length > threshold.
Transport compression is independent. Inbound/decoded byte limits are 64 MiB. No old-server protocol fallback.
Head chunks stay in input order to preserve the server's per-request reversal across batches.
Pivot chunks advance the anchor; preflight includes the anchor's complete encoded size.

Jedis/JedisPool: ping() -> 'PONG', get_resource() -> Jedis, close() and context managers share the synchronous
client lifecycle. Replication is limited to generated messages/stubs. Live cluster/replication validation is deferred.
'''


def render():
    rows = []
    for name in sorted(BUILDERS):
        signature = inspect.signature(getattr(HurriCacheClient, name))
        signature = signature.replace(parameters=[v for k, v in signature.parameters.items() if k != "self"])
        rows.append(f"| `{name}{signature}` |")
    aliases = [f"| `{old}` | `{new}` |" for old, new in sorted(ALIASES.items())]
    return INTRO.replace("JAVA_BASELINE", BASELINE) + "\n".join(rows) + "\n\n" + "\n".join([
        "## Deprecated aliases", "",
        "These preserve positional binding, emit DeprecationWarning, and return current typed/count results.",
        "Incompatible legacy methods, boolean insertions and atomic_create/store/load/load_and_delete are removed.",
        "README.md lists their replacements. No legacy return mode is provided.", "",
        "| Deprecated name | Current name |", "| --- | --- |", *aliases, "", OUTRO,
    ])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    result = render()
    path = ROOT / "API_REFERENCE.md"
    if args.check:
        if path.read_text(encoding="utf-8") != result:
            raise SystemExit("API_REFERENCE.md is stale; run python -m tools.reference")
        print("API reference matches current signatures")
    else:
        path.write_text(result, encoding="utf-8", newline="\n")


if __name__ == "__main__":
    main()
