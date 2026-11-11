"""Behavioral contracts at the current Java protocol boundary, on four clients."""

import asyncio
import dataclasses
import inspect
import math
import os

import pytest
from test_parity import invoke

from hurricache import (
    AsyncHurriCacheClient,
    CasResult,
    HurriCacheClient,
    KeyHintData,
    OrderedPayload,
    OrderedVariant,
    PartialOperationError,
    Payload,
    Variant,
    VariantType,
)
from hurricache.grpc import batching
from hurricache.grpc import cache_pb2 as pb
from hurricache.grpc import coordinator_pb2 as cp
from hurricache.grpc.exceptions import UnavailableError
from hurricache.grpc.operation import operation
from hurricache.grpc.smart_client import _split_target, _topology_from
from hurricache.grpc.utils import create_value, decode_batch, decode_cas, decode_response, decode_value, grpc_target

pytest_plugins = ["test_parity"]
MIXED = [Variant.none(), Variant.of_bytes(b""), Variant.of_long(0), Variant.of_double(0.0),
         Variant.of_long(-(1 << 63)), Variant.of_long((1 << 63) - 1), Variant.of_double(-0.0),
         Variant.of_double(1.25), Variant.of_bytes(b"abc" * 100)]


def reply(stub, rpc, response):
    if stub.asynchronous:
        async def call(request, **kwargs):
            stub.calls.append((rpc, request, kwargs))
            return response
    else:
        def call(request, **kwargs):
            stub.calls.append((rpc, request, kwargs))
            return response
    setattr(stub, rpc, call)


def test_model_equality_and_checked_accessors():
    assert len(set(MIXED[:4])) == 4
    assert Variant.of_long(1) != Variant.of_double(1)
    assert Variant.of_double(0.0) != Variant.of_double(-0.0)
    assert Variant.of_double(float("nan")) == Variant.of_double(-float("nan"))
    assert hash(Variant.of_double(float("nan"))) == hash(Variant.of_double(float("nan")))
    raw = bytearray(b"copy")
    value = Variant.of_bytes(raw)
    raw[0] = 0
    assert value.require_bytes() == b"copy"
    for candidate in MIXED:
        with pytest.raises(dataclasses.FrozenInstanceError):
            candidate.value = None
        for kind, accessor in [(VariantType.BYTES, "require_bytes"), (VariantType.INT, "require_long"),
                               (VariantType.DOUBLE, "require_double")]:
            if candidate.type != kind:
                with pytest.raises(TypeError):
                    getattr(candidate, accessor)()
    assert OrderedVariant.of(Variant.none()).order is None
    assert OrderedVariant.of_int(0, 0) != OrderedVariant.of_int(0)
    assert len({OrderedVariant.of_int(0, 0), OrderedVariant.of_int(0)}) == 2
    assert KeyHintData(1, 2, True) == KeyHintData(1, 2, False)
    assert hash(KeyHintData(1, 2, True)) == hash(KeyHintData(1, 2))
    assert KeyHintData(week_hash=1).weak_hash == 1
    with pytest.raises(ValueError):
        KeyHintData(weak_hash=1, week_hash=2)


@pytest.mark.parametrize("value", MIXED, ids=lambda v: v.type.name + repr(v.value)[:20])
async def test_scalar_typed_requests_and_results(client_form, value, monkeypatch):
    client, stub = client_form
    monkeypatch.setattr("hurricache.grpc.operation.time.time", lambda: 100)
    await invoke(client, "create", "key", KeyHintData(0, 2), value, 500, 17)
    _, request, options = stub.calls[-1]
    assert set(options) == {"timeout"}
    assert request.key.clientId == 17 and request.key.keyHint.weak_hash == 0
    assert not request.value.HasField("lock_info")
    assert request.value.ttl == 100500
    assert decode_value(request.value) == value
    assert request.value.value_type == int(value.type)
    assert request.value.value.WhichOneof("data") == {
        VariantType.NONE: None, VariantType.BYTES: "payload", VariantType.INT: "intPayload", VariantType.DOUBLE: "doublePayload"
    }[value.type]
    assert request.value.compressionInfo.enabled is (value.is_bytes and len(value.require_bytes()) > 8)
    # Return a fully serialized current response, including explicit zero oneofs.
    response = pb.ValueResponse.FromString(pb.ValueResponse(value_unordered=request.value).SerializeToString())
    for name, rpc in [("get", "getValue"), ("pop", "getAndDeleteValue")]:
        reply(stub, rpc, response)
        assert await invoke(client, name, "key") == value
    reply(stub, "updateValue", pb.UpdateValueResponse(value=request.value))
    assert await invoke(client, "replace", "key", value=value) == value
    reply(stub, "updateValue", pb.UpdateValueResponse(result=True))
    assert (await invoke(client, "replace", "key", value=value)).is_none
    reply(stub, "getValue", pb.ValueResponse())
    assert (await invoke(client, "get", "key")).is_none


@pytest.mark.parametrize("wire_type", [None, pb.AUTO, pb.BYTES, pb.INT, pb.DOUBLE])
@pytest.mark.parametrize("payload", [pb.BinaryPayload(), pb.BinaryPayload(payload=b"", size=0),
                                    pb.BinaryPayload(non_cord_payload=b"abc", size=3),
                                    pb.BinaryPayload(intPayload=0), pb.BinaryPayload(doublePayload=0.0)])
def test_oneof_and_explicit_type_precedence(wire_type, payload):
    wire = pb.Value(value=payload)
    if wire_type is not None:
        wire.value_type = wire_type
    decoded = decode_value(wire)
    if wire_type == pb.INT:
        assert decoded == Variant.of_long(payload.intPayload)
    elif wire_type == pb.DOUBLE:
        assert decoded == Variant.of_double(payload.doublePayload)
    else:
        expected = {None: Variant.none(), "payload": Variant.of_bytes(b""),
                    "non_cord_payload": Variant.of_bytes(b"abc"), "intPayload": Variant.of_long(0),
                    "doublePayload": Variant.of_double(0.0)}[payload.WhichOneof("data")]
        assert decoded == expected


async def test_relay_and_optional_hashes(client_form):
    client, stub = client_form
    reply(stub, "createKeyValue", pb.KeyHintResponse(relayed=True, keyHint=pb.KeyHint(weak_hash=0)))
    hint = await invoke(client, "create", "k", value=0)
    assert hint.weak_hash == 0 and hint.strong_hash is None and hint.was_relayed
    await invoke(client, "get", "k", hint)
    assert stub.calls[-1][1].key.keyHint.HasField("weak_hash")
    assert not stub.calls[-1][1].key.keyHint.HasField("strong_hash")


@pytest.mark.parametrize("family", ["list", "vector", "queue", "set", "ordered_set", "map", "ordered_map"])
async def test_mixed_container_continuations(client_form, monkeypatch, family):
    client, stub = client_form
    monkeypatch.setattr(batching, "MAX_RPC_SIZE", 190)
    values = MIXED * 3
    options = {"values": values, "ttl": 500}
    if family == "ordered_set":
        options["values"] = [OrderedVariant.of(v, i) for i, v in enumerate(values)]
    elif family in ("map", "ordered_map"):
        options["keys"] = [OrderedPayload(str(i).encode(), i) if family == "ordered_map" else Payload(str(i).encode())
                           for i in range(len(values))]
    await invoke(client, "create_" + family, b"k", **options)
    assert len(stub.calls) > 1
    assert all(req.ByteSize() <= 190 for _, req, _ in stub.calls)
    field = "value_ordered" if family == "ordered_set" else "value_unordered"
    assert [decode_value(v) for _, req, _ in stub.calls for v in getattr(req, field)] == values
    expiration = stub.calls[0][1].ttl
    for _, request, _ in stub.calls[1:]:
        assert request.key.keyHint == pb.KeyHint(weak_hash=0xFFFFFFFF, strong_hash=2)
        if family in ("map", "ordered_map", "ordered_set"):
            assert all(v.ttl == expiration for v in getattr(request, field))
    if family in ("map", "ordered_map"):
        key_field = "key_ordered" if family == "ordered_map" else "key_unordered"
        assert [k.payload.payload for _, req, _ in stub.calls for k in getattr(req, key_field)] == [str(i).encode() for i in range(len(values))]


def test_ordered_and_map_decoding_presence_and_pairing():
    values = [create_value(value) for value in MIXED]
    batch = pb.BatchValueResponse(key_unordered=[pb.Key(payload=pb.KeyBinaryPayload(payload=bytes([i]), size=1))
                                                 for i in range(len(values))], value_unordered=values)
    assert list(decode_batch(batch).values()) == MIXED
    assert all(type(k) is Payload for k in decode_batch(batch))
    ordered = pb.OrderedValue(value=pb.BinaryPayload(intPayload=0), value_type=pb.INT)
    assert decode_response(pb.ValueResponse(value_ordered=ordered)) == Variant.of_int(0)
    ordered.order = 0
    assert decode_response(pb.ValueResponse(value_ordered=ordered)) == Variant.of_int(0)
    assert decode_batch(pb.BatchValueResponse(value_ordered=[ordered])) == [OrderedVariant.of_int(0, 0)]
    for broken in [pb.BatchValueResponse(key_ordered=[pb.OrderedKey()], value_unordered=[]),
                   pb.BatchValueResponse(key_unordered=[pb.Key()], key_ordered=[pb.OrderedKey()]),
                   pb.BatchValueResponse(value_unordered=values, value_ordered=[ordered])]:
        with pytest.raises(ValueError):
            decode_batch(broken)


@pytest.mark.parametrize("name,argument,rpc", [("exchange", "value", "atomicExchange"), ("add", "delta", "atomicAdd"),
                                              ("sub", "delta", "atomicSub"), ("and_", "mask", "atomicAnd"),
                                              ("or_", "mask", "atomicOr"), ("xor", "mask", "atomicXor")])
async def test_atomics_and_both_selectors(client_form, name, argument, rpc, monkeypatch):
    client, stub = client_form
    monkeypatch.setattr("hurricache.grpc.operation.time.time", lambda: 100)
    for value in (-(1 << 63), (1 << 63)-1, 0) + (() if argument == "mask" else (0.0, -0.0, 1.25)):
        reply(stub, rpc, pb.ValueResponse(value_unordered=create_value(value)))
        for selector in (None, b"entry", "entry", 0, (1 << 64)-1):
            method = name.rstrip("_") + "_in_container" if selector is not None else name
            kwargs = {argument: value, "ttl": 500, "client_id": 0}
            if selector is not None:
                kwargs["selector"] = selector
            actual = await invoke(client, method, b"k", **kwargs)
            assert type(actual) is type(value)
            assert Variant.of_double(actual) == Variant.of_double(value) if isinstance(value, float) else actual == value
            req = stub.calls[-1][1]
            assert req.value.ttl == 100500
            assert not req.value.HasField("lock_info") and not req.value.HasField("compressionInfo")
            assert not req.HasField("container_key") if selector is None else req.HasField("container_key")
            if isinstance(selector, int):
                assert req.container_key.WhichOneof("key") == "pos" and req.container_key.pos == selector
            elif selector is not None:
                assert req.container_key.WhichOneof("key") == "container_key"
                assert req.container_key.container_key.payload.payload == b"entry"


@pytest.mark.parametrize("bad", [True, False, 1 << 63, -(1 << 63)-1, b"1", None])
async def test_invalid_numbers_never_execute(client_form, bad):
    client, stub = client_form
    for name, arg in [("exchange", "value"), ("add", "delta"), ("sub", "delta"), ("xor", "mask"),
                      ("and_", "mask"), ("or_", "mask"), ("compare_and_set", "expected_value"),
                      ("add_in_container", "delta")]:
        with pytest.raises((ValueError, TypeError)):
            await invoke(client, name, b"k", **{arg: bad})
    assert not stub.calls


@pytest.mark.parametrize("bad", [True, False, -1, 1 << 64, 1.5, None])
async def test_invalid_selectors_and_bitwise_double(client_form, bad):
    client, stub = client_form
    with pytest.raises((ValueError, TypeError)):
        await invoke(client, "exchange_in_container", b"k", selector=bad, value=1)
    for name in ("and_", "or_", "xor", "and_in_container", "or_in_container", "xor_in_container"):
        with pytest.raises(TypeError):
            await invoke(client, name, b"k", mask=1.25)
    assert not stub.calls


@pytest.mark.parametrize("observed", [None, 0, -(1 << 63), 0.0, -0.0, 1.25])
async def test_cas_nested_response_and_operand_types(client_form, observed):
    client, stub = client_form
    response = pb.AtomicCasRes(result=False)
    if observed is not None:
        response.expected.value_unordered.CopyFrom(create_value(observed))
        response.expected.keyHint.strong_hash = 0
    reply(stub, "atomicCompareAndSet", response)
    result = await invoke(client, "compare_and_set", b"k", expected_value=0, new_value=1.25, ttl=500)
    req = stub.calls[-1][1]
    assert req.expected.value_type == pb.DOUBLE and req.toSet.value_type == pb.DOUBLE
    assert not req.expected.HasField("ttl") and req.toSet.HasField("ttl")
    assert result.success is False and result.expected_value == observed
    if observed is not None:
        assert type(result.expected_value) is type(observed)
        assert result.hint == KeyHintData(strong_hash=0)
    else:
        assert result == CasResult(False)
    assert decode_cas(pb.AtomicCasRes(expected=pb.ValueResponse())) == CasResult(False)


@pytest.mark.parametrize("name,type", [("get_and_delete_list_elements_in_range", pb.LIST),
                                      ("get_and_delete_vector_elements_in_range", pb.VECTOR),
                                      ("get_and_delete_element_in_range_ordered_set", pb.ORDERED_SET),
                                      ("get_and_delete_element_in_range_ordered_map", pb.ORDERED_MAP)])
@pytest.mark.parametrize("start,end,presence", [(0, 0, (False, False)), (-1, -1, (False, False)),
                                              (None, None, (False, False)), (1, 9, (True, True))])
async def test_destructive_range_presence(client_form, name, type, start, end, presence):
    client, stub = client_form
    stub.shape = "ordered_map" if type == pb.ORDERED_MAP else "ordered_set" if type == pb.ORDERED_SET else "list"
    await invoke(client, name, b"k", pos=start, end=end, reverse=True)
    rpc, req, _ = stub.calls[-1]
    assert rpc == "getAndDeleteElementsInRange" and req.type == type and req.reverse
    assert (req.HasField("pos"), req.HasField("end")) == presence


async def test_ttl_ranges_and_removal_defaults(client_form, monkeypatch):
    client, stub = client_form
    monkeypatch.setattr("hurricache.grpc.operation.time.time", lambda: 100)
    await invoke(client, "set_ttl", b"k", ttl=-1)
    assert stub.calls[-1][1].ttl == (1 << 64)-1
    await invoke(client, "set_ttl", b"k", ttl=0)
    assert stub.calls[-1][1].ttl == 100000
    for end in (-1, 0, 7, 9):
        assert await invoke(client, "remove_at", b"k", pos=7, end=end) == 1
        req = stub.calls[-1][1]
        assert req.HasField("end") is (end > 7)
        assert not req.HasField("type")
    stub.shape = "ordered_set"
    for end in (0, None):
        await invoke(client, "get_element_in_range_ordered_set", b"k", pos=0, end=end)
        req = stub.calls[-1][1]
        assert req.HasField("pos") and req.HasField("end") is (end is not None)


@pytest.mark.parametrize("method", ["push_head", "push_tail", "insert_at", "insert_before", "insert_after"])
async def test_chunk_order_counts_and_preflight(client_form, monkeypatch, method):
    client, stub = client_form
    pivot = method in ("insert_before", "insert_after")
    monkeypatch.setattr(batching, "MAX_RPC_SIZE", 280 if pivot else 150)
    values = [os.urandom(80) for _ in range(4)]
    options = {"pivot": os.urandom(80)} if pivot else {}
    assert await invoke(client, method, b"k", values=values, **options) == 4
    assert len(stub.calls) == 4
    sent = [decode_value(v).require_bytes() for _, req, _ in stub.calls
            for v in (req.value if isinstance(req, pb.AddToValRequest) else req.value_unordered)]
    assert sent == values
    if pivot:
        assert [decode_value(req.pos).require_bytes() for _, req, _ in stub.calls] == [options["pivot"], *values[:-1]]
        assert [req.isBefore for _, req, _ in stub.calls] == [method == "insert_before", False, False, False]
    if method == "insert_at":
        assert [req.pos for _, req, _ in stub.calls] == [0, 1, 2, 3]
    stub.calls.clear()
    assert await invoke(client, method, b"k", values=[], **options) == 0
    assert not stub.calls
    with pytest.raises(ValueError):
        await invoke(client, method, b"k", values=[values[0], os.urandom(500)], **options)
    assert not stub.calls
    stub.fail_at = 2
    with pytest.raises(PartialOperationError) as caught:
        await invoke(client, method, b"k", values=values, **options)
    assert caught.value.completed_chunks == caught.value.completed_items == 1


def test_endpoint_topology_and_ipv6():
    response = cp.RoutingInfoData(max_shards=2, peerRouting=[
        cp.PeerRouting(target=cp.Endpoint(ip="::1", port=50000), role=cp.MASTER, partitionIds=[0, 1]),
        cp.PeerRouting(target=cp.Endpoint(ip="127.0.0.1", port=50001), role=cp.BACKUP, partitionIds=[0, 1])])
    topology = _topology_from(response)
    assert topology.routes[(cp.MASTER, 1)] == "[::1]:50000"
    assert _split_target("[::1]:50000") == ("::1", 50000)
    assert grpc_target("::1", 50000) == "[::1]:50000"
    assert HurriCacheClient("::1").target == "[::1]:50000"
    response.peerRouting[0].target.port = 0
    with pytest.raises(ValueError):
        _topology_from(response)


async def test_native_async_cancellation_and_partial_stream():
    client = AsyncHurriCacheClient()
    began = asyncio.Event()
    cancelled = asyncio.Event()
    class Stub:
        async def getValue(self, request, **kwargs):
            began.set()
            try:
                await asyncio.Future()
            finally:
                cancelled.set()
    client._stub = Stub()
    task = asyncio.create_task(client.get(b"k"))
    await began.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cancelled.is_set()
    assert inspect.iscoroutinefunction(AsyncHurriCacheClient.exchange_in_container)
    await client.close()


async def test_stream_progress_is_never_replayed(client_form):
    client, stub = client_form
    batch = pb.BatchValueResponse(value_unordered=[create_value(0), create_value(b"")])
    if stub.asynchronous:
        async def stream(request, **kwargs):
            stub.calls.append(("getAndDeleteElementsInRange", request, kwargs))
            yield batch
            raise UnavailableError("after progress")
    else:
        def stream(request, **kwargs):
            stub.calls.append(("getAndDeleteElementsInRange", request, kwargs))
            yield batch
            raise UnavailableError("after progress")
    stub.getAndDeleteElementsInRange = stream
    with pytest.raises(PartialOperationError) as caught:
        await invoke(client, "get_and_delete_list_elements_in_range", b"k")
    assert caught.value.completed_chunks == 1 and caught.value.completed_items == 2
    assert len(stub.calls) == 1


@pytest.mark.parametrize("codec", ["gzip", "lz4"])
def test_numeric_values_bypass_compression(codec, monkeypatch):
    if codec == "lz4":
        pytest.importorskip("lz4.block")
    def forbidden(*args):
        raise AssertionError("numeric value reached byte compression")
    monkeypatch.setattr("hurricache.grpc.utils._compressed", forbidden)
    with operation(HurriCacheClient(payload_compression=codec, default_compression_threshold=0), 2):
        for value in MIXED:
            if value.is_bytes:
                continue
            assert decode_value(create_value(value, compress=True)) == value
    assert math.isnan(Variant.of_double(float("nan")).require_double())
