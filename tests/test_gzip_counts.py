"""Independent wire checks for the pinned GZIP/count protocol."""

import gzip
import os

import pytest
from test_parity import invoke

from hurricache import OrderedPayload, PartialOperationError
from hurricache.grpc import batching, utils
from hurricache.grpc import cache_pb2 as pb

pytest_plugins = ["test_parity"]


@pytest.mark.parametrize("name", ["head", "tail", "position_before", "position_after", "position_by_value"])
async def test_count_and_boolean_chunks(client_form, monkeypatch, name):
    client, stub = client_form
    monkeypatch.setattr(batching, "MAX_RPC_SIZE", 150)
    method = "add_element_to_" + name
    kwargs = {"values": [bytes([n]) * 80 for n in range(4)]}
    assert await invoke(client, method + "_count", b"k", **kwargs) == 4
    assert len(stub.calls) == 4
    assert all(req.ByteSize() <= 150 for _, req, _ in stub.calls)
    assert all(pb.DESCRIPTOR.services_by_name["HurriCacheGrpcService"].methods_by_name[rpc].output_type.name == "IntResponse" for rpc, _, _ in stub.calls)
    stub.calls.clear()
    stub.reject_at = 2
    with pytest.raises(PartialOperationError) as caught:
        await invoke(client, method, b"k", **kwargs)
    assert caught.value.completed_chunks == 1
    assert caught.value.completed_items == 1
    assert len(stub.calls) == 2
    stub.calls.clear()
    # A zero count is a valid count result and must not stop count APIs.
    assert await invoke(client, method + "_count", b"k", **kwargs) == 3
    assert len(stub.calls) == 4
    stub.calls.clear()
    assert await invoke(client, method + "_count", b"k", values=[]) == 0
    assert await invoke(client, method, b"k", values=[]) is True
    assert not stub.calls


async def test_creation_encoding_and_absolute_ttl(client_form, monkeypatch):
    client, stub = client_form
    monkeypatch.setattr(batching, "MAX_RPC_SIZE", 360)
    raw = [os.urandom(80) for _ in range(8)]
    for kind in ("map", "ordered_map", "ordered_set"):
        kwargs = {"values": raw, "ttl": 5000}
        if kind == "ordered_set":
            kwargs["values"] = [OrderedPayload(v, i) for i, v in enumerate(raw)]
        else:
            kwargs["keys"] = [OrderedPayload(v, i) for i, v in enumerate(raw)] if kind == "ordered_map" else raw
        stub.calls.clear()
        await invoke(client, "create_" + kind, b"key", **kwargs)
        assert len(stub.calls) > 1
        expiration = stub.calls[0][1].ttl
        for _, request, _ in stub.calls:
            for value in list(request.value_ordered) + list(request.value_unordered):
                assert value.ttl == expiration
                assert value.compressionInfo.enabled is (kind != "map")
                if kind != "map":
                    assert gzip.decompress(value.value.payload) in raw
            for key in request.key_ordered:
                assert gzip.decompress(key.payload.payload) in raw


@pytest.mark.parametrize("corruption", ["header", "checksum", "truncated", "trailing", "declared", "wire_size", "limit", "legacy"])
def test_invalid_gzip(corruption, monkeypatch):
    raw = b"abc" * 1000
    encoded = bytearray(gzip.compress(raw, mtime=0))
    size = len(raw)
    if corruption == "header":
        encoded[0] = 0
    elif corruption == "checksum":
        encoded[-8] ^= 1
    elif corruption == "truncated":
        del encoded[-2:]
    elif corruption == "trailing":
        encoded.extend(b"garbage")
    elif corruption == "declared":
        size -= 1
    elif corruption == "limit":
        monkeypatch.setattr(utils, "MAX_DECODED_BYTES", size - 1)
    elif corruption == "legacy":
        encoded = bytearray(b"\x30abc")  # literal-only raw LZ4, never a supported format
    value = pb.Value(value=pb.BinaryPayload(payload=bytes(encoded), size=len(encoded)), compressionInfo=pb.CompressedInfo(enabled=True, rawSize=size))
    if corruption == "wire_size":
        value.value.size += 1
    with pytest.raises(ValueError):
        utils.decode_value(value)


async def test_weight_and_map_range_wire(client_form):
    client, stub = client_form
    for name, rpc in [("get_element_with_weight", "getElementAtPosition"), ("get_and_remove_element_with_weight", "getAndRemoveElementAtPosition")]:
        assert await invoke(client, name, b"key", pos=0xFFFFFFFFFFFFFFFF) == b"v"
        called, req, _ = stub.calls[-1]
        assert called == rpc and req.pos == 0xFFFFFFFFFFFFFFFF
        assert not req.HasField("type") and not req.HasField("end")
        for weight in (-1, 1 << 64):
            with pytest.raises(ValueError):
                await invoke(client, name, b"key", pos=weight)
    stub.shape = "ordered_map"
    for reverse in (False, True):
        result = await invoke(client, "stream_element_in_range_ordered_map", b"key", pos=7, end=9, reverse=reverse)
        req = stub.calls[-1][1]
        assert req.type == pb.ORDERED_MAP and req.pos == 7 and req.end == 9 and req.reverse is reverse
        assert list(result) == [OrderedPayload(b"entry", 1)]


async def test_count_overflow_preserves_acknowledged_progress(client_form, monkeypatch):
    client, stub = client_form
    monkeypatch.setattr(batching, "MAX_RPC_SIZE", 150)
    original = stub.addElementToTail
    if stub.asynchronous:
        async def reply(request, **kwargs):
            await original(request, **kwargs)
            return pb.IntResponse(size=0xFFFFFFFF)
    else:
        def reply(request, **kwargs):
            original(request, **kwargs)
            return pb.IntResponse(size=0xFFFFFFFF)
    stub.addElementToTail = reply
    with pytest.raises(PartialOperationError) as caught:
        await invoke(client, "add_element_to_tail_count", b"k", values=[b"x" * 80] * 3)
    assert isinstance(caught.value.__cause__, OverflowError)
    assert caught.value.completed_chunks == 2
    assert len(stub.calls) == 2
