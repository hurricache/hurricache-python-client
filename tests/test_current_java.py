"""Retained codec, configuration and lifecycle protections through every transport."""

import builtins
import gzip
import os
from unittest.mock import Mock

import grpc
import pytest
from test_parity import invoke

from hurricache import (
    AsyncHurriCacheClient,
    AsyncHurriCacheSmartClient,
    HurriCacheClient,
    HurriCacheSmartClient,
    OrderedPayload,
    Variant,
)
from hurricache.grpc import batching, utils
from hurricache.grpc import cache_pb2 as pb
from hurricache.grpc.operation import operation, payload_codec
from hurricache.jedis import Jedis, JedisPool

pytest_plugins = ["test_parity"]


def nodes(client):
    return [client, *getattr(client, "_clients", {}).values()]








@pytest.mark.parametrize("legacy", [True, False])
@pytest.mark.parametrize("supplied", [None, 0, 23])
async def test_client_id_policy_in_keys_owners_and_continuations(client_form, monkeypatch, legacy, supplied):
    client, stub = client_form
    for node in nodes(client):
        node._default_client_id = 77
        node._zero_client_id_uses_default = legacy
    expected = 77 if supplied is None or (supplied == 0 and legacy) else supplied
    await invoke(client, "create_key_value", b"k", value=b"v", client_id=supplied)
    req = stub.calls[-1][1]
    assert req.key.clientId == expected
    assert not req.value.HasField("lock_info")
    await invoke(client, "lock_object", b"k", client_id=supplied)
    assert stub.calls[-1][1].clientId == expected
    monkeypatch.setattr(batching, "MAX_RPC_SIZE", 230)
    stub.calls.clear()
    await invoke(client, "create_ordered_map", b"k", keys=[OrderedPayload(bytes([i]) * 60, i) for i in range(4)],
                 values=[os.urandom(60) for _ in range(4)], client_id=supplied)
    assert len(stub.calls) > 1
    for _, req, _ in stub.calls:
        assert req.key.clientId == expected
        assert all(k.clientId == expected for k in req.key_ordered)
        assert all(not v.HasField("lock_info") for v in req.value_unordered)


@pytest.mark.parametrize("codec", ["gzip", "lz4"])
@pytest.mark.parametrize("raw", [b"", b"x" * 7, b"x" * 8, b"x" * 9, os.urandom(4096)], ids=["empty", "below", "at", "above", "random"])
async def test_codecs_and_boundaries(client_form, codec, raw):
    if codec == "lz4":
        lz4 = pytest.importorskip("lz4.block")
    client, stub = client_form
    for node in nodes(client):
        node._payload_compression = codec
    await invoke(client, "create_key_value", raw, value=raw)
    req = stub.calls[-1][1]
    for data, info in [(req.key.payload, req.key.compressionInfo), (req.value.value, req.value.compressionInfo)]:
        assert info.enabled is (len(raw) > 8)
        if info.enabled:
            decoded = gzip.decompress(data.payload) if codec == "gzip" else lz4.decompress(data.payload, uncompressed_size=info.rawSize)
            assert decoded == raw and info.rawSize == len(raw)
        else:
            assert data.payload == raw
    # The stub creates response bytes inside the call's codec context.
    assert await invoke(client, "get_value", b"k") == Variant.of_bytes(b"v")
    assert payload_codec.get() == "gzip"


@pytest.mark.parametrize("corruption", ["bytes", "truncated", "trailing", "size", "raw_size", "limit", "wrong_codec"])
def test_lz4_validation(corruption, monkeypatch):
    lz4 = pytest.importorskip("lz4.block")
    client = HurriCacheClient(payload_compression="lz4", default_compression_threshold=0)
    with operation(client, 2):
        value = utils.create_value(b"abc" * 1000, compress=True)
        assert utils.decode_value(value) == Variant.of_bytes(b"abc" * 1000)
        if corruption == "bytes":
            value.value.payload = b"bad"
        elif corruption == "truncated":
            value.value.payload = value.value.payload[:-1]
        elif corruption == "trailing":
            value.value.payload += b"garbage"
        elif corruption == "size":
            value.value.size += 1
        elif corruption == "raw_size":
            value.compressionInfo.rawSize += 1
        elif corruption == "limit":
            monkeypatch.setattr(utils, "MAX_DECODED_BYTES", 100)
        elif corruption == "wrong_codec":
            value.value.payload = gzip.compress(b"abc" * 1000)
        if corruption != "size":
            value.value.size = len(value.value.payload)
        with pytest.raises(ValueError):
            utils.decode_value(value)
        # Independently specified raw block: literal-only final sequence.
        assert lz4.decompress(b"\x30abc", uncompressed_size=3) == b"abc"


async def test_lz4_ordered_map_and_chunk_continuations(client_form, monkeypatch):
    pytest.importorskip("lz4.block")
    client, stub = client_form
    for node in nodes(client):
        node._payload_compression = "lz4"
    monkeypatch.setattr(batching, "MAX_RPC_SIZE", 300)
    keys = [OrderedPayload(os.urandom(70), i) for i in range(8)]
    values = [os.urandom(70) for _ in keys]
    await invoke(client, "create_ordered_map", b"k", keys=keys, values=values)
    assert len(stub.calls) > 1
    with operation(client, 2):
        assert [utils.decode_key(k) for _, req, _ in stub.calls for k in req.key_ordered] == [k.value for k in keys]
        assert [utils.decode_value(v).require_bytes() for _, req, _ in stub.calls for v in req.value_unordered] == values
    stub.shape = "ordered_map"
    assert await invoke(client, "stream_ordered_map", b"k") == {OrderedPayload(b"entry", 1): Variant.of_bytes(b"v")}


def test_missing_lz4_extra_and_invalid_configuration(monkeypatch):
    original = builtins.__import__

    def missing(name, *args, **kwargs):
        if name.startswith("lz4"):
            raise ImportError("not installed")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", missing)
    for cls in (HurriCacheClient, AsyncHurriCacheClient, HurriCacheSmartClient, AsyncHurriCacheSmartClient):
        with pytest.raises(ImportError, match=r"hurricache\[lz4\]"):
            cls(payload_compression="lz4")
        with pytest.raises(ValueError, match="payload_compression"):
            cls(payload_compression="sniff")
        if cls in (HurriCacheClient, AsyncHurriCacheClient):
            cls()  # GZIP has no dependency on the extra.


@pytest.mark.parametrize("smart", [False, True])
def test_jedis_shared_lifecycle(smart):
    cls = HurriCacheSmartClient if smart else HurriCacheClient
    client = cls.__new__(cls)
    client.close = Mock()
    with JedisPool(client) as pool:
        first, second = pool.get_resource(), pool.get_resource()
        assert first is not second
        with first:
            assert first.ping() == second.ping() == "PONG"
            client.close.assert_not_called()
        client.close.assert_called_once()
        second.close()
    assert client.close.call_count == 3
    with pytest.raises(TypeError):
        Jedis(None)


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_inbound_limit_and_transport_independence(monkeypatch, asynchronous):
    cls = AsyncHurriCacheClient if asynchronous else HurriCacheClient
    namespace = grpc.aio if asynchronous else grpc
    factory = Mock()
    monkeypatch.setattr(namespace, "insecure_channel", factory)
    client = cls(compression=grpc.Compression.Gzip)
    client.channel
    options = factory.call_args.kwargs
    assert options["compression"] == grpc.Compression.Gzip
    assert dict(options["options"])["grpc.max_receive_message_length"] == 64 * 1024 * 1024




@pytest.mark.parametrize("asynchronous", [False, True])
async def test_discovered_nodes_preserve_constructor_settings(monkeypatch, asynchronous):
    from test_smart_clients import _cluster

    pytest.importorskip("lz4.block")
    servers, _, port = _cluster()
    cls = AsyncHurriCacheSmartClient if asynchronous else HurriCacheSmartClient
    client = cls("127.0.0.1", port, payload_compression="lz4", zero_client_id_uses_default=False,
                 default_client_id=77, default_ttl=300, default_compression_threshold=12,
                 compression=grpc.Compression.Gzip, refresh_interval=60)
    try:
        ready = client.wait_until_ready(3)
        if asynchronous:
            await ready
        assert len(client._clients) == 2
        for node in client._clients.values():
            assert node._payload_compression == "lz4"
            assert node._zero_client_id_uses_default is False
            assert node.default_client_id == 77 and node.default_ttl == 300
            assert node.default_compression_threshold == 12
            assert node._compression == grpc.Compression.Gzip
            resolver = node._client_id if asynchronous else node._client_id
            assert resolver(0) == 0 and resolver(None) == 77
    finally:
        closed = client.close()
        if asynchronous:
            await closed
        for server in servers:
            server.stop(0).wait()


@pytest.mark.parametrize("codec", ["gzip", "lz4"])
async def test_compressed_response_decoding(client_form, codec):
    if codec == "lz4":
        lz4 = pytest.importorskip("lz4.block")
    client, stub = client_form
    raw = b"compressed response" * 1000
    encoded = gzip.compress(raw) if codec == "gzip" else lz4.compress(raw, store_size=False)
    response = pb.ValueResponse(value_unordered=pb.Value(
        value=pb.BinaryPayload(payload=encoded, size=len(encoded)),
        compressionInfo=pb.CompressedInfo(enabled=True, rawSize=len(raw))))
    for node in nodes(client):
        node._payload_compression = codec
    if stub.asynchronous:
        async def get(request, **kwargs):
            return response
    else:
        def get(request, **kwargs):
            return response
    stub.getValue = get
    assert await invoke(client, "get_value", b"k") == Variant.of_bytes(raw)
    assert payload_codec.get() == "gzip"


def test_inventory_rejects_unknown_declarations_and_parameters():
    from tools.parity import argument_mapping, declarations, mapping

    source = "interface Contract {\n Fancy<NewType> newOperation(byte[] key);\n}"
    [(match, _)] = declarations(source, interface=True)
    assert mapping(match) == "new_operation"
    assert not hasattr(HurriCacheClient, mapping(match))
    source = "interface Contract {\n Fancy<NewType> getValue(byte[] key, int newFlag);\n}"
    [(match, _)] = declarations(source, interface=True)
    with pytest.raises(ValueError, match="unmapped parameter"):
        argument_mapping(match, HurriCacheClient.get_value)
