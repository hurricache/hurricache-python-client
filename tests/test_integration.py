"""Current-protocol standalone acceptance; requires an explicitly supplied server."""

import inspect
import os
import uuid

import grpc
import pytest
from test_parity import invoke
from test_smart_clients import Coordinator, _server

from hurricache import (
    AsyncHurriCacheClient,
    AsyncHurriCacheSmartClient,
    HurriCacheClient,
    HurriCacheRpcError,
    HurriCacheSmartClient,
    KeyNotFoundError,
    OrderedPayload,
    OrderedVariant,
    Payload,
    Variant,
)
from hurricache.grpc import coordinator_pb2 as cp
from hurricache.grpc import coordinator_pb2_grpc as cpg

TARGET = os.environ.get("HURRICACHE_TEST_TARGET")
CODECS = os.environ.get("HURRICACHE_TEST_CODECS", "gzip,lz4").split(",")
pytestmark = pytest.mark.skipif(not TARGET, reason="set HURRICACHE_TEST_TARGET after pinned standalone startup")


@pytest.fixture(params=[(form, c) for form in ("sync", "async", "smart", "async-smart") for c in CODECS],
                ids=lambda item: "-".join(item))
async def live_client(request):
    form, codec = request.param
    if codec == "lz4":
        pytest.importorskip("lz4.block")
    host, port = TARGET.rsplit(":", 1)
    options = dict(default_timeout=10, payload_compression=codec,
                   default_client_id=77, zero_client_id_uses_default=False)
    coordinator_server = None
    if "smart" in form:
        coordinator = Coordinator(cp.RoutingInfoData(max_shards=1, peerRouting=[cp.PeerRouting(
            target=cp.Endpoint(ip=host.strip("[]"), port=int(port)), role=cp.MASTER, partitionIds=[0])]))
        coordinator_server, coordinator_port = _server(coordinator, cpg.add_CoordinatorServiceServicer_to_server)
        client = (AsyncHurriCacheSmartClient if form == "async-smart" else HurriCacheSmartClient)(
            "127.0.0.1", coordinator_port, refresh_interval=60, **options)
    else:
        client = (AsyncHurriCacheClient if form == "async" else HurriCacheClient)(host.strip("[]"), int(port), **options)
    keys = []
    def key():
        value = "python-02/" + uuid.uuid4().hex
        keys.append(value)
        return value
    try:
        if coordinator_server is not None:
            await invoke(client, "wait_until_ready", 10)
            assert coordinator.calls >= 1
        yield client, key
    finally:
        try:
            for value in keys:
                try:
                    await invoke(client, "remove", value)
                except KeyNotFoundError:
                    pass
        finally:
            try:
                closed = client.close()
                if inspect.isawaitable(closed):
                    await closed
            finally:
                if coordinator_server is not None:
                    coordinator_server.stop(0).wait()


@pytest.mark.parametrize("value", [Variant.none(), Variant.of_bytes(b""), Variant.of_long(0),
                                    Variant.of_double(0.0), Variant.of_double(-0.0), Variant.of_long(-(1 << 63)),
                                    Variant.of_long((1 << 63)-1), Variant.of_double(1.25),
                                    Variant.of_double(float("nan")), Variant.of_double(float("inf")),
                                    Variant.of_double(-float("inf")),
                                    Variant.of_bytes(b"x" * 4096)], ids=lambda v: v.type.name + repr(v.value)[:16])
async def test_live_typed_scalars(live_client, value):
    client, key = live_client
    name = key()
    hint = await invoke(client, "create", name, value=value, client_id=0)
    assert await invoke(client, "get", name, hint, client_id=0) == value
    assert await invoke(client, "replace", name, hint, value=17, client_id=0) == value
    assert await invoke(client, "pop", name, hint, client_id=0) == Variant.of_long(17)


@pytest.mark.parametrize("family", ["list", "vector", "queue", "set", "ordered_set", "map", "ordered_map"])
@pytest.mark.parametrize("include_none", [False, True], ids=["stored-types", "including-none"])
async def test_live_mixed_collections(live_client, family, include_none):
    client, key = live_client
    name = key()
    values = [Variant.of_bytes(b""), Variant.of_long(0), Variant.of_double(0),
              Variant.of_long(-7), Variant.of_double(1.25), Variant.of_bytes(b"abc" * 1024)]
    if include_none:
        values.insert(0, Variant.none())
    options = {"values": values}
    if family == "ordered_set":
        options["values"] = [OrderedVariant.of(v, i+1) for i, v in enumerate(values)]
    elif family in ("map", "ordered_map"):
        options["keys"] = [OrderedPayload(str(i).encode(), i+1) if family == "ordered_map" else Payload(str(i).encode())
                           for i in range(len(values))]
    hint = await invoke(client, "create_" + family, name, **options)
    if family == "queue":
        # Full queue reads are intentionally unsupported; cardinality/pop remain valid.
        with pytest.raises(HurriCacheRpcError):
            await invoke(client, "get_queue", name, hint)
        assert await invoke(client, "get_size", name, hint) == len(values)
        assert [await invoke(client, "pop_head", name, hint) for _ in values] == values
        return
    actual = await invoke(client, "get_" + family, name, hint)
    if family in ("map", "ordered_map"):
        assert actual == dict(zip(options["keys"], values))
    elif family == "ordered_set":
        assert actual == options["values"]
    elif family == "set":
        assert set(actual) == set(values)
    else:
        assert actual == values


async def test_live_numeric_atomics_and_container_selectors(live_client):
    client, key = live_client
    for initial in (10, 10.5):
        name = key()
        hint = await invoke(client, "create", name, value=initial)
        assert await invoke(client, "exchange", name, hint, value=initial) == initial
        await invoke(client, "add", name, hint, delta=2)
        assert (await invoke(client, "get", name, hint)).number_value() == initial + 2
        await invoke(client, "sub", name, hint, delta=2)
        cas = await invoke(client, "compare_and_set", name, hint, expected_value=initial, new_value=1)
        assert cas.success
        assert (await invoke(client, "get", name, hint)).number_value() == 1
    for family, selector in (("map", b"entry"), ("vector", 0)):
        name = key()
        kwargs = {"values": [10]}
        if family == "map":
            kwargs["keys"] = [b"entry"]
        hint = await invoke(client, "create_" + family, name, **kwargs)
        assert await invoke(client, "exchange_in_container", name, hint, selector=selector, value=20) == 10
        await invoke(client, "add_in_container", name, hint, selector=selector, delta=5)
        result = await invoke(client, "get_in_container", name, hint, element_key=b"entry") if family == "map" else await invoke(client, "get_at", name, hint, pos=0)
        assert result.require_long() == 25


@pytest.mark.parametrize("ordered", [False, True])
@pytest.mark.parametrize("initial,new", [(100, 200), (1.5, 2.5)])
async def test_live_container_cas(live_client, ordered, initial, new):
    client, key = live_client
    name = key()
    field = b"field" * 1000  # Also exercise compressed exact selectors.
    selector = OrderedPayload(field, 10) if ordered else field
    hint = await invoke(client, "create_ordered_map" if ordered else "create_map", name,
                        keys=[selector], values=[initial])
    result = await invoke(client, "compare_and_set_in_container", name, hint,
                          selector=selector, expected_value=initial, new_value=new)
    assert result.success and result.expected_value is None
    assert (await invoke(client, "get_in_container", name, hint, element_key=field)).number_value() == new
    result = await invoke(client, "compare_and_set_in_container", name, hint,
                          selector=selector, expected_value=initial, new_value=initial)
    assert not result.success and result.expected_value == new
    assert type(result.expected_value) is type(new)
    if ordered:
        with pytest.raises(KeyNotFoundError):
            await invoke(client, "compare_and_set_in_container", name, hint,
                         selector=OrderedPayload(field, 999), expected_value=new, new_value=initial)
    with pytest.raises(KeyNotFoundError):
        await invoke(client, "compare_and_set_in_container", name, hint, selector=b"missing",
                     expected_value=initial, new_value=new)


async def test_live_set_type_sensitive_equality(live_client):
    client, key = live_client
    name = key()
    values = [Variant.none(), Variant.of_bytes(b""), Variant.of_long(0), Variant.of_double(0),
              Variant.of_double(-0.0), Variant.of_double(float("nan")), Variant.of_double(float("nan")),
              Variant.of_long(1), Variant.of_long(0x100000001)]
    hint = await invoke(client, "create_set", name, values=values)
    actual = await invoke(client, "get_set", name, hint)
    assert len(actual) == len(set(values))
    assert set(actual) == set(values)


@pytest.mark.parametrize("value", [Variant.none(), Variant.of_bytes(b""), Variant.of_double(-0.0),
                                    Variant.of_bytes(b"weighted" * 1024)], ids=["none", "empty", "double", "compressed"])
@pytest.mark.parametrize("weight", [2, (1 << 64)-1])
async def test_live_weighted_scalar_contract(live_client, value, weight):
    client, key = live_client
    name = key()
    other = 3 if weight == 2 else 1
    hint = await invoke(client, "create_ordered_set", name, values=[OrderedVariant.of(value, weight), OrderedVariant.of_int(9, other)])
    for method in ("get_element_with_weight", "get_and_remove_element_with_weight"):
        actual = await invoke(client, method, name, hint, pos=weight)
        assert type(actual) is Variant and actual == value
    assert await invoke(client, "get_ordered_set", name, hint) == [OrderedVariant.of_int(9, other)]


@pytest.mark.parametrize("family", ["list", "vector"])
async def test_live_positional_reads_and_pops(live_client, family):
    client, key = live_client
    name = key()
    values = [Variant.none(), Variant.of_bytes(b""), Variant.of_long(0), Variant.of_double(-0.0),
              Variant.of_bytes(b"position" * 1024)]
    hint = await invoke(client, "create_" + family, name, values=values)
    assert await invoke(client, "get_at", name, hint, pos=3) == values[3]
    assert await invoke(client, "pop_at", name, hint, pos=1) == values[1]
    assert await invoke(client, "remove_at", name, hint, pos=1) == 1
    assert await invoke(client, "get_" + family, name, hint) == [values[0], *values[3:]]


@pytest.mark.parametrize("family", ["list", "vector", "ordered_set", "ordered_map"])
async def test_live_destructive_ranges(live_client, family):
    client, key = live_client
    name = key()
    options = {"values": [10, 20, 30, 40]}
    if family == "ordered_set":
        options["values"] = [OrderedVariant.of_int(v, i+1) for i, v in enumerate(options["values"])]
    if family == "ordered_map":
        options["keys"] = [OrderedPayload(bytes([i]), i+1) for i in range(4)]
    hint = await invoke(client, "create_" + family, name, **options)
    method = "get_and_delete_element_in_range_" + family if family.startswith("ordered") else "get_and_delete_" + family + "_elements_in_range"
    before = await invoke(client, "get_size", name, hint)
    extracted = await invoke(client, method, name, hint, pos=1, end=2)
    after = await invoke(client, "get_size", name, hint)
    assert len(extracted) == 2 and before - after == 2
    remaining = await invoke(client, "get_" + family, name, hint)
    if family == "ordered_map":
        expected = dict(zip(options["keys"], [Variant.of_long(v) for v in options["values"]]))
        assert extracted == dict(list(expected.items())[:2])
        assert remaining == dict(list(expected.items())[2:])
    elif family == "ordered_set":
        assert extracted == options["values"][:2]
        assert remaining == options["values"][2:]
    else:
        assert extracted == [Variant.of_long(20), Variant.of_long(30)]
        assert remaining == [Variant.of_long(10), Variant.of_long(40)]


async def test_live_compressed_key_and_map_update(live_client):
    client, key = live_client
    name = key() + "/" + "x" * 2048
    # Register the expanded key for cleanup.
    hint = await invoke(client, "create", name, value=b"x" * 4096)
    try:
        assert await invoke(client, "get", name, hint) == Variant.of_bytes(b"x" * 4096)
    finally:
        await invoke(client, "remove", name, hint)
    name = key()
    hint = await invoke(client, "create_map", name, keys=[b"entry"], values=[1.5])
    assert await invoke(client, "update", name, hint, element_key=b"entry", value=2) == Variant.of_double(1.5)
    assert await invoke(client, "pop_in_container", name, hint, element_key=b"entry") == Variant.of_long(2)


@pytest.mark.parametrize("family", ["ordered_map", "ordered_set"])
async def test_live_order_updates_exact_matching_and_bounds(live_client, family):
    client, key = live_client
    name = key()
    member = b"weighted-member" * 100
    options = {"keys": [OrderedPayload(member, 10), OrderedPayload(member, 20)], "values": [1.25, 2]}
    if family == "ordered_set":
        options = {"values": [OrderedVariant.of_bytes(member, 10), OrderedVariant.of_bytes(member, 20)]}
    hint = await invoke(client, "create_" + family, name, **options)
    before = await invoke(client, "get_" + family, name, hint)
    with pytest.raises(KeyNotFoundError):
        await invoke(client, "increment_order", name, hint, OrderedPayload(member, 999), 1)
    with pytest.raises(HurriCacheRpcError) as caught:
        await invoke(client, "increment_order", name, hint, OrderedPayload(member, 10), -11)
    assert caught.value.code == grpc.StatusCode.OUT_OF_RANGE
    assert await invoke(client, "get_" + family, name, hint) == before
    assert await invoke(client, "increment_order", name, hint, OrderedPayload(member, 10), 5) is True
    assert await invoke(client, "update", name, hint, OrderedPayload(member, 20), 99) is True
    actual = await invoke(client, "get_" + family, name, hint)
    assert [item.order for item in actual] == [15, 99]
    if family == "ordered_map":
        assert list(actual.values()) == [Variant.of_double(1.25), Variant.of_long(2)]
    else:
        assert [item.require_bytes() for item in actual] == [member, member]
    assert await invoke(client, "update_order", name, hint, OrderedPayload(member, 15), 0) is True
    assert [item.order for item in await invoke(client, "get_" + family, name, hint)] == [0, 99]
