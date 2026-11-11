"""Current Java ordered-map/set weight updates, including actual gRPC oneofs."""

import gzip
import inspect

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
    Variant,
)
from hurricache.grpc import cache_pb2 as pb
from hurricache.grpc import cache_pb2_grpc as rpc
from hurricache.grpc import coordinator_pb2 as cp
from hurricache.grpc import coordinator_pb2_grpc as crpc
from hurricache.grpc.operation import operation
from hurricache.grpc.utils import decode_key

pytest_plugins = ["test_parity"]


@pytest.mark.parametrize("method,amount,field", [("update_order", 0, "new_order"),
                                               ("update_order", (1 << 64)-1, "new_order"),
                                               ("increment_order", 0, "delta_order"),
                                               ("increment_order", -(1 << 63), "delta_order"),
                                               ("increment_order", (1 << 63)-1, "delta_order")])
@pytest.mark.parametrize("codec", ["gzip", "lz4"])
async def test_order_update_fields_and_result(client_form, method, amount, field, codec):
    if codec == "lz4":
        pytest.importorskip("lz4.block")
    client, stub = client_form
    for node in [client, *getattr(client, "_clients", {}).values()]:
        node._payload_compression = codec
    member = OrderedPayload(b"member" * 1000, 0)
    argument = "delta" if field == "delta_order" else "new_order"
    assert await invoke(client, method, b"k", element_key=member, client_id=17, **{argument: amount}) is True
    name, req, options = stub.calls[-1]
    assert name == "updateValueInContainer" and set(options) == {"timeout"}
    assert req.WhichOneof("keys") == "ordered_element_key" and req.WhichOneof("values") == field
    assert getattr(req, field) == amount
    assert req.ordered_element_key.HasField("order") and req.ordered_element_key.order == 0
    assert req.key.clientId == req.ordered_element_key.clientId == 17
    assert req.ordered_element_key.compressionInfo.enabled
    assert not req.HasField("element_key") and not req.HasField("value")
    node = next(iter(client._clients.values())) if hasattr(client, "_clients") else client
    with operation(node, 2):
        assert decode_key(req.ordered_element_key) == member.value
    if stub.asynchronous:
        async def rejected(request, **kwargs):
            return pb.UpdateValueResponse(result=False)
    else:
        def rejected(request, **kwargs):
            return pb.UpdateValueResponse(result=False)
    stub.updateValueInContainer = rejected
    assert await invoke(client, method, b"k", element_key=member, **{argument: amount}) is False


@pytest.mark.parametrize("method", ["update_order", "increment_order"])
@pytest.mark.parametrize("member", [None, b"entry", OrderedPayload(b"entry", None), OrderedPayload(b"entry", -1),
                                    OrderedPayload(b"entry", 1 << 64), OrderedPayload(b"entry", True)])
async def test_invalid_current_order_precedes_rpc(client_form, method, member):
    client, stub = client_form
    with pytest.raises((ValueError, TypeError)):
        await invoke(client, method, b"k", element_key=member)
    assert not stub.calls


@pytest.mark.parametrize("method,parameter,bad", [("update_order", "new_order", -1),
                                                 ("update_order", "new_order", 1 << 64),
                                                 ("increment_order", "delta", -(1 << 63)-1),
                                                 ("increment_order", "delta", 1 << 63),
                                                 ("update_order", "new_order", True),
                                                 ("increment_order", "delta", False),
                                                 ("update_order", "new_order", 1.25),
                                                 ("increment_order", "delta", None)])
async def test_invalid_order_operands_precede_rpc(client_form, method, parameter, bad):
    client, stub = client_form
    with pytest.raises((ValueError, TypeError)):
        await invoke(client, method, b"k", element_key=OrderedPayload(b"entry", 5), **{parameter: bad})
    assert not stub.calls


async def test_update_overload_and_changed_plain_value_field(client_form):
    client, stub = client_form
    assert await invoke(client, "update", b"k", None, OrderedPayload(b"entry", 10), 0) is True
    req = stub.calls[-1][1]
    assert req.WhichOneof("keys") == "ordered_element_key" and req.WhichOneof("values") == "new_order"
    assert req.HasField("new_order") and req.new_order == 0
    assert await invoke(client, "update", b"k", element_key=OrderedPayload(b"entry", 0), new_order=99) is True
    assert stub.calls[-1][1].new_order == 99
    previous = await invoke(client, "update", b"k", element_key=b"entry", value=0)
    assert previous == Variant.of_bytes(b"old")
    req = stub.calls[-1][1]
    assert req.WhichOneof("keys") == "element_key" and req.WhichOneof("values") == "value"
    assert req.value.value.intPayload == 0
    assert req.DESCRIPTOR.fields_by_name["value"].number == 4
    assert req.DESCRIPTOR.fields_by_name["ordered_element_key"].number == 3
    before = len(stub.calls)
    for options in ({"element_key": b"entry", "new_order": 4},
                    {"element_key": OrderedPayload(b"entry", 0), "new_order": 4, "value": 2},
                    {"element_key": OrderedPayload(b"entry", None), "new_order": 4}):
        with pytest.raises((ValueError, TypeError)):
            await invoke(client, "update", b"k", **options)
    assert len(stub.calls) == before


class OrderCache(rpc.HurriCacheGrpcServiceServicer):
    """Independent stateful oracle for the Java exact-member/current-order contract."""

    def __init__(self, codec):
        self.codec = codec
        self.containers = {}

    def raw(self, item):
        payload = item.value if isinstance(item, pb.OrderedValue) else item.payload
        body = payload.payload
        if item.compressionInfo.enabled:
            if self.codec == "gzip":
                return gzip.decompress(body)
            import lz4.block
            return lz4.block.decompress(body, uncompressed_size=item.compressionInfo.rawSize)
        return body

    def createContainer(self, request, context):
        self.containers[request.key.payload.payload] = pb.CreateContainerRequest.FromString(request.SerializeToString())
        return pb.KeyHintResponse(keyHint=pb.KeyHint(weak_hash=0))

    def getContainer(self, request, context):
        stored = self.containers[request.key.payload.payload]
        batch = pb.BatchValueResponse()
        if stored.type == pb.ORDERED_SET:
            batch.value_ordered.extend(sorted(stored.value_ordered, key=lambda value: value.order))
        else:
            for index in sorted(range(len(stored.key_ordered)), key=lambda i: stored.key_ordered[i].order):
                batch.key_ordered.append(stored.key_ordered[index])
                batch.value_unordered.append(stored.value_unordered[index])
        yield batch

    def updateValueInContainer(self, request, context):
        stored = self.containers[request.key.payload.payload]
        assert request.WhichOneof("keys") == "ordered_element_key"
        selector = request.ordered_element_key
        members = stored.value_ordered if stored.type == pb.ORDERED_SET else stored.key_ordered
        for member in members:
            if member.order != selector.order or self.raw(member) != self.raw(selector):
                continue
            target = member.order + request.delta_order if request.WhichOneof("values") == "delta_order" else request.new_order
            if not 0 <= target < (1 << 64):
                context.abort(grpc.StatusCode.OUT_OF_RANGE, "order underflow/overflow")
            member.order = target
            return pb.UpdateValueResponse(result=True)
        context.abort(grpc.StatusCode.NOT_FOUND, "exact member/order does not exist")


@pytest.fixture(params=[(form, codec) for form in ("sync", "async", "smart", "async_smart") for codec in ("gzip", "lz4")])
async def ordered_client(request):
    form, codec = request.param
    if codec == "lz4":
        pytest.importorskip("lz4.block")
    server, port = _server(OrderCache(codec), rpc.add_HurriCacheGrpcServiceServicer_to_server)
    coordinator, coordinator_port = _server(Coordinator(cp.RoutingInfoData(max_shards=1, peerRouting=[
        cp.PeerRouting(target=cp.Endpoint(ip="127.0.0.1", port=port), role=cp.MASTER, partitionIds=[0])])),
        crpc.add_CoordinatorServiceServicer_to_server)
    cls = {"sync": HurriCacheClient, "async": AsyncHurriCacheClient,
           "smart": HurriCacheSmartClient, "async_smart": AsyncHurriCacheSmartClient}[form]
    client = cls("127.0.0.1", coordinator_port if "smart" in form else port,
                 payload_compression=codec, default_compression_threshold=8, default_timeout=3)
    try:
        yield client
    finally:
        closed = client.close()
        if inspect.isawaitable(closed):
            await closed
        server.stop(0).wait()
        coordinator.stop(0).wait()


@pytest.mark.parametrize("family", ["ordered_map", "ordered_set"])
async def test_java_order_scenarios_over_grpc(ordered_client, family):
    client = ordered_client
    member = b"duplicate-member" * 100
    options = {"values": [1.25, 2]}
    if family == "ordered_map":
        options["keys"] = [OrderedPayload(member, 10), OrderedPayload(member, 20)]
    else:
        options["values"] = [OrderedVariant.of_bytes(member, 10), OrderedVariant.of_bytes(member, 20)]
    hint = await invoke(client, "create_" + family, b"k", **options)
    async def snapshot():
        return await invoke(client, "get_" + family, b"k", hint)
    before = await snapshot()
    with pytest.raises(KeyNotFoundError):
        await invoke(client, "increment_order", b"k", hint, OrderedPayload(member, 999), 1)
    with pytest.raises(HurriCacheRpcError) as caught:
        await invoke(client, "increment_order", b"k", hint, OrderedPayload(member, 10), -11)
    assert caught.value.code == grpc.StatusCode.OUT_OF_RANGE
    assert await snapshot() == before
    assert await invoke(client, "increment_order", b"k", hint, OrderedPayload(member, 10), 5) is True
    assert await invoke(client, "update", b"k", hint, OrderedPayload(member, 20), 99) is True
    actual = await snapshot()
    assert [item.order for item in actual] == [15, 99]
    if family == "ordered_map":
        assert list(actual.values()) == [Variant.of_double(1.25), Variant.of_long(2)]
    else:
        assert [item.require_bytes() for item in actual] == [member, member]
    assert await invoke(client, "increment_order", b"k", hint, OrderedPayload(member, 15), -15) is True
    assert [item.order for item in await snapshot()] == [0, 99]
    assert await invoke(client, "update_order", b"k", hint, OrderedPayload(member, 99), (1 << 64)-1) is True
    before = await snapshot()
    with pytest.raises(HurriCacheRpcError) as caught:
        await invoke(client, "increment_order", b"k", hint, OrderedPayload(member, (1 << 64)-1), 1)
    assert caught.value.code == grpc.StatusCode.OUT_OF_RANGE and await snapshot() == before
