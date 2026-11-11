"""Actual gRPC serialization of current typed messages through all four clients."""

from concurrent import futures

import grpc
import pytest
from test_parity import invoke
from test_smart_clients import Coordinator

from hurricache import (
    AsyncHurriCacheClient,
    AsyncHurriCacheSmartClient,
    HurriCacheClient,
    HurriCacheSmartClient,
    Variant,
)
from hurricache.grpc import cache_pb2 as pb
from hurricache.grpc import cache_pb2_grpc as rpc
from hurricache.grpc import coordinator_pb2 as cp
from hurricache.grpc import coordinator_pb2_grpc as crpc


class TypedCache(rpc.HurriCacheGrpcServiceServicer):
    def __init__(self):
        self.scalars = {}
        self.containers = {}

    def createKeyValue(self, request, context):
        self.scalars[request.key.payload.payload] = request.value
        return pb.KeyHintResponse(keyHint=pb.KeyHint(weak_hash=0, strong_hash=1), relayed=True)

    def getValue(self, request, context):
        return pb.ValueResponse(value_unordered=self.scalars[request.key.payload.payload])

    def createContainer(self, request, context):
        self.containers[request.key.payload.payload] = list(request.value_unordered)
        return pb.KeyHintResponse(keyHint=pb.KeyHint(weak_hash=0, strong_hash=1))

    def getContainer(self, request, context):
        values = self.containers[request.key.payload.payload]
        for start in range(0, len(values), 2):
            yield pb.BatchValueResponse(value_unordered=values[start:start+2])

    def atomicAdd(self, request, context):
        key = request.key.payload.payload
        assert request.container_key.WhichOneof("key") == "pos"
        position = request.container_key.pos
        original = self.containers[key][position]
        # This oracle uses raw fields, independently of the Python decoder.
        if request.value.value_type == pb.DOUBLE:
            result = pb.Value(value=pb.BinaryPayload(doublePayload=original.value.doublePayload + request.value.value.doublePayload), value_type=pb.DOUBLE)
        else:
            result = pb.Value(value=pb.BinaryPayload(intPayload=original.value.intPayload + request.value.value.intPayload), value_type=pb.INT)
        self.containers[key][position] = result
        return pb.ValueResponse(value_unordered=original)

    def atomicCompareAndSet(self, request, context):
        actual = self.scalars[request.key.payload.payload]
        return pb.AtomicCasRes(result=False, expected=pb.ValueResponse(value_unordered=actual, keyHint=pb.KeyHint(strong_hash=0)))

    def getAndDeleteElementsInRange(self, request, context):
        values = self.containers[request.key.payload.payload]
        start = request.pos if request.HasField("pos") else 0
        end = request.end if request.HasField("end") else len(values)
        extracted = values[start:end]
        del values[start:end]
        yield pb.BatchValueResponse(value_unordered=extracted)

    def getElementAtPosition(self, request, context):
        assert request.pos == 2
        return pb.ValueResponse(value_ordered=pb.OrderedValue(
            value=pb.BinaryPayload(doublePayload=-0.0), value_type=pb.DOUBLE, order=2))

    getAndDeleteElementAtPosition = getElementAtPosition


@pytest.mark.parametrize("form", ["sync", "async", "smart", "async_smart"])
@pytest.mark.parametrize("codec", ["gzip", "lz4"])
async def test_real_grpc_typed_roundtrips(form, codec):
    if codec == "lz4":
        pytest.importorskip("lz4.block")
    cache = TypedCache()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    rpc.add_HurriCacheGrpcServiceServicer_to_server(cache, server)
    port = server.add_insecure_port("127.0.0.1:0")
    coordinator = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    crpc.add_CoordinatorServiceServicer_to_server(Coordinator(cp.RoutingInfoData(max_shards=1, peerRouting=[
        cp.PeerRouting(target=cp.Endpoint(ip="127.0.0.1", port=port), role=cp.MASTER, partitionIds=[0])])), coordinator)
    coordinator_port = coordinator.add_insecure_port("127.0.0.1:0")
    server.start()
    coordinator.start()
    asynchronous = form.startswith("async")
    cls = {"sync": HurriCacheClient, "async": AsyncHurriCacheClient,
           "smart": HurriCacheSmartClient, "async_smart": AsyncHurriCacheSmartClient}[form]
    client = cls("127.0.0.1", coordinator_port if "smart" in form else port, default_timeout=3,
                 payload_compression=codec, default_compression_threshold=8)
    try:
        if "smart" in form:
            ready = client.wait_until_ready(3)
            if asynchronous:
                await ready
        values = [Variant.none(), Variant.of_bytes(b""), Variant.of_long(0), Variant.of_double(-0.0),
                  Variant.of_long(-(1 << 63)), Variant.of_double(1.25), Variant.of_bytes(b"wire" * 1000)]
        for value in values:
            hint = await invoke(client, "create", "scalar", value=value)
            assert hint.was_relayed
            assert await invoke(client, "get", "scalar", hint) == value
        await invoke(client, "create", "scalar", value=1.25)
        observed = await invoke(client, "compare_and_set", "scalar", expected_value=0, new_value=2)
        assert observed.expected_value == 1.25 and type(observed.expected_value) is float
        assert observed.hint.strong_hash == 0 and observed.hint.weak_hash is None
        hint = await invoke(client, "create_list", "items", values=values)
        assert await invoke(client, "get_list", "items", hint) == values
        assert await invoke(client, "add_in_container", "items", hint, selector=2, delta=7) == 0
        removed = await invoke(client, "get_and_delete_list_elements_in_range", "items", hint, pos=1, end=3)
        assert removed == [Variant.of_bytes(b""), Variant.of_long(7)]
        assert await invoke(client, "get_list", "items", hint) == [values[0], *values[3:]]
        for method in ("get_element_with_weight", "get_and_remove_element_with_weight"):
            actual = await invoke(client, method, "ranked", pos=2)
            assert type(actual) is Variant and actual == Variant.of_double(-0.0)
    finally:
        closed = client.close()
        if asynchronous:
            await closed
        server.stop(0).wait()
        coordinator.stop(0).wait()
