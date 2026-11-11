"""Smoke test an installed distribution without importing the source checkout."""

from __future__ import annotations

import gzip
import importlib.util
import inspect
import sys

import grpc
import tenacity
from google.protobuf import __version__ as protobuf_version

import hurricache
from hurricache.grpc import cache_pb2, coordinator_pb2
from hurricache.grpc.operation import operation
from hurricache.grpc.utils import create_value, decode_value
from hurricache.jedis import JedisPool

assert hurricache.__version__ == "0.2.0"
assert hurricache.Variant.of_long(0) != hurricache.Variant.of_double(0)
assert hurricache.Variant.of_double(0) != hurricache.Variant.of_double(-0.0)
assert hurricache.KeyHintData(1, 2, True) == hurricache.KeyHintData(week_hash=1, strong_hash=2)
assert hurricache.OrderedVariant.of_int(0).order is None
for item in (hurricache.Variant.none(), hurricache.Variant.of_bytes(b""), hurricache.Variant.of_long(0),
             hurricache.Variant.of_double(-0.0), hurricache.Variant.of_long(-(1 << 63))):
    assert decode_value(create_value(item, compress=True)) == item
assert hurricache.HurriCacheClient()._port == 50000
assert cache_pb2.Key is not None
assert coordinator_pb2.RoutingInfoData is not None
assert grpc.__version__
assert protobuf_version
assert gzip.decompress(gzip.compress(b"smoke")) == b"smoke"
assert cache_pb2.DESCRIPTOR.services_by_name["HurriCacheGrpcService"].methods_by_name["addElementToHead"].output_type.name == "IntResponse"
assert inspect.iscoroutinefunction(hurricache.AsyncHurriCacheClient.push_head)
assert inspect.iscoroutinefunction(hurricache.AsyncHurriCacheClient.exchange_in_container)
assert inspect.iscoroutinefunction(hurricache.AsyncHurriCacheClient.compare_and_set_in_container)
assert cache_pb2.AtomicCas.DESCRIPTOR.fields_by_name["element_key"].number == 4
assert cache_pb2.AtomicCas.DESCRIPTOR.fields_by_name["ordered_element_key"].number == 5
assert inspect.iscoroutinefunction(hurricache.AsyncHurriCacheClient.increment_order)
assert inspect.iscoroutinefunction(hurricache.AsyncHurriCacheClient.update_order)
assert cache_pb2.UpdateContainerRequest.DESCRIPTOR.fields_by_name["value"].number == 4
assert cache_pb2.UpdateContainerRequest(new_order=0).WhichOneof("values") == "new_order"
assert cache_pb2.UpdateContainerRequest(delta_order=-5).WhichOneof("values") == "delta_order"
assert cache_pb2.DESCRIPTOR.services_by_name["HurriCacheGrpcService"].methods_by_name["getAndDeleteElementsInRange"].server_streaming
assert tenacity.stop_after_attempt(1)
with hurricache.HurriCacheClient(default_client_id=77, zero_client_id_uses_default=False) as client:
    assert client._client_id(0) == 0 and client._client_id(None) == 77
    assert JedisPool(client).get_resource().ping() == "PONG"
    class OrderStub:
        def __init__(self):
            self.requests = []

        def updateValueInContainer(self, request, **kwargs):
            self.requests.append(cache_pb2.UpdateContainerRequest.FromString(request.SerializeToString()))
            return cache_pb2.UpdateValueResponse(result=True)
    stub = OrderStub()
    client._stub = stub
    assert client.update_order("key", element_key=hurricache.OrderedPayload(b"member", 10), new_order=0)
    assert client.increment_order("key", element_key=hurricache.OrderedPayload(b"member", 10), delta=-5)
    assert stub.requests[0].WhichOneof("keys") == "ordered_element_key"
    assert stub.requests[0].WhichOneof("values") == "new_order" and stub.requests[0].new_order == 0
    assert stub.requests[1].WhichOneof("values") == "delta_order" and stub.requests[1].delta_order == -5
    assert stub.requests[0].key.clientId == stub.requests[0].ordered_element_key.clientId == 77
if "--lz4" in sys.argv:
    import lz4.block

    with operation(hurricache.HurriCacheClient(payload_compression="lz4"), 2):
        value = create_value(b"installed-wheel" * 1000, compress=True)
        assert lz4.block.decompress(value.value.payload, uncompressed_size=value.compressionInfo.rawSize) == b"installed-wheel" * 1000
        assert decode_value(value).require_bytes() == b"installed-wheel" * 1000
else:
    assert importlib.util.find_spec("lz4") is None
    try:
        hurricache.HurriCacheClient(payload_compression="lz4")
    except ImportError as error:
        assert "hurricache[lz4]" in str(error)
    else:
        raise AssertionError("LZ4 must require the optional extra")
print("installed HurriCache smoke test passed")
