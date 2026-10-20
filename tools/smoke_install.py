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

assert hurricache.__version__ == "0.1.0"
assert hurricache.HurriCacheClient()._port == 50000
assert cache_pb2.Key is not None
assert coordinator_pb2.RoutingInfoData is not None
assert grpc.__version__
assert protobuf_version
assert gzip.decompress(gzip.compress(b"smoke")) == b"smoke"
assert cache_pb2.DESCRIPTOR.services_by_name["HurriCacheGrpcService"].methods_by_name["addElementToHead"].output_type.name == "IntResponse"
assert inspect.iscoroutinefunction(hurricache.AsyncHurriCacheClient.add_element_to_head_count)
assert tenacity.stop_after_attempt(1)
with hurricache.HurriCacheClient(default_client_id=77, zero_client_id_uses_default=False) as client:
    assert client._resolve_client_id(0) == 0 and client._resolve_client_id(None) == 77
    assert JedisPool(client).get_resource().ping() == "PONG"
if "--lz4" in sys.argv:
    import lz4.block

    with operation(hurricache.HurriCacheClient(payload_compression="lz4"), 2):
        value = create_value(b"installed-wheel" * 1000, compress=True)
        assert lz4.block.decompress(value.value.payload, uncompressed_size=value.compressionInfo.rawSize) == b"installed-wheel" * 1000
        assert decode_value(value) == b"installed-wheel" * 1000
else:
    assert importlib.util.find_spec("lz4") is None
    try:
        hurricache.HurriCacheClient(payload_compression="lz4")
    except ImportError as error:
        assert "hurricache[lz4]" in str(error)
    else:
        raise AssertionError("LZ4 must require the optional extra")
print("installed HurriCache smoke test passed")
