"""Smoke test an installed distribution without importing the source checkout."""

from __future__ import annotations

import grpc
import lz4.block
import tenacity
from google.protobuf import __version__ as protobuf_version

import hurricache
from hurricache.grpc import cache_pb2, coordinator_pb2

assert hurricache.__version__ == "0.1.0"
assert hurricache.HurriCacheClient()._port == 50000
assert cache_pb2.Key is not None
assert coordinator_pb2.RoutingInfoData is not None
assert grpc.__version__
assert protobuf_version
assert lz4.block.compress(b"smoke")
assert tenacity.stop_after_attempt(1)
print("installed HurriCache smoke test passed")
