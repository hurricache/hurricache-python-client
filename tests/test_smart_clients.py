from __future__ import annotations

from concurrent import futures

import grpc

from hurricache import (
    AsyncHurriCacheSmartClient,
    HurriCacheSmartClient,
    KeyHintData,
    Mode,
)
from hurricache.grpc import cache_pb2, cache_pb2_grpc, coordinator_pb2, coordinator_pb2_grpc
from hurricache.grpc.utils import create_value


class MarkerCache(cache_pb2_grpc.HurriCacheGrpcServiceServicer):
    def __init__(self, marker: bytes, unavailable: bool = False) -> None:
        self.marker = marker
        self.unavailable = unavailable

    def getValue(self, request, context):
        if self.unavailable:
            context.abort(grpc.StatusCode.UNAVAILABLE, "down")
        return cache_pb2.ValueResponse(value_unordered=create_value(self.marker))


class Coordinator(coordinator_pb2_grpc.CoordinatorServiceServicer):
    def __init__(self, topology: coordinator_pb2.RoutingInfoData) -> None:
        self.topology = topology
        self.calls = 0

    def provideGlobalRoutingInfo(self, request, context):
        self.calls += 1
        yield self.topology


def _server(servicer, register):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    register(servicer, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    return server, port


def _cluster(master_unavailable: bool = False):
    master = MarkerCache(b"master", unavailable=master_unavailable)
    backup = MarkerCache(b"backup")
    master_server, master_port = _server(master, cache_pb2_grpc.add_HurriCacheGrpcServiceServicer_to_server)
    backup_server, backup_port = _server(backup, cache_pb2_grpc.add_HurriCacheGrpcServiceServicer_to_server)
    topology = coordinator_pb2.RoutingInfoData(
        max_shards=2,
        peerRouting=[
            coordinator_pb2.PeerRouting(
                target=f"127.0.0.1:{master_port}", role=coordinator_pb2.MASTER, partitionIds=[0, 1]
            ),
            coordinator_pb2.PeerRouting(
                target=f"127.0.0.1:{backup_port}", role=coordinator_pb2.BACKUP, partitionIds=[0, 1]
            ),
        ],
    )
    coordinator = Coordinator(topology)
    coordinator_server, coordinator_port = _server(
        coordinator, coordinator_pb2_grpc.add_CoordinatorServiceServicer_to_server
    )
    return (master_server, backup_server, coordinator_server), coordinator, coordinator_port


def test_sync_smart_readiness_modes_unsigned_hint_and_cleanup() -> None:
    servers, coordinator, port = _cluster()
    client = HurriCacheSmartClient("127.0.0.1", port, default_timeout=1, refresh_interval=60)
    try:
        client.wait_until_ready(2)
        assert coordinator.calls == 1
        assert client.get_value("key", KeyHintData(week_hash=-1)) == b"master"
        with client.mode(Mode.BACKUP):
            assert client.get_value("key", KeyHintData(week_hash=0)) == b"backup"
        assert client.get_value("key", KeyHintData(week_hash=0)) == b"master"
    finally:
        client.close()
        for server in servers:
            server.stop(0).wait()


def test_sync_smart_unavailable_master_falls_back_to_backup() -> None:
    servers, _, port = _cluster(master_unavailable=True)
    client = HurriCacheSmartClient("127.0.0.1", port, default_timeout=1, refresh_interval=60)
    try:
        client.wait_until_ready(2)
        assert client.get_value("key", KeyHintData(week_hash=0)) == b"backup"
    finally:
        client.close()
        for server in servers:
            server.stop(0).wait()


async def test_async_smart_discovers_and_routes() -> None:
    servers, _, port = _cluster()
    client = AsyncHurriCacheSmartClient("127.0.0.1", port, default_timeout=1, refresh_interval=60)
    try:
        await client.wait_until_ready(2)
        client.set_mode(Mode.BACKUP)
        assert await client.get_value("key", KeyHintData(week_hash=0xFFFFFFFF)) == b"backup"
        client.clear_mode()
        assert await client.get_value("key", KeyHintData(week_hash=1)) == b"master"
    finally:
        await client.close()
        for server in servers:
            server.stop(0).wait()
