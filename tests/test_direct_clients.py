from __future__ import annotations

import random
import time
from concurrent import futures

import grpc
import pytest

from hurricache import AsyncHurriCacheClient, HurriCacheClient, LockStatus, LockType, Payload
from hurricache.grpc import cache_pb2, cache_pb2_grpc
from hurricache.grpc.exceptions import DeadlineExceededError
from hurricache.grpc.utils import create_value


class CacheService(cache_pb2_grpc.HurriCacheGrpcServiceServicer):
    def __init__(self) -> None:
        self.created: list[cache_pb2.CreateRequest] = []
        self.lock_requests: list[cache_pb2.LockRequest] = []
        self.deadlines: list[float] = []
        self.container_batches: list[cache_pb2.CreateContainerRequest | cache_pb2.AddToRequest] = []

    def createKeyValue(self, request, context):
        self.created.append(request)
        self.deadlines.append(context.time_remaining())
        return cache_pb2.KeyHintResponse(keyHint=cache_pb2.KeyHint(week_hash=4, strong_hash=8))

    def getValue(self, request, context):
        return cache_pb2.ValueResponse(value_unordered=create_value(b"x" * 2048))

    def getContainer(self, request, context):
        yield cache_pb2.BatchValueResponse(value_unordered=[create_value(b"a"), create_value(b"b")])

    def lockObject(self, request, context):
        self.lock_requests.append(request)
        return cache_pb2.LockResponse(result=cache_pb2.CANT_LOCK, message="busy")

    def getTtl(self, request, context):
        return cache_pb2.TtlResponse(ttl=int(time.time() * 1000) + 500)

    def getSize(self, request, context):
        time.sleep(0.05)
        return cache_pb2.IntResponse(size=1)

    def createContainer(self, request, context):
        self.container_batches.append(request)
        return cache_pb2.KeyHintResponse(keyHint=cache_pb2.KeyHint(week_hash=1, strong_hash=2))

    def addElement(self, request, context):
        self.container_batches.append(request)
        return cache_pb2.IntResponse(size=len(request.value_unordered) + len(request.value_ordered))


@pytest.fixture()
def cache_server():
    service = CacheService()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    cache_pb2_grpc.add_HurriCacheGrpcServiceServicer_to_server(service, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        yield service, port
    finally:
        server.stop(0).wait()


def test_sync_client_deadline_compression_models_and_lock_units(cache_server) -> None:
    service, port = cache_server
    with HurriCacheClient("127.0.0.1", port, default_timeout=0.5) as client:
        hint = client.create_key_value("ключ", value=b"z" * 2048)
        assert (hint.week_hash, hint.strong_hash) == (4, 8)
        assert service.created[0].value.compressionInfo.enabled
        assert service.created[0].key.payload.payload == "ключ".encode()
        assert 0 < service.deadlines[0] <= 0.5
        assert client.get_value("key") == b"x" * 2048
        assert client.get_container("items") == [Payload(b"a"), Payload(b"b")]
        assert client.lock_object("key", lock_type=LockType.WRITE_LOCK, lock_duration=1.25) is LockStatus.CANT_LOCK
        assert service.lock_requests[0].lockDuration == 1250
        assert 0 <= client.get_ttl("key") <= 500
        with pytest.raises(DeadlineExceededError):
            client.get_size("slow", timeout=0.001)


def test_map_pair_validation_happens_before_rpc(cache_server) -> None:
    service, port = cache_server
    with HurriCacheClient("127.0.0.1", port) as client:
        with pytest.raises(ValueError, match="equal lengths"):
            client.create_map("map", keys=[b"a"], values=[])
    assert not service.created


async def test_async_client_uses_aio_and_decodes_stream(cache_server) -> None:
    service, port = cache_server
    async with AsyncHurriCacheClient("127.0.0.1", port, default_timeout=0.5) as client:
        hint = await client.create_key_value("key", value=b"value")
        assert hint.week_hash == 4
        assert await client.get_value("key") == b"x" * 2048
        assert await client.get_container("items") == [Payload(b"a"), Payload(b"b")]
        assert await client.lock_object("key", lock_duration=2) is LockStatus.CANT_LOCK
        with pytest.raises(DeadlineExceededError):
            await client.get_size("slow", timeout=0.001)
    assert service.lock_requests[-1].lockDuration == 2000


def test_large_initial_container_is_split_into_bounded_requests(cache_server) -> None:
    service, port = cache_server
    generator = random.Random(42)
    values = [generator.randbytes(1_300_000) for _ in range(3)]
    with HurriCacheClient("127.0.0.1", port, default_timeout=2) as client:
        client.create_vector("large", values=values)
    assert len(service.container_batches) == 2
    assert sum(len(item.value_unordered) for item in service.container_batches) == 3
    assert all(item.ByteSize() < 4 * 1024 * 1024 - 512 * 1024 for item in service.container_batches)
