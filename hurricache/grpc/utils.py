"""Wire-level helpers shared by synchronous and asynchronous clients."""

from __future__ import annotations

import time
from collections.abc import Iterable
from typing import TypeVar

import lz4.block

from hurricache.grpc import cache_pb2
from hurricache.grpc.models import KeyHintData, OrderedPayload, Payload

COMPRESSION_THRESHOLD = 1024
MAX_RPC_SIZE = 4 * 1024 * 1024 - 512 * 1024
KeyLike = bytes | str
ValueLike = bytes | Payload
T = TypeVar("T")


def as_bytes(value: bytes | bytearray | memoryview | str, *, name: str = "value") -> bytes:
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    raise TypeError(f"{name} must be bytes or str")


def _set_hint(target: object, hint: KeyHintData | None) -> None:
    if hint is None:
        return
    key_hint = target.keyHint
    if hint.week_hash is not None:
        key_hint.week_hash = hint.week_hash & 0xFFFFFFFF
    if hint.strong_hash is not None:
        key_hint.strong_hash = hint.strong_hash & 0xFFFFFFFF


def _compressed(data: bytes) -> tuple[bytes, int | None]:
    if len(data) <= COMPRESSION_THRESHOLD:
        return data, None
    return lz4.block.compress(data, store_size=False), len(data)


def create_key(key: KeyLike, hint: KeyHintData | None = None, client_id: int = 0) -> cache_pb2.Key:
    raw = as_bytes(key, name="key")
    body, raw_size = _compressed(raw)
    result = cache_pb2.Key(payload=cache_pb2.KeyBinaryPayload(payload=body, size=len(body)))
    result.clientId = client_id
    if raw_size is not None:
        result.compressionInfo.enabled = True
        result.compressionInfo.rawSize = raw_size
    _set_hint(result, hint)
    return result


def create_value(value: bytes | bytearray | memoryview | Payload, ttl: int = 0, client_id: int = 0) -> cache_pb2.Value:
    raw = value.value if isinstance(value, Payload) else as_bytes(value)
    body, raw_size = _compressed(raw)
    result = cache_pb2.Value(value=cache_pb2.BinaryPayload(payload=body, size=len(body)))
    if raw_size is not None:
        result.compressionInfo.enabled = True
        result.compressionInfo.rawSize = raw_size
    if ttl > 0:
        result.ttl = int(time.time() * 1000) + ttl
    # Java always attaches lock ownership, including client id zero.
    result.lock_info.type = cache_pb2.NO_LOCK
    result.lock_info.lockedBy = client_id
    return result


def create_ordered_value(
    value: bytes | bytearray | memoryview | Payload,
    order: int = 0,
    ttl: int = 0,
    client_id: int = 0,
) -> cache_pb2.OrderedValue:
    if isinstance(value, OrderedPayload):
        order = value.order
        raw = value.value
    else:
        raw = value.value if isinstance(value, Payload) else as_bytes(value)
    body, raw_size = _compressed(raw)
    result = cache_pb2.OrderedValue(value=cache_pb2.BinaryPayload(payload=body, size=len(body)), order=order)
    if raw_size is not None:
        result.compressionInfo.enabled = True
        result.compressionInfo.rawSize = raw_size
    if ttl > 0:
        result.ttl = int(time.time() * 1000) + ttl
    result.lock_info.type = cache_pb2.NO_LOCK
    result.lock_info.lockedBy = client_id
    return result


def create_ordered_key(
    key: KeyLike | OrderedPayload,
    order: int = 0,
    ttl: int = 0,
    client_id: int = 0,
) -> cache_pb2.OrderedKey:
    del ttl  # OrderedKey has no TTL field; retained for source compatibility.
    if isinstance(key, OrderedPayload):
        order, raw = key.order, key.value
    else:
        raw = as_bytes(key, name="key")
    body, raw_size = _compressed(raw)
    result = cache_pb2.OrderedKey(payload=cache_pb2.KeyBinaryPayload(payload=body, size=len(body)), order=order)
    result.clientId = client_id
    if raw_size is not None:
        result.compressionInfo.enabled = True
        result.compressionInfo.rawSize = raw_size
    return result


def build_get_request(key: KeyLike, hint: KeyHintData | None = None, client_id: int = 0) -> cache_pb2.GetRequest:
    return cache_pb2.GetRequest(key=create_key(key, hint, client_id))


def decode_value(value: cache_pb2.Value | cache_pb2.OrderedValue) -> bytes:
    data = bytes(value.value.payload)
    if value.HasField("compressionInfo") and value.compressionInfo.enabled:
        if not value.compressionInfo.HasField("rawSize"):
            raise ValueError("compressed payload is missing rawSize")
        return lz4.block.decompress(data, uncompressed_size=value.compressionInfo.rawSize)
    return data


def decode_key(key: cache_pb2.Key | cache_pb2.OrderedKey) -> bytes:
    data = bytes(key.payload.payload)
    if key.HasField("compressionInfo") and key.compressionInfo.enabled:
        if not key.compressionInfo.HasField("rawSize"):
            raise ValueError("compressed key is missing rawSize")
        return lz4.block.decompress(data, uncompressed_size=key.compressionInfo.rawSize)
    return data


def decode_hint(message: object, field: str = "keyHint") -> KeyHintData | None:
    if not message.HasField(field):
        return None
    hint = getattr(message, field)
    return KeyHintData(
        week_hash=hint.week_hash if hint.HasField("week_hash") else None,
        strong_hash=hint.strong_hash if hint.HasField("strong_hash") else None,
    )


def decode_batch(batch: cache_pb2.BatchValueResponse):
    """Decode a stream batch without leaking protobuf objects."""
    if batch.key_ordered:
        if len(batch.key_ordered) != len(batch.value_unordered):
            raise ValueError("ordered-map batch contains unpaired keys and values")
        return {
            OrderedPayload(decode_key(key), key.order): Payload(decode_value(value))
            for key, value in zip(batch.key_ordered, batch.value_unordered)
        }
    if batch.key_unordered:
        if len(batch.key_unordered) != len(batch.value_unordered):
            raise ValueError("map batch contains unpaired keys and values")
        return {
            Payload(decode_key(key)): Payload(decode_value(value))
            for key, value in zip(batch.key_unordered, batch.value_unordered)
        }
    if batch.value_ordered:
        return [OrderedPayload(decode_value(value), value.order) for value in batch.value_ordered]
    return [Payload(decode_value(value)) for value in batch.value_unordered]


def chunk_messages(base_size: int, messages: Iterable[T], size_of=lambda item: item.ByteSize()) -> list[list[T]]:
    chunks: list[list[T]] = [[]]
    size = base_size
    for message in messages:
        message_size = int(size_of(message))
        if message_size + base_size > MAX_RPC_SIZE:
            raise ValueError("one element exceeds the maximum HurriCache request size")
        if chunks[-1] and size + message_size > MAX_RPC_SIZE:
            chunks.append([])
            size = base_size
        chunks[-1].append(message)
        size += message_size
    return chunks
