"""Wire-level helpers shared by synchronous and asynchronous clients."""

from __future__ import annotations

import gzip
import io
import zlib
from collections.abc import Iterable
from typing import TypeVar

from hurricache.grpc import cache_pb2
from hurricache.grpc.models import CasResult, KeyHintData, OrderedPayload, OrderedVariant, Payload, Variant
from hurricache.grpc.operation import absolute_ttl, payload_codec, threshold

COMPRESSION_THRESHOLD = 1024
MAX_RPC_SIZE = 4 * 1024 * 1024 - 512 * 1024
MAX_DECODED_BYTES = 64 * 1024 * 1024
KeyLike = bytes | str
ValueLike = bytes | Payload | Variant | int | float
T = TypeVar("T")


def as_bytes(value: bytes | bytearray | memoryview | str, *, name: str = "value") -> bytes:
    if isinstance(value, Payload):
        return value.value
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    raise TypeError(f"{name} must be bytes or str")


def _set_hint(target: object, hint: KeyHintData | None) -> None:
    if hint is None:
        return
    key_hint = target.keyHint
    key_hint.SetInParent()
    if hint.weak_hash is not None:
        key_hint.weak_hash = hint.weak_hash & 0xFFFFFFFF
    if hint.strong_hash is not None:
        key_hint.strong_hash = hint.strong_hash & 0xFFFFFFFF


def _compressed(data: bytes, compress: bool = True) -> tuple[bytes, int | None]:
    if not compress or len(data) <= threshold.get():
        return data, None
    if payload_codec.get() == "lz4":
        import lz4.block

        return lz4.block.compress(data, store_size=False), len(data)
    return gzip.compress(data, mtime=0), len(data)


def create_key(key: KeyLike, hint: KeyHintData | None = None, client_id: int = 0, *, compress: bool = True) -> cache_pb2.Key:
    raw = as_bytes(key, name="key")
    body, raw_size = _compressed(raw, compress)
    result = cache_pb2.Key(payload=cache_pb2.KeyBinaryPayload(payload=body, size=len(body)))
    result.clientId = client_id
    if raw_size is not None:
        result.compressionInfo.enabled = True
        result.compressionInfo.rawSize = raw_size
    _set_hint(result, hint)
    return result


def as_variant(value) -> Variant:
    if isinstance(value, Variant):
        return value
    if isinstance(value, bool):
        raise TypeError("bool is not a typed numeric value")
    if isinstance(value, int):
        return Variant.of_long(value)
    if isinstance(value, float):
        return Variant.of_double(value)
    return Variant.of_bytes(as_bytes(value))


def create_value(value, ttl: int = 0, client_id: int | None = None, *, compress: bool = False) -> cache_pb2.Value:
    variant = as_variant(value)
    result = cache_pb2.Value(value_type=int(variant.type))
    if variant.is_bytes:
        body, raw_size = _compressed(variant.require_bytes(), compress)
        result.value.CopyFrom(cache_pb2.BinaryPayload(payload=body, size=len(body)))
        if raw_size is not None:
            result.compressionInfo.enabled = True
            result.compressionInfo.rawSize = raw_size
    elif variant.is_int:
        result.value.intPayload = variant.require_long()
    elif variant.is_double:
        result.value.doublePayload = variant.require_double()
    if ttl > 0:
        result.ttl = absolute_ttl(ttl)
    if client_id is not None:
        result.lock_info.type = cache_pb2.NO_LOCK
        result.lock_info.lockedBy = client_id
    return result


def create_ordered_value(
    value,
    order: int | None = None,
    ttl: int = 0,
    client_id: int | None = None,
) -> cache_pb2.OrderedValue:
    if isinstance(value, OrderedVariant):
        order, value = value.order, value.variant
    elif isinstance(value, OrderedPayload):
        order, value = value.order, value.value
    unordered = create_value(value, ttl, client_id, compress=True)
    result = cache_pb2.OrderedValue(order=0 if order is None else order)
    # Field numbers differ between Value and OrderedValue.
    for descriptor, field_value in unordered.ListFields():
        if descriptor.message_type is not None:
            getattr(result, descriptor.name).CopyFrom(field_value)
        else:
            setattr(result, descriptor.name, field_value)
    return result


def create_ordered_key(
    key: KeyLike | OrderedPayload,
    order: int | None = 0,
    ttl: int = 0,
    client_id: int = 0,
) -> cache_pb2.OrderedKey:
    del ttl  # OrderedKey has no TTL field; retained for source compatibility.
    if isinstance(key, OrderedPayload):
        order, raw = key.order, key.value
    else:
        raw = as_bytes(key, name="key")
    body, raw_size = _compressed(raw)
    result = cache_pb2.OrderedKey(payload=cache_pb2.KeyBinaryPayload(payload=body, size=len(body)), order=0 if order is None else order)
    result.clientId = client_id
    if raw_size is not None:
        result.compressionInfo.enabled = True
        result.compressionInfo.rawSize = raw_size
    return result


def build_get_request(key: KeyLike, hint: KeyHintData | None = None, client_id: int = 0) -> cache_pb2.GetRequest:
    return cache_pb2.GetRequest(key=create_key(key, hint, client_id))


def _decode(payload, info) -> bytes:
    field = "non_cord_payload" if isinstance(payload, cache_pb2.BinaryPayload) and payload.WhichOneof("data") == "non_cord_payload" else "payload"
    data = bytes(getattr(payload, field))
    if len(data) != payload.size:
        raise ValueError("payload size metadata does not match bytes")
    if not info.enabled:
        if len(data) > MAX_DECODED_BYTES:
            raise ValueError("payload exceeds decoded-size limit")
        return data
    if not info.HasField("rawSize") or info.rawSize <= 0:
        raise ValueError("compressed payload lacks a positive rawSize")
    if info.rawSize > MAX_DECODED_BYTES:
        raise ValueError("payload exceeds decoded-size limit")
    if payload_codec.get() == "lz4":
        import lz4.block

        try:
            decoded = lz4.block.decompress(data, uncompressed_size=info.rawSize)
        except lz4.block.LZ4BlockError as error:
            raise ValueError("invalid LZ4 payload") from error
    else:
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(data)) as stream:
                decoded = stream.read(info.rawSize + 1)
        except (OSError, EOFError, zlib.error) as error:
            raise ValueError("invalid GZIP payload") from error
    if len(decoded) != info.rawSize:
        raise ValueError("decoded size does not match rawSize")
    return decoded


def decode_value(value: cache_pb2.Value | cache_pb2.OrderedValue) -> Variant:
    payload = value.value
    kind = payload.WhichOneof("data")
    # Match Java's explicit numeric type precedence, including absent oneofs.
    if value.value_type == cache_pb2.INT:
        return Variant.of_long(payload.intPayload)
    if value.value_type == cache_pb2.DOUBLE:
        return Variant.of_double(payload.doublePayload)
    if kind == "intPayload":
        return Variant.of_long(payload.intPayload)
    if kind == "doublePayload":
        return Variant.of_double(payload.doublePayload)
    if kind in ("payload", "non_cord_payload"):
        return Variant.of_bytes(_decode(payload, value.compressionInfo))
    return Variant.none()


def decode_response(response):
    if response.HasField("value_unordered"):
        return decode_value(response.value_unordered)
    if response.HasField("value_ordered"):
        # Java's DecompressingVariantObserver returns Variant for both scalar
        # alternatives; ordered collection elements retain their order below.
        return decode_value(response.value_ordered)
    return Variant.none()


def decode_update(response):
    return decode_value(response.value) if response.HasField("value") else Variant.none()


def decode_cas(response):
    if not response.HasField("expected"):
        return CasResult(response.result)
    observed = decode_response(response.expected)
    return CasResult(response.result, None if observed.is_none else observed.number_value(), decode_hint(response.expected))


def decode_key(key: cache_pb2.Key | cache_pb2.OrderedKey) -> bytes:
    return _decode(key.payload, key.compressionInfo)


def decode_hint(message: object, field: str = "keyHint") -> KeyHintData | None:
    if not message.HasField(field):
        return None
    hint = getattr(message, field)
    return KeyHintData(
        weak_hash=hint.weak_hash if hint.HasField("weak_hash") else None,
        strong_hash=hint.strong_hash if hint.HasField("strong_hash") else None,
        was_relayed=bool(getattr(message, "relayed", False)),
    )


def decode_batch(batch: cache_pb2.BatchValueResponse):
    """Decode a stream batch without leaking protobuf objects."""
    if (batch.key_ordered and batch.key_unordered) or (batch.value_ordered and
            (batch.value_unordered or batch.key_ordered or batch.key_unordered)):
        raise ValueError("batch mixes incompatible container representations")
    if batch.key_ordered:
        if len(batch.key_ordered) != len(batch.value_unordered):
            raise ValueError("ordered-map batch contains unpaired keys and values")
        return {
            OrderedPayload(decode_key(key), key.order if key.HasField("order") else None): decode_value(value)
            for key, value in zip(batch.key_ordered, batch.value_unordered)
        }
    if batch.key_unordered:
        if len(batch.key_unordered) != len(batch.value_unordered):
            raise ValueError("map batch contains unpaired keys and values")
        return {
            Payload(decode_key(key)): decode_value(value)
            for key, value in zip(batch.key_unordered, batch.value_unordered)
        }
    if batch.value_ordered:
        return [OrderedVariant(decode_value(value), value.order if value.HasField("order") else None) for value in batch.value_ordered]
    return [decode_value(value) for value in batch.value_unordered]


def grpc_target(host: str, port: int) -> str:
    return f"[{host}]:{port}" if ":" in host and not host.startswith("[") else f"{host}:{port}"


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
