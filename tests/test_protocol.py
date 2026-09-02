from __future__ import annotations

import lz4.block
import pytest

from hurricache import KeyHintData, OrderedPayload, Payload
from hurricache.grpc import cache_pb2
from hurricache.grpc.utils import create_key, create_ordered_key, create_value, decode_batch, decode_value


@pytest.mark.parametrize("length,compressed", [(1024, False), (1025, True)])
def test_java_compression_boundary(length: int, compressed: bool) -> None:
    raw = b"abc" * (length // 3) + b"x" * (length % 3)
    value = create_value(raw)
    assert value.HasField("compressionInfo") is compressed
    assert decode_value(value) == raw
    if compressed:
        assert lz4.block.decompress(value.value.payload, uncompressed_size=length) == raw
        assert value.value.size == len(value.value.payload)


def test_key_supports_utf8_and_nullable_hint_parts() -> None:
    key = create_key("שלום", KeyHintData(week_hash=0xFFFFFFFF), 7)
    assert key.clientId == 7
    assert key.payload.size == len("שלום".encode())
    assert key.keyHint.week_hash == 0xFFFFFFFF
    assert not key.keyHint.HasField("strong_hash")


def test_ordered_key_does_not_assign_nonexistent_ttl_or_lock_fields() -> None:
    key = create_ordered_key(OrderedPayload(b"key", 12), ttl=100, client_id=4)
    assert key.order == 12
    assert key.clientId == 4


def test_decode_all_batch_shapes() -> None:
    unordered = cache_pb2.BatchValueResponse(value_unordered=[create_value(b"a"), create_value(b"b")])
    assert decode_batch(unordered) == [Payload(b"a"), Payload(b"b")]

    ordered = cache_pb2.BatchValueResponse(
        value_ordered=[cache_pb2.OrderedValue(value=create_value(b"x").value, order=9)]
    )
    assert decode_batch(ordered) == [OrderedPayload(b"x", 9)]

    mapping = cache_pb2.BatchValueResponse(
        key_unordered=[create_key(b"k")], value_unordered=[create_value(b"v")]
    )
    assert decode_batch(mapping) == {Payload(b"k"): Payload(b"v")}


def test_map_batch_rejects_unpaired_data() -> None:
    with pytest.raises(ValueError, match="unpaired"):
        decode_batch(cache_pb2.BatchValueResponse(key_unordered=[create_key(b"k")]))
