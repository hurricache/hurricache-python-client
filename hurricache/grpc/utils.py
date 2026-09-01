"""Utility functions for building protobuf Key/Value from Python types."""

from __future__ import annotations

import time

from hurricache.grpc import cache_pb2
from hurricache.grpc.models import KeyHintData, OrderedPayload


def create_key(
    key: bytes,
    hint: KeyHintData | None = None,
    client_id: int = 0,
) -> cache_pb2.Key:
    """Build a protobuf Key from raw bytes and optional KeyHintData.

    Args:
        key: Raw key bytes.
        hint: Optional KeyHintData wrapper. If None or unspecified,
              the KeyHint field will be omitted.
        client_id: Owner client ID.

    Returns:
        Constructed cache_pb2.Key instance.
    """
    key_proto = cache_pb2.Key(
        payload=cache_pb2.KeyBinaryPayload(payload=key, size=len(key)),
    )
    # Always set clientId (server requires it for proper routing)
    key_proto.clientId = client_id
    if hint is not None and hint.is_specified:
        key_proto.keyHint.week_hash = hint.week_hash
        key_proto.keyHint.strong_hash = hint.strong_hash
    return key_proto


def create_value(
    value: bytes,
    ttl: int = 0,
    client_id: int = 0,
) -> cache_pb2.Value:
    """Build a protobuf Value from raw bytes.

    Args:
        value: Raw value bytes.
        ttl: Time-To-Live in milliseconds (relative). If > 0, converted to
             absolute time (current_time + ttl). If 0, ttl field is omitted.
        client_id: Owner client ID. If != 0, lock_info is created.

    Returns:
        Constructed cache_pb2.Value instance.
    """
    val_proto = cache_pb2.Value(
        value=cache_pb2.BinaryPayload(payload=value, size=len(value)),
    )
    if ttl > 0:
        val_proto.ttl = int(time.time() * 1000) + ttl
    if client_id != 0:
        val_proto.lock_info.type = cache_pb2.LockType.NO_LOCK
        val_proto.lock_info.lockedBy = client_id
    return val_proto


def create_ordered_value(
    value: bytes,
    order: int = 0,
    ttl: int = 0,
    client_id: int = 0,
) -> cache_pb2.OrderedValue:
    """Build a protobuf OrderedValue from raw bytes and order.

    Args:
        value: Raw value bytes.
        order: Weight/order for sorting (uint64).
        ttl: Time-To-Live in milliseconds (relative). If > 0, converted to
             absolute time (current_time + ttl). If 0, ttl field is omitted.
        client_id: Owner client ID. If != 0, lock_info is created.

    Returns:
        Constructed cache_pb2.OrderedValue instance.
    """
    val_proto = cache_pb2.OrderedValue(
        value=cache_pb2.BinaryPayload(payload=value, size=len(value)),
        order=order,
    )
    if ttl > 0:
        val_proto.ttl = int(time.time() * 1000) + ttl
    if client_id != 0:
        val_proto.lock_info.type = cache_pb2.LockType.NO_LOCK
        val_proto.lock_info.lockedBy = client_id
    return val_proto


def create_ordered_key(
    key: bytes,
    order: int = 0,
    ttl: int = 0,
    client_id: int = 0,
) -> cache_pb2.OrderedKey:
    """Build a protobuf OrderedKey from raw bytes and order.

    Args:
        key: Raw key bytes.
        order: Weight/order for sorting (uint64).
        ttl: Time-To-Live in milliseconds (relative). If > 0, converted to
             absolute time (current_time + ttl). If 0, ttl field is omitted.
        client_id: Owner client ID. If != 0, lock_info is created.

    Returns:
        Constructed cache_pb2.OrderedKey instance.
    """
    key_proto = cache_pb2.OrderedKey(
        payload=cache_pb2.KeyBinaryPayload(payload=key, size=len(key)),
        order=order,
    )
    if ttl > 0:
        key_proto.ttl = int(time.time() * 1000) + ttl
    if client_id != 0:
        key_proto.lock_info.type = cache_pb2.LockType.NO_LOCK
        key_proto.lock_info.lockedBy = client_id
    return key_proto


def build_get_request(
    key: bytes,
    hint: KeyHintData | None = None,
    client_id: int = 0,
) -> cache_pb2.GetRequest:
    """Build a GetRequest from raw key bytes.

    Args:
        key: Raw key bytes.
        hint: Optional KeyHintData.
        client_id: Owner client ID.

    Returns:
        Constructed cache_pb2.GetRequest instance.
    """
    return cache_pb2.GetRequest(key=create_key(key, hint, client_id))
