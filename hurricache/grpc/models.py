"""Wrapper models for HurriCache protobuf types."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum


class LockType(IntEnum):
    """Lock types for HurriCache objects.

    Mirrors the LockType enum from the protobuf definition.
    """

    NO_LOCK = 0
    WRITE_LOCK = 1
    READ_LOCK = 2
    GLOBAL = 3


@dataclass(frozen=True, slots=True)
class KeyHintData:
    """Wrapper for KeyHint protobuf message.

    Contains hash hints used for fast key lookup and data locality.
    Both fields are optional — the object may be unspecified (None or
    with both hashes as 0).

    Args:
        week_hash: Fast (weak) key hash for primary filtering.
        strong_hash: Cryptographic/full hash for accuracy and bucket distribution.
    """

    week_hash: int = 0
    strong_hash: int = 0

    @classmethod
    def unspecified(cls) -> "KeyHintData":
        """Return an unspecified (empty) KeyHintData."""
        return cls(week_hash=0, strong_hash=0)

    @property
    def is_specified(self) -> bool:
        """Return True if at least one hash is set."""
        return self.week_hash != 0 or self.strong_hash != 0

    def __bool__(self) -> bool:
        return self.is_specified


@dataclass(frozen=True, slots=True)
class CasResult:
    """Result of a Compare-And-Swap (CAS) operation.

    Args:
        success: True if the swap was performed, False otherwise.
        expected_value: The actual value read from the server (old value on success,
                        or actual value on failure).
    """

    success: bool
    expected_value: int


@dataclass(frozen=True, slots=True)
class OrderedPayload:
    """Wrapper for OrderedValue protobuf message.

    Used for ORDERED_SET and ORDERED_MAP containers.
    Contains a value with an associated weight/order for sorting.

    Args:
        value: The data payload (bytes).
        order: The weight/order value (uint64) used for sorting.
    """

    value: bytes
    order: int = 0
