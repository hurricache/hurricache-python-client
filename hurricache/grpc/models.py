"""Small, protobuf-independent public models used by the client API."""

from __future__ import annotations

import math
import struct
from dataclasses import dataclass, field
from enum import Enum, IntEnum


class LockType(IntEnum):
    NO_LOCK = 0
    WRITE_LOCK = 1
    READ_LOCK = 2
    GLOBAL = 3


class LockStatus(IntEnum):
    OK = 0
    CANT_LOCK = 1
    CANT_UNLOCK = 2
    GENERIC_ERROR = 3


class ContainerType(IntEnum):
    UNDEFINED = 0
    VECTOR = 1
    LIST = 2
    QUEUE = 3
    SET = 4
    MAP = 5
    ORDERED_MAP = 6
    ORDERED_SET = 7


class Mode(str, Enum):
    """Smart-client read routing mode."""

    MASTER = "master"
    MASTER_THEN_BACKUP = "master_then_backup"
    # The misspelling is kept as an alias for Java source compatibility.
    MASTER_THAN_BACKUP = "master_then_backup"
    BACKUP = "backup"
    LB_SMART = "load_balanced"


@dataclass(frozen=True, slots=True, init=False)
class KeyHintData:
    """Server-computed routing hashes; each component is independently optional."""

    weak_hash: int | None = None
    strong_hash: int | None = None
    was_relayed: bool = field(default=False, compare=False)

    def __init__(self, weak_hash=None, strong_hash=None, was_relayed=False, *, week_hash=None):
        if week_hash is not None:
            if weak_hash is not None and weak_hash != week_hash:
                raise ValueError("weak_hash and week_hash disagree")
            weak_hash = week_hash
        object.__setattr__(self, "weak_hash", weak_hash)
        object.__setattr__(self, "strong_hash", strong_hash)
        object.__setattr__(self, "was_relayed", was_relayed)

    @property
    def week_hash(self):
        """Compatibility spelling; prefer weak_hash."""
        return self.weak_hash

    @classmethod
    def unspecified(cls) -> "KeyHintData":
        return cls()

    @classmethod
    def of(cls, strong_hash, week_hash, was_relayed=False):
        """Java factory argument order; prefer named constructor arguments."""
        return cls(week_hash, strong_hash, was_relayed)

    @property
    def is_specified(self) -> bool:
        return self.week_hash is not None or self.strong_hash is not None

    def __bool__(self) -> bool:
        return self.is_specified


@dataclass(frozen=True, slots=True)
class Payload:
    value: bytes

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", bytes(self.value))

    @classmethod
    def of(cls, value):
        return cls(value)


@dataclass(frozen=True, slots=True)
class OrderedPayload(Payload):
    order: int | None = 0

    @classmethod
    def of(cls, value, order=None):
        return cls(value, order)


def int64(value):
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("value must be an integer, excluding bool")
    if not -(1 << 63) <= value < (1 << 63):
        raise ValueError("value is outside signed int64")
    return value


class VariantType(IntEnum):
    NONE = 0
    BYTES = 1
    INT = 2
    DOUBLE = 3


@dataclass(frozen=True, slots=True, eq=False)
class Variant:
    """Immutable typed value. Use factories to preserve numeric type and NONE."""

    type: VariantType
    value: bytes | int | float | None = None

    Type = VariantType

    def __post_init__(self):
        object.__setattr__(self, "type", VariantType(self.type))
        if self.type == VariantType.NONE:
            if self.value is not None:
                raise TypeError("NONE cannot contain a value")
        elif self.type == VariantType.BYTES:
            if not isinstance(self.value, (bytes, bytearray, memoryview)):
                raise TypeError("BYTES requires bytes")
            object.__setattr__(self, "value", bytes(self.value))
        elif self.type == VariantType.INT:
            int64(self.value)
        elif self.type == VariantType.DOUBLE:
            if isinstance(self.value, bool) or not isinstance(self.value, (float, int)):
                raise TypeError("DOUBLE requires a number, excluding bool")
            object.__setattr__(self, "value", float(self.value))

    @classmethod
    def none(cls):
        return cls(VariantType.NONE)

    @classmethod
    def of_bytes(cls, value):
        return cls(VariantType.BYTES, value)

    @classmethod
    def of_long(cls, value):
        return cls(VariantType.INT, value)

    of_int = of_long

    @classmethod
    def of_double(cls, value):
        return cls(VariantType.DOUBLE, value)

    @property
    def is_none(self):
        return self.type == VariantType.NONE

    @property
    def is_bytes(self):
        return self.type == VariantType.BYTES

    @property
    def is_int(self):
        return self.type == VariantType.INT

    @property
    def is_double(self):
        return self.type == VariantType.DOUBLE

    def _require(self, expected):
        if self.type != expected:
            raise TypeError(f"Expected {expected.name}, got {self.type.name}")
        return self.value

    def require_bytes(self) -> bytes:
        return self._require(VariantType.BYTES)

    def require_long(self) -> int:
        return self._require(VariantType.INT)

    require_int = require_long

    def require_double(self) -> float:
        return self._require(VariantType.DOUBLE)

    def number_value(self) -> int | float:
        if not (self.is_int or self.is_double):
            raise TypeError(f"Expected numeric value, got {self.type.name}")
        return self.require_long() if self.is_int else self.require_double()

    bytes_value = property(require_bytes)
    long_value = property(require_long)
    int_value = property(require_long)
    double_value = property(require_double)

    def _identity(self):
        # Double.equals/compare canonicalize NaNs and distinguish the two zeros.
        value = self.value
        if self.is_double:
            value = b"nan" if math.isnan(value) else struct.pack(">d", value)
        return self.type, value

    def __eq__(self, other):
        return type(other) is type(self) and self._identity() == other._identity()

    def __hash__(self):
        return hash(self._identity())


@dataclass(frozen=True, slots=True)
class OrderedVariant:
    variant: Variant
    order: int | None = None

    def __post_init__(self):
        if not isinstance(self.variant, Variant):
            raise TypeError("variant must be a Variant")
        if self.order is not None and (isinstance(self.order, bool) or not isinstance(self.order, int)
                                       or not 0 <= self.order < (1 << 64)):
            raise ValueError("order must be an unsigned uint64 or None")

    @classmethod
    def of(cls, variant, order=None):
        return cls(variant, order)

    @classmethod
    def of_bytes(cls, value, order=None):
        return cls(Variant.of_bytes(value), order)

    @classmethod
    def of_int(cls, value, order=None):
        return cls(Variant.of_long(value), order)

    of_long = of_int

    @classmethod
    def of_double(cls, value, order=None):
        return cls(Variant.of_double(value), order)

    @property
    def has_order(self):
        return self.order is not None

    @property
    def type(self):
        return self.variant.type

    @property
    def is_none(self):
        return self.variant.is_none

    @property
    def is_bytes(self):
        return self.variant.is_bytes

    @property
    def is_int(self):
        return self.variant.is_int

    @property
    def is_double(self):
        return self.variant.is_double

    def require_bytes(self):
        return self.variant.require_bytes()

    def require_long(self):
        return self.variant.require_long()

    require_int = require_long

    def require_double(self):
        return self.variant.require_double()

    def number_value(self):
        return self.variant.number_value()

    bytes_value = property(require_bytes)
    long_value = property(require_long)
    int_value = property(require_long)
    double_value = property(require_double)


@dataclass(frozen=True, slots=True)
class CasResult:
    success: bool
    expected_value: int | float | None = None
    hint: KeyHintData | None = None
