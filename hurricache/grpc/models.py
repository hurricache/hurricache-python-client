"""Small, protobuf-independent public models used by the client API."""

from __future__ import annotations

from dataclasses import dataclass
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


@dataclass(frozen=True, slots=True)
class KeyHintData:
    """Server-computed routing hashes; each component is independently optional."""

    week_hash: int | None = None
    strong_hash: int | None = None

    @classmethod
    def unspecified(cls) -> "KeyHintData":
        return cls()

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


@dataclass(frozen=True, slots=True)
class OrderedPayload(Payload):
    order: int = 0


@dataclass(frozen=True, slots=True)
class CasResult:
    success: bool
    expected_value: int | None = None
    hint: KeyHintData | None = None
