"""Integration tests for HurriCache gRPC client against a real server.

Run with:
    pytest tests/test_integration.py -v --tb=short

Server must be running on 127.0.0.1:50000 without authentication.
"""

from __future__ import annotations

import random
import time

import pytest

from hurricache import (
    HurriCacheClient,
    HurriCacheError,
    KeyHintData,
    KeyNotFoundError,
    LockStatus,
    LockType,
    OrderedPayload,
)
from hurricache.grpc import cache_pb2

# ---------------------------------------------------------------------------
# Module-level client (single connection for all tests)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def client():
    """Create a HurriCacheClient connected to the real server."""
    with HurriCacheClient(host="127.0.0.1", port=50000) as c:
        yield c


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _unique_key(prefix: str) -> bytes:
    """Generate a unique key for tests."""
    suffix = f"{int(time.time()*1000000)}{random.randint(1000,9999)}"
    return f"{prefix}_{suffix}".encode()


def _clean(client: HurriCacheClient, key: bytes) -> None:
    """Try to remove a key (ignore errors if it doesn't exist)."""
    try:
        client.remove(key)
    except HurriCacheError:
        pass


# ---------------------------------------------------------------------------
# Key-Value Operations
# ---------------------------------------------------------------------------

class TestKeyValue:
    """Tests for basic key-value operations."""

    @pytest.fixture(autouse=True)
    def _setup(self, client):
        self.client = client
        self.key = _unique_key("test_kv_key")
        self.value = b"test_value_data"
        yield
        _clean(self.client, self.key)

    def test_create_and_get(self, client):
        hint = client.create_key_value(self.key, value=self.value)
        assert isinstance(hint, KeyHintData)
        retrieved = client.get_value(self.key)
        assert retrieved == self.value

    def test_get_nonexistent_raises(self, client):
        with pytest.raises(KeyNotFoundError):
            client.get_value(b"nonexistent_key_12345")

    def test_update_returns_old_value(self, client):
        client.create_key_value(self.key, value=b"old_value")
        old = client.update_value(self.key, value=b"new_value")
        assert old == b"old_value"
        assert client.get_value(self.key) == b"new_value"

    def test_exist_key(self, client):
        client.create_key_value(self.key, value=b"data")
        assert client.exist_key(self.key) is True
        assert client.exist_key(b"nonexistent_key_12345") is False

    def test_remove(self, client):
        client.create_key_value(self.key, value=b"data")
        assert client.remove(self.key) is True
        with pytest.raises(KeyNotFoundError):
            client.get_value(self.key)

    def test_get_and_delete(self, client):
        client.create_key_value(self.key, value=b"delete_me")
        value = client.get_and_delete_value(self.key)
        assert value == b"delete_me"
        with pytest.raises(KeyNotFoundError):
            client.get_value(self.key)

    def test_get_with_hint(self, client):
        hint_resp = client.create_key_value(self.key, value=b"hint_test")
        value = client.get_value(self.key, hint=hint_resp)
        assert value == b"hint_test"


# ---------------------------------------------------------------------------
# TTL Management
# ---------------------------------------------------------------------------

class TestTTL:
    @pytest.fixture(autouse=True)
    def _setup(self, client):
        self.client = client
        self.key = _unique_key("test_ttl_key")
        yield
        _clean(self.client, self.key)

    def test_set_ttl(self, client):
        client.create_key_value(self.key, value=b"data")
        result = client.set_ttl(self.key, ttl=60000)
        assert result is True


# ---------------------------------------------------------------------------
# Lock Management
# ---------------------------------------------------------------------------

class TestLock:
    @pytest.fixture(autouse=True)
    def _setup(self, client):
        self.client = client
        self.key = _unique_key("test_lock_key")
        yield

    def test_lock_and_unlock(self, client):
        client.create_key_value(self.key, value=b"data")
        result = client.lock_object(
            self.key,
            lock_type=LockType.WRITE_LOCK,
            client_id=1,
            lock_duration=5.0,
        )
        assert result is LockStatus.OK

        unlock_result = client.unlock_object(self.key, client_id=1)
        assert unlock_result is LockStatus.OK

    def test_lock_with_read_lock(self, client):
        client.create_key_value(self.key, value=b"data")
        result = client.lock_object(
            self.key,
            lock_type=LockType.READ_LOCK,
            client_id=1,
            lock_duration=5.0,
        )
        assert result is LockStatus.OK

    def test_lock_default_type(self, client):
        client.create_key_value(self.key, value=b"data")
        result = client.lock_object(self.key, client_id=1)
        assert result is LockStatus.OK


# ---------------------------------------------------------------------------
# Atomic Operations
# ---------------------------------------------------------------------------

class TestAtomic:
    @pytest.fixture(autouse=True)
    def _setup(self, client):
        self.client = client
        self.key = _unique_key("test_atomic_key")
        yield
        _clean(self.client, self.key)

    def test_atomic_create_and_load(self, client):
        hint = client.atomic_create(self.key, value=42)
        assert isinstance(hint, KeyHintData)
        val = client.atomic_load(self.key, hint=hint)
        assert val == 42

    def test_atomic_store(self, client):
        hint = client.atomic_create(self.key, value=0)
        hint = client.atomic_store(self.key, hint=hint, value=100)
        assert isinstance(hint, KeyHintData)
        val = client.atomic_load(self.key, hint=hint)
        assert val == 100

    def test_atomic_exchange(self, client):
        hint = client.atomic_create(self.key, value=10)
        old = client.atomic_exchange(self.key, hint=hint, value=20)
        assert old == 10
        val = client.atomic_load(self.key, hint=hint)
        assert val == 20

    def test_atomic_add(self, client):
        hint = client.atomic_create(self.key, value=10)
        old = client.atomic_add(self.key, hint=hint, delta=5)
        assert old == 10
        val = client.atomic_load(self.key, hint=hint)
        assert val == 15

    def test_atomic_sub(self, client):
        hint = client.atomic_create(self.key, value=10)
        old = client.atomic_sub(self.key, hint=hint, delta=3)
        assert old == 10
        val = client.atomic_load(self.key, hint=hint)
        assert val == 7

    def test_atomic_compare_and_set(self, client):
        from hurricache import CasResult

        hint = client.atomic_create(self.key, value=10)
        result = client.atomic_compare_and_set(self.key, hint=hint, expected_value=10, new_value=20)
        assert isinstance(result, CasResult)
        assert result.success is True
        val = client.atomic_load(self.key, hint=hint)
        assert val == 20

        # CAS should fail with wrong expected value
        result = client.atomic_compare_and_set(self.key, hint=hint, expected_value=999, new_value=30)
        assert isinstance(result, CasResult)
        assert result.success is False
        assert result.expected_value == 20

    def test_atomic_load_and_delete(self, client):
        hint = client.atomic_create(self.key, value=42)
        val = client.atomic_load_and_delete(self.key, hint=hint)
        assert val == 42

    def test_atomic_or(self, client):
        hint = client.atomic_create(self.key, value=0b1010)
        old = client.atomic_or(self.key, hint=hint, mask=0b0101)
        assert old == 0b1010
        val = client.atomic_load(self.key, hint=hint)
        assert val == 0b1111

    def test_atomic_and(self, client):
        hint = client.atomic_create(self.key, value=0b1111)
        old = client.atomic_and(self.key, hint=hint, mask=0b1010)
        assert old == 0b1111
        val = client.atomic_load(self.key, hint=hint)
        assert val == 0b1010

    def test_atomic_xor(self, client):
        hint = client.atomic_create(self.key, value=0b1100)
        old = client.atomic_xor(self.key, hint=hint, mask=0b0101)
        assert old == 0b1100
        val = client.atomic_load(self.key, hint=hint)
        assert val == 0b1001


# ---------------------------------------------------------------------------
# Container Operations
# ---------------------------------------------------------------------------

class TestContainerCreate:
    @pytest.fixture(autouse=True)
    def _setup(self, client):
        self.client = client
        self.key = _unique_key("test_container_key")
        self.values = [b"item1", b"item2", b"item3"]
        yield
        _clean(self.client, self.key)

    def test_create_vector(self, client):
        hint = client.create_vector(self.key, values=self.values)
        assert isinstance(hint, KeyHintData)
        size = client.get_size(self.key, hint=hint)
        assert size == len(self.values)

    def test_create_list(self, client):
        hint = client.create_list(self.key, values=self.values)
        assert isinstance(hint, KeyHintData)
        size = client.get_size(self.key, hint=hint)
        assert size == len(self.values)

    def test_create_set(self, client):
        hint = client.create_set(self.key, values=self.values)
        assert isinstance(hint, KeyHintData)
        size = client.get_size(self.key, hint=hint)
        assert size == len(self.values)

    def test_create_queue(self, client):
        hint = client.create_queue(self.key, values=self.values)
        assert isinstance(hint, KeyHintData)
        # Queue: get_size returns 0, use get_head to verify
        head = client.get_head(self.key, hint=hint)
        assert head == self.values[0]


class TestContainerOrderedSet:
    @pytest.fixture(autouse=True)
    def _setup(self, client):
        self.client = client
        self.key = _unique_key("test_ordered_set")
        yield
        _clean(self.client, self.key)

    def test_create_ordered_set(self, client):
        values = [
            OrderedPayload(b"low", order=1),
            OrderedPayload(b"mid", order=2),
            OrderedPayload(b"high", order=3),
        ]
        hint = client.create_ordered_set(self.key, values=values)
        assert isinstance(hint, KeyHintData)
        size = client.get_size(self.key, hint=hint)
        assert size == len(values)

    def test_create_ordered_set_empty(self, client):
        hint = client.create_ordered_set(self.key)
        assert isinstance(hint, KeyHintData)
        size = client.get_size(self.key, hint=hint)
        assert size == 0


class TestContainerOrderedMap:
    @pytest.fixture(autouse=True)
    def _setup(self, client):
        self.client = client
        self.key = _unique_key("test_ordered_map")
        yield
        _clean(self.client, self.key)

    def test_create_ordered_map(self, client):
        keys = [
            OrderedPayload(b"key_low", order=1),
            OrderedPayload(b"key_mid", order=2),
            OrderedPayload(b"key_high", order=3),
        ]
        values = [b"val1", b"val2", b"val3"]
        hint = client.create_ordered_map(self.key, keys=keys, values=values)
        assert isinstance(hint, KeyHintData)
        size = client.get_size(self.key, hint=hint)
        assert size == len(keys)

    def test_create_ordered_map_empty(self, client):
        hint = client.create_ordered_map(self.key)
        assert isinstance(hint, KeyHintData)
        size = client.get_size(self.key, hint=hint)
        assert size == 0


class TestContainerBoundary:
    @pytest.fixture(autouse=True)
    def _setup(self, client):
        self.client = client
        self.key = _unique_key("test_boundary_key")
        self.values = [b"first", b"middle", b"last"]
        yield
        _clean(self.client, self.key)

    def _setup_container(self, client):
        hint = client.create_list(self.key, values=self.values)
        return hint

    def test_get_head(self, client):
        hint = self._setup_container(client)
        value = client.get_head(self.key, hint=hint)
        assert value == b"first"

    def test_get_tail(self, client):
        hint = self._setup_container(client)
        value = client.get_tail(self.key, hint=hint)
        assert value == b"last"

    def test_get_element_at_position(self, client):
        hint = self._setup_container(client)
        value = client.get_element_at_position(self.key, hint=hint, pos=1, type=cache_pb2.ContainerType.LIST)
        assert value == b"middle"

    def test_get_element_at_position_0(self, client):
        hint = self._setup_container(client)
        value = client.get_element_at_position(self.key, hint=hint, pos=0, type=cache_pb2.ContainerType.LIST)
        assert value == b"first"

    def test_get_element_at_position_last(self, client):
        hint = self._setup_container(client)
        value = client.get_element_at_position(self.key, hint=hint, pos=2, type=cache_pb2.ContainerType.LIST)
        assert value == b"last"


class TestContainerPop:
    @pytest.fixture(autouse=True)
    def _setup(self, client):
        self.client = client
        self.key = _unique_key("test_pop_key")
        self.values = [b"first", b"second", b"third"]
        yield
        _clean(self.client, self.key)

    def test_get_and_remove_front(self, client):
        hint = client.create_queue(self.key, values=self.values)
        value = client.get_and_remove_front(self.key, hint=hint)
        assert value == b"first"
        # Queue: get_size returns 0, verify by head
        head = client.get_head(self.key, hint=hint)
        assert head == b"second"

    def test_get_and_remove_tail(self, client):
        hint = client.create_list(self.key, values=self.values)
        value = client.get_and_remove_tail(self.key, hint=hint)
        assert value == b"third"
        size = client.get_size(self.key, hint=hint)
        assert size == 2


class TestContainerRemove:
    @pytest.fixture(autouse=True)
    def _setup(self, client):
        self.client = client
        self.key = _unique_key("test_remove_key")
        self.values = [b"first", b"second", b"third"]
        yield
        _clean(self.client, self.key)

    def _setup_container(self, client):
        hint = client.create_list(
            self.key,
            values=self.values,
        )
        return hint

    def test_remove_head(self, client):
        hint = self._setup_container(client)
        result = client.remove_head(self.key, hint=hint)
        assert result is True
        size = client.get_size(self.key, hint=hint)
        assert size == 2
        head = client.get_head(self.key, hint=hint)
        assert head == b"second"

    def test_remove_tail(self, client):
        hint = self._setup_container(client)
        result = client.remove_tail(self.key, hint=hint)
        assert result is True
        size = client.get_size(self.key, hint=hint)
        assert size == 2
        tail = client.get_tail(self.key, hint=hint)
        assert tail == b"second"

    def test_remove_element_at_position(self, client):
        hint = self._setup_container(client)
        result = client.remove_element_at_position(self.key, hint=hint, pos=1, type=cache_pb2.ContainerType.LIST)
        assert result is True
        size = client.get_size(self.key, hint=hint)
        assert size == 2


class TestContainerAdd:
    @pytest.fixture(autouse=True)
    def _setup(self, client):
        self.client = client
        self.key = _unique_key("test_add_key")
        self.initial_values = [b"first"]
        yield
        _clean(self.client, self.key)

    def _setup_container(self, client):
        hint = client.create_list(
            self.key,
            values=self.initial_values,
        )
        return hint

    def test_add_element_to_tail(self, client):
        hint = self._setup_container(client)
        result = client.add_element_to_tail(self.key, hint=hint, values=[b"second", b"third"])
        assert result is True
        size = client.get_size(self.key, hint=hint)
        assert size == 3
        tail = client.get_tail(self.key, hint=hint)
        assert tail == b"third"

    def test_add_element_to_head(self, client):
        hint = self._setup_container(client)
        result = client.add_element_to_head(self.key, hint=hint, values=[b"zero"])
        assert result is True
        size = client.get_size(self.key, hint=hint)
        assert size == 2
        head = client.get_head(self.key, hint=hint)
        assert head == b"zero"

    def test_add_element(self, client):
        hint = self._setup_container(client)
        count = client.add_element(self.key, hint=hint, values=[b"new1", b"new2"])
        assert count == 2


class TestContainerGetAndDeleteInContainer:
    @pytest.fixture(autouse=True)
    def _setup(self, client):
        self.client = client
        self.key = _unique_key("test_map_key")
        self.keys = [b"key1", b"key2"]
        self.values = [b"value1", b"value2"]
        yield
        _clean(self.client, self.key)

    def _setup_container(self, client):
        hint = client.create_map(self.key, keys=self.keys, values=self.values)
        return hint

    def test_get_value_in_container(self, client):
        hint = self._setup_container(client)
        value = client.get_value_in_container(self.key, b"key1", hint=hint)
        assert value == b"value1"

    def test_exist_key_in_container(self, client):
        hint = self._setup_container(client)
        assert client.exist_key_in_container(self.key, b"key1", hint=hint) is True
        assert client.exist_key_in_container(self.key, b"nonexistent", hint=hint) is False

    def test_update_value_in_container(self, client):
        hint = self._setup_container(client)
        old = client.update_value_in_container(self.key, b"key1", b"updated", hint=hint)
        assert old == b"value1"
        value = client.get_value_in_container(self.key, b"key1", hint=hint)
        assert value == b"updated"

    def test_get_and_delete_value_in_container(self, client):
        hint = self._setup_container(client)
        value = client.get_and_delete_value_in_container(self.key, b"key1", hint=hint)
        assert value == b"value1"
        with pytest.raises(KeyNotFoundError):
            client.get_value_in_container(self.key, b"key1", hint=hint)

    def test_remove_in_container(self, client):
        hint = self._setup_container(client)
        count = client.remove_in_container(self.key, b"key1", hint=hint)
        assert count >= 0
        with pytest.raises(KeyNotFoundError):
            client.get_value_in_container(self.key, b"key1", hint=hint)


# ---------------------------------------------------------------------------
# Error Handling
# ---------------------------------------------------------------------------

class TestErrors:
    @pytest.fixture(autouse=True)
    def _setup(self, client):
        self.client = client

    def test_key_not_found_on_get(self, client):
        with pytest.raises(KeyNotFoundError):
            client.get_value(b"totally_nonexistent_key_xyz")

    def test_key_not_found_on_get_and_delete(self, client):
        with pytest.raises(KeyNotFoundError):
            client.get_and_delete_value(b"totally_nonexistent_key_xyz")

    def test_key_not_found_on_get_head(self, client):
        with pytest.raises(KeyNotFoundError):
            client.get_head(b"totally_nonexistent_key_xyz")

    def test_key_not_found_on_get_tail(self, client):
        with pytest.raises(KeyNotFoundError):
            client.get_tail(b"totally_nonexistent_key_xyz")

    def test_key_not_found_on_atomic_load(self, client):
        with pytest.raises(KeyNotFoundError):
            client.atomic_load(b"totally_nonexistent_key_xyz")
