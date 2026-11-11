"""Standalone continuations, ordering, TTL and lock acceptance without expected failures."""

import asyncio
import os
import time

import pytest
from test_parity import invoke

from hurricache import LockStatus, LockType, OrderedPayload, OrderedVariant, Variant
from hurricache.grpc import batching

pytestmark = pytest.mark.skipif(not os.environ.get("HURRICACHE_TEST_TARGET"), reason="requires pinned standalone startup")
pytest_plugins = ["test_integration"]


@pytest.mark.parametrize("family", ["list", "vector", "queue", "set", "ordered_set", "map", "ordered_map"])
async def test_live_creation_continuations(live_client, monkeypatch, family):
    client, key = live_client
    values = [Variant.of_bytes(bytes([i]) * 60) if i % 3 == 0 else Variant.of_long(i) if i % 3 == 1 else Variant.of_double(i+0.5)
              for i in range(18)]
    options = {"values": values}
    if family == "ordered_set":
        options["values"] = [OrderedVariant.of(v, i+1) for i, v in enumerate(values)]
    elif family in ("map", "ordered_map"):
        options["keys"] = [OrderedPayload(str(i).encode(), i+1) if family == "ordered_map" else str(i).encode()
                           for i in range(len(values))]
    name = key()
    monkeypatch.setattr(batching, "MAX_RPC_SIZE", 250)
    hint = await invoke(client, "create_" + family, name, **options)
    if family == "queue":
        assert await invoke(client, "get_size", name, hint) == len(values)
        assert [await invoke(client, "pop_head", name, hint) for _ in values] == values
        return
    actual = await invoke(client, "get_" + family, name, hint)
    if isinstance(actual, dict):
        actual = list(actual.values())
    assert len(actual) == len(values)
    if family not in ("map", "ordered_map", "set"):
        assert actual == options["values"]
    else:
        assert set(actual) == set(values)


@pytest.mark.parametrize("operation", ["push_head", "push_tail", "insert_at", "insert_before", "insert_after"])
async def test_live_chunked_insertion_order(live_client, monkeypatch, operation):
    client, key = live_client
    values = [os.urandom(100) for _ in range(8)]
    results = []
    # Advancing pivot anchors carry a full preceding value, so reserve space
    # for both that anchor and one compressed element in each request.
    for budget in (350 if operation in ("insert_before", "insert_after") else 250, 3670016):
        monkeypatch.setattr(batching, "MAX_RPC_SIZE", budget)
        name = key()
        hint = await invoke(client, "create_list", name, values=[b"pivot"])
        options = {"values": values}
        if operation in ("insert_before", "insert_after"):
            options["pivot"] = b"pivot"
        if operation == "insert_at":
            options["pos"] = 1
        assert await invoke(client, operation, name, hint, **options) == len(values)
        results.append(await invoke(client, "get_list", name, hint))
    assert results[0] == results[1]


async def test_live_lock_expiration_and_ttl(live_client):
    client, key = live_client
    name = key()
    hint = await invoke(client, "create", name, value=1, ttl=10000)
    assert 0 < await invoke(client, "get_ttl", name, hint) <= 10000
    assert await invoke(client, "set_ttl", name, hint, ttl=-1)
    assert await invoke(client, "get_ttl", name, hint) == -1
    assert await invoke(client, "lock", name, hint, LockType.WRITE_LOCK, 77, 2) == LockStatus.OK
    try:
        assert await invoke(client, "lock", name, hint, LockType.WRITE_LOCK, 78, 2) != LockStatus.OK
        await asyncio.sleep(2.1)
        # The standalone server expires locks on a coarse timer; allow its next
        # tick rather than requiring an exact wall-clock boundary.
        deadline = time.monotonic() + 2
        while True:
            status = await invoke(client, "lock", name, hint, LockType.WRITE_LOCK, 78, 2)
            if status == LockStatus.OK or time.monotonic() >= deadline:
                break
            await asyncio.sleep(0.05)
        assert status == LockStatus.OK
    finally:
        await invoke(client, "unlock", name, hint, client_id=78)
        await invoke(client, "unlock", name, hint, client_id=77)
