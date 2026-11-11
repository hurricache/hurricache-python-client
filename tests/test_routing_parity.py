import inspect

import pytest
from test_parity import OPERATIONS, Stub, invoke, operation_arguments, operation_shape  # noqa: F401

from hurricache import AsyncHurriCacheClient, HurriCacheClient, KeyHintData, Mode, PartialOperationError, Variant
from hurricache.grpc import coordinator_pb2 as cp
from hurricache.grpc.exceptions import FailedPreconditionError, UnavailableError
from hurricache.grpc.smart_client import _is_write, _Topology

pytest_plugins = ["test_parity"]

# Independent transcription of execute(...) in the pinned Java smart client,
# plus explicitly retained read/create aliases. All remaining operations use
# executeWrite(...), independently of the implementation's policy collection.
CONFIGURED = frozenset("""
get contains_key remove get_size peek_head peek_tail get_ttl create replace
get_at get_in_container contains_in_container get_container get_list get_vector
get_queue get_set get_map get_ordered_map get_ordered_set get_range
get_element_in_range_ordered_set get_element_in_range_ordered_map
get_element_with_weight get_and_remove_element_with_weight
create_key_value get_value update_value update_key_value exist_key
get_head get_front get_tail stream_list stream_vector stream_queue stream_set
stream_map stream_ordered_map stream_ordered_set
""".split())

@pytest.mark.parametrize("name", OPERATIONS)
async def test_every_routing_policy(client_form, name):
    client, master_stub = client_form
    if not hasattr(client, "_clients"):
        pytest.skip("smart dispatch only")
    asynchronous = isinstance(next(iter(client._clients.values())), AsyncHurriCacheClient)
    node = (AsyncHurriCacheClient if asynchronous else HurriCacheClient)(default_timeout=2)
    backup_stub = Stub(asynchronous)
    node._stub = backup_stub
    client._clients["backup:1"] = node
    client._topology = _Topology(1, {(cp.MASTER, 0): "node:1", (cp.BACKUP, 0): "backup:1"}, frozenset(client._clients))
    client._configured_mode = Mode.BACKUP
    kwargs = operation_arguments(name)
    master_stub.shape = backup_stub.shape = operation_shape(name)
    await invoke(client, name, b"key", **kwargs)
    write = name not in CONFIGURED
    assert _is_write(name) is write
    assert bool(master_stub.calls) is write
    assert bool(backup_stub.calls) is not write


@pytest.mark.parametrize("route", [None, "node:1", "unknown:1", "backup:1"])
async def test_reroute_is_known_different_and_final(client_form, route):
    client, stub = client_form
    if not hasattr(client, "_clients"):
        pytest.skip("smart dispatch only")
    asynchronous = isinstance(next(iter(client._clients.values())), AsyncHurriCacheClient)
    second = (AsyncHurriCacheClient if asynchronous else HurriCacheClient)(default_timeout=2)
    second_stub = Stub(asynchronous)
    second._stub = second_stub
    client._clients["backup:1"] = second
    client._topology = _Topology(1, {(cp.MASTER, 0): "node:1", (cp.BACKUP, 0): "backup:1"}, frozenset(client._clients))
    calls = []
    def fail(*args, **kwargs):
        calls.append(1)
        raise FailedPreconditionError("route", route)
    async def async_fail(*args, **kwargs):
        return fail(*args, **kwargs)
    stub.getValue = async_fail if asynchronous else fail
    if route == "backup:1":
        assert await invoke(client, "get_value", b"k") == Variant.of_bytes(b"v")
        assert len(second_stub.calls) == 1
    else:
        with pytest.raises(FailedPreconditionError):
            await invoke(client, "get_value", b"k")
        assert not second_stub.calls
    assert len(calls) == 1


async def test_partial_never_falls_back_and_override_is_local(client_form, monkeypatch):
    from hurricache.grpc import batching
    client, stub = client_form
    if not hasattr(client, "_clients"):
        pytest.skip("smart dispatch only")
    asynchronous = isinstance(next(iter(client._clients.values())), AsyncHurriCacheClient)
    second = (AsyncHurriCacheClient if asynchronous else HurriCacheClient)(default_timeout=2)
    second_stub = Stub(asynchronous)
    second._stub = second_stub
    client._clients["backup:1"] = second
    client._topology = _Topology(1, {(cp.MASTER, 0): "node:1", (cp.BACKUP, 0): "backup:1"}, frozenset(client._clients))
    monkeypatch.setattr(batching, "MAX_RPC_SIZE", 180)
    stub.fail_at = 2
    with pytest.raises(PartialOperationError):
        await invoke(client, "push_tail", b"k", values=[bytes(range(60))] * 6)
    assert not second_stub.calls
    with client.mode(Mode.BACKUP):
        await invoke(client, "pop", b"k", KeyHintData(0xFFFFFFFF))
    assert len(second_stub.calls) == 1
    assert client._mode_override.get() is None


async def test_removed_channel_waits_for_active_users(client_form):
    client, stub = client_form
    if not hasattr(client, "_clients"):
        pytest.skip("smart lifecycle only")
    old = client._clients["node:1"]
    client._credentials = None
    client._active = 1
    topology = _Topology(1, {(cp.MASTER, 0): "new:1"}, frozenset({"new:1"}))
    result = client._apply_topology(topology)
    if inspect.isawaitable(result):
        await result
    assert not old._closed
    assert old in client._retired
    new = client._clients["new:1"]
    assert new.default_compression_threshold == 8
    assert new.default_ttl == 5000
    new._stub = Stub(isinstance(new, AsyncHurriCacheClient))
    client._active = 0
    await invoke(client, "get_value", b"k")
    assert old._closed


async def test_fallback_has_only_two_attempts(client_form):
    client, stub = client_form
    if not hasattr(client, "_clients"):
        pytest.skip("smart dispatch only")
    asynchronous = isinstance(next(iter(client._clients.values())), AsyncHurriCacheClient)
    second = (AsyncHurriCacheClient if asynchronous else HurriCacheClient)(default_timeout=2)
    second_stub = Stub(asynchronous)
    second._stub = second_stub
    client._clients["backup:1"] = second
    client._topology = _Topology(1, {(cp.MASTER, 0): "node:1", (cp.BACKUP, 0): "backup:1"}, frozenset(client._clients))
    client._configured_mode = Mode.MASTER_THEN_BACKUP
    stub.fail_at = second_stub.fail_at = 1
    with pytest.raises(UnavailableError):
        await invoke(client, "get_value", b"k")
    assert len(stub.calls) == len(second_stub.calls) == 1
