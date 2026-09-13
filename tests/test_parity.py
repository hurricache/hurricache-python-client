"""Pinned Java contract coverage across all four Python client forms."""

import inspect
import threading
import time

import pytest
from google.protobuf.message_factory import GetMessageClass

from hurricache import (
    AsyncHurriCacheClient,
    AsyncHurriCacheSmartClient,
    HurriCacheClient,
    HurriCacheSmartClient,
    Mode,
    OrderedPayload,
    PartialOperationError,
)
from hurricache.grpc import batching
from hurricache.grpc import cache_pb2 as pb
from hurricache.grpc import coordinator_pb2 as coordinator
from hurricache.grpc.exceptions import DeadlineExceededError, UnavailableError
from hurricache.grpc.operation import configure, lock_expiration
from hurricache.grpc.smart_client import _Topology
from hurricache.grpc.utils import create_key, create_ordered_key, create_ordered_value, create_value


class Stub:
    def __init__(self, asynchronous=False):
        self.asynchronous = asynchronous
        self.calls = []
        self.shape = "list"
        self.fail_at = None
        self.reject_at = None
        self.delay = 0

    def __getattr__(self, name):
        descriptor = pb.DESCRIPTOR.services_by_name["HurriCacheGrpcService"].methods_by_name[name]

        def result(request, **kwargs):
            self.calls.append((name, request, kwargs))
            if self.delay:
                time.sleep(self.delay)
            if len(self.calls) == self.fail_at:
                raise UnavailableError("injected failure")
            response = GetMessageClass(descriptor.output_type)()
            fields = response.DESCRIPTOR.fields_by_name
            if "keyHint" in fields:
                response.keyHint.CopyFrom(pb.KeyHint(week_hash=0xFFFFFFFF, strong_hash=2))
            if "size" in fields:
                response.size = sum(len(getattr(request, f)) for f in ("value_unordered", "value_ordered", "values", "keys", "value") if hasattr(request, f)) or 1
                if len(self.calls) == self.reject_at:
                    response.size = 0
            if "value" in fields:
                if fields["value"].message_type:
                    response.value.CopyFrom(create_value(b"old"))
                else:
                    response.value = len(self.calls) != self.reject_at
            if "result" in fields:
                response.result = 1 if fields["result"].type == fields["result"].TYPE_BOOL else 0
            if "value_unordered" in fields:
                if descriptor.server_streaming:
                    if self.shape == "ordered_set":
                        response.value_ordered.append(create_ordered_value(OrderedPayload(b"v", 1)))
                    else:
                        response.value_unordered.append(create_value(b"v", compress=True))
                    if self.shape == "map":
                        response.key_unordered.append(create_key(b"entry"))
                    if self.shape == "ordered_map":
                        response.key_ordered.append(create_ordered_key(OrderedPayload(b"entry", 1)))
                else:
                    response.value_unordered.CopyFrom(create_value(b"v"))
            return response

        if descriptor.server_streaming:
            if self.asynchronous:
                async def stream(request, **kwargs):
                    yield result(request, **kwargs)
                return stream
            return lambda request, **kwargs: iter([result(request, **kwargs)])
        if self.asynchronous:
            async def unary(request, **kwargs):
                return result(request, **kwargs)
            return unary
        return result


@pytest.fixture(params=["sync", "async", "smart", "async_smart"])
def client_form(request):
    asynchronous = request.param.startswith("async")
    direct = AsyncHurriCacheClient if asynchronous else HurriCacheClient
    node = direct(default_timeout=2, default_compression_threshold=8, default_ttl=5000)
    stub = Stub(asynchronous)
    node._stub = stub
    if "smart" not in request.param:
        return node, stub
    cls = AsyncHurriCacheSmartClient if asynchronous else HurriCacheSmartClient
    smart = cls.__new__(cls)
    configure(smart, 5000, 8)
    smart._active = 0
    smart._retired = []
    smart._default_timeout = 2
    smart._default_client_id = 0
    smart._readiness_timeout = 2
    smart._topology = _Topology(1, {(coordinator.MASTER, 0): "node:1"}, frozenset({"node:1"}))
    smart._clients = {"node:1": node}
    smart._configured_mode = Mode.MASTER
    import contextvars
    smart._mode_override = contextvars.ContextVar("test_mode", default=None)
    smart._random_shard = 0
    smart._ready = threading.Event()
    if asynchronous:
        async def ready(timeout=None):
            pass
        smart.wait_until_ready = ready
    else:
        smart._lock = threading.RLock()
        smart.wait_until_ready = lambda timeout=None: None
    return smart, stub


async def invoke(client, name, *args, **kwargs):
    result = getattr(client, name)(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


OPERATIONS = sorted(name for name, method in vars(HurriCacheClient).items() if not name.startswith("_") and name != "close" and inspect.isfunction(method))



def operation_arguments(name):
    kwargs = {}
    signature = inspect.signature(getattr(HurriCacheClient, name))
    params = signature.parameters
    if "element_key" in params:
        kwargs["element_key"] = b"entry"
    if "value" in params:
        kwargs["value"] = 7 if name.startswith("atomic_") else b"new"
    if "values" in params:
        kwargs["values"] = [OrderedPayload(b"item", 7)] if name in {
            "create_ordered_set", "add_element_ordered", "add_element_ordered_set", "add_element_with_weight"
        } else [b"item"]
    if "keys" in params:
        kwargs["keys"] = [OrderedPayload(b"entry", 7)] if "ordered_map" in name else [b"entry"]
        if name in {"add_element_ordered", "add_element_unordered", "add_element", "remove_from_container", "remove_from_container_by_key_value"}:
            kwargs.pop("keys")
    if "pivot" in params:
        kwargs["pivot"] = b"pivot"
    if "pos" in params:
        kwargs["pos"] = b"pivot" if "by_value" in name else 7
    if "end" in params:
        kwargs["end"] = 9
    return kwargs


def operation_shape(name):
    if "ordered_map" in name:
        return "ordered_map"
    if "range_ordered" in name or name == "stream_ordered_set":
        return "ordered_set"
    if name == "stream_map":
        return "map"
    return "list"

@pytest.mark.parametrize("name", OPERATIONS)
async def test_every_operation(client_form, name):
    client, stub = client_form
    kwargs = operation_arguments(name)
    stub.shape = operation_shape(name)
    await invoke(client, name, b"key", **kwargs)
    assert stub.calls, name
    assert all(0 < options["timeout"] <= 2 for _, _, options in stub.calls)


@pytest.mark.parametrize("length", [0, 7, 8, 9])
async def test_compression_policy_and_defaults(client_form, length):
    client, stub = client_form
    data = b"x" * length
    await invoke(client, "create_key_value", data, value=data)
    scalar = stub.calls[-1][1]
    assert scalar.key.HasField("compressionInfo") == (length > 8)
    assert scalar.value.HasField("compressionInfo") == (length > 8)
    assert scalar.value.ttl > int(time.time() * 1000)
    await invoke(client, "create_key_value", b"k", value=data, ttl=0)
    assert not stub.calls[-1][1].value.HasField("ttl")
    for name in ["create_list", "create_vector", "create_queue", "create_set"]:
        await invoke(client, name, b"k", values=[data])
        assert not stub.calls[-1][1].value_unordered[0].HasField("compressionInfo")
    await invoke(client, "create_map", b"k", keys=[data], values=[data])
    request = stub.calls[-1][1]
    assert not request.key_unordered[0].HasField("compressionInfo")
    assert not request.value_unordered[0].HasField("compressionInfo")
    assert request.value_unordered[0].ttl > int(time.time() * 1000)
    await invoke(client, "create_ordered_map", b"k", keys=[OrderedPayload(data, 1)], values=[data])
    assert stub.calls[-1][1].value_unordered[0].HasField("compressionInfo") == (length > 8)
    await invoke(client, "create_ordered_set", b"k", values=[OrderedPayload(data, 1)])
    ordered = stub.calls[-1][1].value_ordered[0]
    assert ordered.HasField("compressionInfo") == (length > 8)
    assert ordered.ttl > int(time.time() * 1000)
    for name in ["add_element_to_tail", "add_element_to_head", "add_element_to_position"]:
        await invoke(client, name, b"k", values=[data])
        assert not stub.calls[-1][1].value_unordered[0].HasField("compressionInfo")
    await invoke(client, "get_value_in_container", b"k", data)
    assert not stub.calls[-1][1].element_key.HasField("compressionInfo")
    await invoke(client, "remove_from_container_by_key_value", b"k", keys=[data], values=[data])
    request = stub.calls[-1][1]
    assert request.keys[0].HasField("compressionInfo") == (length > 8)
    assert not request.values[0].HasField("compressionInfo")


async def test_preflight_continuation_and_partial(client_form, monkeypatch):
    client, stub = client_form
    monkeypatch.setattr(batching, "MAX_RPC_SIZE", 180)
    with pytest.raises(ValueError):
        await invoke(client, "create_list", b"k", values=[b"a", b"x" * 200])
    assert not stub.calls
    values = [bytes([i]) * 60 for i in range(6)]
    await invoke(client, "create_list", b"k", values=values)
    assert stub.calls[0][0] == "createContainer"
    assert all(name == "addElementToTail" for name, _, _ in stub.calls[1:])
    assert sum(len(req.value_unordered) for _, req, _ in stub.calls) == len(values)
    assert all(req.ByteSize() <= 180 for _, req, _ in stub.calls)
    assert all(req.key.keyHint.week_hash == 0xFFFFFFFF for _, req, _ in stub.calls[1:])
    stub.calls.clear()
    stub.fail_at = 2
    with pytest.raises(PartialOperationError) as caught:
        await invoke(client, "add_element_to_tail", b"k", values=values)
    assert caught.value.completed_chunks == 1
    assert caught.value.completed_items > 0
    assert isinstance(caught.value.__cause__, UnavailableError)
    assert len(stub.calls) == 2


async def test_one_deadline_and_lock_presence(client_form, monkeypatch):
    client, stub = client_form
    await invoke(client, "lock_object", b"k")
    assert not stub.calls[-1][1].HasField("lockDuration")
    await invoke(client, "lock_object", b"k", lock_duration=1.25)
    assert int(time.time()) <= stub.calls[-1][1].lockDuration <= int(time.time()) + 2
    for duration in [-1, float("inf"), 0xFFFFFFFF]:
        with pytest.raises(ValueError):
            await invoke(client, "lock_object", b"k", lock_duration=duration)
    monkeypatch.setattr(batching, "MAX_RPC_SIZE", 180)
    stub.calls.clear()
    stub.delay = 0.02
    with pytest.raises(PartialOperationError) as caught:
        await invoke(client, "add_element_to_tail", b"k", values=[b"x" * 60] * 8, timeout=0.01)
    assert isinstance(caught.value.__cause__, DeadlineExceededError)
    assert len(stub.calls) == 1


def test_fractional_lock_uses_java_milliseconds(monkeypatch):
    monkeypatch.setattr("hurricache.grpc.operation.time.time", lambda: 100.9999)
    assert lock_expiration(0.0009) == 100
    assert lock_expiration(0.001) == 101


@pytest.mark.parametrize("cls", [HurriCacheClient, AsyncHurriCacheClient, HurriCacheSmartClient, AsyncHurriCacheSmartClient])
def test_invalid_configuration(cls):
    with pytest.raises(ValueError):
        cls(default_compression_threshold=-1)
    with pytest.raises(ValueError):
        cls(default_ttl=-1)
