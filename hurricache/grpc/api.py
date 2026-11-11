"""Current Java API: transport-independent requests and operation recipes.

Every public operation is installed with the same signature on both transports.
The async driver awaits grpc.aio directly; recipes never perform I/O themselves.
"""

from __future__ import annotations

import inspect
import time
import warnings
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from functools import wraps
from typing import Any, no_type_check

from hurricache.grpc import cache_pb2 as pb
from hurricache.grpc.batching import check, continuation, plan
from hurricache.grpc.exceptions import HurriCacheError, PartialOperationError
from hurricache.grpc.models import (
    KeyHintData,
    LockStatus,
    LockType,
    OrderedPayload,
    OrderedVariant,
    Payload,
    Variant,
    int64,
)
from hurricache.grpc.operation import absolute_ttl, lock_expiration, operations
from hurricache.grpc.utils import (
    create_key,
    create_ordered_key,
    create_ordered_value,
    create_value,
    decode_cas,
    decode_hint,
    decode_response,
    decode_update,
)


def identity(value):
    return value


def count(response):
    return response.size


def boolean(response):
    return response.value


def hint_result(response):
    return decode_hint(response) or KeyHintData(was_relayed=response.relayed)


def number(response):
    return decode_response(response).number_value()


def integer(response):
    return decode_response(response).require_long()


@dataclass
class Call:
    rpc: str
    request: Any
    decoder: Callable = identity
    kind: str = "unary"
    shape: str | None = None


BUILDERS: dict[str, Callable] = {}
WRITE_OPERATIONS: set[str] = set()


def api(*, write=False):
    def register(method):
        BUILDERS[method.__name__] = method
        if write:
            WRITE_OPERATIONS.add(method.__name__)
        return method
    return register


def _get(self, key, hint, client_id):
    return pb.GetRequest(key=self._key(key, hint, client_id))


def _entry(self, key, hint, element_key, client_id, *, compress=False):
    return pb.ContainerGetRequest(key=self._key(key, hint, client_id),
                                  element_key=create_key(element_key, client_id=self._client_id(client_id), compress=compress))


def _unsigned(value, bits=64):
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("position must be an integer, excluding bool")
    if not 0 <= value < (1 << bits):
        raise ValueError(f"position is outside uint{bits}")
    return value


def _numeric(value, *, bits=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("atomic value must be int or float, excluding bool")
    if bits or isinstance(value, int):
        int64(value)
    return value


def _selector(value):
    if isinstance(value, bool) or not isinstance(value, (int, bytes, bytearray, memoryview, str)):
        raise TypeError("selector must be a byte/string entry key or integer position")
    return value


@api()
def create(self, key, hint=None, value=Variant.none(), ttl=None, client_id=None, **kwargs):
    return Call("createKeyValue", pb.CreateRequest(key=self._key(key, hint, client_id),
                value=create_value(value, ttl, compress=True)), hint_result)


@api()
def replace(self, key, hint=None, value=Variant.none(), ttl=None, client_id=None, **kwargs):
    return Call("updateValue", pb.UpdateRequest(key=self._key(key, hint, client_id),
                value=create_value(value, ttl, compress=True)), decode_update)


@api()
def contains_key(self, key, hint=None, client_id=None, *, element_key=None, **kwargs):
    if element_key is not None:
        return Call("existInContainerByKey", _entry(self, key, hint, element_key, client_id), boolean)
    return Call("existKey", _get(self, key, hint, client_id), boolean)


@api()
def remove(self, key, hint=None, client_id=None, *, element_key=None, **kwargs):
    if element_key is not None:
        return Call("deleteFromContainerByKey", _entry(self, key, hint, element_key, client_id), count)
    return Call("remove", _get(self, key, hint, client_id), boolean)


@api(write=True)
def update(self, key, hint=None, element_key=b"", value=Variant.none(), client_id=None, *, new_order=None, **kwargs):
    if isinstance(element_key, OrderedPayload):
        if new_order is not None and value != Variant.none():
            raise TypeError("supply either a positional order or new_order, not both")
        order = value if new_order is None else new_order
        return _order_update(self, key, hint, element_key, order, client_id)
    if new_order is not None:
        raise TypeError("new_order requires an OrderedPayload with the current order")
    entry = _entry(self, key, hint, element_key, client_id)
    return Call("updateValueInContainer", pb.UpdateContainerRequest(key=entry.key, element_key=entry.element_key,
                value=create_value(value, compress=True)), decode_update)


def _order_update(self, key, hint, element_key, amount, client_id, *, increment=False):
    if not isinstance(element_key, OrderedPayload) or element_key.order is None:
        raise ValueError("element_key must be an OrderedPayload carrying the current order")
    _unsigned(element_key.order)
    amount = int64(amount) if increment else _unsigned(amount)
    request = pb.UpdateContainerRequest(
        key=self._key(key, hint, client_id),
        ordered_element_key=create_ordered_key(element_key, client_id=self._client_id(client_id)),
    )
    setattr(request, "delta_order" if increment else "new_order", amount)
    return Call("updateValueInContainer", request, lambda response: response.result)


@api(write=True)
def update_order(self, key, hint=None, element_key=None, new_order=0, client_id=None, **kwargs):
    """Move exactly one ordered-map/set entry, identified by bytes and current order."""
    return _order_update(self, key, hint, element_key, new_order, client_id)


@api(write=True)
def increment_order(self, key, hint=None, element_key=None, delta=0, client_id=None, **kwargs):
    """Adjust an ordered entry's order by a signed int64 delta, preserving its value."""
    return _order_update(self, key, hint, element_key, delta, client_id, increment=True)


@api(write=True)
def set_ttl(self, key, hint=None, ttl=0, client_id=None, **kwargs):
    return Call("setTtl", pb.TtlRequest(key=self._key(key, hint, client_id),
                ttl=(1 << 64) - 1 if ttl < 0 else absolute_ttl(ttl)), boolean)


@api()
def get_ttl(self, key, hint=None, client_id=None, **kwargs):
    return Call("getTtl", _get(self, key, hint, client_id),
                lambda r: r.ttl - int(time.time() * 1000) if r.HasField("ttl") else -1)


@api(write=True)
def lock(self, key, hint=None, lock_type=LockType.NO_LOCK, client_id=None, lock_duration=0, **kwargs):
    return Call("lockObject", pb.LockRequest(key=self._key(key, hint, client_id), lockType=int(LockType(lock_type)),
                clientId=self._client_id(client_id), lockDuration=lock_expiration(lock_duration)),
                lambda r: LockStatus(r.result))


@api(write=True)
def unlock(self, key, hint=None, client_id=None, **kwargs):
    return Call("unlockObject", pb.UnLockRequest(key=self._key(key, hint, client_id),
                clientId=self._client_id(client_id)), lambda r: LockStatus(r.result))


def _simple(name, rpc, decoder=decode_response, *, write=False, stream=False, shape=None):
    def method(self, key, hint=None, client_id=None, **kwargs):
        return Call(rpc, _get(self, key, hint, client_id), decoder, "stream" if stream else "unary", shape)
    method.__name__ = name
    api(write=write)(method)


for _name, _rpc, _write in [
    ("get", "getValue", False), ("pop", "getAndDeleteValue", True),
    ("peek_head", "getHead", False), ("peek_tail", "getTail", False),
    ("pop_head", "getAndDeleteHead", True), ("pop_tail", "getAndDeleteTail", True),
]:
    _simple(_name, _rpc, write=_write)
for _name, _rpc in [("delete_head", "deleteHead"), ("delete_tail", "deleteTail")]:
    _simple(_name, _rpc, count, write=True)
_simple("get_size", "getSize", count)
_simple("get_container", "getContainer", stream=True)
for _shape in ("list", "vector", "queue", "set", "map", "ordered_map", "ordered_set"):
    _simple("get_" + _shape, "getContainer", stream=True, shape=_shape)


def _collection(name, container_type):
    def method(self, key, hint=None, values=None, ttl=None, client_id=None, **kwargs):
        request = pb.CreateContainerRequest(key=self._key(key, hint, client_id), type=container_type)
        if ttl:
            request.ttl = absolute_ttl(ttl)
        if container_type == pb.ORDERED_SET:
            request.value_ordered.extend(create_ordered_value(v, ttl=ttl) for v in (values or ()))
        else:
            request.value_unordered.extend(create_value(v, compress=True) for v in (values or ()))
        return Call("createContainer", request, hint_result, "create")
    method.__name__ = name
    api(write=True)(method)


for _shape, _type in [("vector", pb.VECTOR), ("list", pb.LIST), ("queue", pb.QUEUE), ("set", pb.SET),
                       ("ordered_set", pb.ORDERED_SET)]:
    _collection("create_" + _shape, _type)


def _pairs(keys, values):
    if isinstance(keys, Mapping):
        if values is not None:
            raise TypeError("values must be omitted when keys is a mapping")
        return list(keys), list(keys.values())
    keys, values = list(keys or ()), list(values or ())
    if len(keys) != len(values):
        raise ValueError("map keys and values must have equal lengths")
    return keys, values


@no_type_check
def _map(name, ordered, creation):
    def build(self, key, hint, keys, values, ttl, client_id):
        keys, values = _pairs(keys, values)
        cid = self._client_id(client_id)
        request_type = pb.CreateContainerRequest if creation else pb.AddToRequest
        request = request_type(key=self._key(key, hint, cid), type=pb.ORDERED_MAP if ordered else pb.MAP)
        if creation and ttl:
            request.ttl = absolute_ttl(ttl)
        if ordered:
            request.key_ordered.extend(create_ordered_key(k, client_id=cid) for k in keys)
        elif creation:
            request.key_unordered.extend(create_key(k, client_id=cid) for k in keys)
        else:
            # Java putAll sends raw entry keys without clientId presence.
            for k in keys:
                wire = create_key(k, compress=False)
                wire.ClearField("clientId")
                request.key_unordered.append(wire)
        request.value_unordered.extend(create_value(v, ttl if creation else 0, compress=True) for v in values)
        return Call("createContainer" if creation else "addElement", request,
                    hint_result if creation else count, "create" if creation else "batch")
    if creation:
        def method(self, key, hint=None, keys=None, values=None, ttl=None, client_id=None, **kwargs):
            return build(self, key, hint, keys, values, ttl, client_id)
    else:
        def method(self, key, hint=None, keys=None, values=None, client_id=None, **kwargs):
            return build(self, key, hint, keys, values, 0, client_id)
    method.__name__ = name
    api(write=True)(method)


_map("create_map", False, True)
_map("create_ordered_map", True, True)
_map("put_all", False, False)
_map("add_element_ordered_map", True, False)


def _position(name, rpc, *, deletion=False):
    def method(self, key, hint=None, pos=0, client_id=None, **kwargs):
        return Call(rpc, pb.KeyPositionRequest(key=self._key(key, hint, client_id), pos=_unsigned(pos)),
                    count if deletion else decode_response)
    method.__name__ = name
    api(write=rpc.startswith("getAndDelete") or deletion)(method)


_position("get_at", "getElementAtPosition")
_position("pop_at", "getAndDeleteElementAtPosition")
# These two weighted operations deliberately use configured routing in Java.
_position("get_element_with_weight", "getElementAtPosition")
_position("get_and_remove_element_with_weight", "getAndDeleteElementAtPosition")
WRITE_OPERATIONS.discard("get_and_remove_element_with_weight")


@api(write=True)
def remove_at(self, key, hint=None, pos=0, end=0, client_id=None, **kwargs):
    request = pb.KeyPositionRequest(key=self._key(key, hint, client_id), pos=_unsigned(pos))
    if isinstance(end, bool) or not isinstance(end, int):
        raise TypeError("end must be an integer")
    if end >= (1 << 64):
        raise ValueError("end is outside uint64")
    if end > pos:
        request.end = end
    return Call("deleteElementAtPosition", request, count)


def _range(name, container_type=None, destructive=False):
    def method(self, key, hint=None, pos=0, end=0, reverse=False, client_id=None, **kwargs):
        return _range_call(self, key, hint, container_type, pos, end, reverse, client_id, destructive)
    method.__name__ = name
    api(write=destructive)(method)


def _range_call(self, key, hint, container_type, pos, end, reverse, client_id, destructive=False):
    request = pb.KeyPositionRequest(key=self._key(key, hint, client_id), type=container_type, reverse=reverse)
    for field_name, value in (("pos", pos), ("end", end)):
        # Java destructive ranges omit nonpositive bounds. Python additionally
        # accepts None for an unbounded range, preserving protobuf presence.
        if value is not None:
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError("range bounds must be integers or None")
            if destructive and value <= 0:
                continue
            setattr(request, field_name, _unsigned(value))
    shape = "ordered_map" if container_type == pb.ORDERED_MAP else "ordered_set" if container_type == pb.ORDERED_SET else "list"
    return Call("getAndDeleteElementsInRange" if destructive else "getElementsInRange", request,
                kind="stream", shape=shape)


@api()
def get_range(self, key, hint=None, type=pb.LIST, pos=0, end=0, client_id=None, **kwargs):
    if type not in (pb.LIST, pb.VECTOR, pb.SET):
        raise ValueError("get_range supports LIST, VECTOR and SET")
    return _range_call(self, key, hint, type, pos, end, False, client_id)


for _shape, _type in [("ordered_set", pb.ORDERED_SET), ("ordered_map", pb.ORDERED_MAP)]:
    _range("get_element_in_range_" + _shape, _type)
    _range("get_and_delete_element_in_range_" + _shape, _type, True)
for _shape, _type in [("list", pb.LIST), ("vector", pb.VECTOR)]:
    _range("get_and_delete_" + _shape + "_elements_in_range", _type, True)


def _in_container(name, rpc, decoder=decode_response, *, write=False):
    def method(self, key, hint=None, element_key=b"", client_id=None, **kwargs):
        return Call(rpc, _entry(self, key, hint, element_key, client_id), decoder)
    method.__name__ = name
    api(write=write)(method)


_in_container("get_in_container", "getValueInContainerByKey")
_in_container("pop_in_container", "getAndDeleteValueInContainer", write=True)
_in_container("contains_in_container", "existInContainerByKey", boolean)
_in_container("remove_in_container", "deleteFromContainerByKey", count, write=True)


@api(write=True)
def remove_from_container_by_key(self, key, hint=None, container_key=b"", client_id=None, **kwargs):
    if isinstance(container_key, Variant):
        container_key = container_key.require_bytes()
    return Call("deleteFromContainerByKey", _entry(self, key, hint, container_key, client_id, compress=True), count)


@api(write=True)
def remove_from_container_by_key_and_value(self, key, hint=None, keys=None, values=None, client_id=None, **kwargs):
    return Call("deleteFromContainerByKeyValue", pb.RemoveFromContainerRequest(key=self._key(key, hint, client_id),
                keys=[create_key(k, client_id=self._client_id(client_id)) for k in (keys or ())],
                values=[create_value(v, compress=True) for v in (values or ())]), count, "batch")


def _insert(name, rpc, *, pos=False, ordered=False, append=False):
    def method(self, key, hint=None, values=None, client_id=None, **kwargs):
        request = pb.AddToRequest(key=self._key(key, hint, client_id))
        if append:
            request.pos = 0xFFFFFFFF
        field_name = "value_ordered" if ordered else "value_unordered"
        getattr(request, field_name).extend((create_ordered_value(v) if ordered else create_value(v, compress=True))
                                           for v in (values or ()))
        return Call(rpc, request, count, "batch")
    method.__name__ = name
    api(write=True)(method)


_insert("push_head", "addElementToHead")
_insert("push_tail", "addElementToTail")
_insert("add_element_unordered", "addElement", append=True)
_insert("add_element_with_weight", "addElement", ordered=True)


@api(write=True)
def insert_at(self, key, hint=None, values=None, pos=0, client_id=None, **kwargs):
    request = pb.AddToRequest(key=self._key(key, hint, client_id), pos=_unsigned(pos, 32),
                              value_unordered=[create_value(v, compress=True) for v in (values or ())])
    return Call("addElement", request, count, "batch")


def _pivot(name, before):
    def method(self, key, hint=None, values=None, pivot=Variant.none(), client_id=None, **kwargs):
        request = pb.AddToValRequest(key=self._key(key, hint, client_id), isBefore=before,
                    pos=create_value(pivot, compress=True), value=[create_value(v, compress=True) for v in (values or ())])
        return Call("addElementToPositionByValue", request, count, "batch")
    method.__name__ = name
    api(write=True)(method)


_pivot("insert_before", True)
_pivot("insert_after", False)


def _atomic_call(self, key, hint, value, ttl, client_id, rpc, selector=None, bits=False):
    value = _numeric(value, bits=bits)
    request = pb.CreateRequest(key=self._key(key, hint, client_id), value=create_value(value, ttl))
    if selector is not None:
        if isinstance(selector, int):
            request.container_key.pos = _unsigned(selector)
        else:
            request.container_key.container_key.CopyFrom(create_key(selector, client_id=self._client_id(client_id)))
    return Call(rpc, request, integer if bits else number)


@no_type_check
def _atomic(name, rpc, parameter, bits=False, container=False):
    # Separate functions keep meaningful inspect signatures for each family.
    if container and parameter == "value":
        def method(self, key, hint=None, selector=b"", value=0, ttl=None, client_id=None, **kwargs):
            return _atomic_call(self, key, hint, value, ttl, client_id, rpc, _selector(selector), bits)
    elif container and parameter == "delta":
        def method(self, key, hint=None, selector=b"", delta=0, ttl=None, client_id=None, **kwargs):
            return _atomic_call(self, key, hint, delta, ttl, client_id, rpc, _selector(selector), bits)
    elif container:
        def method(self, key, hint=None, selector=b"", mask=0, ttl=None, client_id=None, **kwargs):
            return _atomic_call(self, key, hint, mask, ttl, client_id, rpc, _selector(selector), bits)
    elif parameter == "value":
        def method(self, key, hint=None, value=0, ttl=None, client_id=None, **kwargs):
            return _atomic_call(self, key, hint, value, ttl, client_id, rpc, bits=bits)
    elif parameter == "delta":
        def method(self, key, hint=None, delta=0, ttl=None, client_id=None, **kwargs):
            return _atomic_call(self, key, hint, delta, ttl, client_id, rpc, bits=bits)
    else:
        def method(self, key, hint=None, mask=0, ttl=None, client_id=None, **kwargs):
            return _atomic_call(self, key, hint, mask, ttl, client_id, rpc, bits=bits)
    method.__name__ = name
    api(write=True)(method)


for _name, _parameter in [("exchange", "value"), ("add", "delta"), ("sub", "delta"),
                          ("and", "mask"), ("or", "mask"), ("xor", "mask")]:
    _rpc = "atomic" + _name.capitalize()
    _atomic(_name + "_" if _name in ("and", "or") else _name, _rpc, _parameter, _parameter == "mask")
    _atomic(_name + "_in_container", _rpc, _parameter, _parameter == "mask", True)


@api(write=True)
def compare_and_set(self, key, hint=None, expected_value=0, new_value=0, ttl=None, client_id=None, **kwargs):
    _numeric(expected_value)
    _numeric(new_value)
    # Java overloads select double if either operand is a double.
    if isinstance(expected_value, float) or isinstance(new_value, float):
        expected_value, new_value = float(expected_value), float(new_value)
    return Call("atomicCompareAndSet", pb.AtomicCas(key=self._key(key, hint, client_id),
                expected=create_value(expected_value), toSet=create_value(new_value, ttl)), decode_cas)


@api(write=True)
def compare_and_set_in_container(self, key, hint=None, selector=b"", expected_value=0, new_value=0,
                                 client_id=None, **kwargs):
    """CAS a map field, or an ordered-map field selected by exact bytes and order."""
    _numeric(expected_value)
    _numeric(new_value)
    if isinstance(expected_value, float) or isinstance(new_value, float):
        expected_value, new_value = float(expected_value), float(new_value)
    request = pb.AtomicCas(key=self._key(key, hint, client_id),
                           expected=create_value(expected_value), toSet=create_value(new_value, ttl=0))
    if isinstance(selector, OrderedPayload):
        if selector.order is None:
            raise ValueError("ordered selector must carry an order")
        _unsigned(selector.order)
        request.ordered_element_key.CopyFrom(create_ordered_key(
            selector, client_id=self._client_id(client_id)))
    elif isinstance(selector, (bytes, str)):
        request.element_key.CopyFrom(create_key(selector, client_id=self._client_id(client_id), compress=True))
    else:
        raise TypeError("container CAS selector must be bytes, str, or OrderedPayload")
    return Call("atomicCompareAndSet", request, decode_cas)


def recipe(call):
    """Yield unary calls, aggregating progress identically for both drivers."""
    if call.kind == "create":
        request = call.request
        if request.HasField("ttl"):
            for value in list(request.value_unordered) + list(request.value_ordered):
                if request.type in (pb.MAP, pb.ORDERED_MAP, pb.ORDERED_SET):
                    value.ttl = request.ttl
        chunks = plan(request, creation=True)
        for chunk, _ in chunks[1:]:
            check(continuation(chunk, pb.KeyHint(weak_hash=0xFFFFFFFF, strong_hash=0xFFFFFFFF)))
        response = yield Call("createContainer", chunks[0][0])
        hint = hint_result(response)
        done, acknowledged = chunks[0][1], 1
        rpc = "addElementToTail" if request.type in (pb.LIST, pb.VECTOR, pb.QUEUE) else "addElement"
        for chunk, size in chunks[1:]:
            try:
                wire_hint = response.keyHint if response.HasField("keyHint") else None
                result = yield Call(rpc, continuation(chunk, wire_hint))
                if request.type in (pb.LIST, pb.VECTOR, pb.QUEUE) and result.size == 0:
                    raise HurriCacheError("server rejected continuation chunk")
            except Exception as error:
                raise PartialOperationError(acknowledged, done, hint) from error
            acknowledged += 1
            done += size
        return hint
    if call.kind == "batch":
        chunks = plan(call.request)
        # Head RPCs reverse their values as they prepend them. Forward chunks
        # preserve the same full reversal as a single RPC. Pivot chunks advance
        # their anchor in the planner, preserving ordinary insertion order.
        total, done, acknowledged = 0, 0, 0
        for chunk, size in chunks:
            if not size:
                continue
            try:
                response = yield Call(call.rpc, chunk)
            except Exception as error:
                if acknowledged:
                    raise PartialOperationError(acknowledged, done, total) from error
                raise
            if total + response.size > 0xFFFFFFFF:
                raise PartialOperationError(acknowledged + 1, done + size, total) from OverflowError("count exceeds uint32")
            total += response.size
            done += size
            acknowledged += 1
        return total
    return call.decoder((yield call))


def stream_shape(value, shape):
    if shape in ("map", "ordered_map") and not value:
        return {}
    if shape is not None and bool(value):
        if isinstance(value, dict) != (shape in ("map", "ordered_map")):
            raise ValueError("unexpected container response shape")
        expected = OrderedPayload if shape == "ordered_map" else Payload if shape == "map" else OrderedVariant if shape == "ordered_set" else Variant
        if not all(type(item) is expected for item in value):
            raise ValueError("unexpected container element representation")
    return value


# Only aliases whose positional binding is identical are retained here.
ALIASES = {
    "create_key_value": "create", "get_value": "get", "update_value": "replace", "update_key_value": "replace",
    "get_and_delete_value": "pop", "exist_key": "contains_key", "lock_object": "lock", "unlock_object": "unlock",
    "get_head": "peek_head", "get_front": "peek_head", "get_tail": "peek_tail",
    "get_and_remove_front": "pop_head", "get_and_remove_tail": "pop_tail",
    "remove_head": "delete_head", "remove_tail": "delete_tail",
    "atomic_exchange": "exchange", "atomic_add": "add", "atomic_sub": "sub", "atomic_and": "and_",
    "atomic_or": "or_", "atomic_xor": "xor", "atomic_compare_and_set": "compare_and_set",
}
for _shape in ("list", "vector", "queue", "set", "map", "ordered_set", "ordered_map"):
    ALIASES["stream_" + _shape] = "get_" + _shape


def install(cls, *, asynchronous=False):
    def wrapper(name, builder):
        parameters = inspect.signature(builder).parameters
        def transport_options(kwargs):
            return {key: value for key, value in kwargs.items() if key not in parameters}

        if asynchronous:
            @wraps(builder)
            async def method(self, *args, **kwargs):
                call = builder(self, *args, **kwargs)
                kwargs = transport_options(kwargs)
                if call.kind == "stream":
                    result = await self._stream(name, getattr(self.stub, call.rpc), call.request, **kwargs)
                    return stream_shape(result, call.shape)
                steps = recipe(call)
                try:
                    step = next(steps)
                    while True:
                        try:
                            response = await self._unary(name, getattr(self.stub, step.rpc), step.request, **kwargs)
                        except Exception as error:
                            step = steps.throw(error)
                        else:
                            step = steps.send(response)
                except StopIteration as result:
                    return result.value
                finally:
                    steps.close()
        else:
            @wraps(builder)
            def method(self, *args, **kwargs):
                call = builder(self, *args, **kwargs)
                key = args[0] if args else kwargs.get("key")
                kwargs = transport_options(kwargs)
                if call.kind == "stream":
                    stream = self._call(name, getattr(self.stub, call.rpc), call.request, **kwargs)
                    return stream_shape(self._collect_stream(name, stream, key), call.shape)
                steps = recipe(call)
                try:
                    step = next(steps)
                    while True:
                        try:
                            response = self._call(name, getattr(self.stub, step.rpc), step.request, **kwargs)
                        except Exception as error:
                            step = steps.throw(error)
                        else:
                            step = steps.send(response)
                except StopIteration as result:
                    return result.value
                finally:
                    steps.close()
        return method

    for name, builder in BUILDERS.items():
        setattr(cls, name, wrapper(name, builder))
    operations(cls)

    def alias(old, new):
        target = getattr(cls, new)
        if asynchronous:
            @wraps(target)
            async def method(self, *args, **kwargs):
                warnings.warn(f"{old} is deprecated; use {new}", DeprecationWarning, stacklevel=2)
                return await target(self, *args, **kwargs)
        else:
            @wraps(target)
            def method(self, *args, **kwargs):
                warnings.warn(f"{old} is deprecated; use {new}", DeprecationWarning, stacklevel=2)
                return target(self, *args, **kwargs)
        return method

    for old, new in ALIASES.items():
        setattr(cls, old, alias(old, new))
    return cls
