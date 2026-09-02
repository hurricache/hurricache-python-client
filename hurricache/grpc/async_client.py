"""Native :mod:`grpc.aio` HurriCache client."""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from typing import Any

import grpc

from hurricache.grpc import cache_pb2, cache_pb2_grpc
from hurricache.grpc.exceptions import KeyNotFoundError, mapped_rpc_error
from hurricache.grpc.models import CasResult, KeyHintData, LockStatus, LockType
from hurricache.grpc.utils import (
    MAX_RPC_SIZE,
    KeyLike,
    build_get_request,
    create_key,
    create_ordered_key,
    create_ordered_value,
    create_value,
    decode_batch,
    decode_hint,
    decode_key,
    decode_value,
)


def _identity(value: Any) -> Any:
    return value


def _decode_value_response(response: Any) -> bytes:
    if response.HasField("value_unordered"):
        return decode_value(response.value_unordered)
    if response.HasField("value_ordered"):
        return decode_value(response.value_ordered)
    raise KeyNotFoundError(b"", "Value response is empty")


class AsyncHurriCacheClient:
    """Asynchronous direct client backed exclusively by ``grpc.aio``."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 50000,
        default_client_id: int = 0,
        default_timeout: float = 1.0,
        credentials: grpc.ChannelCredentials | None = None,
        compression: grpc.Compression | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._default_client_id = default_client_id
        self._default_timeout = default_timeout
        self._credentials = credentials
        self._compression = compression
        self._channel: grpc.aio.Channel | None = None
        self._stub: cache_pb2_grpc.HurriCacheGrpcServiceStub | None = None

    @property
    def channel(self) -> grpc.aio.Channel:
        if self._channel is None:
            target = f"{self._host}:{self._port}"
            if self._credentials is None:
                self._channel = grpc.aio.insecure_channel(target, compression=self._compression)
            else:
                self._channel = grpc.aio.secure_channel(target, self._credentials, compression=self._compression)
            self._stub = cache_pb2_grpc.HurriCacheGrpcServiceStub(self._channel)
        return self._channel

    @property
    def stub(self) -> cache_pb2_grpc.HurriCacheGrpcServiceStub:
        if self._stub is None:
            _ = self.channel
        assert self._stub is not None
        return self._stub

    def _client_id(self, client_id: int | None) -> int:
        return self._default_client_id if client_id in (None, 0) else client_id

    def _key(self, key: KeyLike, hint: KeyHintData | None, client_id: int | None) -> cache_pb2.Key:
        return create_key(key, hint, self._client_id(client_id))

    def _get(self, key: KeyLike, hint: KeyHintData | None, client_id: int | None) -> cache_pb2.GetRequest:
        return build_get_request(key, hint, self._client_id(client_id))

    async def _unary(
        self,
        name: str,
        rpc: Callable[..., Any],
        request: Any,
        decoder: Callable[[Any], Any] = _identity,
        **kwargs: Any,
    ) -> Any:
        kwargs.setdefault("timeout", self._default_timeout)
        try:
            return decoder(await rpc(request, **kwargs))
        except grpc.aio.AioRpcError as error:
            key = decode_key(request.key) if hasattr(request, "key") else b""
            raise mapped_rpc_error(name, error, key) from error

    async def _stream(self, name: str, rpc: Callable[..., Any], request: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("timeout", self._default_timeout)
        result: Any = None
        try:
            call = rpc(request, **kwargs)
            async for batch in call:
                decoded = decode_batch(batch)
                if result is None:
                    result = {} if isinstance(decoded, dict) else []
                result.update(decoded) if isinstance(result, dict) else result.extend(decoded)
        except grpc.aio.AioRpcError as error:
            key = decode_key(request.key) if hasattr(request, "key") else b""
            raise mapped_rpc_error(name, error, key) from error
        return [] if result is None else result

    async def close(self, grace: float | None = None) -> None:
        if self._channel is not None:
            await self._channel.close(grace)
            self._channel = None
            self._stub = None

    async def __aenter__(self) -> "AsyncHurriCacheClient":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()

    async def lock_object(
        self,
        key: KeyLike,
        hint: KeyHintData | None = None,
        lock_type: LockType = LockType.NO_LOCK,
        client_id: int | None = None,
        lock_duration: float = 0,
        **kwargs: Any,
    ) -> LockStatus:
        cid = self._client_id(client_id)
        request = cache_pb2.LockRequest(
            key=self._key(key, hint, cid), lockType=int(lock_type), clientId=cid, lockDuration=int(lock_duration * 1000)
        )
        return await self._unary("lock_object", self.stub.lockObject, request, lambda r: LockStatus(r.result), **kwargs)

    async def unlock_object(
        self, key: KeyLike, hint: KeyHintData | None = None, client_id: int | None = None, **kwargs: Any
    ) -> LockStatus:
        cid = self._client_id(client_id)
        request = cache_pb2.UnLockRequest(key=self._key(key, hint, cid), clientId=cid)
        return await self._unary("unlock_object", self.stub.unlockObject, request, lambda r: LockStatus(r.result), **kwargs)

    async def set_ttl(
        self, key: KeyLike, hint: KeyHintData | None = None, ttl: int = 0, client_id: int | None = None, **kwargs: Any
    ) -> bool:
        request = cache_pb2.TtlRequest(key=self._key(key, hint, client_id), ttl=int(time.time() * 1000) + ttl)
        return await self._unary("set_ttl", self.stub.setTtl, request, lambda r: r.value, **kwargs)

    async def get_ttl(
        self, key: KeyLike, hint: KeyHintData | None = None, client_id: int | None = None, **kwargs: Any
    ) -> int:
        def decode(response: Any) -> int:
            return response.ttl - int(time.time() * 1000) if response.HasField("ttl") else -1

        return await self._unary("get_ttl", self.stub.getTtl, self._get(key, hint, client_id), decode, **kwargs)

    async def create_key_value(
        self,
        key: KeyLike,
        hint: KeyHintData | None = None,
        value: bytes = b"",
        ttl: int = 0,
        client_id: int | None = None,
        **kwargs: Any,
    ) -> KeyHintData:
        cid = self._client_id(client_id)
        request = cache_pb2.CreateRequest(key=self._key(key, hint, cid), value=create_value(value, ttl, cid))
        return await self._unary(
            "create_key_value",
            self.stub.createKeyValue,
            request,
            lambda r: decode_hint(r) or KeyHintData.unspecified(),
            **kwargs,
        )

    async def get_value(
        self, key: KeyLike, hint: KeyHintData | None = None, client_id: int | None = None, **kwargs: Any
    ) -> bytes:
        return await self._unary(
            "get_value", self.stub.getValue, self._get(key, hint, client_id), _decode_value_response, **kwargs
        )

    async def get_and_delete_value(
        self, key: KeyLike, hint: KeyHintData | None = None, client_id: int | None = None, **kwargs: Any
    ) -> bytes:
        return await self._unary(
            "get_and_delete_value",
            self.stub.getAndDeleteValue,
            self._get(key, hint, client_id),
            _decode_value_response,
            **kwargs,
        )

    async def exist_key(
        self, key: KeyLike, hint: KeyHintData | None = None, client_id: int | None = None, **kwargs: Any
    ) -> bool:
        return await self._unary("exist_key", self.stub.existKey, self._get(key, hint, client_id), lambda r: r.value, **kwargs)

    async def update_value(
        self,
        key: KeyLike,
        hint: KeyHintData | None = None,
        value: bytes = b"",
        ttl: int = 0,
        client_id: int | None = None,
        **kwargs: Any,
    ) -> bytes:
        cid = self._client_id(client_id)
        request = cache_pb2.UpdateRequest(key=self._key(key, hint, cid), value=create_value(value, ttl, cid))
        return await self._unary(
            "update_value", self.stub.updateValue, request, lambda r: decode_value(r.value) if r.HasField("value") else b"", **kwargs
        )

    async def remove(
        self, key: KeyLike, hint: KeyHintData | None = None, client_id: int | None = None, **kwargs: Any
    ) -> bool:
        return await self._unary("remove", self.stub.remove, self._get(key, hint, client_id), lambda r: r.value, **kwargs)

    async def _create_container(
        self,
        key: KeyLike,
        hint: KeyHintData | None,
        container_type: int,
        values: Sequence[Any] | None,
        keys: Sequence[Any] | None,
        ttl: int,
        client_id: int | None,
        *,
        ordered_values: bool = False,
        ordered_keys: bool = False,
        **kwargs: Any,
    ) -> KeyHintData:
        cid = self._client_id(client_id)
        if keys is not None and len(keys) != len(values or ()):
            raise ValueError("map keys and values must have equal lengths")
        value_messages = [
            create_ordered_value(v) if ordered_values else create_value(v, 0, cid) for v in (values or ())
        ]
        key_messages = [create_ordered_key(k, client_id=cid) if ordered_keys else create_key(k, None, cid) for k in (keys or ())]
        request = cache_pb2.CreateContainerRequest(key=self._key(key, hint, cid), type=container_type)
        if ttl > 0:
            request.ttl = int(time.time() * 1000) + ttl
        value_field = request.value_ordered if ordered_values else request.value_unordered
        key_field = request.key_ordered if ordered_keys else request.key_unordered
        index = 0
        for index, value_message in enumerate(value_messages):
            pair_size = value_message.ByteSize() + (key_messages[index].ByteSize() if key_messages else 0)
            if request.ByteSize() + pair_size > MAX_RPC_SIZE:
                break
            value_field.append(value_message)
            if key_messages:
                key_field.append(key_messages[index])
        else:
            index = len(value_messages)
        hint_result = await self._unary(
            "create_container",
            self.stub.createContainer,
            request,
            lambda r: decode_hint(r) or KeyHintData.unspecified(),
            **kwargs,
        )
        if index < len(value_messages):
            tail_values = list(values or ())[index:]
            tail_keys = list(keys or ())[index:] if keys else None
            if ordered_values or ordered_keys:
                await self.add_element_ordered(key, hint_result, tail_values, tail_keys, client_id=cid, **kwargs)
            else:
                await self.add_element(key, hint_result, tail_values, tail_keys, client_id=cid, **kwargs)
        return hint_result

    async def create_vector(self, key: KeyLike, hint=None, values=None, ttl=0, client_id=None, **kwargs):
        return await self._create_container(key, hint, cache_pb2.VECTOR, values, None, ttl, client_id, **kwargs)

    async def create_list(self, key: KeyLike, hint=None, values=None, ttl=0, client_id=None, **kwargs):
        return await self._create_container(key, hint, cache_pb2.LIST, values, None, ttl, client_id, **kwargs)

    async def create_queue(self, key: KeyLike, hint=None, values=None, ttl=0, client_id=None, **kwargs):
        return await self._create_container(key, hint, cache_pb2.QUEUE, values, None, ttl, client_id, **kwargs)

    async def create_set(self, key: KeyLike, hint=None, values=None, ttl=0, client_id=None, **kwargs):
        return await self._create_container(key, hint, cache_pb2.SET, values, None, ttl, client_id, **kwargs)

    async def create_map(self, key: KeyLike, hint=None, keys=None, values=None, ttl=0, client_id=None, **kwargs):
        return await self._create_container(key, hint, cache_pb2.MAP, values, keys, ttl, client_id, **kwargs)

    async def create_ordered_set(self, key: KeyLike, hint=None, values=None, ttl=0, client_id=None, **kwargs):
        return await self._create_container(
            key, hint, cache_pb2.ORDERED_SET, values, None, ttl, client_id, ordered_values=True, **kwargs
        )

    async def create_ordered_map(self, key: KeyLike, hint=None, keys=None, values=None, ttl=0, client_id=None, **kwargs):
        return await self._create_container(
            key, hint, cache_pb2.ORDERED_MAP, values, keys, ttl, client_id, ordered_keys=True, **kwargs
        )

    async def get_container(self, key: KeyLike, hint=None, client_id=None, **kwargs):
        return await self._stream("get_container", self.stub.getContainer, self._get(key, hint, client_id), **kwargs)

    async def get_size(self, key: KeyLike, hint=None, client_id=None, **kwargs) -> int:
        return await self._unary("get_size", self.stub.getSize, self._get(key, hint, client_id), lambda r: r.size, **kwargs)

    async def _value_rpc(self, name: str, rpc: Callable[..., Any], request: Any, **kwargs: Any) -> bytes:
        return await self._unary(name, rpc, request, _decode_value_response, **kwargs)

    async def get_tail(self, key: KeyLike, hint=None, client_id=None, **kwargs):
        return await self._value_rpc("get_tail", self.stub.getTail, self._get(key, hint, client_id), **kwargs)

    async def get_head(self, key: KeyLike, hint=None, client_id=None, **kwargs):
        return await self._value_rpc("get_head", self.stub.getHead, self._get(key, hint, client_id), **kwargs)

    get_front = get_head

    def _container_get(self, key: KeyLike, element_key: KeyLike, hint, element_hint, client_id):
        cid = self._client_id(client_id)
        return cache_pb2.ContainerGetRequest(
            key=self._key(key, hint, cid), element_key=create_key(element_key, element_hint, cid)
        )

    async def get_value_in_container(
        self, key: KeyLike, element_key: KeyLike, hint=None, element_hint=None, client_id=None, **kwargs
    ):
        request = self._container_get(key, element_key, hint, element_hint, client_id)
        return await self._value_rpc("get_value_in_container", self.stub.getValueInContainer, request, **kwargs)

    async def exist_key_in_container(
        self, key: KeyLike, element_key: KeyLike, hint=None, element_hint=None, client_id=None, **kwargs
    ):
        request = self._container_get(key, element_key, hint, element_hint, client_id)
        return await self._unary(
            "exist_key_in_container", self.stub.existKeyInContainer, request, lambda r: r.value, **kwargs
        )

    contains_container_key = exist_key_in_container

    async def get_and_remove_front(self, key: KeyLike, hint=None, client_id=None, **kwargs):
        return await self._value_rpc(
            "get_and_remove_front", self.stub.getAndRemoveFront, self._get(key, hint, client_id), **kwargs
        )

    async def get_and_remove_tail(self, key: KeyLike, hint=None, client_id=None, **kwargs):
        return await self._value_rpc(
            "get_and_remove_tail", self.stub.getAndRemoveTail, self._get(key, hint, client_id), **kwargs
        )

    def _position(self, key, hint, pos, end, reverse, container_type, client_id):
        request = cache_pb2.KeyPositionRequest(
            key=self._key(key, hint, client_id), pos=pos, reverse=reverse
        )
        if end is not None:
            request.end = end
        if container_type is not None:
            request.type = int(container_type)
        return request

    async def get_element_at_position(
        self, key: KeyLike, hint=None, pos=0, type=cache_pb2.UNDEFINED, client_id=None, **kwargs
    ):
        request = self._position(key, hint, pos, None, False, type, client_id)
        return await self._value_rpc("get_element_at_position", self.stub.getElementAtPosition, request, **kwargs)

    async def get_and_remove_element_at_position(
        self, key: KeyLike, hint=None, pos=0, type=cache_pb2.UNDEFINED, client_id=None, **kwargs
    ):
        request = self._position(key, hint, pos, None, False, type, client_id)
        return await self._value_rpc(
            "get_and_remove_element_at_position", self.stub.getAndRemoveElementAtPosition, request, **kwargs
        )

    async def get_element_in_range(
        self, key: KeyLike, hint=None, pos=0, end=0, type=cache_pb2.UNDEFINED, reverse=False, client_id=None, **kwargs
    ):
        request = self._position(key, hint, pos, end, reverse, type, client_id)
        return await self._stream("get_element_in_range", self.stub.getElementInRange, request, **kwargs)

    async def get_and_delete_value_in_container(
        self, key: KeyLike, element_key: KeyLike, hint=None, element_hint=None, client_id=None, **kwargs
    ):
        request = self._container_get(key, element_key, hint, element_hint, client_id)
        return await self._value_rpc(
            "get_and_delete_value_in_container", self.stub.getAndDeleteValueInContainer, request, **kwargs
        )

    async def update_value_in_container(
        self, key: KeyLike, element_key: KeyLike, value=b"", hint=None, element_hint=None, ttl=0, client_id=None, **kwargs
    ):
        cid = self._client_id(client_id)
        request = cache_pb2.UpdateContainerRequest(
            key=self._key(key, hint, cid),
            element_key=create_key(element_key, element_hint, cid),
            value=create_value(value, ttl, cid),
        )
        return await self._unary(
            "update_value_in_container",
            self.stub.updateValueInContainer,
            request,
            lambda r: decode_value(r.value) if r.HasField("value") else b"",
            **kwargs,
        )

    async def _bool_get(self, name, rpc, key, hint, client_id, **kwargs):
        return await self._unary(name, rpc, self._get(key, hint, client_id), lambda r: r.value, **kwargs)

    async def remove_head(self, key: KeyLike, hint=None, client_id=None, **kwargs):
        return await self._bool_get("remove_head", self.stub.removeHead, key, hint, client_id, **kwargs)

    async def remove_tail(self, key: KeyLike, hint=None, client_id=None, **kwargs):
        return await self._bool_get("remove_tail", self.stub.removeTail, key, hint, client_id, **kwargs)

    async def remove_element_at_position(
        self, key: KeyLike, hint=None, pos=0, type=cache_pb2.UNDEFINED, client_id=None, **kwargs
    ):
        request = self._position(key, hint, pos, None, False, type, client_id)
        return await self._unary(
            "remove_element_at_position", self.stub.removeElementAtPosition, request, lambda r: r.value, **kwargs
        )

    async def remove_from_container_by_key_value(
        self, key: KeyLike, hint=None, type=cache_pb2.UNDEFINED, values=None, keys=None, client_id=None, **kwargs
    ):
        cid = self._client_id(client_id)
        request = cache_pb2.RemoveFromContainerRequest(
            key=self._key(key, hint, cid),
            values=[create_value(v, client_id=cid) for v in (values or ())],
            keys=[create_key(k, client_id=cid) for k in (keys or ())],
        )
        request.type = int(type)
        return await self._unary(
            "remove_from_container_by_key_value", self.stub.removeFromContainerByKeyValue, request, lambda r: r.size, **kwargs
        )

    async def remove_in_container(
        self, key: KeyLike, element_key: KeyLike, hint=None, element_hint=None, client_id=None, **kwargs
    ):
        request = self._container_get(key, element_key, hint, element_hint, client_id)
        return await self._unary("remove_in_container", self.stub.removeInContainer, request, lambda r: r.size, **kwargs)

    async def _send_add_chunks(self, name, rpc, request, **kwargs):
        if request.key_unordered:
            pairs = list(zip(request.key_unordered, request.value_unordered, strict=True))
            pair_fields = ("key_unordered", "value_unordered")
        elif request.key_ordered:
            paired_values = request.value_unordered or request.value_ordered
            pairs = list(zip(request.key_ordered, paired_values, strict=True))
            pair_fields = ("key_ordered", "value_unordered" if request.value_unordered else "value_ordered")
        elif request.value_ordered:
            pairs = [(None, item) for item in request.value_ordered]
            pair_fields = ("", "value_ordered")
        else:
            pairs = [(None, item) for item in request.value_unordered]
            pair_fields = ("", "value_unordered")
        if not pairs:
            response = await self._unary(name, rpc, request, **kwargs)
            return response.size if hasattr(response, "size") else response.value
        offset = 0
        total = 0
        all_ok = True
        size_response = False
        while offset < len(pairs):
            chunk = cache_pb2.AddToRequest(key=request.key)
            for optional in ("type", "ttl", "pos"):
                if request.HasField(optional):
                    setattr(chunk, optional, getattr(request, optional))
            consumed = 0
            for key_item, value_item in pairs[offset:]:
                extra = value_item.ByteSize() + (key_item.ByteSize() if key_item is not None else 0)
                if chunk.ByteSize() + extra > MAX_RPC_SIZE:
                    if consumed == 0:
                        raise ValueError("one element exceeds the maximum HurriCache request size")
                    break
                getattr(chunk, pair_fields[1]).append(value_item)
                if key_item is not None:
                    getattr(chunk, pair_fields[0]).append(key_item)
                consumed += 1
            response = await self._unary(name, rpc, chunk, **kwargs)
            if hasattr(response, "size"):
                size_response = True
                total += response.size
            else:
                all_ok = all_ok and response.value
            offset += consumed
        return total if size_response else all_ok

    async def _add_unordered(self, name, rpc, key, hint, values, keys, ttl, client_id, pos=None, **kwargs):
        cid = self._client_id(client_id)
        if keys is not None and len(keys) != len(values or ()):
            raise ValueError("map keys and values must have equal lengths")
        request = cache_pb2.AddToRequest(
            key=self._key(key, hint, cid),
            value_unordered=[create_value(v, ttl, cid) for v in (values or ())],
            key_unordered=[create_key(k, client_id=cid) for k in (keys or ())],
        )
        if pos is not None:
            request.pos = pos
        return await self._send_add_chunks(name, rpc, request, **kwargs)

    async def add_element_to_tail(self, key: KeyLike, hint=None, values=None, ttl=0, client_id=None, **kwargs):
        return await self._add_unordered(
            "add_element_to_tail", self.stub.addElementToTail, key, hint, values, None, ttl, client_id, **kwargs
        )

    async def add_element_to_head(self, key: KeyLike, hint=None, values=None, ttl=0, client_id=None, **kwargs):
        return await self._add_unordered(
            "add_element_to_head", self.stub.addElementToHead, key, hint, values, None, ttl, client_id, **kwargs
        )

    async def add_element(self, key: KeyLike, hint=None, values=None, keys=None, ttl=0, client_id=None, **kwargs):
        return await self._add_unordered(
            "add_element", self.stub.addElement, key, hint, values, keys, ttl, client_id, **kwargs
        )

    add_element_hash_map = add_element

    async def add_element_unordered(
        self, key: KeyLike, hint=None, values=None, keys=None, pos=0, ttl=0, client_id=None, **kwargs
    ):
        return await self._add_unordered(
            "add_element_unordered", self.stub.addElement, key, hint, values, keys, ttl, client_id, pos, **kwargs
        )

    async def add_element_ordered(
        self, key: KeyLike, hint=None, values=None, keys=None, pos=0, ttl=0, client_id=None, **kwargs
    ):
        cid = self._client_id(client_id)
        values = values or ()
        keys = keys or ()
        request = cache_pb2.AddToRequest(key=self._key(key, hint, cid), pos=pos)
        if keys:
            if len(keys) != len(values):
                raise ValueError("ordered-map keys and values must have equal lengths")
            request.key_ordered.extend(create_ordered_key(k, client_id=cid) for k in keys)
            request.value_unordered.extend(create_value(v, ttl, cid) for v in values)
        else:
            request.value_ordered.extend(create_ordered_value(v, ttl=ttl, client_id=cid) for v in values)
        return await self._send_add_chunks("add_element_ordered", self.stub.addElement, request, **kwargs)

    async def add_element_to_position_by_value(
        self, key: KeyLike, hint=None, pos=b"", is_before=True, values=None, ttl=0, client_id=None, **kwargs
    ):
        cid = self._client_id(client_id)
        request = cache_pb2.AddToValRequest(
            key=self._key(key, hint, cid),
            ttl=ttl,
            isBefore=is_before,
            pos=create_value(pos, client_id=cid),
            value=[create_value(v, client_id=cid) for v in (values or ())],
        )
        return await self._unary(
            "add_element_to_position_by_value", self.stub.addElementToPositionByValue, request, lambda r: r.value, **kwargs
        )

    async def add_element_to_position_before(self, key: KeyLike, hint=None, pivot=b"", values=None, client_id=None, **kwargs):
        return await self.add_element_to_position_by_value(
            key, hint, pivot, True, values, client_id=client_id, **kwargs
        )

    async def add_element_to_position_after(self, key: KeyLike, hint=None, pivot=b"", values=None, client_id=None, **kwargs):
        return await self.add_element_to_position_by_value(
            key, hint, pivot, False, values, client_id=client_id, **kwargs
        )

    async def _atomic(
        self, name, rpc, key, hint=None, value=0, ttl=0, client_id=None, decoder=lambda r: r.val, **kwargs
    ):
        cid = self._client_id(client_id)
        request = cache_pb2.AtomicCreate(key=self._key(key, hint, cid), val=cache_pb2.AtomicValue(val=value))
        if ttl > 0:
            request.ttl = int(time.time() * 1000) + ttl
        request.lock_info.type = cache_pb2.NO_LOCK
        request.lock_info.lockedBy = cid
        return await self._unary(name, rpc, request, decoder, **kwargs)

    async def atomic_load(self, key: KeyLike, hint=None, client_id=None, **kwargs):
        return await self._unary("atomic_load", self.stub.atomicLoad, self._get(key, hint, client_id), lambda r: r.val, **kwargs)

    async def atomic_load_and_delete(self, key: KeyLike, hint=None, client_id=None, **kwargs):
        return await self._unary(
            "atomic_load_and_delete", self.stub.atomicLoadAndDelete, self._get(key, hint, client_id), lambda r: r.val, **kwargs
        )

    async def atomic_create(self, key: KeyLike, hint=None, value=0, ttl=0, client_id=None, **kwargs):
        return await self._atomic(
            "atomic_create", self.stub.atomicCreate, key, hint, value, ttl, client_id,
            lambda r: decode_hint(r) or KeyHintData.unspecified(), **kwargs
        )

    async def atomic_store(self, key: KeyLike, hint=None, value=0, ttl=0, client_id=None, **kwargs):
        return await self._atomic(
            "atomic_store", self.stub.atomicStore, key, hint, value, ttl, client_id,
            lambda r: decode_hint(r) or KeyHintData.unspecified(), **kwargs
        )

    async def atomic_exchange(self, key: KeyLike, hint=None, value=0, ttl=0, client_id=None, **kwargs):
        return await self._atomic("atomic_exchange", self.stub.atomicExchange, key, hint, value, ttl, client_id, **kwargs)

    async def atomic_add(self, key: KeyLike, hint=None, delta=0, ttl=0, client_id=None, **kwargs):
        return await self._atomic("atomic_add", self.stub.atomicAdd, key, hint, delta, ttl, client_id, **kwargs)

    async def atomic_sub(self, key: KeyLike, hint=None, delta=0, ttl=0, client_id=None, **kwargs):
        return await self._atomic("atomic_sub", self.stub.atomicSub, key, hint, delta, ttl, client_id, **kwargs)

    async def atomic_or(self, key: KeyLike, hint=None, mask=0, ttl=0, client_id=None, **kwargs):
        return await self._atomic("atomic_or", self.stub.atomicOr, key, hint, mask, ttl, client_id, **kwargs)

    async def atomic_and(self, key: KeyLike, hint=None, mask=0, ttl=0, client_id=None, **kwargs):
        return await self._atomic("atomic_and", self.stub.atomicAnd, key, hint, mask, ttl, client_id, **kwargs)

    async def atomic_xor(self, key: KeyLike, hint=None, mask=0, ttl=0, client_id=None, **kwargs):
        return await self._atomic("atomic_xor", self.stub.atomicXor, key, hint, mask, ttl, client_id, **kwargs)

    async def atomic_compare_and_set(
        self, key: KeyLike, hint=None, expected_value=0, new_value=0, ttl=0, client_id=None, **kwargs
    ) -> CasResult:
        cid = self._client_id(client_id)
        request = cache_pb2.AtomicCas(
            key=self._key(key, hint, cid),
            expected=cache_pb2.AtomicValue(val=expected_value),
            toSet=cache_pb2.AtomicValue(val=new_value),
        )
        if ttl > 0:
            request.ttl = int(time.time() * 1000) + ttl
        request.lock_info.type = cache_pb2.NO_LOCK
        request.lock_info.lockedBy = cid
        return await self._unary(
            "atomic_compare_and_set",
            self.stub.atomicCompareAndSet,
            request,
            lambda r: CasResult(
                r.result, r.expected.val if r.HasField("expected") else None, decode_hint(r)
            ),
            **kwargs,
        )
