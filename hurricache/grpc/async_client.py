from __future__ import annotations

from collections.abc import Callable
from typing import Any

import grpc

from hurricache.grpc import cache_pb2, cache_pb2_grpc
from hurricache.grpc.api import install
from hurricache.grpc.batching import check
from hurricache.grpc.exceptions import PartialOperationError, mapped_rpc_error
from hurricache.grpc.models import KeyHintData
from hurricache.grpc.operation import Defaults, configure, remaining
from hurricache.grpc.utils import KeyLike, create_key, decode_batch, decode_key, grpc_target


def _identity(value):
    return value


class AsyncHurriCacheClient(Defaults):
    def __init__(
        self,
        host: str = "localhost",
        port: int = 50000,
        default_client_id: int = 0,
        default_timeout: float = 1.0,
        credentials: grpc.ChannelCredentials | None = None,
        compression: grpc.Compression | None = None,
        *,
        default_ttl: int = 0,
        default_compression_threshold: int = 1024,
        payload_compression: str = "gzip",
        zero_client_id_uses_default: bool = True,
    ) -> None:
        configure(self, default_ttl, default_compression_threshold, payload_compression, zero_client_id_uses_default)
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
            target = grpc_target(self._host, self._port)
            options = [("grpc.max_receive_message_length", 64 * 1024 * 1024)]
            if self._credentials is None:
                self._channel = grpc.aio.insecure_channel(target, options=options, compression=self._compression)
            else:
                self._channel = grpc.aio.secure_channel(
                    target, self._credentials, options=options, compression=self._compression
                )
            self._stub = cache_pb2_grpc.HurriCacheGrpcServiceStub(self._channel)
        return self._channel

    @property
    def stub(self) -> cache_pb2_grpc.HurriCacheGrpcServiceStub:
        if self._stub is None:
            _ = self.channel
        assert self._stub is not None
        return self._stub

    def _client_id(self, client_id: int | None) -> int:
        if client_id is None or (client_id == 0 and self._zero_client_id_uses_default):
            return self._default_client_id
        return client_id

    def _key(self, key: KeyLike, hint: KeyHintData | None, client_id: int | None) -> cache_pb2.Key:
        return create_key(key, hint, self._client_id(client_id))

    async def _unary(
        self,
        name: str,
        rpc: Callable[..., Any],
        request: Any,
        decoder: Callable[[Any], Any] = _identity,
        **kwargs: Any,
    ) -> Any:
        kwargs["timeout"] = remaining(kwargs.get("timeout", self._default_timeout))
        try:
            check(request)
            return decoder(await rpc(request, **kwargs))
        except grpc.aio.AioRpcError as error:
            key = decode_key(request.key) if hasattr(request, "key") else b""
            raise mapped_rpc_error(name, error, key) from error

    async def _stream(self, name: str, rpc: Callable[..., Any], request: Any, **kwargs: Any) -> Any:
        kwargs["timeout"] = remaining(kwargs.get("timeout", self._default_timeout))
        result: Any = None
        completed_chunks = 0
        completed_items = 0
        try:
            check(request)
            call = rpc(request, **kwargs)
            async for batch in call:
                remaining()
                decoded = decode_batch(batch)
                completed_chunks += 1
                completed_items += len(decoded)
                if result is None:
                    result = {} if isinstance(decoded, dict) else []
                elif isinstance(result, dict) != isinstance(decoded, dict):
                    raise ValueError("stream changes container representation")
                result.update(decoded) if isinstance(result, dict) else result.extend(decoded)
        except grpc.aio.AioRpcError as error:
            key = decode_key(request.key) if hasattr(request, "key") else b""
            mapped = mapped_rpc_error(name, error, key)
            if completed_chunks:
                raise PartialOperationError(completed_chunks, completed_items, result) from mapped
            raise mapped from error
        except Exception as error:
            if completed_chunks:
                raise PartialOperationError(completed_chunks, completed_items, result) from error
            raise
        return [] if result is None else result

    async def close(self, grace: float | None = None) -> None:
        self._closed = True
        if self._channel is not None:
            await self._channel.close(grace)
            self._channel = None
            self._stub = None

    async def __aenter__(self) -> "AsyncHurriCacheClient":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()

install(AsyncHurriCacheClient, asynchronous=True)
