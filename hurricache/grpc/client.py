from __future__ import annotations

from typing import Any

import grpc

from hurricache.grpc import cache_pb2, cache_pb2_grpc
from hurricache.grpc.api import install
from hurricache.grpc.batching import check
from hurricache.grpc.exceptions import PartialOperationError, mapped_rpc_error
from hurricache.grpc.models import KeyHintData
from hurricache.grpc.operation import Defaults, configure, remaining
from hurricache.grpc.utils import create_key, decode_batch, decode_key, grpc_target


def _handle_rpc_error(method, error, key=b""):
    raise mapped_rpc_error(method, error, key)


class HurriCacheClient(Defaults):
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
    ):
        configure(self, default_ttl, default_compression_threshold, payload_compression, zero_client_id_uses_default)
        self._host = host
        self._port = port
        self._default_client_id = default_client_id
        self._default_timeout = default_timeout
        self._credentials = credentials
        self._compression = compression
        self._channel: grpc.Channel | None = None
        self._stub: cache_pb2_grpc.HurriCacheGrpcServiceStub | None = None

    @property
    def channel(self) -> grpc.Channel:
        if self._channel is None:
            options: list[tuple[str, object]] = [("grpc.max_receive_message_length", 64 * 1024 * 1024)]
            if self._credentials is not None:
                self._channel = grpc.secure_channel(
                    grpc_target(self._host, self._port),
                    self._credentials,
                    options=options,
                    compression=self._compression,
                )
            else:
                self._channel = grpc.insecure_channel(
                    grpc_target(self._host, self._port),
                    options=options,
                    compression=self._compression,
                )
            self._stub = cache_pb2_grpc.HurriCacheGrpcServiceStub(self._channel)
        return self._channel

    @property
    def stub(self) -> cache_pb2_grpc.HurriCacheGrpcServiceStub:
        if self._stub is None:
            _ = self.channel
        assert self._stub is not None
        return self._stub

    def close(self) -> None:
        self._closed = True
        if self._channel is not None:
            self._channel.close()
            self._channel = None
            self._stub = None

    def __enter__(self) -> "HurriCacheClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def _client_id(self, client_id: int | None) -> int:
        if client_id is None or (client_id == 0 and self._zero_client_id_uses_default):
            return self._default_client_id
        return client_id

    def _key(
        self,
        key: bytes,
        hint: KeyHintData | None = None,
        client_id: int | None = None,
    ) -> cache_pb2.Key:
        cid = self._client_id(client_id)
        return create_key(key, hint, cid)

    def _call(self, method_name: str, rpc_callable, *args, **kwargs) -> grpc.Call:
        """Execute an RPC call and handle errors."""
        kwargs["timeout"] = remaining(kwargs.get("timeout", self._default_timeout))
        try:
            if args:
                check(args[0])
            return rpc_callable(*args, **kwargs)
        except grpc.RpcError as e:
            request = args[0] if args else None
            key = decode_key(request.key) if request is not None and hasattr(request, "key") else b""
            _handle_rpc_error(method_name, e, key)
            raise  # pragma: no cover

    def _collect_stream(self, method_name: str, stream, key: bytes | str):
        result: Any = None
        completed_chunks = 0
        completed_items = 0
        try:
            for batch in stream:
                remaining()
                decoded = decode_batch(batch)
                completed_chunks += 1
                completed_items += len(decoded)
                if result is None:
                    result = {} if isinstance(decoded, dict) else []
                elif isinstance(result, dict) != isinstance(decoded, dict):
                    raise ValueError("stream changes container representation")
                if isinstance(result, dict):
                    result.update(decoded)
                else:
                    result.extend(decoded)
        except grpc.RpcError as error:
            mapped = mapped_rpc_error(method_name, error, key.encode() if isinstance(key, str) else key)
            if completed_chunks:
                raise PartialOperationError(completed_chunks, completed_items, result) from mapped
            raise mapped from error
        except Exception as error:
            if completed_chunks:
                raise PartialOperationError(completed_chunks, completed_items, result) from error
            raise
        return [] if result is None else result

install(HurriCacheClient, asynchronous=False)
