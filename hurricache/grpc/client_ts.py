"""Attaches the x-hc-client-ts-bin metadata to every call — client send time in
milliseconds since Unix epoch (UTC/GMT-0), uint64 little-endian.
The server uses it to stamp write versions and to honestly reject writes
delayed by the network (stale write). Matches the Java client's
ClientSendTimeInterceptor."""

from __future__ import annotations

import time

import grpc

METADATA_KEY = "x-hc-client-ts-bin"


def _client_ts_bytes() -> bytes:
    return int(time.time() * 1000).to_bytes(8, "little")


class _SyncClientCallDetails(grpc.ClientCallDetails):
    def __init__(self, details: grpc.ClientCallDetails, metadata: list):
        self.method = details.method
        self.timeout = details.timeout
        self.metadata = metadata
        self.credentials = details.credentials
        self.wait_for_ready = details.wait_for_ready
        self.compression = details.compression


def _with_client_ts(details):
    metadata = list(details.metadata or [])
    metadata.append((METADATA_KEY, _client_ts_bytes()))
    return metadata


def _sync_details(details: grpc.ClientCallDetails):
    return _SyncClientCallDetails(details, _with_client_ts(details))


def _aio_details(details: grpc.aio.ClientCallDetails):
    return grpc.aio.ClientCallDetails(
        method=details.method,
        timeout=details.timeout,
        metadata=_with_client_ts(details),
        credentials=details.credentials,
        wait_for_ready=details.wait_for_ready,
    )


class ClientSendTimeInterceptor(
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.StreamStreamClientInterceptor,
):
    def intercept_unary_unary(self, continuation, client_call_details, request):
        return continuation(_sync_details(client_call_details), request)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        return continuation(_sync_details(client_call_details), request)

    def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        return continuation(_sync_details(client_call_details), request_iterator)

    def intercept_stream_stream(self, continuation, client_call_details, request_iterator):
        return continuation(_sync_details(client_call_details), request_iterator)


class AsyncClientSendTimeInterceptor(
    grpc.aio.UnaryUnaryClientInterceptor,
    grpc.aio.UnaryStreamClientInterceptor,
    grpc.aio.StreamUnaryClientInterceptor,
    grpc.aio.StreamStreamClientInterceptor,
):
    async def intercept_unary_unary(self, continuation, client_call_details, request):
        return await continuation(_aio_details(client_call_details), request)

    async def intercept_unary_stream(self, continuation, client_call_details, request):
        return await continuation(_aio_details(client_call_details), request)

    async def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        return await continuation(_aio_details(client_call_details), request_iterator)

    async def intercept_stream_stream(self, continuation, client_call_details, request_iterator):
        return await continuation(_aio_details(client_call_details), request_iterator)
