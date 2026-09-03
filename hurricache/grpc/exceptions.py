"""Custom exceptions for HurriCache client."""

from __future__ import annotations

import grpc


class HurriCacheError(Exception):
    """Base exception for HurriCache client errors."""


class KeyNotFoundError(HurriCacheError):
    """Raised when a key is not found or type is incorrect.

    Corresponds to gRPC status NOT_FOUND.
    """

    def __init__(self, key: bytes, message: str = "Key not found or type is not correct"):
        self.key = key
        super().__init__(message)


class PermissionDeniedError(HurriCacheError):
    """Raised when access to an object is denied.

    Corresponds to gRPC status PERMISSION_DENIED.
    """

    def __init__(self, key: bytes, message: str = "Can't access to the object"):
        self.key = key
        super().__init__(message)


class InvalidArgumentError(HurriCacheError, ValueError):
    """The server rejected a malformed or unsupported operation."""


class DeadlineExceededError(HurriCacheError, TimeoutError):
    """The RPC did not finish before its deadline."""


class UnavailableError(HurriCacheError, ConnectionError):
    """The selected cache or coordinator is unavailable."""


class FailedPreconditionError(HurriCacheError):
    """The request must be rerouted or the object is in an incompatible state."""

    def __init__(self, message: str, route: str | None = None):
        self.route = route
        super().__init__(message)


class CancelledError(HurriCacheError):
    """The RPC or stream was cancelled."""


class HurriCacheRpcError(HurriCacheError):
    """Raised for other gRPC RPC errors.

    Wraps the original grpc.RpcError with additional context.
    """

    def __init__(self, method: str, error: grpc.RpcError):
        self.method = method
        self.code = error.code() if hasattr(error, "code") and callable(error.code) else None
        self.details = error.details() if hasattr(error, "details") and callable(error.details) else str(error)
        super().__init__(f"RPC error on {method}: {self.code} - {self.details}")


def mapped_rpc_error(method: str, error: grpc.RpcError, key: bytes = b"") -> HurriCacheError:
    code = error.code() if callable(getattr(error, "code", None)) else None
    details = error.details() if callable(getattr(error, "details", None)) else str(error)
    if code == grpc.StatusCode.NOT_FOUND:
        return KeyNotFoundError(key, details)
    if code == grpc.StatusCode.PERMISSION_DENIED:
        return PermissionDeniedError(key, details)
    if code == grpc.StatusCode.INVALID_ARGUMENT:
        return InvalidArgumentError(details)
    if code == grpc.StatusCode.DEADLINE_EXCEEDED:
        return DeadlineExceededError(details)
    if code == grpc.StatusCode.UNAVAILABLE:
        return UnavailableError(details)
    if code == grpc.StatusCode.FAILED_PRECONDITION:
        route = None
        metadata_fn = getattr(error, "trailing_metadata", None)
        if callable(metadata_fn):
            metadata = metadata_fn() or ()
            route = next((value for key_name, value in metadata if key_name == "x-fastcache-route"), None)
        return FailedPreconditionError(details, route)
    if code == grpc.StatusCode.CANCELLED:
        return CancelledError(details)
    return HurriCacheRpcError(method, error)
