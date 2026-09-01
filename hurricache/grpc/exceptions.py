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


class HurriCacheRpcError(HurriCacheError):
    """Raised for other gRPC RPC errors.

    Wraps the original grpc.RpcError with additional context.
    """

    def __init__(self, method: str, error: grpc.RpcError):
        self.method = method
        self.code = error.code() if hasattr(error, "code") and callable(error.code) else None
        self.details = error.details() if hasattr(error, "details") and callable(error.details) else str(error)
        super().__init__(f"RPC error on {method}: {self.code} - {self.details}")
