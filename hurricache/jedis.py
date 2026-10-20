"""The active Java Jedis adapter API, backed by a shared synchronous client.

Redis commands commented out in Java are intentionally outside this adapter.
Closing a resource or its pool closes the shared client.
"""

from __future__ import annotations

from hurricache.grpc.client import HurriCacheClient
from hurricache.grpc.smart_client import HurriCacheSmartClient

SyncClient = HurriCacheClient | HurriCacheSmartClient


class Jedis:
    def __init__(self, client: SyncClient):
        if not isinstance(client, (HurriCacheClient, HurriCacheSmartClient)):
            raise TypeError("client must be a synchronous HurriCache client")
        self._client = client

    def ping(self) -> str:
        return "PONG"

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> Jedis:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()


class JedisPool:
    def __init__(self, client: SyncClient):
        self._client = Jedis(client)._client

    def get_resource(self) -> Jedis:
        return Jedis(self._client)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> JedisPool:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()
