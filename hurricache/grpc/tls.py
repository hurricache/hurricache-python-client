"""TLS configuration for HurriCache clients.

Mirrors the server-side ``tls`` section semantics:

- no ``TlsConfig`` (or ``tls=None``) -> plaintext channel, as before;
- ``ca_file`` -> the server chain is verified against this CA. The hostname
  (CN/SAN) is not verified unless ``server_name`` is set;
- ``cert_file`` + ``key_file`` -> client certificate for mTLS (mandatory when
  the server has ``ca`` configured in its tls section);
- ``server_name`` -> expected server certificate name: overrides the target
  name used for the hostname check and SNI.

Note: grpc-python cannot do "encryption without verification", so ``ca_file``
is mandatory for TLS. For a self-signed server certificate pass the server
certificate itself as ``ca_file``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import grpc


@dataclass(frozen=True)
class TlsConfig:
    ca_file: str
    cert_file: str | None = None
    key_file: str | None = None
    server_name: str | None = None

    def build(self) -> tuple[grpc.ChannelCredentials, list[tuple[str, object]]]:
        """Builds channel credentials and extra channel options.

        Raises ValueError on inconsistent configuration and OSError on
        unreadable/empty files — misconfiguration surfaces at client
        construction time, not on the first RPC.
        """
        if not self.ca_file:
            raise ValueError("TlsConfig: ca_file is required (grpc-python cannot skip server verification)")
        if bool(self.cert_file) != bool(self.key_file):
            raise ValueError("TlsConfig: cert_file and key_file must be set together")

        root = _read_pem(self.ca_file, "ca_file")
        private_key = _read_pem(self.key_file, "key_file") if self.key_file else None
        certificate_chain = _read_pem(self.cert_file, "cert_file") if self.cert_file else None

        credentials = grpc.ssl_channel_credentials(
            root_certificates=root,
            private_key=private_key,
            certificate_chain=certificate_chain,
        )
        options: list[tuple[str, object]] = []
        if self.server_name:
            options = [
                ("grpc.ssl_target_name_override", self.server_name),
                ("grpc.default_authority", self.server_name),
            ]
        return credentials, options


def _read_pem(path: str | None, field: str) -> bytes:
    assert path is not None
    data = Path(path).read_bytes()
    if not data:
        raise ValueError(f"TlsConfig: {field} '{path}' is empty")
    return data


def resolve_channel_security(
    credentials: grpc.ChannelCredentials | None,
    tls: TlsConfig | None,
) -> tuple[grpc.ChannelCredentials | None, list[tuple[str, object]]]:
    """Merges the legacy raw-credentials argument with TlsConfig.

    Returns (credentials_or_none, extra_channel_options). ``credentials`` and
    ``tls`` are mutually exclusive.
    """
    if tls is not None and credentials is not None:
        raise ValueError("Pass either 'credentials' or 'tls', not both")
    if tls is None:
        return credentials, []
    creds, options = tls.build()
    return creds, options
