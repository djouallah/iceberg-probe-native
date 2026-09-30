"""Run configuration, assembled from the environment: the OneLake endpoints, the catalog-cache
lifetime each engine is given, the DuckDB transport rule, and `Config` (workspace, lakehouse).
"""

from __future__ import annotations

import os
import platform
from dataclasses import dataclass

# The OneLake host prefix: `onelake` is the public endpoint. ONELAKE_HOST points every probe at
# another channel -- that channel's host name is set in the shell and never written here.
_HOST = os.environ.get("ONELAKE_HOST", "onelake")
ICEBERG_ENDPOINT = f"https://{_HOST}.table.fabric.microsoft.com/iceberg"
ONELAKE_DFS = f"{_HOST}.dfs.fabric.microsoft.com"
ONELAKE_BLOB = f"{_HOST}.blob.fabric.microsoft.com"
STORAGE_SCOPE = "https://storage.azure.com/.default"

# How long an engine may cache catalog metadata. One number, each engine derives its own spelling.
CATALOG_CACHE_SECONDS = 900  # 15 minutes


def azure_transport() -> str | None:
    """Which HTTP transport DuckDB's azure extension should use, or None to leave its default.

    THE SINGLE MOST EXPENSIVE THING TO GET WRONG HERE, because getting it wrong does not look
    like a transport problem. DuckDB's azure extension has its own HTTP stack, separate from the
    iceberg extension's. On a Linux runner its `default` transport fails the OneLake TLS
    handshake ("Problem with the SSL CA cert (path? access rights?)"), while `curl` respects the
    system CA bundle and works.

    So the Iceberg ATTACH SUCCEEDS -- that is a plain HTTPS REST call made by the iceberg
    extension -- and then every single data-file read fails with

        IOException: AzureStorageFileSystem could not open file: 'abfss://...'

    which reads exactly like a missing storage credential and is not one. A genuinely bad
    credential says `Unauthorized`.

    On Windows DuckDB's bundled libcurl has no CA bundle, so there the default (WinHTTP), which
    trusts the system cert store, is the one that works.

    An explicit AZURE_TRANSPORT_OPTION_TYPE always wins.
    """
    override = os.environ.get("AZURE_TRANSPORT_OPTION_TYPE")
    if override:
        return override
    return None if platform.system() == "Windows" else "curl"


@dataclass(frozen=True)
class Config:
    workspace_id: str
    lakehouse_id: str

    @property
    def warehouse(self) -> str:
        """What the Iceberg REST catalog calls `warehouse`: GUID/GUID. A name does not resolve."""
        return f"{self.workspace_id}/{self.lakehouse_id}"

    @property
    def base_path(self) -> str:
        return f"abfss://{self.workspace_id}@{ONELAKE_DFS}/{self.lakehouse_id}"

    @classmethod
    def from_env(cls) -> Config:
        return cls(
            workspace_id=os.environ["FABRIC_WORKSPACE_ID"],
            lakehouse_id=os.environ["FABRIC_LAKEHOUSE_ID"],
        )
