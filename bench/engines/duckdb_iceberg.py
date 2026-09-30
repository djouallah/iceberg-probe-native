"""DuckDB's write-capable ATTACH of the OneLake Iceberg REST catalog.

TWO ATTACH FLAGS, both about writing:

* `STAGE_CREATE_TABLES false` -- OneLake refuses the commit that finishes a staged create, so the
  table is created in one request instead of created-then-committed.
* `SKIP_CREATE_TABLE_METADATA_UPDATES true` -- DuckDB follows a CREATE TABLE with a second commit
  that updates the fresh table's metadata. This flag fully initialises the metadata in the create
  itself.

Storage goes through the azure extension under an `access_token` secret, with
`ACCESS_DELEGATION_MODE 'none'`. See config.azure_transport for the transport rule.
"""

from __future__ import annotations

from bench.config import ICEBERG_ENDPOINT, Config, azure_transport

CATALOG = "onelake"


def attach(conn, cfg: Config, token: str) -> None:
    """The write-capable ATTACH on one connection: transport, storage secret, the two flags."""
    conn.sql(f"""
        SET GLOBAL azure_transport_option_type = '{azure_transport() or "default"}';
        SET preserve_insertion_order = false;

        CREATE OR REPLACE SECRET onelake_storage (
            TYPE azure, PROVIDER access_token, ACCESS_TOKEN '{token}');

        ATTACH OR REPLACE '{cfg.warehouse}' AS {CATALOG} (
            TYPE ICEBERG,
            ENDPOINT '{ICEBERG_ENDPOINT}',
            TOKEN '{token}',
            ACCESS_DELEGATION_MODE 'none',
            STAGE_CREATE_TABLES false,
            SKIP_CREATE_TABLE_METADATA_UPDATES true);
    """)
