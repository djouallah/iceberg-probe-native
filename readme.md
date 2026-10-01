# OneLake Iceberg REST catalog: what the native engines can do

pyiceberg, DuckDB, Sail and chDB: engines with their own Iceberg implementation, no JVM.

`yes` works · `no` refused · `no-op` accepted but not applied · `na` the engine has no such
operation · `—` not probed

| Operation | pyiceberg | DuckDB | Sail | chDB |
|---|---|---|---|---|
| Version | 0.12.0 | 2.0.0 alpha (dev2609250715) | 0.7.2 | 4.4.0 (ClickHouse 26.9.2.1) |
| Language | Python | C++ | Rust | C++ |
| Iceberg implementation | own ¹³ | own | own, on DataFusion | ClickHouse's own |
| SQL | na | yes | yes | yes |
| CREATE TABLE | yes | yes | yes | no ⁸ |
| INSERT / append | yes | yes | yes | yes ⁸ |
| DELETE | yes ⁷ | yes | no ¹ | yes |
| Read and write storage with the engine's own token | yes ² | yes ² | yes ² | yes ² |
| Credential vending | no ³ | yes ³ | na | na |
| Drop table with purge | yes | yes | yes | no ⁹ |
| Create / drop namespace | yes | yes | yes | na |
| CREATE TABLE AS SELECT | na | yes | no ⁴ | no ⁸ |
| CREATE OR REPLACE TABLE | na | na | na | no ⁸ |
| UPDATE | na | yes | yes | yes ¹⁰ |
| MERGE INTO / upsert | yes | yes | yes | na |
| MERGE ... WHEN NOT MATCHED BY SOURCE | na | yes ¹⁶ | yes | na |
| INSERT OVERWRITE, whole table | yes | yes ⁵ | yes | na |
| INSERT OVERWRITE, one partition / by filter | yes | yes ⁵ | na | na |
| TRUNCATE | na | yes ⁶ | na | na |
| Partitioned table | yes | yes | yes | yes |
| Partition transform: bucket | yes | yes | yes | yes |
| Partition transform: truncate | yes | yes | yes | yes |
| Partition transforms: year / month / day / hour | yes | yes | yes | yes |
| Types: decimal, date, timestamp, timestamptz, uuid, binary | yes | yes | yes ¹⁴ | yes ¹⁵ |
| Nested types: struct, list, map | yes | yes | yes | yes |
| Add column | yes | yes | na | yes |
| Drop column | yes | yes | na | yes |
| Rename column | yes | yes | na | yes |
| Type promotion (int → long) | yes | yes | na | yes |
| Partition evolution | yes | yes | na | na |
| Write after partition evolution | yes | yes | na | na |
| Set table property | yes | yes | na | na |
| Sort order at create | yes | yes | na | no ⁸ |
| Sort order evolution | yes | yes | na | na |
| Time travel | yes | yes | yes | yes |
| Metadata tables | yes | yes | na | yes |
| Compaction | na | yes | na | na ¹¹ |
| Expire snapshots | yes | na | na | no ¹² |
| Create branch | yes | na | na | na |
| Create tag | yes | na | na | na |
| Write to a branch | yes | na | na | na |
| Roll back to a snapshot | yes | yes | na | na |

## Blocked by the OneLake catalog

The catalog itself refuses or ignores these, so no engine can do them.

| Operation | Catalog |
|---|---|
| Staged create (`stage-create: true`) | no: `400 Malformed request` |
| Rename table | no: `406` |
| Drop table without purge (`purgeRequested=false`) | no: `405` |
| registerTable | no: not declared in `/v1/config` |
| Multi-table transactions | no: `405` |
| Update namespace properties | no: `405` |
| Server-side scan planning | no: not declared in `/v1/config` |
| Views | no |

## Notes

1. Sail writes equality deletes
   ([lakehq/sail#2698](https://github.com/lakehq/sail/issues/2698)).
2. No vending: the engine signs storage itself with an Entra token for OneLake. pyiceberg
   `adls.token`; Sail `AZURE_STORAGE_TOKEN`; DuckDB `CREATE SECRET (TYPE azure, PROVIDER
   access_token)`; chDB `onelake_bearer_token` on its `DataLakeCatalog` database, which signs the
   catalog calls too.
3. The catalog hands out a SAS token scoped to the table's folder (`GET .../credentials`, and in
   `loadTable` when asked with `X-Iceberg-Access-Delegation`), only as `storage-credentials`, with
   the table `config` empty. DuckDB with `ACCESS_DELEGATION_MODE 'vended_credentials'` and no
   storage secret reads a table and inserts into it. pyiceberg finds no credential: the
   credential's prefix is `https://<host>/<ws>/...`, the table's location is
   `abfss://<ws>@<host>/...`, and pyiceberg matches by prefix.
4. Sail through its generic Iceberg REST catalog: `Iceberg table location must be an absolute
   path or URL`. Plain `CREATE TABLE` works.
5. DuckDB has no `INSERT OVERWRITE`; its overwrite is `DELETE` + `INSERT` in one transaction.
6. Not a metadata-only operation in DuckDB.
7. Copy-on-write only: on a merge-on-read table pyiceberg warns and rewrites the data files
   instead of writing position deletes.
8. chDB does not create tables in the catalog. With no engine, a `CREATE TABLE` in its
   `DataLakeCatalog` database becomes a local MergeTree (`MergeTree storages require data path`).
   ClickHouse's own tests create a catalog table with an explicit engine and location (`ENGINE =
   IcebergAzure(...)`, `write_full_path_in_iceberg_metadata = 1`), but that engine does not carry
   the database's `onelake_bearer_token`: `401 ... Bearer token is not present in the request`.
   chDB writes only to tables something else created; every chDB `yes` was read on a table
   pyiceberg created.
9. ClickHouse's REST catalog drops with `purgeRequested=False` hard-coded, which the catalog
   refuses (`405`).
10. `ALTER TABLE ... UPDATE`. `UPDATE ... SET` is not implemented for Iceberg tables.
11. ClickHouse compacts manifests only (`OPTIMIZE TABLE ... MANIFEST`, from 100 manifests up); it
    never rewrites data files.
12. chDB: `expire_snapshots is not supported for Iceberg tables backed by a transactional
    catalog`.
13. Python, with pyarrow writing the parquet files. iceberg-rust comes in only through the optional
    `pyiceberg-core` extra, which computes the bucket and year / month / day / hour partition
    values; the partition-transform rows were read with it installed.
14. All but `uuid`: Spark SQL has no UUID type.
15. A `timestamp` (no zone) is written shifted by chDB's session time zone: `03:04` written on a
    UTC+10 machine reads back as `17:04` the day before. With `SET session_timezone = 'UTC'` it is
    written as given.
16. One UPDATE or DELETE action per `MERGE`: `WHEN MATCHED THEN UPDATE` together with `WHEN NOT
    MATCHED BY SOURCE THEN DELETE` is refused (`MERGE INTO with Iceberg only supports a single
    UPDATE/DELETE action currently`).
## Other readings

- Optimistic concurrency holds: a commit against a stale `assert-ref-snapshot-id` is refused
  with `CommitFailedException`.
- A create with no `location` is accepted; the catalog assigns one.
- `GET /v1/config` declares endpoints for namespaces, tables, rename and per-table
  `credentials`.

## Where these readings come from

The OneLake Iceberg REST catalog, a private preview with no published documentation. Every cell
is a reading taken by sending the request, not a property of the product, and the surface moves:
re-run rather than trust this page. Actual deployment time in production may differ.

Read on a laptop with `az login` on 2026-09-30: pyiceberg 0.12.0, DuckDB 2.0.0.dev2609250715,
Sail 0.7.2, and chDB 4.4.0 under WSL, as chDB has no Windows build. chDB 4.4.0 embeds ClickHouse
26.9.2.1, the current stable line; ClickHouse master has the same Iceberg `EXECUTE` commands
(`expire_snapshots`, `remove_orphan_files`) and the same drop.

The partition-transform, type, type-promotion, write-after-evolution and `NOT MATCHED BY SOURCE`
rows were read on 2026-10-01: pyiceberg (with
`pyiceberg-core` 0.10.1), Sail and chDB under WSL, DuckDB with the CLI v2.1.0-alpha43762.

```bash
ONELAKE_HOST=<host> FABRIC_WORKSPACE_ID=... FABRIC_LAKEHOUSE_ID=... PYTHONPATH=. \
  python .github/scripts/catalog_capability.py      # or _duckdb.py, _sail.py, _chdb.py
```
