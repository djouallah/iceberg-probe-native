# OneLake Iceberg REST catalog: what the native engines can do

pyiceberg, DuckDB and Sail: engines with their own Iceberg implementation, no JVM.

`yes` works · `no` refused · `no-op` accepted but not applied · `na` the engine has no such operation · `—` not probed · `?` the probe could not ask

| Operation | pyiceberg | DuckDB | Sail |
|---|---|---|---|
| Version | 0.12.0 | v2.0.0-alpha46057 | 0.7.2 |
| Language | Python | C++ | Rust |
| Iceberg implementation | own, pyarrow for the files | own | own, on DataFusion |
| CREATE TABLE | yes | yes | yes |
| INSERT / append | yes | yes | yes |
| INSERT ... SELECT | na | yes | na |
| DELETE | yes | yes | yes |
| UPDATE | na | yes | yes |
| MERGE INTO / upsert | no ¹ | no ² | yes |
| MERGE with one action | na | yes | na |
| MERGE ... WHEN NOT MATCHED BY SOURCE | na | no ³ | yes |
| INSERT OVERWRITE, whole table | no ⁴ | no ⁵ | yes |
| INSERT OVERWRITE, one partition / by filter | no ⁶ | no ⁷ | no ⁸ |
| Several writes in one transaction | na | no ⁹ | na |
| TRUNCATE | na | yes | no ¹⁰ |
| CREATE TABLE AS SELECT | na | yes | yes |
| CREATE OR REPLACE TABLE | na | no ¹¹ | no ¹² |
| Partitioned table | yes | yes | yes |
| Partition transform: bucket | yes | yes | yes |
| Partition transform: truncate | yes | yes | yes |
| Partition transforms: year / month / day / hour | yes | yes | yes |
| Types: decimal, date, timestamp, timestamptz, uuid, binary | yes | yes | no ¹³ |
| Nested types: struct, list, map | yes | yes | yes |
| format-version 3 | no-op ¹⁴ | no-op ¹⁵ | no-op ¹⁶ |
| Add column | yes | yes | no ¹⁷ |
| Drop column | yes | yes | no ¹⁸ |
| Rename column | yes | yes | no ¹⁹ |
| Type promotion (int → long) | yes | yes | no ²⁰ |
| Partition evolution | yes | yes | no ²¹ |
| Write after partition evolution | yes | yes | na |
| Set table property | yes | yes | no ²² |
| Sort order at create | yes | yes | na |
| Sort order evolution | yes | yes | no ²³ |
| Time travel | yes | yes | yes |
| Metadata tables | yes | yes | no ²⁴ |
| Compaction | na | yes | no ²⁵ |
| Expire snapshots | yes | no ²⁶ | no ²⁷ |
| Create branch | yes | no ²⁸ | no ²⁹ |
| Create tag | yes | na | no ³⁰ |
| Drop table with purge | yes | yes | yes |
| Create / drop namespace | yes | yes | yes |
| Credential vending | no ³¹ | yes | na |
| A commit against a stale snapshot is refused | yes | na | na |

<!-- blocked:start -->
## Blocked by the OneLake catalog

The catalog itself refuses or ignores these, so no engine can do them.

| Operation | Catalog |
|---|---|
| format-version 3 | no-op: the table is created at v2 |
| Staged create (`stage-create: true`) | no: `400 Malformed request` |
| More than one snapshot in one commit (MERGE with two actions, overwrite, a transaction with more than one write) | no: `400 Only one instance of each update type is allowed per request. Duplicate types: add-snapshot` |
| Roll back to a snapshot | no-op: `refs.main` moves, `current-snapshot-id` does not |
| Write to a branch | no-op: readers of main see the write; `current-snapshot-id` moves to the branch's snapshot, `refs.main` does not |
| Rename table | no: `501 The Iceberg rename table operation is not supported.` |
| Drop table without purge (`purgeRequested=false`) | no: `405` |
| registerTable | no: not declared in `/v1/config` |
| Multi-table transactions | no: `405` |
| Update namespace properties | no: `405` |
| Server-side scan planning | no: not declared in `/v1/config` |
| Views | no |
<!-- blocked:end -->

## Notes

1. pyiceberg, MERGE INTO / upsert: upsert: `400 Only one instance of each update type is allowed per request. Duplicate types: add-snapshot, set-snapshot-ref`
2. DuckDB, MERGE INTO / upsert: MERGE INTO: `Only one instance of each update type is allowed per request. Duplicate types: add-snapshot`
3. DuckDB, MERGE ... WHEN NOT MATCHED BY SOURCE: MERGE ... WHEN NOT MATCHED BY SOURCE THEN DELETE: `Only one instance of each update type is allowed per request. Duplicate types: add-snapshot`
4. pyiceberg, INSERT OVERWRITE, whole table: overwrite the whole table: `400 Only one instance of each update type is allowed per request. Duplicate types: add-snapshot, set-snapshot-ref`
5. DuckDB, INSERT OVERWRITE, whole table: overwrite: DELETE + INSERT in one transaction: `Only one instance of each update type is allowed per request. Duplicate types: add-snapshot`
6. pyiceberg, INSERT OVERWRITE, one partition / by filter: overwrite by filter: `400 Only one instance of each update type is allowed per request. Duplicate types: add-snapshot, set-snapshot-ref`
7. DuckDB, INSERT OVERWRITE, one partition / by filter: one-partition overwrite: DELETE WHERE p + INSERT in one transaction: `Only one instance of each update type is allowed per request. Duplicate types: add-snapshot`
8. Sail, INSERT OVERWRITE, one partition / by filter: INSERT OVERWRITE ... PARTITION (p = 'a'): `UnsupportedOperationException: PARTITION for write`
9. DuckDB, Several writes in one transaction: transaction: INSERT + INSERT: `Only one instance of each update type is allowed per request. Duplicate types: add-snapshot`; transaction: UPDATE + INSERT: `Only one instance of each update type is allowed per request. Duplicate types: add-snapshot`; transaction: UPDATE + UPDATE: `Only one instance of each update type is allowed per request. Duplicate types: add-snapshot`
10. Sail, TRUNCATE: TRUNCATE TABLE: `IllegalArgumentException: invalid argument: found TRUNCATE at 0:8 expected something else, ';', statement, or end of input`
11. DuckDB, CREATE OR REPLACE TABLE: CREATE OR REPLACE TABLE: `Not implemented Error: CREATE OR REPLACE not supported in DuckDB-Iceberg. Please use separate Drop and Create Statements`; CREATE OR REPLACE TABLE ... AS SELECT: `Not implemented Error: CREATE OR REPLACE not supported in DuckDB-Iceberg. Please use separate Drop and Create Statements`
12. Sail, CREATE OR REPLACE TABLE: CREATE OR REPLACE TABLE ... AS SELECT, existing table: `AnalysisException: not supported: Replace table is not supported yet`
13. Sail, Types: decimal, date, timestamp, timestamptz, uuid, binary: types: decimal, date, timestamp, timestamptz, uuid, binary: `works: ['decimal', 'date', 'timestamp', 'timestamptz', 'binary']; refused: uuid: IllegalArgumentException: invalid argument: found UUID at 84:88 expected data type`
14. pyiceberg, format-version 3: createTable at format-version 3: `asked for format-version 3, the table came back at 2`
15. DuckDB, format-version 3: format-version 3: `asked for format-version 3, the table came back at 2`
16. Sail, format-version 3: format-version 3: `asked for format-version 3, the table came back at 2`
17. Sail, Add column: ALTER TABLE ADD COLUMN: `UnsupportedOperationException: unsupported ALTER TABLE operation`
18. Sail, Drop column: ALTER TABLE DROP COLUMN: `UnsupportedOperationException: unsupported ALTER TABLE operation`
19. Sail, Rename column: ALTER TABLE RENAME COLUMN: `UnsupportedOperationException: unsupported ALTER TABLE operation`
20. Sail, Type promotion (int → long): ALTER COLUMN c TYPE BIGINT (int -> long): `AnalysisException: external error: This feature is not implemented: ALTER TABLE is not yet supported for catalog-managed Iceberg tables: onelake._bench_capability.sl_37930015241_1_promote`
21. Sail, Partition evolution: ALTER TABLE ADD PARTITION FIELD (partition evolution): `IllegalArgumentException: invalid argument: found FIELD at 81:86 expected '('`
22. Sail, Set table property: ALTER TABLE SET TBLPROPERTIES: `AnalysisException: external error: This feature is not implemented: ALTER TABLE is not yet supported for catalog-managed Iceberg tables: onelake._bench_capability.sl_37930015241_1_props`
23. Sail, Sort order evolution: ALTER TABLE ... WRITE ORDERED BY (sort order): `IllegalArgumentException: invalid argument: found WRITE at 66:71 expected '.', 'RENAME', 'PARTITION', 'ADD', 'DROP', 'ALTER', 'CHANGE', 'REPLACE', 'SET', 'UNSET', or 'RECOVER'`
24. Sail, Metadata tables: metadata tables (t.snapshots): `IllegalArgumentException: invalid argument: table reference: [Identifier("onelake"), Identifier("_bench_capability"), Identifier("sl_37930015241_1_inspect"), Identifier("snapshots")]`
25. Sail, Compaction: CALL system.rewrite_data_files (compaction): `IllegalArgumentException: invalid argument: found CALL at 0:4 expected something else, ';', statement, or end of input`
26. DuckDB, Expire snapshots: iceberg_expire_snapshots: `Catalog Error: Table Function with name iceberg_expire_snapshots does not exist! Did you mean "iceberg_snapshots"?`
27. Sail, Expire snapshots: CALL system.expire_snapshots: `IllegalArgumentException: invalid argument: found CALL at 0:4 expected something else, ';', statement, or end of input`
28. DuckDB, Create branch: ALTER TABLE ... CREATE BRANCH: `Parser Error: syntax error at or near "CREATE" LINE 1: ... TABLE onelake."_bench_capability"."dk_37930015241_1_branch" CREATE BRANCH probe_branch ^^^^^^`
29. Sail, Create branch: ALTER TABLE ... CREATE BRANCH: `IllegalArgumentException: invalid argument: found CREATE at 66:72 expected '.', 'RENAME', 'PARTITION', 'ADD', 'DROP', 'ALTER', 'CHANGE', 'REPLACE', 'SET', 'UNSET', or 'RECOVER'`
30. Sail, Create tag: ALTER TABLE ... CREATE TAG: `IllegalArgumentException: invalid argument: found CREATE at 63:69 expected '.', 'RENAME', 'PARTITION', 'ADD', 'DROP', 'ALTER', 'CHANGE', 'REPLACE', 'SET', 'UNSET', or 'RECOVER'`
31. pyiceberg, Credential vending: read and append on vended credentials alone: `pyiceberg resolved no vended credential (no adls.sas-token key in the table's FileIO); the table's metadata location is 'abfss://1c52481c-0523-4a5a-bbde-fdc932bd77c2@onelake.dfs.fabric.microsoft.com/ac303243-4441-4885-9e7d-f4f5e7af194c/Tables/_bench_capability/cap_37930015241_1_vended/metadata/0000…`

## DuckDB: isolation levels and transactions

DuckDB is the only one with transactions. Writer B (pyiceberg) commits between DuckDB's
read and DuckDB's commit; the race is injected at the REST commit through a local proxy
(`bench/race.py`), so it is deterministic. Every DuckDB connection runs
`SET iceberg_use_metadata_log = false`
([duckdb-iceberg#1475](https://github.com/duckdb/duckdb-iceberg/issues/1475)).

`refused` DuckDB's commit fails and B's change stands · `retried` both changes kept ·
`skew` a row computed from a stale read is committed beside B's ·
`lost` B's change is gone · `broken` the race could not be run

| DuckDB writes, B commits in between | serializable | snapshot | no retries |
|---|---|---|---|
| INSERT, B appends | retried | retried | refused |
| `INSERT INTO t SELECT max(id) + 1, sum(v) FROM t`, B appends | skew | skew | refused |
| DELETE a row, B appends | refused | retried | refused |
| UPDATE another row, B appends | refused | refused | refused |
| MERGE on another row, B appends | refused | refused | refused |
| DELETE a row, B deletes another row | refused | refused | refused |
| UPDATE the row B updated | broken | broken | broken |
| MERGE on the row B updated | broken | broken | broken |
| Overwrite (DELETE + INSERT in one transaction), B appends | refused | refused | refused |

Columns are table properties: `serializable` nothing set; `snapshot` `write.delete.isolation-level = snapshot`, `write.update.isolation-level = snapshot`, `write.merge.isolation-level = snapshot`; `no retries` `commit.retry.num-retries = 0`.

| Transaction (`BEGIN ... COMMIT`), B commits in the middle | Outcome | What came back |
|---|---|---|
| BEGIN; read; B appends; read again | repeatable | `reads 3, B appends, reads 3; COMMIT ok` |
| B commits after BEGIN, before the first read | at first read | `BEGIN, B appends, the first read sees 4 rows; B appends again, the next read sees 4` |
| read v, B changes it, UPDATE v + 1, COMMIT | broken | `400 Only one instance of each update type is allowed per request. Duplicate types: add-snapshot, set-snapshot-ref` |
| read count, B appends, INSERT the count, COMMIT | skew | `reads count 3, B appends, INSERT (100, count); COMMIT ok; commits [409, 200]; [(1, 10), (2, 20), (3, 30), (4, 40), (100, 3)]` |
| the same on a commit.retry.num-retries = 0 table | refused | `reads count 3, B appends, INSERT (100, count); COMMIT RuntimeError: RuntimeError: TransactionContext Error: Failed to commit: Failed to commit Iceberg transaction: Request to 'http://127.0.0.1:36497/…` |
| INSERT + UPDATE + DELETE in one transaction | broken | `INSERT + UPDATE + DELETE; COMMIT RuntimeError: RuntimeError: TransactionContext Error: Failed to commit: Failed to commit Iceberg transaction: Request to 'http://127.0.0.1:36497/iceberg/v1' returned …` |
| INSERT + DELETE, then ROLLBACK | nothing sent | `INSERT + DELETE, ROLLBACK; commits [none]; [(1, 10), (2, 20), (3, 30)]` |
| a failing statement inside the transaction | all rolled back | `INSERT ok, then INSERT failed (RuntimeError: RuntimeError: Conversion Error: Could not convert string 'not a number' to …); COMMIT ok; [(1, 10), (2, 20), (3, 30)]` |
| own uncommitted rows, inside and from another connection | yes | `uncommitted INSERT: this transaction reads 4, another connection 3; COMMIT ok; catalog 4 rows` |
| INSERT into two tables in one transaction | refused | `INSERT into two tables; COMMIT at the second INSERT: RuntimeError: RuntimeError: TransactionContext Error: Iceberg REST Catalog cannot commit this transaction atomically because it would require mult…` |
| two DuckDB transactions update the same row | second refused | `both read v = 10, +1 and +100; first COMMIT ok; second COMMIT RuntimeError: RuntimeError: TransactionContext Error: Failed to commit: Failed to commit Iceberg transaction: Request to 'http://127.0.0.…` |

| Statements in one `BEGIN ... COMMIT`, no concurrent writer | Outcome | What came back |
|---|---|---|
| TRUNCATE, INSERT | refused | `COMMIT refused: RuntimeError: RuntimeError: TransactionContext Error: Failed to commit: Failed to commit Iceberg transaction: Request to 'http://127.0.0.1:36497/iceberg/v1' returned a non-200 status …` |
| DELETE all, INSERT | refused | `COMMIT refused: RuntimeError: RuntimeError: TransactionContext Error: Failed to commit: Failed to commit Iceberg transaction: Request to 'http://127.0.0.1:36497/iceberg/v1' returned a non-200 status …` |
| DROP TABLE, CREATE TABLE the same name with a new column, INSERT | refused | `CREATE refused: RuntimeError: RuntimeError: Not implemented Error: Cannot create table deleted within a transaction: onelake._bench_capability.iso_dk_37930015241_1_cb_drop_create; t (('id', 'v'), [(1…` |
| CREATE a copy AS SELECT, DROP TABLE, CREATE TABLE the same name AS SELECT from the copy | PARTIAL | `DROP refused: RuntimeError: RuntimeError: TransactionContext Error: Iceberg REST Catalog cannot commit this transaction atomically because it mixes table updates with rename/drop requests; t (('id', …` |
| CREATE OR REPLACE TABLE t AS SELECT ... FROM t | refused | `CREATE refused: RuntimeError: RuntimeError: Not implemented Error: CREATE OR REPLACE not supported in DuckDB-Iceberg. Please use separate Drop and Create Statements; t (('id', 'v'), [(1, 10), (2, 20)…` |
| CREATE TABLE, INSERT | works | `t (('id', 'v'), [(1, 10), (2, 20), (3, 30)]); n (('id', 'v'), [(1, 1)]); sent ['POST tables 200', 'POST n 200']` |
| CREATE TABLE AS SELECT from the seeded table | works | `t (('id', 'v'), [(1, 10), (2, 20), (3, 30)]); n (('id', 'v'), [(1, 20), (2, 40), (3, 60)]); sent ['POST tables 200', 'POST n 200']` |
| CREATE TABLE, INSERT into another, existing table | PARTIAL | `INSERT refused: RuntimeError: RuntimeError: TransactionContext Error: Iceberg REST Catalog cannot commit this transaction atomically because it would require multiple table commit requests without at…` |
| ADD COLUMN, INSERT a row that fills it | works | `t (('id', 'v', 'w'), [(1, 10, None), (2, 20, None), (3, 30, None), (4, 40, 400)]); n None; sent ['POST t 200']` |
| ADD COLUMN, UPDATE it | works | `t (('id', 'v', 'w'), [(1, 10, 100), (2, 20, 200), (3, 30, 300)]); n None; sent ['POST t 200']` |
| RENAME COLUMN, INSERT | works | `t (('id', 'v2'), [(1, 10), (2, 20), (3, 30), (4, 40)]); n None; sent ['POST t 200']` |
| DROP COLUMN, INSERT | works | `t (('id',), [(1,), (2,), (3,), (4,)]); n None; sent ['POST t 200']` |
| SET PARTITIONED BY (bucket(4, id)), INSERT | works | `t (('id', 'v'), [(1, 10), (2, 20), (3, 30), (4, 40)]); n None; sent ['POST t 200']; spec ['bucket[4]']` |
| INSERT, UPDATE the row just inserted | refused | `COMMIT refused: RuntimeError: RuntimeError: TransactionContext Error: Failed to commit: Failed to commit Iceberg transaction: Request to 'http://127.0.0.1:36497/iceberg/v1' returned a non-200 status …` |
| INSERT, DELETE the row just inserted | refused | `COMMIT refused: RuntimeError: RuntimeError: TransactionContext Error: Failed to commit: Failed to commit Iceberg transaction: Request to 'http://127.0.0.1:36497/iceberg/v1' returned a non-200 status …` |
| UPDATE a row, then DELETE it | refused | `COMMIT refused: RuntimeError: RuntimeError: TransactionContext Error: Failed to commit: Failed to commit Iceberg transaction: Request to 'http://127.0.0.1:36497/iceberg/v1' returned a non-200 status …` |
| MERGE, then MERGE the same row again | refused | `COMMIT refused: RuntimeError: RuntimeError: TransactionContext Error: Failed to commit: Failed to commit Iceberg transaction: Request to 'http://127.0.0.1:36497/iceberg/v1' returned a non-200 status …` |
| INSERT ... SELECT from the table, DELETE the originals | refused | `COMMIT refused: RuntimeError: RuntimeError: TransactionContext Error: Failed to commit: Failed to commit Iceberg transaction: Request to 'http://127.0.0.1:36497/iceberg/v1' returned a non-200 status …` |
| TRUNCATE, INSERT, ROLLBACK | works | `t (('id', 'v'), [(1, 10), (2, 20), (3, 30)]); n None; sent []` |
| DROP TABLE, ROLLBACK | works | `t (('id', 'v'), [(1, 10), (2, 20), (3, 30)]); n None; sent []` |
| DROP TABLE, CREATE TABLE the same name, ROLLBACK | refused | `CREATE refused: RuntimeError: RuntimeError: Not implemented Error: Cannot create table deleted within a transaction: onelake._bench_capability.iso_dk_37930015241_1_cb_drop_create_rollback; t (('id', …` |
| CREATE TABLE, INSERT, ROLLBACK | WRONG | `t (('id', 'v'), [(1, 10), (2, 20), (3, 30)]); n (('id', 'v'), []); sent ['POST tables 200']; expected t (('id', 'v'), [(1, 10), (2, 20), (3, 30)]), n None` |

## Where these readings come from

The OneLake Iceberg REST catalog in production, read by CI (`.github/workflows/capability.yml`), which writes this file. Every cell is a reading taken by sending the request, not a property of the product: re-run rather than trust it.

- pyiceberg: 0.12.0, run 37930015241, 2026-10-09
- duckdb: v2.0.0-alpha46057, run 37930015241, 2026-10-09
- sail: 0.7.2, run 37930015241, 2026-10-09
- duckdb_isolation: v2.0.0-alpha46057, run 37930015241, 2026-10-09
