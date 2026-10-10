# OneLake Iceberg REST catalog: what the native engines can do

Polars, DuckDB, Sail and chDB: engines with their own Iceberg implementation, no JVM.

`yes` works · `no` refused · `no-op` accepted but not applied · `na` the engine has no such operation · `—` not probed · `?` the probe could not ask

| Operation | Polars | DuckDB | Sail | chDB |
|---|---|---|---|---|
| Version | 2.0.0 | v2.0.0-alpha46057 | 0.7.2 | 4.4.0 |
| Language | Rust | C++ | Rust | C++ |
| Iceberg implementation | own parquet reader and writer, pyiceberg for the catalog and commit | own | own, on DataFusion | ClickHouse's own |
| CREATE TABLE | na | yes | yes | no ¹ |
| INSERT / append | yes | yes | yes | yes |
| INSERT ... SELECT | na | yes | na | yes |
| DELETE | na | yes | yes | yes |
| UPDATE | na | yes | yes | yes |
| MERGE INTO / upsert | na | no ² | yes | no ³ |
| MERGE with one action | na | yes | yes | no ⁴ |
| MERGE ... WHEN NOT MATCHED BY SOURCE | na | no ⁵ | yes | no ⁶ |
| INSERT OVERWRITE, whole table | no ⁷ | no ⁸ | yes | no ⁹ |
| INSERT OVERWRITE, one partition / by filter | na | no ¹⁰ | no ¹¹ | no ¹² |
| Several writes in one transaction | na | no ¹³ | na | na |
| TRUNCATE | na | yes | no ¹⁴ | no ¹⁵ |
| CREATE TABLE AS SELECT | na | yes | yes | no ¹⁶ |
| CREATE OR REPLACE TABLE | no ¹⁷ | no ¹⁸ | no ¹⁹ | na |
| Partitioned table | yes | yes | yes | yes |
| Partition transform: bucket | no ²⁰ | yes | yes | yes |
| Partition transform: truncate | yes | yes | yes | yes |
| Partition transforms: year / month / day / hour | yes | yes | yes | yes |
| Types: decimal, date, timestamp, timestamptz, uuid, binary | yes | yes | no ²¹ | yes |
| Nested types: struct, list, map | yes | yes | yes | yes |
| format-version 3 | na | no-op ²² | no-op ²³ | na |
| Add column | yes | yes | no ²⁴ | yes |
| Drop column | na | yes | no ²⁵ | yes |
| Rename column | na | yes | no ²⁶ | yes |
| Type promotion (int → long) | yes | yes | no ²⁷ | yes |
| Partition evolution | na | yes | no ²⁸ | no ²⁹ |
| Write after partition evolution | yes | yes | na | na |
| Set table property | na | yes | no ³⁰ | no ³¹ |
| Sort order at create | na | yes | na | — |
| Sort order evolution | na | yes | no ³² | no ³³ |
| Time travel | yes | yes | yes | yes |
| Metadata tables | na | yes | no ³⁴ | no ³⁵ |
| Compaction | na | yes | no ³⁶ | no-op ³⁷ |
| Expire snapshots | na | no ³⁸ | no ³⁹ | no ⁴⁰ |
| Create branch | na | no ⁴¹ | no ⁴² | no ⁴³ |
| Create tag | na | na | no ⁴⁴ | no ⁴⁵ |
| Drop table with purge | na | yes | yes | no ⁴⁶ |
| Create / drop namespace | na | yes | yes | na |
| Credential vending | na | yes | na | na |
| A commit against a stale snapshot is refused | yes | na | na | na |
| Concurrent append: both kept | yes | yes | yes | yes |
| Concurrent writer: DELETE loses nothing | na | yes | yes | yes |
| Concurrent writer: UPDATE loses nothing | na | yes | yes | yes |

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

1. chDB, CREATE TABLE: CREATE TABLE: `ChdbError: Code: 79. DB::Exception: MergeTree storages require data path. (INCORRECT_FILE_NAME)`
2. DuckDB, MERGE INTO / upsert: MERGE INTO: `Only one instance of each update type is allowed per request. Duplicate types: add-snapshot`
3. chDB, MERGE INTO / upsert: MERGE INTO: `ChdbError: Code: 62. DB::Exception: Syntax error: failed at position 12 (onelake): onelake.`_bench_capability.ch_37949375865_1_merge` t USING (SELECT * FROM values('id Int64, v Int64', (1, 777), (9, 90))) s ON t.id = s.id WHEN MATCHED THEN UPD... Expected end of query. (SYNTAX_ERROR)`
4. chDB, MERGE with one action: MERGE INTO: `ChdbError: Code: 62. DB::Exception: Syntax error: failed at position 12 (onelake): onelake.`_bench_capability.ch_37949375865_1_merge` t USING (SELECT * FROM values('id Int64, v Int64', (1, 777), (9, 90))) s ON t.id = s.id WHEN MATCHED THEN UPD... Expected end of query. (SYNTAX_ERROR)`
5. DuckDB, MERGE ... WHEN NOT MATCHED BY SOURCE: MERGE ... WHEN NOT MATCHED BY SOURCE THEN DELETE: `Only one instance of each update type is allowed per request. Duplicate types: add-snapshot`
6. chDB, MERGE ... WHEN NOT MATCHED BY SOURCE: MERGE ... WHEN NOT MATCHED BY SOURCE THEN DELETE: `ChdbError: Code: 62. DB::Exception: Syntax error: failed at position 12 (onelake): onelake.`_bench_capability.ch_37949375865_1_mergesrc` t USING (SELECT * FROM values('id Int64, v Int64', (1, 10), (9, 90))) s ON t.id = s.id WHEN NOT MATCHED TH... Expected end of query. (SYNTAX_ERROR)`
7. Polars, INSERT OVERWRITE, whole table: sink_iceberg(mode='overwrite'): `400 Only one instance of each update type is allowed per request. Duplicate types: add-snapshot, set-snapshot-ref`
8. DuckDB, INSERT OVERWRITE, whole table: overwrite: DELETE + INSERT in one transaction: `Only one instance of each update type is allowed per request. Duplicate types: add-snapshot`
9. chDB, INSERT OVERWRITE, whole table: INSERT OVERWRITE: `ChdbError: Code: 62. DB::Exception: Syntax error: failed at position 18 (onelake): onelake.`_bench_capability.ch_37949375865_1_overwrite` SELECT * FROM values('id Int64, v Int64', (1, 100), (2, 200)). Expected one of: token, Comma, FROM, PREWHERE, WHERE, GROUP BY, WITH, HAVING, WINDOW, QUALIFY, ORD…`
10. DuckDB, INSERT OVERWRITE, one partition / by filter: one-partition overwrite: DELETE WHERE p + INSERT in one transaction: `Only one instance of each update type is allowed per request. Duplicate types: add-snapshot`
11. Sail, INSERT OVERWRITE, one partition / by filter: INSERT OVERWRITE ... PARTITION (p = 'a'): `UnsupportedOperationException: PARTITION for write`
12. chDB, INSERT OVERWRITE, one partition / by filter: INSERT OVERWRITE ... PARTITION (p = 'a'): `ChdbError: Code: 62. DB::Exception: Syntax error: failed at position 18 (onelake): onelake.`_bench_capability.ch_37949375865_1_ovwpart` PARTITION (p = 'a') VALUES (9, 90). Expected one of: token, Comma, FROM, PREWHERE, WHERE, GROUP BY, WITH, HAVING, WINDOW, QUALIFY, ORDER BY, LIMIT, OFFSET, FETCH, …`
13. DuckDB, Several writes in one transaction: transaction: INSERT + INSERT: `Only one instance of each update type is allowed per request. Duplicate types: add-snapshot`; transaction: UPDATE + INSERT: `Only one instance of each update type is allowed per request. Duplicate types: add-snapshot`; transaction: UPDATE + UPDATE: `Only one instance of each update type is allowed per request. Duplicate types: add-snapshot`
14. Sail, TRUNCATE: TRUNCATE TABLE: `IllegalArgumentException: invalid argument: found TRUNCATE at 0:8 expected something else, ';', statement, or end of input`
15. chDB, TRUNCATE: TRUNCATE TABLE: `ChdbError: Code: 48. DB::Exception: Truncate is not supported for data lake engine. (NOT_IMPLEMENTED)`
16. chDB, CREATE TABLE AS SELECT: CREATE TABLE ... AS SELECT: `ChdbError: Code: 79. DB::Exception: MergeTree storages require data path. (INCORRECT_FILE_NAME)`
17. Polars, CREATE OR REPLACE TABLE: sink_iceberg(mode='overwrite', schema_mode='overwrite'): `400 Only one instance of each update type is allowed per request. Duplicate types: add-schema, set-current-schema, add-snapshot, set-snapshot-ref`
18. DuckDB, CREATE OR REPLACE TABLE: CREATE OR REPLACE TABLE: `Not implemented Error: CREATE OR REPLACE not supported in DuckDB-Iceberg. Please use separate Drop and Create Statements`; CREATE OR REPLACE TABLE ... AS SELECT: `Not implemented Error: CREATE OR REPLACE not supported in DuckDB-Iceberg. Please use separate Drop and Create Statements`
19. Sail, CREATE OR REPLACE TABLE: CREATE OR REPLACE TABLE ... AS SELECT, existing table: `AnalysisException: not supported: Replace table is not supported yet`
20. Polars, Partition transform: bucket: write a table partitioned by bucket(4, id): `works: none; refused: bucket: NotImplementedError: NotImplementedError: sink to Iceberg table with 'bucket[4]' partition transform`
21. Sail, Types: decimal, date, timestamp, timestamptz, uuid, binary: types: decimal, date, timestamp, timestamptz, uuid, binary: `works: ['decimal', 'date', 'timestamp', 'timestamptz', 'binary']; refused: uuid: IllegalArgumentException: invalid argument: found UUID at 84:88 expected data type`
22. DuckDB, format-version 3: format-version 3: `asked for format-version 3, the table came back at 2`
23. Sail, format-version 3: format-version 3: `asked for format-version 3, the table came back at 2`
24. Sail, Add column: ALTER TABLE ADD COLUMN: `UnsupportedOperationException: unsupported ALTER TABLE operation`
25. Sail, Drop column: ALTER TABLE DROP COLUMN: `UnsupportedOperationException: unsupported ALTER TABLE operation`
26. Sail, Rename column: ALTER TABLE RENAME COLUMN: `UnsupportedOperationException: unsupported ALTER TABLE operation`
27. Sail, Type promotion (int → long): ALTER COLUMN c TYPE BIGINT (int -> long): `AnalysisException: external error: This feature is not implemented: ALTER TABLE is not yet supported for catalog-managed Iceberg tables: onelake._bench_capability.sl_38012150653_1_promote`
28. Sail, Partition evolution: ALTER TABLE ADD PARTITION FIELD (partition evolution): `IllegalArgumentException: invalid argument: found FIELD at 81:86 expected '('`
29. chDB, Partition evolution: ALTER TABLE ADD PARTITION FIELD (partition evolution): `ChdbError: Code: 62. DB::Exception: Syntax error: failed at position 70 (PARTITION): PARTITION FIELD bucket(4, id). Expected one of: COLUMN, INDEX, STATISTICS, PROJECTION, CONSTRAINT, end of query. (SYNTAX_ERROR)`
30. Sail, Set table property: ALTER TABLE SET TBLPROPERTIES: `AnalysisException: external error: This feature is not implemented: ALTER TABLE is not yet supported for catalog-managed Iceberg tables: onelake._bench_capability.sl_38012150653_1_props`
31. chDB, Set table property: ALTER TABLE SET TBLPROPERTIES: `ChdbError: Code: 62. DB::Exception: Syntax error: failed at position 64 (SET): SET TBLPROPERTIES ('probed-at' = '37949375865_1'). Expected one of: ON, a list of ALTER commands, ALTER command, ADD COLUMN, RENAME COLUMN, MATERIALIZE COLUMN, DROP PARTITION, DROP PART, FORGET PARTITION, DROP DETACHED P…`
32. Sail, Sort order evolution: ALTER TABLE ... WRITE ORDERED BY (sort order): `IllegalArgumentException: invalid argument: found WRITE at 66:71 expected '.', 'RENAME', 'PARTITION', 'ADD', 'DROP', 'ALTER', 'CHANGE', 'REPLACE', 'SET', 'UNSET', or 'RECOVER'`
33. chDB, Sort order evolution: ALTER TABLE MODIFY ORDER BY (sort order evolution): `ChdbError: Code: 48. DB::Exception: Alter of type 'MODIFY_ORDER_BY' is not supported by Iceberg storage. (NOT_IMPLEMENTED)`
34. Sail, Metadata tables: metadata tables (t.snapshots): `IllegalArgumentException: invalid argument: table reference: [Identifier("onelake"), Identifier("_bench_capability"), Identifier("sl_38012150653_1_inspect"), Identifier("snapshots")]`
35. chDB, Metadata tables: metadata (system.iceberg_history): `400 Malformed request`
36. Sail, Compaction: CALL system.rewrite_data_files (compaction): `IllegalArgumentException: invalid argument: found CALL at 0:4 expected something else, ';', statement, or end of input`
37. chDB, Compaction: OPTIMIZE TABLE (compaction): `returned success and data files went 2 -> 2`
38. DuckDB, Expire snapshots: iceberg_expire_snapshots: `Catalog Error: Table Function with name iceberg_expire_snapshots does not exist! Did you mean "iceberg_snapshots"?`
39. Sail, Expire snapshots: CALL system.expire_snapshots: `IllegalArgumentException: invalid argument: found CALL at 0:4 expected something else, ';', statement, or end of input`
40. chDB, Expire snapshots: ALTER TABLE ... EXECUTE expire_snapshots: `ChdbError: Code: 48. DB::Exception: expire_snapshots is not supported for Iceberg tables backed by a transactional catalog. (NOT_IMPLEMENTED)`
41. DuckDB, Create branch: ALTER TABLE ... CREATE BRANCH: `Parser Error: syntax error at or near "CREATE" LINE 1: ... TABLE onelake."_bench_capability"."dk_38011645322_1_branch" CREATE BRANCH probe_branch ^^^^^^`
42. Sail, Create branch: ALTER TABLE ... CREATE BRANCH: `IllegalArgumentException: invalid argument: found CREATE at 66:72 expected '.', 'RENAME', 'PARTITION', 'ADD', 'DROP', 'ALTER', 'CHANGE', 'REPLACE', 'SET', 'UNSET', or 'RECOVER'`
43. chDB, Create branch: ALTER TABLE ... CREATE BRANCH: `ChdbError: Code: 62. DB::Exception: Syntax error: failed at position 65 (CREATE): CREATE BRANCH probe_branch. Expected one of: ON, a list of ALTER commands, ALTER command, ADD COLUMN, RENAME COLUMN, MATERIALIZE COLUMN, DROP PARTITION, DROP PART, FORGET PARTITION, DROP DETACHED PARTITION, DROP DETAC…`
44. Sail, Create tag: ALTER TABLE ... CREATE TAG: `IllegalArgumentException: invalid argument: found CREATE at 63:69 expected '.', 'RENAME', 'PARTITION', 'ADD', 'DROP', 'ALTER', 'CHANGE', 'REPLACE', 'SET', 'UNSET', or 'RECOVER'`
45. chDB, Create tag: ALTER TABLE ... CREATE TAG: `ChdbError: Code: 62. DB::Exception: Syntax error: failed at position 62 (CREATE): CREATE TAG probe_tag. Expected one of: ON, a list of ALTER commands, ALTER command, ADD COLUMN, RENAME COLUMN, MATERIALIZE COLUMN, DROP PARTITION, DROP PART, FORGET PARTITION, DROP DETACHED PARTITION, DROP DETACHED PA…`
46. chDB, Drop table with purge: DROP TABLE (chDB sends purgeRequested=false): `ChdbError: Code: 736. DB::Exception: Failed to drop table DB::HTTPException: Received error from remote server https://onelake.table.fabric.microsoft.com/iceberg/v1/namespaces/_bench_capability/tables/ch_37949375865_1_droppable?purgeRequested=False. HTTP status code: 405 'Method Not Allowed', body …`

## DuckDB: isolation levels and transactions

DuckDB is the only one with transactions. Writer B (pyiceberg) commits between DuckDB's
read and DuckDB's commit; the race is injected at the REST commit through a local proxy
(`bench/race.py`), so it is deterministic. Every DuckDB connection runs
`SET iceberg_use_metadata_log = false`
([duckdb-iceberg#1475](https://github.com/duckdb/duckdb-iceberg/issues/1475)).

`refused` DuckDB's commit fails and B's change stands · `retried` both changes kept ·
`lost` B's change is gone · `broken` the race could not be run

| DuckDB writes, B commits in between | serializable | snapshot | no retries |
|---|---|---|---|
| INSERT, B appends | retried | retried | refused |
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
| INSERT + UPDATE + DELETE in one transaction | broken | `INSERT + UPDATE + DELETE; COMMIT RuntimeError: RuntimeError: TransactionContext Error: Failed to commit: Failed to commit Iceberg transaction: Request to 'http://127.0.0.1:44139/iceberg/v1' returned …` |
| INSERT + DELETE, then ROLLBACK | nothing sent | `INSERT + DELETE, ROLLBACK; commits [none]; [(1, 10), (2, 20), (3, 30)]` |
| a failing statement inside the transaction | all rolled back | `INSERT ok, then INSERT failed (RuntimeError: RuntimeError: Conversion Error: Could not convert string 'not a number' to …); COMMIT ok; [(1, 10), (2, 20), (3, 30)]` |
| own uncommitted rows, inside and from another connection | yes | `uncommitted INSERT: this transaction reads 4, another connection 3; COMMIT ok; catalog 4 rows` |
| INSERT into two tables in one transaction | refused | `INSERT into two tables; COMMIT at the second INSERT: RuntimeError: RuntimeError: TransactionContext Error: Iceberg REST Catalog cannot commit this transaction atomically because it would require mult…` |
| two DuckDB transactions update the same row | second refused | `both read v = 10, +1 and +100; first COMMIT ok; second COMMIT RuntimeError: RuntimeError: TransactionContext Error: Failed to commit: Failed to commit Iceberg transaction: Request to 'http://127.0.0.…` |

| Statements in one `BEGIN ... COMMIT`, no concurrent writer | Outcome | What came back |
|---|---|---|
| TRUNCATE, INSERT | refused | `COMMIT refused: RuntimeError: RuntimeError: TransactionContext Error: Failed to commit: Failed to commit Iceberg transaction: Request to 'http://127.0.0.1:44139/iceberg/v1' returned a non-200 status …` |
| DELETE all, INSERT | refused | `COMMIT refused: RuntimeError: RuntimeError: TransactionContext Error: Failed to commit: Failed to commit Iceberg transaction: Request to 'http://127.0.0.1:44139/iceberg/v1' returned a non-200 status …` |
| DROP TABLE, CREATE TABLE the same name with a new column, INSERT | refused | `CREATE refused: RuntimeError: RuntimeError: Not implemented Error: Cannot create table deleted within a transaction: onelake._bench_capability.iso_dk_37953552712_1_cb_drop_create; t (('id', 'v'), [(1…` |
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
| INSERT, UPDATE the row just inserted | refused | `COMMIT refused: RuntimeError: RuntimeError: TransactionContext Error: Failed to commit: Failed to commit Iceberg transaction: Request to 'http://127.0.0.1:44139/iceberg/v1' returned a non-200 status …` |
| INSERT, DELETE the row just inserted | refused | `COMMIT refused: RuntimeError: RuntimeError: TransactionContext Error: Failed to commit: Failed to commit Iceberg transaction: Request to 'http://127.0.0.1:44139/iceberg/v1' returned a non-200 status …` |
| UPDATE a row, then DELETE it | refused | `COMMIT refused: RuntimeError: RuntimeError: TransactionContext Error: Failed to commit: Failed to commit Iceberg transaction: Request to 'http://127.0.0.1:44139/iceberg/v1' returned a non-200 status …` |
| MERGE, then MERGE the same row again | refused | `COMMIT refused: RuntimeError: RuntimeError: TransactionContext Error: Failed to commit: Failed to commit Iceberg transaction: Request to 'http://127.0.0.1:44139/iceberg/v1' returned a non-200 status …` |
| INSERT ... SELECT from the table, DELETE the originals | refused | `COMMIT refused: RuntimeError: RuntimeError: TransactionContext Error: Failed to commit: Failed to commit Iceberg transaction: Request to 'http://127.0.0.1:44139/iceberg/v1' returned a non-200 status …` |
| TRUNCATE, INSERT, ROLLBACK | works | `t (('id', 'v'), [(1, 10), (2, 20), (3, 30)]); n None; sent []` |
| DROP TABLE, ROLLBACK | works | `t (('id', 'v'), [(1, 10), (2, 20), (3, 30)]); n None; sent []` |
| DROP TABLE, CREATE TABLE the same name, ROLLBACK | refused | `CREATE refused: RuntimeError: RuntimeError: Not implemented Error: Cannot create table deleted within a transaction: onelake._bench_capability.iso_dk_37953552712_1_cb_drop_create_rollback; t (('id', …` |
| CREATE TABLE, INSERT, ROLLBACK | WRONG | `t (('id', 'v'), [(1, 10), (2, 20), (3, 30)]); n (('id', 'v'), []); sent ['POST tables 200']; expected t (('id', 'v'), [(1, 10), (2, 20), (3, 30)]), n None` |

## Where these readings come from

The OneLake Iceberg REST catalog in production, read by CI (`.github/workflows/capability.yml`), which writes this file. Every cell is a reading taken by sending the request, not a property of the product: re-run rather than trust it.

- polars: 2.0.0, run 37949375865, 2026-10-09
- duckdb: v2.0.0-alpha46057, run 38011645322, 2026-10-10
- sail: 0.7.2, run 38012150653, 2026-10-10
- chdb: 4.4.0, run 37949375865, 2026-10-09
- duckdb_isolation: v2.0.0-alpha46057, run 37953552712, 2026-10-09
