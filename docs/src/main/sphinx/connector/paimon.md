# Paimon connector

The Paimon connector enables Trino to read and write
[Apache Paimon](https://paimon.apache.org/) tables. It uses the Paimon 1.5
catalog and table APIs together with Trino's native ORC, Parquet, and file
system implementations.

## Requirements

To use the connector, you need:

- A Paimon catalog and warehouse reachable from every Trino coordinator and
  worker.
- Network access to the metadata service used by the catalog, when applicable.
- Native file system configuration for the warehouse. The recommended S3
  configuration uses Trino native S3 and does not require Hadoop.

For object storage configuration, see [](/object-storage),
[](/object-storage/file-system-azure), [](/object-storage/file-system-gcs),
and [](/object-storage/file-system-s3).

## General configuration

Create a catalog properties file such as `etc/catalog/paimon.properties` with
the connector name, warehouse, catalog implementation, and any catalog-specific
properties.

### Filesystem catalog on S3

The following example configures a filesystem Paimon catalog on an S3-compatible
object store:

```properties
connector.name=paimon
warehouse=s3://example-bucket/warehouse
metastore=filesystem

fs.native-s3.enabled=true
fs.hadoop.enabled=false
s3.endpoint=https://s3.example.net
s3.access-key=EXAMPLE_ACCESS_KEY
s3.secret-key=EXAMPLE_SECRET_KEY
s3.region=us-east-1
s3.path-style-access=true
```

The connector accepts `s3.access-key` and `s3.secret-key`. Existing deployments
using the compatible `s3.aws-access-key`, `s3.aws-secret-key`, `s3a.*`, or
`fs.s3a.*` names are also accepted and normalized before Paimon creates the
catalog.

### JDBC catalog with concurrent writers

For a JDBC catalog, configure the JDBC connection and a catalog lock. This is
required for safe concurrent `KEY_DYNAMIC` primary-key writes from independent
Trino coordinators.

```properties
connector.name=paimon
warehouse=s3://example-bucket/warehouse
metastore=jdbc
uri=jdbc:postgresql://metadata.example.net:5432/paimon
jdbc.user=paimon
jdbc.password=secret
catalog-key=production-paimon

lock.enabled=true
lock.type=jdbc
lock-acquire-timeout=10m
lock-check-max-sleep=1s

fs.native-s3.enabled=true
fs.hadoop.enabled=false
s3.region=us-east-1
```

The connector validates `KEY_DYNAMIC` global primary keys inside Paimon's atomic
snapshot-commit path. Catalog and lock combinations that cannot provide that
atomic validation fail before the write instead of accepting a potentially stale
key route.

### Hive metastore catalog

For a Hive metastore catalog, configure the Hive metastore URI or Hive
configuration directory. The connector reads and writes data through Trino's
native file system (no Hadoop required), while table metadata is managed by
the Hive metastore.

```properties
connector.name=paimon
warehouse=hdfs://nameservice/warehouse
metastore=hive
uri=thrift://hive-metastore.example.net:9083

fs.native-s3.enabled=true
fs.hadoop.enabled=false
s3.region=us-east-1
```

Alternatively, provide the Hive configuration directory:

```properties
connector.name=paimon
metastore=hive
hive-conf-dir=/etc/hive/conf
```

When using a Hive metastore catalog, set `hive.config.resources` to the
paths of Hadoop XML configuration files (such as `core-site.xml` and
`hdfs-site.xml`) so that the Paimon catalog can resolve HDFS paths:

```properties
hive.config.resources=/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml
```

## Configuration properties

:::{list-table} Paimon configuration properties
:widths: 32, 53, 15
:header-rows: 1

* - Property name
  - Description
  - Default
* - `warehouse`
  - Paimon warehouse location. Required by the selected Paimon catalog.
  -
* - `metastore`
  - Paimon catalog type, for example `filesystem` or `jdbc`.
  - Paimon default
* - `uri`
  - Catalog URI. For a JDBC catalog this is the JDBC connection URL and it is
    required whenever `metastore` is `jdbc`.
  -
* - `jdbc.user`
  - JDBC catalog user.
  -
* - `jdbc.password`
  - JDBC catalog password.
  -
* - `catalog-key`
  - Stable Paimon catalog identity used for catalog and lock coordination.
  -
* - `lock.enabled`
  - Enable the Paimon catalog lock.
  - Paimon default
* - `lock.type`
  - Paimon lock implementation, such as `jdbc` for a JDBC catalog.
  - Paimon default
* - `lock-acquire-timeout`
  - Maximum time to acquire the catalog lock.
  - Paimon default
* - `lock-check-max-sleep`
  - Maximum interval between catalog-lock checks.
  - Paimon default
* - `fs.native-s3.enabled`
  - Enable Trino native S3 access for the Paimon warehouse. Unset by default,
    which leaves file system selection to Paimon.
  - Paimon default
* - `fs.hadoop.enabled`
  - Enable Paimon Hadoop file system support. Set to `false` when using Trino
    native S3.
  - Paimon default
* - `s3.endpoint`, `s3.region`, `s3.path-style-access`
  - Native S3 endpoint, region, and path-style setting forwarded to Paimon.
  -
* - `s3.access-key`, `s3.secret-key`
  - Access credentials for S3-compatible object storage. The secret key is a
    sensitive catalog property.
  -
* - `s3.signer-type`, `s3.signing-algorithm`, `s3.endpoint.region`
  - Additional native S3 settings forwarded to Paimon.
  -
* - `s3a.*`, `fs.s3a.*`
  - Aliases of the `s3.*` settings above for S3A-compatible configurations.
    When several aliases of the same setting are configured, `s3.*` wins, then
    `s3a.*`, then `fs.s3a.*`.
  -
* - `table.type`
  - Paimon table type, such as `MANAGED` or `EXTERNAL`.
  - Paimon default
* - `case-sensitive`, `allow-upper-case`
  - Paimon catalog identifier matching rules.
  - Paimon default
* - `sync-all-properties`
  - Forward table properties to Paimon as table options.
  - Paimon default
* - `format-table.enabled`
  - Expose Paimon format tables where the selected catalog supports them.
  - Paimon default
* - `file-io.allow-cache`
  - Paimon FileIO caching. The connector always reaches the warehouse through
    Trino FileIO, so `resolving-file-io.enabled` is ignored and logged when set.
  - Paimon default
* - `cache-enabled`, `cache.*`
  - Paimon catalog and table caches, such as `cache.expire-after-access`,
    `cache.manifest.max-memory`, or `cache.snapshot.max-num-per-table`.
  - Paimon default
* - `local-cache.enabled`, `local-cache.dir`, `local-cache.max-size`,
    `local-cache.block-size`, `local-cache.whitelist`
  - Paimon local disk cache.
  - Paimon default
* - `client-pool-size`, `client-pool-cache.keys`,
    `client-pool-cache.eviction-interval-ms`
  - JDBC and Hive metastore client pool settings.
  - Paimon default
* - `hive-conf-dir`, `hadoop-conf-dir`, `metastore.client.class`,
    `location-in-properties`, `hive.skip-update-stats`
  - Hive metastore catalog settings.
  - Paimon default
* - `lock-key-max-length`
  - Maximum length of a Paimon catalog lock key.
  - Paimon default
* - `alter-table-cascade`
  - Cascade Paimon table alterations.
  - Paimon default
* - `write.spill-path`
  - Local directory used by spillable Paimon writers.
  - JVM temporary directory
* - `catalog.session-cache.maximum-size`
  - Maximum number of session-specific Paimon catalog instances retained by the
    connector.
  - `1000`
:::

Except for `write.spill-path` and `catalog.session-cache.maximum-size`, every
property in this table is forwarded to the Paimon catalog as an option with the
same name. Unlisted Paimon catalog options can be set the same way.

`hive.config.resources` is resolved by the connector before the Paimon catalog is
created, so it is not a Paimon catalog option and is not part of this table.

## Catalog session properties

:::{list-table} Paimon catalog session properties
:widths: 30, 15, 40, 15
:header-rows: 1

* - Property name
  - Type
  - Description
  - Default
* - `scan_timestamp_millis`
  - bigint
  - Read the latest snapshot committed at or before this Unix timestamp in
    milliseconds.
  -
* - `scan_snapshot_id`
  - bigint
  - Read a specific Paimon snapshot. Must not be negative.
  -
* - `scan_tag_name`
  - varchar
  - Read the snapshot referenced by a Paimon tag.
  -
* - `scan_file_creation_time_millis`
  - bigint
  - Read the latest snapshot whose data files were created at or before this
    Unix timestamp in milliseconds.
  -
* - `scan_creation_time_millis`
  - bigint
  - Read the latest snapshot created at or before this Unix timestamp in
    milliseconds.
  -
* - `insert_existing_partitions_behavior`
  - varchar
  - Behavior for inserts into an existing partition: `ERROR`, `APPEND`, or
    `OVERWRITE`.
  - `APPEND`
* - `minimum_split_weight`
  - double
  - Minimum scheduling weight assigned to a split, greater than `0` and at most
    `1`.
  - `0.05`
* - `dynamic_filtering_wait_timeout`
  - varchar
  - Maximum time split generation waits for dynamic filters, for example `1s` or
    `500ms`.
  - `0s`
:::

## Table properties

Use the `WITH` clause of `CREATE TABLE` or `ALTER TABLE SET PROPERTIES` to
define Paimon option properties. The connector exposes the documented Paimon 1.5
`CoreOptions` as string-valued properties, converting periods and hyphens in a
Paimon option name to underscores. For example, Paimon's `file.format` and
`merge-engine` options are `file_format` and `merge_engine` in Trino. Snapshot
and tag selectors and the incremental-read options are runtime-only and are not
table properties; use the catalog session properties or a table function
instead. Paimon's scan fallback branch options
(`scan.fallback-snapshot-branch`, `scan.fallback-delta-branch`,
`scan.fallback-branch`, `scan.fallback-branch-read-fail-fast`, and
`scan.primary-branch`) are table properties, because Paimon resolves the read
branch from the table option.

In addition to Paimon options, the connector provides these structural table
properties:

:::{list-table} Paimon structural table properties
:widths: 30, 20, 50
:header-rows: 1

* - Property name
  - Type
  - Description
* - `primary_key`
  - `array(varchar)`
  - Columns that form the primary key. Omit the property for append-only
    tables. This property is set when the table is created.
* - `partitioned_by`
  - `array(varchar)`
  - Columns used to partition the table. This property is set when the table
    is created.
:::

For example:

```sql
CREATE TABLE paimon.sales.orders (
    order_id BIGINT,
    order_date DATE,
    customer_id BIGINT,
    total_amount DECIMAL(12, 2)
)
WITH (
    primary_key = ARRAY['order_id', 'order_date'],
    partitioned_by = ARRAY['order_date'],
    bucket = '4',
    file_format = 'PARQUET',
    merge_engine = 'DEDUPLICATE'
);
```

Consult the Paimon 1.5 documentation for the valid values and semantics of
forwarded Paimon options such as `bucket`, `file_format`, `merge_engine`,
`changelog_producer`, and snapshot-retention settings.

## Type mapping

The connector maps Paimon logical types to Trino types as follows. The file
format restrictions described below still apply when a table is read or written
through a Trino format provider.

:::{list-table} Paimon to Trino type mapping
:widths: 45, 55
:header-rows: 1

* - Paimon type
  - Trino type
* - `BOOLEAN`, `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE`
  - Corresponding Trino scalar type (`INT` maps to `INTEGER` and `FLOAT` to
    `REAL`).
* - `DECIMAL(p, s)`, `CHAR(n)`, `VARCHAR(n)`, `STRING`
  - Corresponding `DECIMAL`, `CHAR`, or `VARCHAR` type.
* - `BINARY`, `VARBINARY`, `BLOB`
  - `VARBINARY`
* - `DATE`, `TIME(p)`, `TIMESTAMP(p)`
  - Corresponding `DATE`, `TIME(min(p, 3))`, or `TIMESTAMP(p)` type.
* - `TIMESTAMP WITH LOCAL TIME ZONE(p)`
  - `TIMESTAMP(p) WITH TIME ZONE`
* - `ARRAY(T)`, `MAP(K, V)`, `ROW(...)`
  - Corresponding Trino collection or row type.
* - `VARIANT`
  - `JSON`
* - `VECTOR(T)`, `MULTISET(T)`
  - `ARRAY(T)` and `MAP(T, INTEGER)`, respectively.
:::

When creating tables, Trino maps `INTEGER` to Paimon `INT`, `REAL` to
`FLOAT`, unbounded `VARCHAR` to `STRING`, `VARBINARY` to `VARBINARY`, and
`TIMESTAMP WITH TIME ZONE` to `TIMESTAMP WITH LOCAL TIME ZONE`. Trino `JSON`
creates Paimon `VARIANT`. Paimon stores time values with millisecond precision,
so writes of `TIME(p)` require `p` to be at most `3`.

## SQL support

The connector supports the generally available Trino read statements and common
schema, table, and view management operations. It supports `INSERT`, `DELETE`,
`UPDATE`, `MERGE`, `TRUNCATE TABLE`, and `CREATE TABLE AS SELECT` where the
underlying Paimon table mode supports the operation.

Paimon system tables, such as `$snapshots`, `$tags`, and `$manifests`, are
available with Paimon's quoted table-name syntax. For example:

```sql
SELECT * FROM paimon.sales."orders$snapshots";
```

### Time travel

Set one of the catalog session properties before reading a table:

```sql
SET SESSION paimon.scan_snapshot_id = 42;
SELECT * FROM paimon.sales.orders;
```

The `scan_timestamp_millis`, `scan_tag_name`, `scan_file_creation_time_millis`,
and `scan_creation_time_millis` session properties expose the corresponding
Paimon scan options. Do not combine conflicting version selectors in one query.

`FOR VERSION AS OF` on a table reads the same Paimon scan version selector:

```sql
SELECT * FROM paimon.sales.orders FOR VERSION AS OF 'tag-daily';
SELECT * FROM paimon.sales.orders FOR VERSION AS OF 42;
```

String, character, and integer version literals are supported; other types fail
with a `NOT_SUPPORTED` error. The table version takes precedence over a session
selector set in the same query.

### Write behavior

`insert_existing_partitions_behavior` controls writes to existing partitions:

```sql
SET SESSION paimon.insert_existing_partitions_behavior = 'APPEND';
```

Supported values are `ERROR`, `APPEND`, and `OVERWRITE`. `KEY_DYNAMIC` tables
require an atomic-capable Paimon catalog lock as described in the JDBC example.

## File formats and type limitations

The connector reads and writes Paimon Parquet and ORC tables through Trino's
format providers. Paimon `BLOB`, `VARIANT`, `VECTOR`, and `MULTISET` values are
not supported by these providers. ORC writes with Paimon `TIME` columns are
rejected; use Parquet or Paimon's native writer for such tables.

## Statistics and performance

The connector maps Paimon snapshot statistics to Trino table and column
statistics. If a historical Paimon snapshot does not contain a statistics file,
or contains no usable table row count, the connector derives a row count from
the planned Paimon splits only when that count is exact. Primary-key tables
without merged split row counts remain unknown rather than receiving an unsafe
estimate.

`SHOW STATS` and `EXPLAIN` are useful to verify the cardinalities visible to the
optimizer. Paimon column NDV statistics are required for join-output estimates;
without them Trino intentionally keeps the join output unknown while retaining
the exact input cardinalities.

## Monitoring

The connector publishes runtime counters to JMX under the object name
`trino.plugin.paimon:type=PaimonConnectorStats,name=<catalog-name>`:

- `SplitCount`, `SplitWeight`, `SplitRowCount`: splits planned for reads.
- `WriteCommitCount`, `WriteCommitBytes`, `WriteCommitFailures`,
  `WriteCommitTimeNanos`, `WriteBytesPerCommit`: Paimon write commits.
- `CatalogCacheHits`, `CatalogCacheMisses`: session catalog cache lookups.

## Known limitations

- The connector always reaches the Paimon warehouse through Trino FileIO, so
  Paimon's `resolving-file-io.enabled` option has no effect. A configured
  `resolving-file-io.enabled=true` is logged and then forced to `false`.
- Paimon format tables are read-only in Trino. Paimon exposes a format table
  writer, but the connector builds its writers from a `FileStoreTable`, so
  `INSERT` into a format table fails with a `NOT_SUPPORTED` error. Set
  `format-table.enabled` to make format tables visible for reads.
- The session catalog cache retains one Paimon catalog per session identity and
  evicts the least recently used entry once
  `catalog.session-cache.maximum-size` is exceeded, closing that entry once it is
  idle. A statement that is still using an evicted catalog fails with a
  closed-catalog error, so keep the default cache size when many identities or
  credentials run in parallel with a small configured cache.
- Row-level `UPDATE`, `DELETE`, and `MERGE` do not detect a concurrent modification
  of the rows they read. The statement scans the target rows and then commits its
  result against the current table state, so a row changed after the scan can be
  overwritten or removed. Do not run row-level mutations concurrently with writes
  to the same rows.
- The metadata-delete fallback used by `DELETE` on bucket-unaware and fixed-bucket
  tables verifies only the visible row count of the affected scope before
  truncating it. A concurrent delete and insert that leaves the row count of that
  scope unchanged is not detected.
- `system.table_changes` returns table columns only, without the change kind of each
  row, so a deleted row or the before-value of an updated row is indistinguishable
  from an inserted row. Use `$audit_log` or `$binlog` on a table with
  `changelog_producer` enabled to read change kinds.
- The Trino-managed ORC writer (`file_format` `ORC` with the Trino no-Hadoop
  format provider) rejects pre-epoch timestamps that carry a fractional second,
  for example `1969-12-31 23:59:59.999`. Trino's ORC writer stores the seconds of
  such values truncated towards zero, so the file holds a value one second away
  from the value Trino wrote. Write those values to `PARQUET` tables or let
  Paimon's native writer store them instead.
- Column and field names are matched case-insensitively. A table with two
  columns that differ only by case cannot be used as a write target, and schema
  changes for such a column are rejected as ambiguous.
- `ALTER SCHEMA RENAME` is not supported. Paimon's catalog API does not expose a
  schema rename primitive.
- `COMMENT ON COLUMN view.col` is not supported. Paimon `ViewChange` does not
  expose view column comment mutations.
- Query retries are not supported.
- Partial partition `DELETE` is not supported. Only complete partition deletes
  are optimized through Paimon `truncatePartitions`.
- Materialized views are not supported.
- Role-based access control (`GRANT`/`REVOKE`) is not implemented.
- Views are only supported on JDBC and Hive catalogs, not on the filesystem
  catalog.
- The connector does not support compatibility with `trinodb/trino:master`.
  Porting to Trino master is tracked separately.
