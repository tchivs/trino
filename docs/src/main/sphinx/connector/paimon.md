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
  - Catalog URI. For a JDBC catalog this is the JDBC connection URL.
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
  - Enable Trino native S3 access for the Paimon warehouse.
  - `false`
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
* - `write.spill-path`
  - Local directory used by spillable Paimon writers.
  -
* - `catalog.session-cache.maximum-size`
  - Maximum number of session-specific Paimon catalog instances retained by the
    connector.
  - `1000`
:::

## Catalog session properties

:::{list-table} Paimon catalog session properties
:widths: 32, 53, 15
:header-rows: 1

* - Property name
  - Description
  - Default
* - `scan_timestamp_millis`
  - Read the latest snapshot committed at or before this Unix timestamp in
    milliseconds.
  -
* - `scan_snapshot_id`
  - Read a specific Paimon snapshot.
  -
* - `scan_tag_name`
  - Read the snapshot referenced by a Paimon tag.
  -
* - `scan_file_creation_time_millis`
  - Read the latest snapshot whose data files were created at or before this
    Unix timestamp in milliseconds.
  -
* - `scan_creation_time_millis`
  - Read the latest snapshot created at or before this Unix timestamp in
    milliseconds.
  -
* - `insert_existing_partitions_behavior`
  - Behavior for inserts into an existing partition: `ERROR`, `APPEND`, or
    `OVERWRITE`.
  - `APPEND`
* - `minimum_split_weight`
  - Minimum scheduling weight assigned to a split.
  - `0.05`
* - `dynamic_filtering_wait_timeout`
  - Maximum time split generation waits for dynamic filters.
  - `0s`
:::

## Table properties

Use the `WITH` clause of `CREATE TABLE` or `ALTER TABLE SET PROPERTIES` to
define Paimon option properties. The connector exposes the documented Paimon 1.5
`CoreOptions` as string-valued properties, converting periods and hyphens in a
Paimon option name to underscores. For example, Paimon's `file.format` and
`merge-engine` options are `file_format` and `merge_engine` in Trino. Scan and
other runtime-only Paimon options are not table properties; use the catalog
session properties instead.

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

## Known limitations

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
