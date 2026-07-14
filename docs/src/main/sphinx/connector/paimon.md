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
