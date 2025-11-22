# Paimon connector

```{raw} html
<img src="../_static/img/paimon.png" class="connector-logo">
```

The Paimon connector allows querying data stored in [Apache Paimon](https://paimon.apache.org/)
tables. Apache Paimon is a streaming data lake platform that supports high-speed
data ingestion, changelog tracking, and efficient data organization. It unifies
streaming and batch processing with features like:

- **Schema Evolution**: Add, delete, and modify columns without rewriting data
- **Time Travel**: Query historical versions of tables using timestamps, snapshot IDs, or tags
- **Merge Engines**: Support for DEDUPLICATE, PARTIAL_UPDATE, and AGGREGATE operations
- **File Formats**: Native support for ORC, Parquet, and AVRO

## Requirements

To use the Paimon connector, you need:

- Apache Paimon version 1.3 or higher
- Network access to the Paimon warehouse location
- A Hive Metastore service (HMS) or compatible metastore for metadata management
- Access to object storage (S3, Azure, GCS) or HDFS where Paimon data is stored

## General configuration

To configure the Paimon connector, create a catalog properties file in
`etc/catalog` named, for example, `paimon.properties`, to mount the Paimon
connector as the `paimon` catalog. Create the file with the following contents,
replacing the connection properties as appropriate for your setup:

```properties
connector.name=paimon
warehouse=s3://my-bucket/warehouse
hive.metastore.uri=thrift://example.net:9083
```

### Configuration properties

The following table describes general catalog configuration properties for the
connector:

:::{list-table} Paimon configuration properties
:widths: 35, 50, 15
:header-rows: 1

* - Property name
  - Description
  - Default
* - `connector.name`
  - Must be set to `paimon` to use the Paimon connector.
  -
* - `warehouse`
  - The warehouse location URI. This is the root directory where Paimon stores
    all database and table data. Supports S3 (`s3://`), Azure (`abfs://`),
    GCS (`gs://`), and HDFS (`hdfs://`) paths.
  -
* - `hive.metastore.uri`
  - The URI of the Hive Metastore service. Use `thrift://` protocol for HMS.
  -
* - `fs.native-s3.enabled`
  - Enable Trino's native S3 file system. Required for MinIO and S3-compatible
    storage.
  - `false`
* - `fs.hadoop.enabled`
  - Enable Hadoop file system support. Set to `false` when using S3-only
    configuration.
  - `true`
* - `s3.endpoint`
  - S3 endpoint URL. For AWS S3, use regional endpoints like
    `https://s3.us-east-1.amazonaws.com`. For MinIO, use your MinIO server
    URL (e.g., `http://192.168.1.100:9000`).
  -
* - `s3.path-style-access`
  - Use path-style access for S3 URLs. Set to `true` for MinIO and some
    S3-compatible storage systems.
  - `false`
* - `s3.access-key`
  - AWS access key ID or MinIO access key for authentication.
  -
* - `s3.secret-key`
  - AWS secret access key or MinIO secret key for authentication.
  -
* - `s3.region`
  - AWS region name (e.g., `us-east-1`). Required for AWS S3. For MinIO, can
    be set to any valid region name.
  -
:::

(paimon-file-system-configuration)=
## File system access configuration

Paimon supports reading and writing data stored in various file systems and object
stores. The connector uses the underlying file system implementations and requires
appropriate configuration:

- [](/object-storage/file-system-azure)
- [](/object-storage/file-system-gcs)
- [](/object-storage/file-system-s3)

### S3 configuration example

For AWS S3 storage, add the following properties to your catalog
configuration file:

```properties
connector.name=paimon
warehouse=s3://my-bucket/warehouse
hive.metastore.uri=thrift://example.net:9083

# AWS S3 configuration
s3.endpoint=https://s3.amazonaws.com
s3.region=us-east-1
s3.path-style-access=false
```

You can also configure S3 credentials using environment variables or IAM roles.

### MinIO configuration example

For MinIO or other S3-compatible storage, use the following configuration:

```properties
connector.name=paimon
warehouse=s3://bucket-name/warehouse/paimon/schema-name
hive.metastore.uri=thrift://example.net:9083

# MinIO S3-compatible storage configuration
fs.native-s3.enabled=true
s3.endpoint=http://192.168.240.77:9005
s3.path-style-access=true
s3.access-key=admin
s3.secret-key=your-secret-key
s3.region=us-east-1

# Disable HDFS when using S3
fs.hadoop.enabled=false
```

:::{note}
For MinIO:
- Set `fs.native-s3.enabled=true` to use Trino's native S3 file system
- Set `s3.path-style-access=true` for path-style URLs (required for MinIO)
- Use `http://` or `https://` protocol in the endpoint
- Set `fs.hadoop.enabled=false` to disable HDFS when using S3
:::

## Type mapping

### Paimon to Trino type mapping

The connector maps Paimon types to the corresponding Trino types according to
the following table:

:::{list-table} Paimon to Trino type mapping
:widths: 40, 60
:header-rows: 1

* - Paimon type
  - Trino type
* - `BOOLEAN`
  - `BOOLEAN`
* - `TINYINT`
  - `TINYINT`
* - `SMALLINT`
  - `SMALLINT`
* - `INT`
  - `INTEGER`
* - `BIGINT`
  - `BIGINT`
* - `FLOAT`
  - `REAL`
* - `DOUBLE`
  - `DOUBLE`
* - `DECIMAL(p,s)`
  - `DECIMAL(p,s)`
* - `CHAR(n)`
  - `CHAR(n)`
* - `VARCHAR(n)`
  - `VARCHAR(n)`
* - `STRING`
  - `VARCHAR`
* - `BINARY(n)`
  - `VARBINARY`
* - `VARBINARY`
  - `VARBINARY`
* - `BYTES`
  - `VARBINARY`
* - `DATE`
  - `DATE`
* - `TIME`
  - `TIME(3)` - Fixed millisecond precision
* - `TIMESTAMP(p)`
  - `TIMESTAMP(p)` - Precision 0 to 12
* - `TIMESTAMP WITH LOCAL TIME ZONE`
  - `TIMESTAMP(6) WITH TIME ZONE`
* - `ARRAY<T>`
  - `ARRAY(T)`
* - `MAP<K,V>`
  - `MAP(K,V)`
* - `ROW(...)`
  - `ROW(...)`
:::

:::{note}
The TIME type in Paimon is always mapped to `TIME(3)` in Trino with millisecond
precision, regardless of the precision specified in Paimon.
:::

### Trino to Paimon type mapping

When creating tables or inserting data from Trino to Paimon, Trino types are
mapped to Paimon types as follows:

:::{list-table} Trino to Paimon type mapping
:widths: 40, 60
:header-rows: 1

* - Trino type
  - Paimon type
* - `BOOLEAN`
  - `BOOLEAN`
* - `TINYINT`
  - `TINYINT`
* - `SMALLINT`
  - `SMALLINT`
* - `INTEGER`
  - `INT`
* - `BIGINT`
  - `BIGINT`
* - `REAL`
  - `FLOAT`
* - `DOUBLE`
  - `DOUBLE`
* - `DECIMAL(p,s)`
  - `DECIMAL(p,s)`
* - `CHAR(n)`
  - `CHAR(n)` - Maximum length 255
* - `VARCHAR(n)` or `VARCHAR`
  - `VARCHAR(n)` or `STRING`
* - `VARBINARY`
  - `VARBINARY` or `BYTES`
* - `DATE`
  - `DATE`
* - `TIME` or `TIME(p)`
  - `TIME`
* - `TIMESTAMP(p)`
  - `TIMESTAMP(p)` - Precision preserved (0-12)
* - `TIMESTAMP(p) WITH TIME ZONE`
  - `TIMESTAMP WITH LOCAL TIME ZONE`
* - `ARRAY(T)`
  - `ARRAY<T>`
* - `MAP(K,V)`
  - `MAP<K,V>`
* - `ROW(...)`
  - `ROW(...)`
:::

(paimon-sql-support)=
## SQL support

The connector provides read and write access to data and metadata in Paimon
tables. In addition to the {ref}`globally available <sql-globally-available>`
and {ref}`read operation <sql-read-operations>` statements, the connector
supports the following features:

- {doc}`/sql/insert`
- {doc}`/sql/delete`
- {doc}`/sql/merge`
- {doc}`/sql/create-table`
- {doc}`/sql/create-table-as`
- {doc}`/sql/drop-table`
- {doc}`/sql/alter-table`
- {doc}`/sql/create-schema`
- {doc}`/sql/drop-schema`

### Basic usage examples

Create a schema in the Paimon catalog:

```sql
CREATE SCHEMA paimon.example_schema;
```

Create a table with partitioning and bucketing:

```sql
CREATE TABLE paimon.example_schema.orders (
  order_id BIGINT,
  customer_id BIGINT,
  order_date DATE,
  total_amount DECIMAL(10,2),
  status VARCHAR
)
WITH (
  primary_key = ARRAY['order_id'],
  partitioned_by = ARRAY['order_date'],
  bucket = '4',
  file_format = 'ORC'
);
```

Insert data into the table:

```sql
INSERT INTO paimon.example_schema.orders VALUES
  (1, 100, DATE '2024-01-01', 99.99, 'completed'),
  (2, 101, DATE '2024-01-02', 149.50, 'pending'),
  (3, 102, DATE '2024-01-03', 75.25, 'completed');
```

Query the table:

```sql
SELECT customer_id, SUM(total_amount) as total
FROM paimon.example_schema.orders
WHERE order_date >= DATE '2024-01-01'
GROUP BY customer_id;
```

### Time travel queries

Paimon supports querying historical versions of tables using time travel syntax.

Query a table as of a specific timestamp:

```sql
SELECT * FROM orders
FOR VERSION AS OF TIMESTAMP '2024-01-01 10:00:00 UTC';
```

Query a table as of a specific snapshot ID:

```sql
SELECT * FROM orders FOR VERSION AS OF 123;
```

Query a table using a named tag:

```sql
SELECT * FROM orders FOR VERSION AS OF 'snapshot-tag-1';
```

:::{note}
Time travel queries are read-only. You cannot insert, update, or delete data
when querying historical snapshots.
:::

### Procedures

The connector provides procedures for managing Paimon tables and snapshots.

#### Create a tag

Create a named tag for a specific table snapshot:

```sql
CALL paimon.system.create_tag(
  schema_name => 'example_schema',
  table_name => 'orders',
  tag_name => 'monthly_snapshot',
  snapshot_id => 123
);
```

### Table functions

#### table_changes

The `table_changes` function reads incremental changes between two snapshots,
enabling Change Data Capture (CDC) scenarios:

```sql
SELECT *
FROM TABLE(paimon.system.table_changes(
  schema_name => 'example_schema',
  table_name => 'orders',
  incremental_between => '1,10'
));
```

This returns all changes (inserts, updates, deletes) between snapshot 1 and
snapshot 10, including the change type in the `_row_kind` column.

(paimon-schema-table-management)=
## Schema and table management

### Schema operations

Create a new schema:

```sql
CREATE SCHEMA paimon.example_schema;
```

Drop an empty schema:

```sql
DROP SCHEMA paimon.example_schema;
```

Drop a schema and all its tables:

```sql
DROP SCHEMA paimon.example_schema CASCADE;
```

List all schemas:

```sql
SHOW SCHEMAS FROM paimon;
```

:::{note}
The connector does not support `RENAME SCHEMA`.
:::

### Table operations

#### CREATE TABLE

Create a basic table:

```sql
CREATE TABLE paimon.example_schema.users (
  user_id BIGINT,
  username VARCHAR,
  created_at TIMESTAMP(6)
);
```

Create a partitioned table with bucketing:

```sql
CREATE TABLE paimon.example_schema.events (
  event_id BIGINT,
  event_type VARCHAR,
  event_time TIMESTAMP(6),
  event_date DATE
)
WITH (
  primary_key = ARRAY['event_id'],
  partitioned_by = ARRAY['event_date'],
  bucket = '8',
  file_format = 'PARQUET'
);
```

#### CREATE TABLE AS SELECT

Create a table from a query result:

```sql
CREATE TABLE paimon.example_schema.daily_orders
WITH (
  primary_key = ARRAY['order_date'],
  bucket = '4'
)
AS
SELECT order_date, COUNT(*) as order_count, SUM(total_amount) as daily_total
FROM paimon.example_schema.orders
GROUP BY order_date;
```

#### DROP TABLE

Drop a table:

```sql
DROP TABLE paimon.example_schema.users;
```

#### RENAME TABLE

Rename a table within the same schema:

```sql
ALTER TABLE paimon.example_schema.users RENAME TO customers;
```

:::{note}
Renaming tables across different schemas is not supported.
:::

#### TRUNCATE TABLE

Delete all rows from a table while preserving the table structure:

```sql
TRUNCATE TABLE paimon.example_schema.staging_data;
```

### Column operations

#### ADD COLUMN

Add a new column to a table:

```sql
ALTER TABLE paimon.example_schema.users
ADD COLUMN email VARCHAR;
```

Add a column with a comment:

```sql
ALTER TABLE paimon.example_schema.users
ADD COLUMN phone_number VARCHAR
COMMENT 'User primary phone number';
```

#### DROP COLUMN

Remove a column from a table:

```sql
ALTER TABLE paimon.example_schema.users
DROP COLUMN phone_number;
```

#### RENAME COLUMN

Rename an existing column:

```sql
ALTER TABLE paimon.example_schema.users
RENAME COLUMN username TO user_name;
```

#### ALTER COLUMN TYPE

Change the data type of a column:

```sql
ALTER TABLE paimon.example_schema.users
ALTER COLUMN user_id SET DATA TYPE VARCHAR;
```

:::{warning}
Type changes must be compatible. For example, you cannot change a VARCHAR to an
INTEGER if the column contains non-numeric values.
:::

#### COMMENT ON COLUMN

Add or update a column comment:

```sql
COMMENT ON COLUMN paimon.example_schema.users.email
IS 'User email address for notifications';
```

### Nested field operations

Paimon supports schema evolution for nested fields in `ROW` types.

#### ADD FIELD

Add a new field to a nested structure:

```sql
ALTER TABLE paimon.example_schema.users
ADD FIELD address.zip_code VARCHAR;
```

#### DROP FIELD

Remove a field from a nested structure:

```sql
ALTER TABLE paimon.example_schema.users
DROP FIELD address.apartment_number;
```

#### RENAME FIELD

Rename a nested field:

```sql
ALTER TABLE paimon.example_schema.users
RENAME FIELD address.street TO address.street_name;
```

#### SET FIELD TYPE

Change the type of a nested field:

```sql
ALTER TABLE paimon.example_schema.users
SET DATA TYPE address.zip_code = INTEGER;
```

(paimon-table-properties)=
### Table properties

Table properties are specified using the `WITH` clause in `CREATE TABLE` or
`ALTER TABLE SET PROPERTIES` statements.

:::{list-table} Paimon table properties
:widths: 30, 15, 55
:header-rows: 1

* - Property name
  - Type
  - Description
* - `primary_key`
  - `array(varchar)`
  - List of columns that form the primary key. Empty array for append-only tables.
* - `partitioned_by`
  - `array(varchar)`
  - List of columns to partition the table by.
* - `bucket`
  - `varchar`
  - Number of buckets for hash distribution. Required for HASH_FIXED bucket mode.
    Use `-1` for BUCKET_UNAWARE mode (append-only).
* - `file_format`
  - `varchar`
  - File format for data storage. Options: `ORC` (default), `PARQUET`, `AVRO`.
* - `merge_engine`
  - `varchar`
  - Merge engine type. Options: `DEDUPLICATE` (default), `PARTIAL_UPDATE`,
    `AGGREGATE`.
* - `changelog_producer`
  - `varchar`
  - Changelog producer type. Options: `NONE` (default), `INPUT`,
    `FULL_COMPACTION`.
* - `snapshot_time_retained`
  - `varchar`
  - Duration to retain snapshots. Example: `1h`, `7d`, `2w`.
* - `snapshot_num_retained`
  - `varchar`
  - Minimum number of snapshots to retain. Snapshots within this count are not
    expired.
* - `compaction_min_file_num`
  - `varchar`
  - Minimum number of files to trigger automatic compaction.
* - `compaction_max_file_num`
  - `varchar`
  - Maximum number of files to include in a single compaction.
:::

:::{note}
All Apache Paimon `CoreOptions` configuration properties are automatically
available as table properties. Property names are converted by replacing `.` and
`-` with `_`. For example, `snapshot.time-retained` becomes
`snapshot_time_retained`.
:::

Example of setting table properties:

```sql
CREATE TABLE paimon.example_schema.metrics (
  metric_name VARCHAR,
  metric_value DOUBLE,
  recorded_at TIMESTAMP(6)
)
WITH (
  primary_key = ARRAY['metric_name', 'recorded_at'],
  bucket = '4',
  file_format = 'PARQUET',
  merge_engine = 'DEDUPLICATE',
  snapshot_time_retained = '7d',
  snapshot_num_retained = '10'
);
```

### Metadata tables

Paimon provides system tables for accessing metadata about tables, snapshots,
files, and partitions. These tables are accessed using the `$` prefix.

#### Snapshots table

View snapshot history:

```sql
SELECT * FROM "paimon.example_schema.orders$snapshots"
ORDER BY snapshot_id DESC
LIMIT 10;
```

The snapshots table includes columns such as `snapshot_id`, `schema_id`,
`commit_user`, `commit_identifier`, `commit_kind`, and `commit_time`.

#### Files table

List all data files in a table:

```sql
SELECT file_path, file_size_in_bytes, record_count
FROM "paimon.example_schema.orders$files";
```

#### Partitions table

View partition information:

```sql
SELECT * FROM "paimon.example_schema.orders$partitions";
```

### Schema evolution

Paimon supports schema evolution, allowing you to modify table structures without
rewriting existing data files.

Supported operations:
- Adding new columns (new data files include the column, old files return NULL)
- Dropping columns (column data is ignored when reading old files)
- Renaming columns (column mapping is updated in metadata)
- Changing column types (must be compatible conversions)
- Adding/modifying nested fields in ROW types

Example workflow:

```sql
-- Initial table
CREATE TABLE paimon.example_schema.products (
  product_id BIGINT,
  product_name VARCHAR
)
WITH (primary_key = ARRAY['product_id'], bucket = '4');

-- Add a new column
ALTER TABLE paimon.example_schema.products
ADD COLUMN category VARCHAR;

-- Queries automatically handle schema differences
-- Old data files return NULL for the category column
SELECT * FROM paimon.example_schema.products;
```

(paimon-data-management)=
## Data management

### INSERT

Insert data into a table using the `INSERT` statement:

```sql
INSERT INTO paimon.example_schema.orders VALUES
  (1, 100, DATE '2024-01-01', 99.99, 'completed');
```

Insert data from a query:

```sql
INSERT INTO paimon.example_schema.orders
SELECT order_id, customer_id, order_date, total_amount, status
FROM staging.raw_orders
WHERE order_date >= CURRENT_DATE - INTERVAL '7' DAY;
```

### INSERT OVERWRITE

To overwrite existing data in partitions, configure the session property:

```sql
SET SESSION paimon.insert_existing_partitions_behavior = 'OVERWRITE';

INSERT INTO paimon.example_schema.orders VALUES
  (1, 100, DATE '2024-01-01', 99.99, 'completed');
```

Available behaviors:
- `APPEND` (default): Append new rows to existing partitions
- `OVERWRITE`: Replace all data in affected partitions
- `ERROR`: Fail if inserting into existing partitions

### DELETE

Delete rows from a table:

```sql
DELETE FROM paimon.example_schema.orders
WHERE status = 'cancelled';
```

Delete with a complex condition:

```sql
DELETE FROM paimon.example_schema.orders
WHERE order_date < CURRENT_DATE - INTERVAL '365' DAY
  AND status IN ('completed', 'cancelled');
```

:::{warning}
DELETE is only supported for tables using HASH_FIXED bucket mode. Tables using
BUCKET_UNAWARE mode do not support DELETE operations.
:::

### UPDATE

The connector does not support the `UPDATE` statement. Instead, use the `MERGE`
statement to update existing rows.

### MERGE

The `MERGE` statement allows you to perform insert, update, and delete operations
in a single statement:

```sql
MERGE INTO paimon.example_schema.orders AS target
USING staging.order_updates AS source
ON target.order_id = source.order_id
WHEN MATCHED AND source.status = 'deleted' THEN DELETE
WHEN MATCHED THEN UPDATE SET
  total_amount = source.total_amount,
  status = source.status
WHEN NOT MATCHED THEN INSERT VALUES (
  source.order_id,
  source.customer_id,
  source.order_date,
  source.total_amount,
  source.status
);
```

:::{warning}
MERGE is only supported for tables using HASH_FIXED bucket mode. Ensure your
table has the `bucket` property set to a positive integer when creating the table.
:::

## Session properties

The connector provides session properties to control query behavior and
performance:

:::{list-table} Paimon session properties
:widths: 40, 20, 15, 25
:header-rows: 1

* - Property name
  - Type
  - Default
  - Description
* - `scan_timestamp_millis`
  - `bigint`
  - `null`
  - Timestamp in milliseconds for time travel queries.
* - `scan_snapshot_id`
  - `bigint`
  - `null`
  - Snapshot ID for time travel queries.
* - `minimum_split_weight`
  - `double`
  - `0.05`
  - Minimum weight for table splits. Range: 0.0 to 1.0. Higher values create
    fewer, larger splits.
* - `insert_existing_partitions_behavior`
  - `varchar`
  - `'APPEND'`
  - Behavior when inserting into existing partitions. Options: `APPEND`,
    `OVERWRITE`, `ERROR`.
* - `dynamic_filtering_wait_timeout`
  - `varchar`
  - `'0s'`
  - Maximum duration to wait for dynamic filters in join queries. Accepts
    duration values like `10s`, `1m`.
:::

Example session configuration:

```sql
-- Enable time travel to a specific timestamp
SET SESSION paimon.scan_timestamp_millis = 1704067200000;

-- Configure split generation
SET SESSION paimon.minimum_split_weight = 0.1;

-- Enable partition overwrite
SET SESSION paimon.insert_existing_partitions_behavior = 'OVERWRITE';

-- Enable dynamic filtering with timeout
SET SESSION paimon.dynamic_filtering_wait_timeout = '10s';
```

(paimon-performance)=
## Performance

### Query optimization features

The connector supports several query optimization techniques to improve
performance:

:::{list-table} Supported optimization features
:widths: 40, 15, 45
:header-rows: 1

* - Feature
  - Supported
  - Description
* - Predicate pushdown
  - Yes
  - Filter predicates are pushed down to the Paimon storage layer to reduce
    data scanning.
* - VARCHAR equality pushdown
  - Yes
  - Equality predicates on VARCHAR columns are pushed down to storage.
* - VARCHAR inequality pushdown
  - Yes
  - Range predicates (>, <, >=, <=) on VARCHAR columns are pushed down.
* - Projection pushdown
  - Yes
  - Only required columns are read from storage (column pruning).
* - Limit pushdown
  - Yes
  - LIMIT clause is pushed down to reduce rows read.
* - Top-N pushdown
  - Yes
  - ORDER BY ... LIMIT queries are optimized with Top-N pushdown.
* - Dynamic filtering
  - Yes
  - Join conditions can dynamically filter fact tables during split generation.
* - Aggregation pushdown
  - No
  - Aggregations are computed in Trino.
* - Join pushdown
  - No
  - Joins are computed in Trino.
:::

### Performance tuning

#### Split weight configuration

The `minimum_split_weight` session property controls the granularity of data
splits. Adjust this value based on your workload:

```sql
-- For small files or interactive queries, use smaller splits
SET SESSION paimon.minimum_split_weight = 0.01;

-- For large scans or batch processing, use larger splits
SET SESSION paimon.minimum_split_weight = 0.2;
```

#### Dynamic filtering

Enable dynamic filtering to reduce data scanning in join queries:

```sql
SET SESSION paimon.dynamic_filtering_wait_timeout = '30s';

SELECT f.*, d.category
FROM fact_table f
JOIN dimension_table d ON f.dim_id = d.id
WHERE d.category = 'Electronics';
```

In this example, the filter on `dimension_table` is applied to `fact_table`
during split generation, reducing the amount of data scanned.

#### File format selection

Choose the appropriate file format based on your workload:

- **ORC** (default): Best compression ratio and query performance for most workloads
- **Parquet**: Better compatibility with other systems, good for complex nested data
- **AVRO**: Best for schema evolution scenarios with frequent schema changes

```sql
CREATE TABLE paimon.example_schema.analytics (
  ...
)
WITH (
  file_format = 'PARQUET'
);
```

#### Partitioning strategies

Effective partitioning improves query performance through partition pruning:

```sql
-- Partition by date for time-based queries
CREATE TABLE paimon.example_schema.events (
  event_id BIGINT,
  event_data VARCHAR,
  event_timestamp TIMESTAMP(6),
  event_date DATE
)
WITH (
  partitioned_by = ARRAY['event_date'],
  bucket = '8'
);

-- Queries with date filters benefit from partition pruning
SELECT * FROM events
WHERE event_date = DATE '2024-01-01';
```

#### Bucketing configuration

For tables with HASH_FIXED bucket mode, choose an appropriate bucket count:

- **Too few buckets**: May cause data skew and hot spots
- **Too many buckets**: Increases small file count and overhead

General guidelines:
- Start with 1-2 buckets per CPU core in your cluster
- Adjust based on data volume and query patterns
- Monitor file sizes and query performance

```sql
CREATE TABLE paimon.example_schema.large_table (
  ...
)
WITH (
  primary_key = ARRAY['id'],
  bucket = '16'  -- Adjust based on cluster size and data volume
);
```

## Limitations

The connector has the following limitations:

### DDL limitations

- `RENAME SCHEMA` is not supported
- `RENAME TABLE` across different schemas is not supported
- `CREATE OR REPLACE TABLE` is not supported; use `DROP TABLE` then `CREATE TABLE`
- Column default values are not supported
- Materialized views are not supported

### DML limitations

- `UPDATE` statement is not supported; use `MERGE` instead
- `DELETE` and `MERGE` are only supported for tables using HASH_FIXED bucket mode
- Tables using BUCKET_UNAWARE mode (append-only) only support INSERT
- DYNAMIC bucket mode is not currently supported
- Multi-statement transactions are not supported

### Query optimization limitations

- Expression pushdown is not supported (e.g., `WHERE LOWER(name) = 'alice'`)
- Aggregation pushdown is not supported
- Join pushdown is not supported
- Dereference pushdown for nested field access is not supported

### Other limitations

- Table statistics collection via `ANALYZE` is not supported
- The connector does not support native queries
- Long table names or schema names may be limited by the underlying file system
- Concurrent schema changes may result in errors that are not TrinoException types
