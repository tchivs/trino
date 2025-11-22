# Paimon Connector Documentation

## Documentation Status

✅ **Documentation Created** (2025-11-22)

The comprehensive Paimon connector documentation has been created at:
- **Location:** `/opt/source/trino/docs/src/main/sphinx/connector/paimon.md`
- **Lines:** 992
- **Index Updated:** `/opt/source/trino/docs/src/main/sphinx/connector.md`

### Documentation Statistics

- **Headings:** 62
- **SQL Code Examples:** 46
- **Configuration Examples:** 2
- **Tables:** 6 (configuration properties, type mappings, session properties, etc.)
- **Main Sections:** 11

### Contents Overview

1. **Requirements** - Version requirements, HMS, file systems
2. **General configuration** - Catalog properties, warehouse setup
3. **File system access** - S3, HDFS, Azure, GCS configuration
4. **Type mapping** - Bidirectional Paimon ↔ Trino type mapping
5. **SQL support** - DDL, DML, time travel, procedures, table functions
6. **Schema and table management** - CREATE/ALTER/DROP operations, metadata tables
7. **Data management** - INSERT, DELETE, MERGE operations
8. **Session properties** - Time travel, split config, dynamic filtering
9. **Performance** - Query optimization, tuning strategies
10. **Limitations** - Known limitations and restrictions

### Key Features Documented

- ✅ Time Travel (timestamp, snapshot ID, tag-based queries)
- ✅ Schema Evolution (add/drop/rename columns and nested fields)
- ✅ Table Functions (`table_changes` for CDC)
- ✅ Metadata Tables (`$snapshots`, `$files`, `$partitions`)
- ✅ Bucketing Strategies (HASH_FIXED, BUCKET_UNAWARE)
- ✅ File Formats (ORC, Parquet, AVRO)
- ✅ Query Optimization (predicate/limit/projection pushdown)

## Pending: Logo Image

**Required File:** `/opt/source/trino/docs/src/main/sphinx/static/img/paimon.png`

### Logo Requirements

1. **Format:** PNG with transparent background
2. **Dimensions:** Approximately 200x200 pixels
3. **Source:** Apache Paimon official logo
4. **Download:** https://paimon.apache.org/ (check official website or GitHub)

### How to Add the Logo

Once you have the logo file:

```bash
# Copy the logo to the correct location
cp /path/to/paimon-logo.png /opt/source/trino/docs/src/main/sphinx/static/img/paimon.png

# Verify the file
ls -lh /opt/source/trino/docs/src/main/sphinx/static/img/paimon.png

# Add to git
git add docs/src/main/sphinx/static/img/paimon.png

# Commit
git commit -m "Add Paimon connector logo"
```

The documentation already references the logo at the top of the file, so it will
automatically appear once the image file is in place.

## Building the Documentation

### Full Build (requires all Trino modules)

```bash
# From the Trino root directory
./mvnw clean install -pl docs -DskipTests

# Or with specific Maven path
export JAVA_HOME=/root/.jdks/temurin-24.0.2
export PATH=$JAVA_HOME/bin:$PATH
/opt/bigdata/apache-maven-3.9.11/bin/mvn clean install -pl docs -DskipTests
```

**Note:** Full documentation build requires other Trino modules to be built first.
If you encounter dependency errors, build the full project first:

```bash
./mvnw clean install -DskipTests
```

### Quick Validation (format check only)

```bash
cd /opt/source/trino/docs/src/main/sphinx/connector

# Check basic statistics
wc -l paimon.md
grep -c '^#' paimon.md       # Count headings
grep -c '```sql' paimon.md   # Count SQL blocks
grep -c ':::{list-table}' paimon.md  # Count tables

# Verify code blocks are closed
CODE_BLOCKS=$(grep -c '```' paimon.md)
if [ $((CODE_BLOCKS % 2)) -eq 0 ]; then
  echo "✓ All code blocks properly closed"
else
  echo "✗ Code block mismatch"
fi
```

## Viewing the Documentation

After building, the HTML documentation will be available at:
```
/opt/source/trino/docs/target/html/connector/paimon.html
```

Open it in a web browser to verify rendering and links.

## Integration with Main Codebase

The documentation is ready to be included in the main Trino codebase pull request:

### Commit History

```
1c9a1e7 - Add Paimon connector documentation
75085f1 - Refactor PaimonRow to use TypeConverters utility class
a0acf88 - Fix TIME(3) type support in Paimon connector
1df9a66 - Fix TrinoRowTest TINYINT type usage
...
```

### Files Modified/Created

```
docs/src/main/sphinx/connector/paimon.md         (new, 992 lines)
docs/src/main/sphinx/connector.md                (modified, added index)
plugin/trino-paimon/src/main/java/...           (connector implementation)
```

## Next Steps

1. ✅ Connector implementation completed (397 tests passing)
2. ✅ Documentation created (992 lines, 11 chapters)
3. ⏳ Add Paimon logo (`paimon.png`)
4. ⏳ Optional: Build docs locally to verify
5. ⏳ Create pull request with both connector and documentation

## References

- **Connector Code:** `/opt/source/trino/plugin/trino-paimon/`
- **Documentation:** `/opt/source/trino/docs/src/main/sphinx/connector/paimon.md`
- **Similar Docs:** `iceberg.md`, `delta-lake.md`, `hudi.md` in same directory
- **Apache Paimon:** https://paimon.apache.org/

## Contact

For questions about the documentation or connector implementation, refer to:
- Trino documentation guidelines: `/opt/source/trino/docs/README.md`
- Connector development guide: `/opt/source/trino/CLAUDE.md`

---

**Documentation Created:** 2025-11-22
**Format:** MyST Markdown
**Status:** Complete (pending logo image)
