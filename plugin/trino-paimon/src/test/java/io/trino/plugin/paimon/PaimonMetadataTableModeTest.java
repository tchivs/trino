/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.paimon;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.PointerType;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.VarbinaryType;
import io.trino.testing.TestingConnectorSession;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FullTextQuery;
import org.apache.paimon.predicate.FullTextSearch;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.ColStats;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FullTextSearchTable;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.VectorSearchTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.system.AuditLogTable;
import org.apache.paimon.table.system.RowTrackingTable;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_COMMIT_ERROR;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_METADATA_ERROR;
import static io.trino.plugin.paimon.PaimonSchemaProperties.COMMENT_PROPERTY;
import static io.trino.plugin.paimon.PaimonSchemaProperties.LOCATION_PROPERTY;
import static io.trino.plugin.paimon.PaimonSchemaProperties.OWNER_PROPERTY;
import static io.trino.plugin.paimon.PaimonSessionProperties.SCAN_SNAPSHOT;
import static io.trino.spi.StandardErrorCode.COLUMN_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.READ_ONLY_VIOLATION;
import static io.trino.spi.StandardErrorCode.SCHEMA_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.expression.Constant.TRUE;
import static io.trino.spi.expression.StandardFunctions.ADD_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.apache.paimon.catalog.Catalog.SYSTEM_DATABASE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PaimonMetadataTableModeTest
{
    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
            .build();

    @Test
    public void testMetadataRejectsNullDependencies()
    {
        assertThatThrownBy(() -> new PaimonMetadata(null, TESTING_TYPE_MANAGER))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("catalog is null");

        assertThatThrownBy(() -> new PaimonMetadata(new TestingPaimonCatalog(table()), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("typeManager is null");
    }

    @Test
    public void testSystemSchemaIsExposed()
    {
        PaimonMetadata metadata = new PaimonMetadata(new SchemaQueryCatalog(), TESTING_TYPE_MANAGER);

        assertThat(metadata.schemaExists(SESSION, SYSTEM_DATABASE_NAME)).isTrue();
        assertThat(metadata.listSchemaNames(SESSION)).containsExactly("alpha", "beta", SYSTEM_DATABASE_NAME);
    }

    @Test
    public void testSystemSchemaListsGlobalSystemTables()
    {
        PaimonMetadata metadata = new PaimonMetadata(new SchemaQueryCatalog(), TESTING_TYPE_MANAGER);

        assertThat(metadata.listTables(SESSION, Optional.of(SYSTEM_DATABASE_NAME)))
                .containsExactlyElementsOf(SystemTableLoader.loadGlobalTableNames().stream()
                        .map(table -> new SchemaTableName(SYSTEM_DATABASE_NAME, table))
                        .toList());
    }

    @Test
    public void testSystemSchemaWritesAreRejected()
    {
        PaimonMetadata metadata = new PaimonMetadata(new CapturingDdlCatalog(), TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName(SYSTEM_DATABASE_NAME, "all_tables"),
                List.of(new ColumnMetadata("id", BIGINT)));
        PaimonTableHandle systemTableHandle = new PaimonTableHandle(SYSTEM_DATABASE_NAME, "all_tables", Map.of());

        assertTrinoError(() -> metadata.createTable(SESSION, tableMetadata, io.trino.spi.connector.SaveMode.FAIL),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon create table is not supported for the system schema 'sys'");
        assertTrinoError(() -> metadata.renameTable(
                        SESSION,
                        systemTableHandle,
                        new SchemaTableName(SYSTEM_DATABASE_NAME, "catalog_options")),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon rename table is not supported for the system schema 'sys'");
        assertTrinoError(() -> metadata.dropTable(SESSION, systemTableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon drop table is not supported for the system schema 'sys'");
    }

    @Test
    public void testSystemSchemaDdlAndAlterOperationsAreRejected()
    {
        PaimonMetadata metadata = new PaimonMetadata(new CapturingDdlCatalog(), TESTING_TYPE_MANAGER);
        PaimonTableHandle systemTableHandle = new PaimonTableHandle(SYSTEM_DATABASE_NAME, "all_tables", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("database_name", DataTypes.STRING());

        assertTrinoError(() -> metadata.createSchema(SESSION, SYSTEM_DATABASE_NAME, Map.of(), null),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon create schema is not supported for the system schema 'sys'");
        assertTrinoError(() -> metadata.dropSchema(SESSION, SYSTEM_DATABASE_NAME, false),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon drop schema is not supported for the system schema 'sys'");
        assertTrinoError(() -> metadata.setSchemaAuthorization(SESSION, SYSTEM_DATABASE_NAME,
                        new TrinoPrincipal(PrincipalType.USER, "schema_owner")),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon set schema authorization is not supported for the system schema 'sys'");
        assertTrinoError(() -> metadata.setTableProperties(SESSION, systemTableHandle, Map.of("bucket", Optional.of("4"))),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon set table properties is not supported for the system schema 'sys'");
        assertTrinoError(() -> metadata.addColumn(SESSION, systemTableHandle, new ColumnMetadata("extra", INTEGER)),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon add column is not supported for the system schema 'sys'");
        assertTrinoError(() -> metadata.renameColumn(SESSION, systemTableHandle, columnHandle, "renamed"),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon rename column is not supported for the system schema 'sys'");
        assertTrinoError(() -> metadata.dropColumn(SESSION, systemTableHandle, columnHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon drop column is not supported for the system schema 'sys'");
        assertTrinoError(() -> metadata.setTableComment(SESSION, systemTableHandle, Optional.of("comment")),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon set table comment is not supported for the system schema 'sys'");
        assertTrinoError(() -> metadata.setColumnComment(SESSION, systemTableHandle, columnHandle, Optional.of("comment")),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon set column comment is not supported for the system schema 'sys'");
        assertTrinoError(() -> metadata.setColumnType(SESSION, systemTableHandle, columnHandle, VARCHAR),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon set column type is not supported for the system schema 'sys'");
        assertTrinoError(() -> metadata.dropNotNullConstraint(SESSION, systemTableHandle, columnHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon drop not null constraint is not supported for the system schema 'sys'");
        assertTrinoError(() -> metadata.addField(SESSION, systemTableHandle, List.of("database_name"), "nested", VARCHAR, false),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon add field is not supported for the system schema 'sys'");
        assertTrinoError(() -> metadata.dropField(SESSION, systemTableHandle, columnHandle, List.of("nested")),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon drop field is not supported for the system schema 'sys'");
        assertTrinoError(() -> metadata.renameField(SESSION, systemTableHandle, List.of("database_name", "nested"), "renamed"),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon rename field is not supported for the system schema 'sys'");
        assertTrinoError(() -> metadata.setFieldType(SESSION, systemTableHandle, List.of("database_name", "nested"), VARCHAR),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon set field type is not supported for the system schema 'sys'");
        assertTrinoError(() -> metadata.truncateTable(SESSION, systemTableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon truncate table is not supported for the system schema 'sys'");
        assertTrinoError(() -> metadata.applyDelete(SESSION, systemTableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon delete is not supported for the system schema 'sys'");
        assertTrinoError(() -> metadata.executeDelete(SESSION, systemTableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon delete is not supported for the system schema 'sys'");
    }

    @Test
    public void testTableStatisticsUsesPaimonSnapshotStats()
    {
        org.apache.paimon.types.RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.BIGINT()),
                DataTypes.FIELD(1, "name", DataTypes.STRING()),
                DataTypes.FIELD(2, "extra", DataTypes.INT()),
                DataTypes.FIELD(3, "event_date", DataTypes.DATE()),
                DataTypes.FIELD(4, "event_time", DataTypes.TIMESTAMP(6)));
        Statistics statistics = new Statistics(7, 3, 100L, 4096L, Map.of(
                "id", ColStats.newColStats(0, 20L, 1L, 99L, 5L, 8L, 8L),
                "missing", ColStats.newColStats(9, 1L, null, null, 0L, 4L, 4L),
                "name", ColStats.newColStats(1, null, BinaryString.fromString("a"), BinaryString.fromString("z"),
                        25L, 12L, 64L),
                "event_date", ColStats.newColStats(3, null, 10, 20, 0L, 4L, 4L),
                "event_time", ColStats.newColStats(4, null, Timestamp.fromMicros(1_000_000L),
                        Timestamp.fromMicros(2_500_000L), 0L, 8L, 8L)));
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(statisticsTable(rowType, Optional.of(statistics))),
                TESTING_TYPE_MANAGER);

        TableStatistics tableStatistics = metadata.getTableStatistics(SESSION,
                new PaimonTableHandle("schema", "table", Map.of()));

        assertThat(tableStatistics.getRowCount().getValue()).isEqualTo(100);
        assertThat(tableStatistics.getColumnStatistics()).hasSize(4);

        ColumnStatistics idStats = tableStatistics.getColumnStatistics()
                .get(PaimonColumnHandle.of("id", DataTypes.BIGINT()));
        assertThat(idStats.getDistinctValuesCount().getValue()).isEqualTo(20);
        assertThat(idStats.getNullsFraction().getValue()).isEqualTo(0.05);
        assertThat(idStats.getDataSize().getValue()).isEqualTo(760);
        assertThat(idStats.getRange()).contains(new DoubleRange(1, 99));

        ColumnStatistics nameStats = tableStatistics.getColumnStatistics()
                .get(PaimonColumnHandle.of("name", DataTypes.STRING()));
        assertThat(nameStats.getDistinctValuesCount().isUnknown()).isTrue();
        assertThat(nameStats.getNullsFraction().getValue()).isEqualTo(0.25);
        assertThat(nameStats.getDataSize().getValue()).isEqualTo(900);
        assertThat(nameStats.getRange()).isEmpty();

        ColumnStatistics dateStats = tableStatistics.getColumnStatistics()
                .get(PaimonColumnHandle.of("event_date", DataTypes.DATE()));
        assertThat(dateStats.getRange()).contains(new DoubleRange(10, 20));

        ColumnStatistics timestampStats = tableStatistics.getColumnStatistics()
                .get(PaimonColumnHandle.of("event_time", DataTypes.TIMESTAMP(6)));
        assertThat(timestampStats.getRange()).contains(new DoubleRange(1_000_000, 2_500_000));

        assertThat(tableStatistics.getColumnStatistics())
                .doesNotContainKey(PaimonColumnHandle.of("missing", DataTypes.INT()));
    }

    @Test
    public void testTableStatisticsReturnsUnknownWhenPaimonStatsAreMissingOrUnreadable()
    {
        org.apache.paimon.types.RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));

        PaimonMetadata missingStatsMetadata = new PaimonMetadata(new TestingPaimonCatalog(statisticsTable(rowType,
                Optional.empty())), TESTING_TYPE_MANAGER);
        assertThat(missingStatsMetadata.getTableStatistics(SESSION, new PaimonTableHandle("schema", "table", Map.of())))
                .isEqualTo(TableStatistics.empty());

        PaimonMetadata failingStatsMetadata = new PaimonMetadata(new TestingPaimonCatalog(failingStatisticsTable(rowType)),
                TESTING_TYPE_MANAGER);
        assertThat(failingStatsMetadata.getTableStatistics(SESSION, new PaimonTableHandle("schema", "table", Map.of())))
                .isEqualTo(TableStatistics.empty());
    }

    @Test
    public void testTableStatisticsDoesNotApplyFullTableStatsToFilteredOrLimitedHandles()
    {
        org.apache.paimon.types.RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));
        Statistics statistics = new Statistics(7, 3, 100L, 4096L, Map.of());
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(statisticsTable(rowType, Optional.of(statistics))),
                TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThat(metadata.getTableStatistics(SESSION, tableHandle.copy(OptionalLong.of(10))))
                .isEqualTo(TableStatistics.empty());

        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        assertThat(metadata.getTableStatistics(SESSION, tableHandle.copy(TupleDomain.withColumnDomains(Map.of(
                columnHandle,
                Domain.singleValue(BIGINT, 1L))))))
                .isEqualTo(TableStatistics.empty());
        assertThat(metadata.getTableStatistics(SESSION, new PaimonTableHandle("schema", "table", Map.of(
                CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2"))))
                .isEqualTo(TableStatistics.empty());

        TableStatistics emptyFilteredStats = metadata.getTableStatistics(SESSION, tableHandle.copy(TupleDomain.none()));
        assertThat(emptyFilteredStats.getRowCount().getValue()).isEqualTo(0);
        assertThat(emptyFilteredStats.getColumnStatistics()).isEmpty();

        TableStatistics zeroLimitStats = metadata.getTableStatistics(SESSION, tableHandle.copy(OptionalLong.of(0)));
        assertThat(zeroLimitStats.getRowCount().getValue()).isEqualTo(0);
        assertThat(zeroLimitStats.getColumnStatistics()).isEmpty();
    }

    @Test
    public void testVersionedQueriesAreRejectedForSystemTables()
    {
        PaimonMetadata metadata = new PaimonMetadata(new CapturingDdlCatalog(), TESTING_TYPE_MANAGER);

        assertTrinoError(
                () -> metadata.getTableHandle(
                        SESSION,
                        new SchemaTableName(SYSTEM_DATABASE_NAME, "catalog_options"),
                        Optional.empty(),
                        Optional.of(new ConnectorTableVersion(PointerType.TARGET_ID, INTEGER, 1L))),
                NOT_SUPPORTED.toErrorCode(),
                PaimonTableHandle.UNSUPPORTED_HISTORICAL_READ_MESSAGE);
        assertTrinoError(
                () -> metadata.getTableHandle(
                        SESSION,
                        new SchemaTableName("schema", "table$tags"),
                        Optional.empty(),
                        Optional.of(new ConnectorTableVersion(PointerType.TARGET_ID, INTEGER, 1L))),
                NOT_SUPPORTED.toErrorCode(),
                PaimonTableHandle.UNSUPPORTED_HISTORICAL_READ_MESSAGE);
        assertTrinoError(
                () -> metadata.getTableHandle(
                        SESSION,
                        new SchemaTableName("schema", "table$branch_feature$tags"),
                        Optional.empty(),
                        Optional.of(new ConnectorTableVersion(PointerType.TARGET_ID, INTEGER, 1L))),
                NOT_SUPPORTED.toErrorCode(),
                PaimonTableHandle.UNSUPPORTED_HISTORICAL_READ_MESSAGE);
    }

    @Test
    public void testInsertLayoutRequiresFileStoreTable()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertUnsupportedFileStoreTable(() -> metadata.getInsertLayout(SESSION, tableHandle),
                "Paimon insert layout requires FileStoreTable");
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testMetadataFileStoreBoundariesRejectSearchWrapperTables()
            throws Exception
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        Slice fragment = commitFragment();

        PaimonMetadata vectorSearchMetadata = new PaimonMetadata(new TestingPaimonCatalog(VectorSearchTable.create(
                innerTable(),
                new VectorSearch(new float[] {1.0f}, 1, "embedding"))), TESTING_TYPE_MANAGER);
        assertTrinoError(() -> vectorSearchMetadata.getRowChangeParadigm(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon vector search tables are not supported by the Trino connector");
        assertTrinoError(() -> vectorSearchMetadata.getInsertLayout(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon vector search tables are not supported by the Trino connector");
        assertTrinoError(() -> vectorSearchMetadata.getMergeRowIdColumnHandle(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon vector search tables are not supported by the Trino connector");
        assertTrinoError(() -> vectorSearchMetadata.getUpdateLayout(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon vector search tables are not supported by the Trino connector");
        assertTrinoError(() -> vectorSearchMetadata.beginMerge(SESSION, tableHandle, RetryMode.NO_RETRIES),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon vector search tables are not supported by the Trino connector");
        assertTrinoError(() -> vectorSearchMetadata.truncateTable(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon vector search tables are not supported by the Trino connector");
        assertTrinoError(() -> vectorSearchMetadata.applyDelete(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon vector search tables are not supported by the Trino connector");
        assertTrinoError(() -> vectorSearchMetadata.finishInsert(SESSION, tableHandle, List.of(fragment), List.of()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon vector search tables are not supported by the Trino connector");

        PaimonMetadata fullTextSearchMetadata = new PaimonMetadata(new TestingPaimonCatalog(FullTextSearchTable.create(
                innerTable(),
                new FullTextSearch(FullTextQuery.match("paimon", "content"), 1))), TESTING_TYPE_MANAGER);
        assertTrinoError(() -> fullTextSearchMetadata.getRowChangeParadigm(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon full-text search tables are not supported by the Trino connector");
        assertTrinoError(() -> fullTextSearchMetadata.getInsertLayout(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon full-text search tables are not supported by the Trino connector");
        assertTrinoError(() -> fullTextSearchMetadata.getMergeRowIdColumnHandle(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon full-text search tables are not supported by the Trino connector");
        assertTrinoError(() -> fullTextSearchMetadata.getUpdateLayout(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon full-text search tables are not supported by the Trino connector");
        assertTrinoError(() -> fullTextSearchMetadata.beginMerge(SESSION, tableHandle, RetryMode.NO_RETRIES),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon full-text search tables are not supported by the Trino connector");
        assertTrinoError(() -> fullTextSearchMetadata.truncateTable(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon full-text search tables are not supported by the Trino connector");
        assertTrinoError(() -> fullTextSearchMetadata.applyDelete(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon full-text search tables are not supported by the Trino connector");
        assertTrinoError(() -> fullTextSearchMetadata.finishInsert(SESSION, tableHandle, List.of(fragment), List.of()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon full-text search tables are not supported by the Trino connector");
    }

    @Test
    public void testMergeRowIdRequiresFileStoreTable()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertUnsupportedFileStoreTable(() -> metadata.getRowChangeParadigm(SESSION, tableHandle),
                "Paimon row-level change requires FileStoreTable");
        assertUnsupportedFileStoreTable(() -> metadata.getMergeRowIdColumnHandle(SESSION, tableHandle),
                "Paimon merge row id requires FileStoreTable");
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testMergeRowIdRequiresPrimaryKeys()
    {
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                new AtomicBoolean(),
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())),
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())),
                List.of());
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(() -> metadata.getRowChangeParadigm(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(), "Paimon row-level change requires primary keys");
        assertTrinoError(() -> metadata.getMergeRowIdColumnHandle(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(), "Paimon merge row id requires primary keys");
        assertTrinoError(() -> metadata.getUpdateLayout(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(), "Paimon update layout requires primary keys");
        assertTrinoError(() -> metadata.beginMerge(SESSION, tableHandle, RetryMode.NO_RETRIES),
                NOT_SUPPORTED.toErrorCode(), "Paimon merge requires primary keys");
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testRowLevelDeleteRequiresSupportedMergeEngine()
    {
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                new AtomicBoolean(),
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())),
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())),
                List.of("id"),
                List.of("id"),
                "id",
                Map.of(CoreOptions.MERGE_ENGINE.key(), CoreOptions.MergeEngine.FIRST_ROW.toString()));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(() -> metadata.getRowChangeParadigm(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon row-level change is not supported for this table: Merge engine first-row can not support batch delete.");
        assertTrinoError(() -> metadata.getMergeRowIdColumnHandle(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon merge row id is not supported for this table: Merge engine first-row can not support batch delete.");
        assertTrinoError(() -> metadata.getUpdateLayout(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon update layout is not supported for this table: Merge engine first-row can not support batch delete.");
        assertTrinoError(() -> metadata.beginMerge(SESSION, tableHandle, RetryMode.NO_RETRIES),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon merge is not supported for this table: Merge engine first-row can not support batch delete.");
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testMergeRowIdUsesPrimaryKeyFieldsInPrimaryKeyOrder()
    {
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                new AtomicBoolean(),
                DataTypes.ROW(
                        DataTypes.FIELD(0, "value", DataTypes.STRING()),
                        DataTypes.FIELD(1, "id", DataTypes.INT()),
                        DataTypes.FIELD(2, "date", DataTypes.STRING())),
                DataTypes.ROW(
                        DataTypes.FIELD(0, "value", DataTypes.STRING()),
                        DataTypes.FIELD(1, "id", DataTypes.INT()),
                        DataTypes.FIELD(2, "date", DataTypes.STRING())),
                List.of("date", "id"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        PaimonColumnHandle rowId = (PaimonColumnHandle) metadata.getMergeRowIdColumnHandle(SESSION, tableHandle);

        assertThat(rowId.getColumnName()).isEqualTo(PaimonColumnHandle.TRINO_ROW_ID_NAME);
        assertThat(((org.apache.paimon.types.RowType) rowId.logicalType()).getFieldNames())
                .containsExactly("date", "id");
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testMergeRowIdUsesLatestSchema()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                copiedWithLatestSchema,
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())),
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(1, "updated_key", DataTypes.STRING())),
                List.of("updated_key"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        PaimonColumnHandle rowId = (PaimonColumnHandle) metadata.getMergeRowIdColumnHandle(SESSION, tableHandle);

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(((org.apache.paimon.types.RowType) rowId.logicalType()).getFieldNames())
                .containsExactly("updated_key");
    }

    @Test
    public void testRowTrackingBaseTableColumnsExposeHiddenMetadataColumns()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        org.apache.paimon.types.RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "name", DataTypes.STRING()));
        FileStoreTable table = rowTrackingFileStoreTable(copiedWithLatestSchema, rowType);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonTableHandle handle = new PaimonTableHandle("schema", "table", Map.of());

        List<ColumnMetadata> columns = handle.columnMetadatas(catalog.forSession(SESSION), TESTING_TYPE_MANAGER, SESSION);

        assertThat(columns)
                .extracting(ColumnMetadata::getName)
                .containsExactly("id", "name", "_row_id", "_sequence_number");
        assertThat(columns)
                .extracting(ColumnMetadata::isHidden)
                .containsExactly(false, false, true, true);
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testRowTrackingSystemTableColumnsAreVisible()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        org.apache.paimon.types.RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "name", DataTypes.STRING()));
        Table table = new RowTrackingTable(rowTrackingFileStoreTable(copiedWithLatestSchema, rowType));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonTableHandle handle = new PaimonTableHandle("schema", "table", Map.of());

        List<ColumnMetadata> columns = handle.columnMetadatas(catalog.forSession(SESSION), TESTING_TYPE_MANAGER, SESSION);

        assertThat(columns)
                .extracting(ColumnMetadata::getName)
                .containsExactly("id", "name", "_row_id", "_sequence_number");
        assertThat(columns)
                .extracting(ColumnMetadata::isHidden)
                .containsExactly(false, false, false, false);
    }

    @Test
    public void testAuditLogSystemTableSequenceNumberColumnIsVisible()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        org.apache.paimon.types.RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "pk", DataTypes.INT()),
                DataTypes.FIELD(1, "pt", DataTypes.INT()),
                DataTypes.FIELD(2, "col1", DataTypes.INT()));
        Table table = new AuditLogTable(sequenceNumberEnabledFileStoreTable(copiedWithLatestSchema, rowType));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonTableHandle handle = new PaimonTableHandle("schema", "table", Map.of());

        List<ColumnMetadata> columns = handle.columnMetadatas(catalog.forSession(SESSION), TESTING_TYPE_MANAGER, SESSION);

        assertThat(columns)
                .extracting(ColumnMetadata::getName)
                .containsExactly("rowkind", "_sequence_number", "pk", "pt", "col1");
        assertThat(columns)
                .extracting(ColumnMetadata::isHidden)
                .containsExactly(false, false, false, false, false);
    }

    @Test
    public void testGetColumnMetadataUsesVisibleSystemTableColumns()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        org.apache.paimon.types.RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "pk", DataTypes.INT()),
                DataTypes.FIELD(1, "pt", DataTypes.INT()),
                DataTypes.FIELD(2, "col1", DataTypes.INT()));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(
                new AuditLogTable(sequenceNumberEnabledFileStoreTable(copiedWithLatestSchema, rowType)));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("_sequence_number",
                org.apache.paimon.table.SpecialFields.SEQUENCE_NUMBER.type());

        ColumnMetadata columnMetadata = metadata.getColumnMetadata(SESSION, tableHandle, columnHandle);

        assertThat(columnMetadata.getName()).isEqualTo("_sequence_number");
        assertThat(columnMetadata.isHidden()).isFalse();
    }

    @Test
    public void testGetColumnMetadataKeepsMergeRowIdHidden()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of(PaimonColumnHandle.TRINO_ROW_ID_NAME,
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())));

        ColumnMetadata columnMetadata = metadata.getColumnMetadata(SESSION, tableHandle, columnHandle);

        assertThat(columnMetadata.getName()).isEqualTo(PaimonColumnHandle.TRINO_ROW_ID_NAME);
        assertThat(columnMetadata.isHidden()).isTrue();
    }

    @Test
    public void testGetColumnMetadataReturnsOrdinaryColumnFromHandle()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        org.apache.paimon.types.RowType staleRowType = DataTypes.ROW(
                new DataField(0, "id", DataTypes.INT(), "stale comment"));
        org.apache.paimon.types.RowType latestRowType = DataTypes.ROW(
                new DataField(0, "id", DataTypes.INT(), "latest comment"));
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(fileStoreTable(
                        BucketMode.HASH_FIXED,
                        copiedWithLatestSchema,
                        staleRowType,
                        latestRowType,
                        List.of("id"))),
                TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("id", DataTypes.INT());

        ColumnMetadata columnMetadata = metadata.getColumnMetadata(SESSION, tableHandle, columnHandle);

        assertThat(columnMetadata.getName()).isEqualTo("id");
        assertThat(columnMetadata.getComment()).contains("latest comment");
        assertThat(columnMetadata.isHidden()).isFalse();
        assertThat(copiedWithLatestSchema).isTrue();
    }

    @Test
    public void testGetColumnMetadataPreservesHistoricalSchema()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        org.apache.paimon.types.RowType staleRowType = DataTypes.ROW(
                new DataField(0, "id", DataTypes.INT(), "snapshot comment"));
        org.apache.paimon.types.RowType latestRowType = DataTypes.ROW(
                new DataField(0, "id", DataTypes.INT(), "latest comment"));
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(fileStoreTable(
                        BucketMode.HASH_FIXED,
                        copiedWithLatestSchema,
                        staleRowType,
                        latestRowType,
                        List.of("id"))),
                TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("id", DataTypes.INT());
        ConnectorSession historicalSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(SCAN_SNAPSHOT, 9L))
                .build();

        ColumnMetadata columnMetadata = metadata.getColumnMetadata(historicalSession, tableHandle, columnHandle);

        assertThat(columnMetadata.getName()).isEqualTo("id");
        assertThat(columnMetadata.getComment()).contains("snapshot comment");
        assertThat(copiedWithLatestSchema).isFalse();
    }

    @Test
    public void testGetColumnMetadataFallsBackToOrdinaryHandleAfterDdlRemovesColumn()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        org.apache.paimon.types.RowType staleRowType = DataTypes.ROW(
                new DataField(0, "id", DataTypes.INT(), "stale comment"),
                new DataField(1, "order_status", DataTypes.STRING(), "old comment"));
        org.apache.paimon.types.RowType latestRowType = DataTypes.ROW(
                new DataField(0, "id", DataTypes.INT(), "stale comment"));
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(fileStoreTable(
                        BucketMode.HASH_FIXED,
                        copiedWithLatestSchema,
                        staleRowType,
                        latestRowType,
                        List.of("id"))),
                TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle staleColumnHandle = PaimonColumnHandle.of("order_status", DataTypes.STRING());

        ColumnMetadata columnMetadata = metadata.getColumnMetadata(SESSION, tableHandle, staleColumnHandle);

        assertThat(columnMetadata).isEqualTo(staleColumnHandle.getColumnMetadata());
        assertThat(copiedWithLatestSchema).isTrue();
    }

    @Test
    public void testGetColumnHandlesMapsColumnNamesToHandles()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(SESSION, tableHandle);

        assertThat(columnHandles).hasSize(1);
        assertThat(columnHandles).containsKey("id");
        assertThat(columnHandles.get("id")).isInstanceOf(PaimonColumnHandle.class);
        assertThat(((PaimonColumnHandle) columnHandles.get("id")).getColumnName()).isEqualTo("id");
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testGetTableMetadata()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(fileStoreTable(
                BucketMode.HASH_FIXED,
                new AtomicBoolean(),
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())),
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())),
                List.of(),
                List.of("id"),
                "id"));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);

        assertThat(tableMetadata.getTable()).isEqualTo(new SchemaTableName("schema", "table"));
        assertThat(tableMetadata.getColumns()).extracting(ColumnMetadata::getName).containsExactly("id");
        assertThat(tableMetadata.getProperties())
                .containsEntry(PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, List.of("id"))
                .containsEntry("bucket", "7")
                .containsEntry("bucket_key", "id");
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testGetTablePropertiesReturnsEmptyProperties()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        ConnectorTableProperties properties = metadata.getTableProperties(SESSION, tableHandle);

        assertThat(properties).isNotNull();
    }

    @Test
    public void testBeginInsertReturnsHandleWithWriteColumns()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.INT());

        ConnectorInsertTableHandle insertHandle = metadata.beginInsert(SESSION, tableHandle, List.of(id), RetryMode.NO_RETRIES);

        assertThat(insertHandle).isInstanceOf(PaimonTableHandle.class);
        PaimonTableHandle result = (PaimonTableHandle) insertHandle;
        assertThat(result.getWriteColumns()).hasValueSatisfying(writeColumns ->
                assertThat(writeColumns).extracting(PaimonColumnHandle::getColumnName).containsExactly("id"));
    }

    @Test
    public void testListTableColumnsSkipsMissingTables()
    {
        PaimonCatalog catalog = new PaimonCatalog(new Options(), unsupportedFileSystemFactory()) {
            @Override
            public void initSession(ConnectorSession connectorSession) {}

            @Override
            public Catalog forSession(ConnectorSession connectorSession)
            {
                return this;
            }

            @Override
            public List<String> listTables(String databaseName)
            {
                return List.of("existing", "missing");
            }

            @Override
            public Table getTable(Identifier identifier)
                    throws Catalog.TableNotExistException
            {
                if (identifier.getObjectName().equals("missing")) {
                    throw new Catalog.TableNotExistException(identifier);
                }
                return fileStoreTable(BucketMode.HASH_FIXED);
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        Map<SchemaTableName, List<ColumnMetadata>> columns = metadata.listTableColumns(SESSION, new SchemaTablePrefix("schema"));

        assertThat(columns).hasSize(1);
        assertThat(columns).containsKey(new SchemaTableName("schema", "existing"));
    }

    @Test
    public void testStreamTableColumns()
    {
        PaimonCatalog catalog = new PaimonCatalog(new Options(), unsupportedFileSystemFactory()) {
            @Override
            public void initSession(ConnectorSession connectorSession) {}

            @Override
            public Catalog forSession(ConnectorSession connectorSession)
            {
                return this;
            }

            @Override
            public List<String> listTables(String databaseName)
            {
                return List.of("table");
            }

            @Override
            public Table getTable(Identifier identifier)
            {
                return fileStoreTable(BucketMode.HASH_FIXED);
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        Iterator<TableColumnsMetadata> columns = metadata.streamTableColumns(SESSION, new SchemaTablePrefix("schema"));

        assertThat(columns.hasNext()).isTrue();
        TableColumnsMetadata tableColumns = columns.next();
        assertThat(tableColumns.getTable()).isEqualTo(new SchemaTableName("schema", "table"));
        assertThat(tableColumns.getColumns()).hasValueSatisfying(list ->
                assertThat(list).extracting(ColumnMetadata::getName).containsExactly("id"));
        assertThat(columns.hasNext()).isFalse();
    }

    @Test
    public void testMergeRowIdFailsWhenPrimaryKeyIsMissingFromTableSchema()
    {
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                new AtomicBoolean(),
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())),
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())),
                List.of("missing"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThatThrownBy(() -> metadata.getMergeRowIdColumnHandle(SESSION, tableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon primary key 'missing' is not present in table schema [id]");
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testUpdateLayoutRequiresFileStoreTable()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertUnsupportedFileStoreTable(() -> metadata.getUpdateLayout(SESSION, tableHandle),
                "Paimon update layout requires FileStoreTable");
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testInsertAndUpdateLayoutsUseLatestSchema()
            throws IOException
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        org.apache.paimon.types.RowType staleRowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "old_bucket", DataTypes.INT()));
        org.apache.paimon.types.RowType latestRowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "new_bucket", DataTypes.INT()));
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                copiedWithLatestSchema,
                staleRowType,
                latestRowType,
                List.of("id", "new_bucket"),
                "new_bucket");
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        ConnectorTableLayout insertLayout = metadata.getInsertLayout(SESSION, tableHandle).orElseThrow();
        TableSchema insertSchema = partitioningSchema(insertLayout.getPartitioning().orElseThrow());

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(insertLayout.getPartitionColumns()).containsExactly("id", "new_bucket");
        assertThat(insertSchema.fieldNames()).containsExactly("id", "new_bucket");
        assertThat(insertSchema.bucketKeys()).containsExactly("new_bucket");

        TableSchema updateSchema = partitioningSchema(metadata.getUpdateLayout(SESSION, tableHandle).orElseThrow());
        assertThat(updateSchema.fieldNames()).containsExactly("id", "new_bucket");
        assertThat(updateSchema.bucketKeys()).containsExactly("new_bucket");
    }

    @Test
    public void testFixedBucketInsertLayoutUsesPartitionAndBucketKeys()
            throws IOException
    {
        org.apache.paimon.types.RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "dt", DataTypes.INT()),
                DataTypes.FIELD(1, "id", DataTypes.INT()),
                DataTypes.FIELD(2, "bucket_key", DataTypes.INT()));
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                new AtomicBoolean(),
                rowType,
                rowType,
                List.of("dt"),
                List.of("dt", "id", "bucket_key"),
                "bucket_key");
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        ConnectorTableLayout insertLayout = metadata.getInsertLayout(SESSION, tableHandle).orElseThrow();

        assertThat(insertLayout.getPartitionColumns()).containsExactly("dt", "bucket_key");
    }

    @Test
    public void testInsertLayoutIgnoresSessionScanSnapshotAndHandleStartupSelections()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions = new AtomicReference<>();
        FileStoreTable table = writePlanningFileStoreTable(copiedWithLatestSchema, copyWithoutTimeTravelOptions);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(
                        "custom.option", "value",
                        CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2",
                        CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key(), "delta",
                        CoreOptions.INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT.key(), "true"));
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(SCAN_SNAPSHOT, 9L))
                .build();

        assertThat(metadata.getInsertLayout(session, tableHandle)).isPresent();

        assertThat(copyWithoutTimeTravelOptions.get()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "custom.option", "value"));
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testDynamicBucketWritePlanningUsesSingleNodeLayoutAndRejectsRowLevelChanges()
    {
        org.apache.paimon.types.RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(fileStoreTable(
                BucketMode.HASH_DYNAMIC,
                new AtomicBoolean(),
                rowType,
                rowType,
                List.of(),
                List.of("id"))),
                TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        ConnectorTableLayout insertLayout = metadata.getInsertLayout(SESSION, tableHandle).orElseThrow();
        assertThat(insertLayout.getPartitionColumns()).isEmpty();
        assertThat(insertLayout.getPartitioning().orElseThrow())
                .isInstanceOfSatisfying(PaimonPartitioningHandle.class, handle -> assertThat(handle.isSingleNode()).isTrue());

        assertTrinoError(() -> metadata.getRowChangeParadigm(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Unsupported table bucket mode: HASH_DYNAMIC for Paimon row-level change. Dynamic-bucket row-level writes require Flink-style two-stage bucket assignment and dynamic bucket index coordination; this Trino connector currently supports HASH_DYNAMIC INSERT only");
        assertTrinoError(() -> metadata.getMergeRowIdColumnHandle(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Unsupported table bucket mode: HASH_DYNAMIC for Paimon merge row id. Dynamic-bucket row-level writes require Flink-style two-stage bucket assignment and dynamic bucket index coordination; this Trino connector currently supports HASH_DYNAMIC INSERT only");
        assertTrinoError(() -> metadata.getUpdateLayout(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Unsupported table bucket mode: HASH_DYNAMIC for Paimon update layout. Dynamic-bucket row-level writes require Flink-style two-stage bucket assignment and dynamic bucket index coordination; this Trino connector currently supports HASH_DYNAMIC INSERT only");
        assertTrinoError(() -> metadata.beginMerge(SESSION, tableHandle, RetryMode.NO_RETRIES),
                NOT_SUPPORTED.toErrorCode(),
                "Unsupported table bucket mode: HASH_DYNAMIC for Paimon merge. Dynamic-bucket row-level writes require Flink-style two-stage bucket assignment and dynamic bucket index coordination; this Trino connector currently supports HASH_DYNAMIC INSERT only");
    }

    @Test
    public void testLayoutSerializationFailuresUsePaimonMetadataError()
    {
        IOException failure = new IOException("schema serialization failed");
        FileStoreTable table = nonSerializableSchemaFileStoreTable(failure);
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThatThrownBy(() -> metadata.getInsertLayout(SESSION, tableHandle))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to prepare Paimon insert layout for table 'schema.table'");
                    assertThat(exception.getCause()).isInstanceOf(IOException.class);
                });
        assertThatThrownBy(() -> metadata.getUpdateLayout(SESSION, tableHandle))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to prepare Paimon update layout for table 'schema.table'");
                    assertThat(exception.getCause()).isInstanceOf(IOException.class);
                });
    }

    @Test
    public void testBeginMergeRequiresFileStoreTable()
    {
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table()), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertUnsupportedFileStoreTable(() -> metadata.beginMerge(SESSION, tableHandle, RetryMode.NO_RETRIES),
                "Paimon merge requires FileStoreTable");
    }

    @Test
    public void testBeginMergeRejectsQueryRetriesBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(() -> metadata.beginMerge(SESSION, tableHandle, RetryMode.RETRIES_ENABLED),
                NOT_SUPPORTED.toErrorCode(), "This connector does not support query retries");
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testBeginMergeRequiresPrimaryKeyBucketMode()
    {
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(fileStoreTable(BucketMode.BUCKET_UNAWARE)),
                TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(() -> metadata.beginMerge(SESSION, tableHandle, RetryMode.NO_RETRIES),
                NOT_SUPPORTED.toErrorCode(), "Unsupported table bucket mode: BUCKET_UNAWARE for Paimon merge");
    }

    @Test
    public void testBeginMergeUsesLatestSchemaForExplicitWriteColumns()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        org.apache.paimon.types.RowType staleRowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
        org.apache.paimon.types.RowType latestRowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "name", DataTypes.STRING()));
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, copiedWithLatestSchema, staleRowType,
                latestRowType);
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        ConnectorMergeTableHandle mergeHandle = metadata.beginMerge(SESSION, tableHandle, RetryMode.NO_RETRIES);

        assertThat(copiedWithLatestSchema).isTrue();
        PaimonTableHandle writeHandle = (PaimonTableHandle) mergeHandle.getTableHandle();
        assertThat(writeHandle.getWriteColumns()).hasValueSatisfying(writeColumns ->
                assertThat(writeColumns).extracting(PaimonColumnHandle::getColumnName)
                        .containsExactly("id", "name"));
    }

    @Test
    public void testMergeMetadataPlanningIgnoresSessionScanSnapshotAndHandleStartupSelections()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions = new AtomicReference<>();
        FileStoreTable table = writePlanningFileStoreTable(copiedWithLatestSchema, copyWithoutTimeTravelOptions);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(
                        "custom.option", "value",
                        CoreOptions.INCREMENTAL_TO_AUTO_TAG.key(), "2024-12-04"));
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(SCAN_SNAPSHOT, 9L))
                .build();

        assertThat(metadata.getMergeRowIdColumnHandle(session, tableHandle)).isInstanceOf(PaimonColumnHandle.class);
        assertThat(metadata.getUpdateLayout(session, tableHandle)).isPresent();
        assertThat(metadata.beginMerge(session, tableHandle, RetryMode.NO_RETRIES))
                .isInstanceOf(PaimonMergeTableHandle.class);

        assertThat(copyWithoutTimeTravelOptions.get()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "custom.option", "value"));
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testTruncateRequiresFileStoreTable()
    {
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table()), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertUnsupportedFileStoreTable(() -> metadata.truncateTable(SESSION, tableHandle),
                "Paimon truncate table requires FileStoreTable");
        assertUnsupportedFileStoreTable(() -> metadata.applyDelete(SESSION, tableHandle),
                "Paimon delete requires FileStoreTable");
        assertUnsupportedFileStoreTable(() -> metadata.executeDelete(SESSION, tableHandle),
                "Paimon delete requires FileStoreTable");
    }

    @Test
    public void testCommitRequiresFileStoreTable()
            throws Exception
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        Slice fragment = commitFragment();

        assertUnsupportedFileStoreTable(() -> metadata.finishInsert(SESSION, tableHandle, List.of(fragment), List.of()),
                "Paimon commit writes requires FileStoreTable");
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testCommitUsesLatestSchemaBeforeCreatingBatchWriteBuilder()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        FileStoreTable table = commitFileStoreTable(copiedWithLatestSchema, committed);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThat(metadata.finishInsert(SESSION, tableHandle, List.of(commitFragment()), List.of()))
                .isEmpty();

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(committed).isTrue();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testCommitIgnoresSessionScanSnapshotAndHandleStartupSelections()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions = new AtomicReference<>();
        FileStoreTable table = commitFileStoreTable(copiedWithLatestSchema, committed, copyWithoutTimeTravelOptions);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(
                        "custom.option", "value",
                        CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2",
                        CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key(), "delta",
                        CoreOptions.INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT.key(), "true"));
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(SCAN_SNAPSHOT, 9L))
                .build();

        assertThat(metadata.finishInsert(session, tableHandle, List.of(commitFragment()), List.of()))
                .isEmpty();

        assertThat(copyWithoutTimeTravelOptions.get()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "custom.option", "value"));
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(committed).isTrue();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testInsertOverwriteAppliesToFinishInsertOnly()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        AtomicBoolean overwriteEnabled = new AtomicBoolean();
        FileStoreTable table = commitFileStoreTable(copiedWithLatestSchema, committed, new AtomicReference<>(), null,
                overwriteEnabled);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        ConnectorSession overwriteSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR,
                        PaimonSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE.name()))
                .build();

        assertThat(metadata.finishInsert(overwriteSession, tableHandle, List.of(commitFragment()), List.of()))
                .isEmpty();

        assertThat(overwriteEnabled).isTrue();
        assertThat(committed).isTrue();
    }

    @Test
    public void testInsertOverwriteCommitsEmptyFragments()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        AtomicBoolean overwriteEnabled = new AtomicBoolean();
        FileStoreTable table = commitFileStoreTable(copiedWithLatestSchema, committed, new AtomicReference<>(), null,
                overwriteEnabled);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        ConnectorSession overwriteSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR,
                        PaimonSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE.name()))
                .build();

        assertThat(metadata.finishInsert(overwriteSession, tableHandle, List.of(), List.of()))
                .isEmpty();

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(overwriteEnabled).isTrue();
        assertThat(committed).isTrue();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testInsertOverwriteDoesNotApplyToFinishMerge()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        AtomicBoolean overwriteEnabled = new AtomicBoolean();
        FileStoreTable table = commitFileStoreTable(copiedWithLatestSchema, committed, new AtomicReference<>(), null,
                overwriteEnabled);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        ConnectorSession overwriteSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR,
                        PaimonSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE.name()))
                .build();

        metadata.finishMerge(overwriteSession, new PaimonMergeTableHandle(tableHandle), List.of(commitFragment()), List.of());

        assertThat(overwriteEnabled).isFalse();
        assertThat(committed).isTrue();
    }

    @Test
    public void testInsertErrorRejectsExistingNonPartitionedTable()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        FileStoreTable table = commitFileStoreTable(
                copiedWithLatestSchema,
                committed,
                new AtomicReference<>(),
                null,
                new AtomicBoolean(),
                List.of(new PartitionEntry(BinaryRow.EMPTY_ROW, 1, 1, 1, 1, 1)),
                List.of(),
                Map.of());
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        ConnectorSession errorSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR,
                        PaimonSessionProperties.InsertExistingPartitionsBehavior.ERROR.name()))
                .build();
        Slice fragment = commitFragment();

        assertTrinoError(() -> metadata.finishInsert(errorSession, new PaimonTableHandle("schema", "table", Map.of()),
                        List.of(fragment), List.of()),
                READ_ONLY_VIOLATION.toErrorCode(),
                "Cannot insert into an existing non-partitioned Paimon table: schema.table");
        assertThat(committed).isFalse();
    }

    @Test
    public void testInsertErrorRejectsExistingPartition()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        BinaryRow partition = partitionRow("p1");
        FileStoreTable table = commitFileStoreTable(
                copiedWithLatestSchema,
                committed,
                new AtomicReference<>(),
                null,
                new AtomicBoolean(),
                List.of(new PartitionEntry(partition, 1, 1, 1, 1, 1)),
                List.of("pt"),
                Map.of());
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        ConnectorSession errorSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR,
                        PaimonSessionProperties.InsertExistingPartitionsBehavior.ERROR.name()))
                .build();
        Slice fragment = commitFragment(partition);

        assertTrinoError(() -> metadata.finishInsert(errorSession, new PaimonTableHandle("schema", "table", Map.of()),
                        List.of(fragment), List.of()),
                READ_ONLY_VIOLATION.toErrorCode(),
                "Cannot insert into an existing partition of Paimon table: schema.table");
        assertThat(committed).isFalse();
    }

    @Test
    public void testInsertErrorAllowsNewPartition()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        FileStoreTable table = commitFileStoreTable(
                copiedWithLatestSchema,
                committed,
                new AtomicReference<>(),
                null,
                new AtomicBoolean(),
                List.of(new PartitionEntry(partitionRow("p1"), 1, 1, 1, 1, 1)),
                List.of("pt"),
                Map.of());
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        ConnectorSession errorSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR,
                        PaimonSessionProperties.InsertExistingPartitionsBehavior.ERROR.name()))
                .build();
        Slice fragment = commitFragment(partitionRow("p2"));

        assertThat(metadata.finishInsert(errorSession, new PaimonTableHandle("schema", "table", Map.of()),
                List.of(fragment), List.of())).isEmpty();
        assertThat(committed).isTrue();
    }

    @Test
    public void testInsertOverwriteRejectsPartitionedTableWithoutDynamicPartitionOverwrite()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        AtomicBoolean overwriteEnabled = new AtomicBoolean();
        FileStoreTable table = commitFileStoreTable(
                copiedWithLatestSchema,
                committed,
                new AtomicReference<>(),
                null,
                overwriteEnabled,
                List.of(),
                List.of("pt"),
                Map.of(CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key(), "false"));
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        ConnectorSession overwriteSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR,
                        PaimonSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE.name()))
                .build();
        Slice fragment = commitFragment(partitionRow("p1"));

        assertTrinoError(() -> metadata.finishInsert(overwriteSession, new PaimonTableHandle("schema", "table", Map.of()),
                        List.of(fragment), List.of()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon insert overwrite requires dynamic-partition-overwrite=true for partitioned tables");
        assertThat(overwriteEnabled).isFalse();
        assertThat(committed).isFalse();
    }

    @Test
    public void testTruncateUsesLatestSchemaBeforeCreatingBatchWriteBuilder()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        FileStoreTable table = truncateFileStoreTable(copiedWithLatestSchema, truncated);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        metadata.truncateTable(SESSION, new PaimonTableHandle("schema", "table", Map.of()));

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isTrue();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testApplyDeleteAcceptsUnfilteredFileStoreTable()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        FileStoreTable table = truncateFileStoreTable(copiedWithLatestSchema, truncated);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        Optional<ConnectorTableHandle> deleteHandle = metadata.applyDelete(SESSION, tableHandle);

        assertThat(deleteHandle).contains(tableHandle);
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testApplyDeleteDoesNotAcceptFilteredTableHandle()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        FileStoreTable table = truncateFileStoreTable(copiedWithLatestSchema, truncated);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(id, Domain.singleValue(INTEGER, 1L))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThat(metadata.applyDelete(SESSION, tableHandle)).isEmpty();
        assertThat(copiedWithLatestSchema).isFalse();
        assertThat(truncated).isFalse();
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testExecuteDeleteUsesPaimonTruncateFastPath()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        FileStoreTable table = truncateFileStoreTable(copiedWithLatestSchema, truncated);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        assertThat(metadata.executeDelete(SESSION, new PaimonTableHandle("schema", "table", Map.of())))
                .isEmpty();

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isTrue();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testExecuteDeleteRejectsFilteredTableHandle()
    {
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED)),
                TESTING_TYPE_MANAGER);
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(id, Domain.singleValue(INTEGER, 1L))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThatThrownBy(() -> metadata.executeDelete(SESSION, tableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon delete requires an unfiltered table handle");
    }

    @Test
    public void testEmptyCommitDoesNotInitializeCatalog()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThat(metadata.finishInsert(SESSION, tableHandle, List.of(), List.of()))
                .isEmpty();
        assertThat(catalog.initialized).isFalse();

        assertThat(metadata.finishCreateTable(SESSION, tableHandle, List.of(), List.of()))
                .isEmpty();
        assertThat(catalog.initialized).isFalse();

        metadata.finishMerge(SESSION, new PaimonMergeTableHandle(tableHandle), List.of(), List.of());
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testCommitFragmentsAreValidatedBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThatThrownBy(() -> metadata.finishInsert(null, tableHandle, List.of(), List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.finishCreateTable(null, tableHandle, List.of(), List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.finishMerge(null, new PaimonMergeTableHandle(tableHandle), List.of(),
                List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.finishInsert(SESSION, tableHandle, null, List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fragments is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.finishCreateTable(SESSION, tableHandle, null, List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fragments is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.finishMerge(SESSION, new PaimonMergeTableHandle(tableHandle), null,
                List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fragments is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.finishInsert(SESSION, tableHandle, Collections.singletonList(null),
                List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fragments contains null fragment");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.finishMerge(SESSION, new PaimonMergeTableHandle(tableHandle),
                Collections.singletonList(null), List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fragments contains null fragment");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.finishInsert(SESSION, tableHandle, List.of(Slices.wrappedBuffer(new byte[] {
                1, 2, 3})), List.of()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_COMMIT_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to deserialize Paimon commit fragment");
                    assertThat(exception.getCause()).isInstanceOf(IOException.class);
                });
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.finishMerge(SESSION, new PaimonMergeTableHandle(tableHandle),
                List.of(Slices.wrappedBuffer(new byte[] {1, 2, 3})), List.of()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_COMMIT_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to deserialize Paimon commit fragment");
                    assertThat(exception.getCause()).isInstanceOf(IOException.class);
                });
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testFinishInsertCommitFailuresUsePaimonCommitError()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        RuntimeException commitFailure = new RuntimeException("commit failed");
        FileStoreTable table = commitFileStoreTable(copiedWithLatestSchema, committed, new AtomicReference<>(),
                commitFailure);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThatThrownBy(() -> metadata.finishInsert(SESSION, tableHandle, List.of(commitFragment()), List.of()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_COMMIT_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to commit Paimon write fragments");
                    assertThat(exception.getCause()).isSameAs(commitFailure);
                });
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(committed).isFalse();
    }

    @Test
    public void testApplyLimitInitializesCatalogBeforeFilteredTableLookup()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(id, Domain.singleValue(INTEGER, 1L))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThat(metadata.applyLimit(SESSION, tableHandle, 10))
                .isPresent()
                .get()
                .extracting(result -> (PaimonTableHandle) result.getHandle())
                .satisfies(handle -> assertThat(handle.getLimit()).hasValue(10));
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testApplyLimitRefreshesLatestFileStoreSchemaForPartitionFilter()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                copiedWithLatestSchema,
                DataTypes.ROW(DataTypes.FIELD(0, "old_id", DataTypes.INT())),
                DataTypes.ROW(DataTypes.FIELD(0, "new_id", DataTypes.INT())),
                List.of("new_id"),
                List.of("new_id"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle newId = PaimonColumnHandle.of("new_id", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(newId, Domain.singleValue(INTEGER, 1L))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThat(metadata.applyLimit(SESSION, tableHandle, 10))
                .isPresent()
                .get()
                .extracting(result -> (PaimonTableHandle) result.getHandle())
                .satisfies(handle -> assertThat(handle.getLimit()).hasValue(10));
        assertThat(copiedWithLatestSchema).isTrue();
    }

    @Test
    public void testApplyLimitShortCircuitsExistingLimitBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(id, Domain.singleValue(INTEGER, 1L))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(5));

        assertThat(metadata.applyLimit(SESSION, tableHandle, 10))
                .isEmpty();
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testApplyLimitShortCircuitsTupleDomainNoneBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.none(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThat(metadata.applyLimit(SESSION, tableHandle, 10))
                .isPresent()
                .get()
                .extracting(result -> (PaimonTableHandle) result.getHandle())
                .satisfies(handle -> {
                    assertThat(handle.getFilter()).isEqualTo(TupleDomain.none());
                    assertThat(handle.getLimit()).hasValue(10);
                });
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testApplyFilterValidatesInputsBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        ConnectorTableHandle wrongTableHandle = new ConnectorTableHandle() {};

        assertThatThrownBy(() -> metadata.applyFilter(null, tableHandle, Constraint.alwaysTrue()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.applyFilter(SESSION, wrongTableHandle, Constraint.alwaysTrue()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon filter pushdown requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.applyFilter(SESSION, tableHandle, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("constraint is null");
        assertThat(catalog.initialized).isFalse();

        ColumnHandle wrongColumnHandle = new ColumnHandle() {};
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(Map.of(
                wrongColumnHandle, Domain.singleValue(INTEGER, 1L))));
        assertThatThrownBy(() -> metadata.applyFilter(SESSION, tableHandle, constraint))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon filter pushdown requires PaimonColumnHandle, got: %s",
                        wrongColumnHandle.getClass().getName());
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testApplyFilterShortCircuitsTrivialConstraintsBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThat(metadata.applyFilter(SESSION, tableHandle, Constraint.alwaysTrue()))
                .isEmpty();
        assertThat(catalog.initialized).isFalse();

        assertThat(metadata.applyFilter(SESSION, tableHandle, Constraint.alwaysFalse()))
                .isPresent()
                .get()
                .satisfies(result -> {
                    assertThat(((PaimonTableHandle) result.getHandle()).getFilter()).isEqualTo(TupleDomain.none());
                    assertThat(result.getRemainingFilter()).isEqualTo(TupleDomain.all());
                    assertThat(result.getRemainingExpression()).contains(TRUE);
                });
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testApplyFilterShortCircuitsTupleDomainNoneHandleBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.none(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(Map.of(
                id, Domain.singleValue(INTEGER, 1L))));

        assertThat(metadata.applyFilter(SESSION, tableHandle, constraint)).isEmpty();
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testApplyFilterSkipsNonEmptyPushdownAfterAcceptedLimitBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(5));
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(Map.of(
                id, Domain.singleValue(INTEGER, 1L))));

        assertThat(metadata.applyFilter(SESSION, tableHandle, constraint)).isEmpty();
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testApplyFilterStillShortCircuitsAlwaysFalseAfterAcceptedLimitBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(5));

        assertThat(metadata.applyFilter(SESSION, tableHandle, Constraint.alwaysFalse()))
                .isPresent()
                .get()
                .satisfies(result -> {
                    assertThat(((PaimonTableHandle) result.getHandle()).getFilter()).isEqualTo(TupleDomain.none());
                    assertThat(result.getRemainingFilter()).isEqualTo(TupleDomain.all());
                    assertThat(result.getRemainingExpression()).contains(TRUE);
                });
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testApplyProjectionValidatesInputsBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        ConnectorTableHandle wrongTableHandle = new ConnectorTableHandle() {};

        assertThatThrownBy(() -> metadata.applyProjection(null, tableHandle, List.of(new Variable("id", BIGINT)),
                Map.of("id", id)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.applyProjection(SESSION, wrongTableHandle, List.of(new Variable("id",
                BIGINT)), Map.of("id", id)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon projection pushdown requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.applyProjection(SESSION, tableHandle, null, Map.of("id", id)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("projections is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.applyProjection(SESSION, tableHandle, List.of(new Variable("id", BIGINT)),
                null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("assignments is null");
        assertThat(catalog.initialized).isFalse();

        ColumnHandle wrongColumnHandle = new ColumnHandle() {};
        assertThatThrownBy(() -> metadata.applyProjection(SESSION, tableHandle, List.of(new Variable("id", BIGINT)),
                Map.of("id", wrongColumnHandle)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon projection pushdown requires PaimonColumnHandle, got: %s",
                        wrongColumnHandle.getClass().getName());
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testApplyProjectionIsOrderSensitive()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        PaimonColumnHandle name = PaimonColumnHandle.of("name", DataTypes.STRING());
        PaimonTableHandle projectedHandle = new PaimonTableHandle("schema", "table", Map.of())
                .copy(Optional.of(List.of(id, name)));

        assertThat(metadata.applyProjection(SESSION, projectedHandle,
                List.of(new Variable("id", BIGINT), new Variable("name", io.trino.spi.type.VarcharType.VARCHAR)),
                assignments(id, name)))
                .isEmpty();

        assertThat(metadata.applyProjection(SESSION, projectedHandle,
                List.of(new Variable("name", io.trino.spi.type.VarcharType.VARCHAR), new Variable("id", BIGINT)),
                assignments(name, id)))
                .isPresent()
                .get()
                .satisfies(result -> assertThat(((PaimonTableHandle) result.getHandle()).getProjectedColumns())
                        .hasValueSatisfying(columns -> assertThat(columns).containsExactly(name, id)));
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testApplyProjectionUsesProjectionOrderInsteadOfAssignmentMapOrder()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        PaimonColumnHandle name = PaimonColumnHandle.of("name", DataTypes.STRING());
        PaimonTableHandle projectedHandle = new PaimonTableHandle("schema", "table", Map.of())
                .copy(Optional.of(List.of(id, name)));

        Map<String, ColumnHandle> assignments = new LinkedHashMap<>();
        assignments.put("name_7", name);
        assignments.put("id_8", id);

        assertThat(metadata.applyProjection(SESSION, projectedHandle,
                List.of(new Variable("id_8", BIGINT), new Variable("name_7", io.trino.spi.type.VarcharType.VARCHAR)),
                assignments))
                .isEmpty();
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testApplyProjectionDeduplicatesRepeatedProjectionVariables()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThat(metadata.applyProjection(SESSION, tableHandle,
                List.of(new Call(BIGINT, ADD_FUNCTION_NAME,
                        List.of(new Variable("id_8", BIGINT), new Variable("id_8", BIGINT)))),
                Map.of("id_8", id)))
                .isPresent()
                .get()
                .satisfies(result -> {
                    assertThat(((PaimonTableHandle) result.getHandle()).getProjectedColumns())
                            .hasValueSatisfying(columns -> assertThat(columns).containsExactly(id));
                    assertThat(result.getAssignments())
                            .extracting(Assignment::getVariable)
                            .containsExactly("id_8");
                });
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testApplyProjectionOrdersExpressionInputsByFirstUse()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        PaimonColumnHandle amount = PaimonColumnHandle.of("amount", DataTypes.BIGINT());
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        Map<String, ColumnHandle> assignments = new LinkedHashMap<>();
        assignments.put("amount_3", amount);
        assignments.put("id_8", id);

        assertThat(metadata.applyProjection(SESSION, tableHandle,
                List.of(new Call(BIGINT, ADD_FUNCTION_NAME,
                        List.of(new Variable("id_8", BIGINT), new Variable("amount_3", BIGINT)))),
                assignments))
                .isPresent()
                .get()
                .satisfies(result -> {
                    assertThat(((PaimonTableHandle) result.getHandle()).getProjectedColumns())
                            .hasValueSatisfying(columns -> assertThat(columns).containsExactly(id, amount));
                    assertThat(result.getAssignments())
                            .extracting(Assignment::getVariable)
                            .containsExactly("id_8", "amount_3");
                });
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testApplyLimitValidatesInputsBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        ConnectorTableHandle wrongTableHandle = new ConnectorTableHandle() {};

        assertThatThrownBy(() -> metadata.applyLimit(null, tableHandle, 10))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.applyLimit(SESSION, wrongTableHandle, 10))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon limit pushdown requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.applyLimit(SESSION, tableHandle, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("limit must be non-negative");
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testFinishCreateTableRequiresPaimonTableHandle()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());

        assertThat(PaimonMetadata.getOutputTableHandle(tableHandle)).isSameAs(tableHandle);

        assertThatThrownBy(() -> PaimonMetadata.getOutputTableHandle(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableHandle is null");

        ConnectorOutputTableHandle wrongHandle = new ConnectorOutputTableHandle() {};
        assertThatThrownBy(() -> PaimonMetadata.getOutputTableHandle(wrongHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon finish create table requires PaimonTableHandle, got: %s",
                        wrongHandle.getClass().getName());

        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        assertThatThrownBy(() -> metadata.finishCreateTable(SESSION, wrongHandle, List.of(), List.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon finish create table requires PaimonTableHandle, got: %s",
                        wrongHandle.getClass().getName());
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testFinishInsertRequiresPaimonTableHandle()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThat(PaimonMetadata.getInsertTableHandle(tableHandle)).isSameAs(tableHandle);

        assertThatThrownBy(() -> PaimonMetadata.getInsertTableHandle(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("insertHandle is null");

        ConnectorInsertTableHandle wrongHandle = new ConnectorInsertTableHandle() {};
        assertThatThrownBy(() -> PaimonMetadata.getInsertTableHandle(wrongHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon finish insert requires PaimonTableHandle, got: %s",
                        wrongHandle.getClass().getName());
    }

    @Test
    public void testFinishMergeRequiresPaimonTableHandle()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThat(PaimonMetadata.getMergeTableHandle(new PaimonMergeTableHandle(tableHandle))).isSameAs(tableHandle);

        assertThatThrownBy(() -> PaimonMetadata.getMergeTableHandle(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("mergeTableHandle is null");

        assertThatThrownBy(() -> PaimonMetadata.getMergeTableHandle(mergeTableHandle(null)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("mergeTableHandle tableHandle is null");

        ConnectorTableHandle wrongTableHandle = new ConnectorTableHandle() {};
        assertThatThrownBy(() -> PaimonMetadata.getMergeTableHandle(mergeTableHandle(wrongTableHandle)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon finish merge requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
    }

    @Test
    public void testMetadataTableHandleValidation()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThat(PaimonMetadata.getTableHandle("testing", tableHandle)).isSameAs(tableHandle);

        assertThatThrownBy(() -> PaimonMetadata.getTableHandle("testing", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableHandle is null");

        ConnectorTableHandle wrongTableHandle = new ConnectorTableHandle() {};
        assertThatThrownBy(() -> PaimonMetadata.getTableHandle("testing", wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon testing requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
    }

    @Test
    public void testMetadataColumnHandleValidation()
    {
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("id", DataTypes.INT());

        assertThat(PaimonMetadata.getColumnHandle("testing", columnHandle)).isSameAs(columnHandle);

        assertThatThrownBy(() -> PaimonMetadata.getColumnHandle("testing", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("columnHandle is null");

        ColumnHandle wrongColumnHandle = new ColumnHandle() {};
        assertThatThrownBy(() -> PaimonMetadata.getColumnHandle("testing", wrongColumnHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon testing requires PaimonColumnHandle, got: %s",
                        wrongColumnHandle.getClass().getName());
    }

    @Test
    public void testCommonMetadataEntrypointsRequirePaimonTableHandle()
    {
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED)),
                TESTING_TYPE_MANAGER);
        ConnectorTableHandle wrongTableHandle = new ConnectorTableHandle() {};
        TestingPaimonCatalog beginMergeCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata beginMergeMetadata = new PaimonMetadata(beginMergeCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog tableMetadataCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata tableMetadata = new PaimonMetadata(tableMetadataCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog columnHandlesCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata columnHandlesMetadata = new PaimonMetadata(columnHandlesCatalog, TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.getInsertLayout(SESSION, wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon insert layout requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.beginInsert(SESSION, wrongTableHandle,
                List.of(PaimonColumnHandle.of("id", DataTypes.INT())), RetryMode.NO_RETRIES))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon begin insert requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.getMergeRowIdColumnHandle(SESSION, wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon merge row id requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.getUpdateLayout(SESSION, wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon update layout requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.beginMerge(SESSION, wrongTableHandle, RetryMode.NO_RETRIES))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon begin merge requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> beginMergeMetadata.beginMerge(SESSION, wrongTableHandle, RetryMode.NO_RETRIES))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon begin merge requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThat(beginMergeCatalog.initialized).isFalse();
        assertThatThrownBy(() -> tableMetadata.getTableMetadata(SESSION, wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon table metadata requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThat(tableMetadataCatalog.initialized).isFalse();
        assertThatThrownBy(() -> columnHandlesMetadata.getColumnHandles(SESSION, wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon column handles requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThat(columnHandlesCatalog.initialized).isFalse();
    }

    @Test
    public void testColumnMetadataRequiresPaimonHandles()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        ColumnHandle wrongColumnHandle = new ColumnHandle() {};

        assertThatThrownBy(() -> metadata.getColumnMetadata(SESSION, new ConnectorTableHandle() {},
                PaimonColumnHandle.of("id", DataTypes.INT())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Paimon column metadata requires PaimonTableHandle");
        assertThatThrownBy(() -> metadata.getColumnMetadata(SESSION, tableHandle, wrongColumnHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon column metadata requires PaimonColumnHandle, got: %s",
                        wrongColumnHandle.getClass().getName());
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testGetColumnHandlesInitializesCatalogBeforeTableLookup()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(SESSION, tableHandle);

        assertThat(catalog.initialized).isTrue();
        assertThat(columnHandles.keySet()).containsExactly("id");
        assertThat(columnHandles.get("id")).isInstanceOf(PaimonColumnHandle.class);
    }

    @Test
    public void testCommonMetadataEntrypointsRejectNullSessionBeforeCatalogInitialization()
    {
        TestingPaimonCatalog tableMetadataCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata tableMetadata = new PaimonMetadata(tableMetadataCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog columnHandlesCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata columnHandlesMetadata = new PaimonMetadata(columnHandlesCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog schemaExistsCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata schemaExistsMetadata = new PaimonMetadata(schemaExistsCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog listSchemasCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata listSchemasMetadata = new PaimonMetadata(listSchemasCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog versionedTableHandleCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata versionedTableHandleMetadata = new PaimonMetadata(versionedTableHandleCatalog,
                TESTING_TYPE_MANAGER);
        TestingPaimonCatalog directTableHandleCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata directTableHandleMetadata = new PaimonMetadata(directTableHandleCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog tablePropertiesCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata tablePropertiesMetadata = new PaimonMetadata(tablePropertiesCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog rowChangeCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata rowChangeMetadata = new PaimonMetadata(rowChangeCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog insertLayoutCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata insertLayoutMetadata = new PaimonMetadata(insertLayoutCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog mergeRowIdCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata mergeRowIdMetadata = new PaimonMetadata(mergeRowIdCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog updateLayoutCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata updateLayoutMetadata = new PaimonMetadata(updateLayoutCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog beginInsertCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata beginInsertMetadata = new PaimonMetadata(beginInsertCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog beginMergeCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata beginMergeMetadata = new PaimonMetadata(beginMergeCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog columnMetadataCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata columnMetadata = new PaimonMetadata(columnMetadataCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog listTableColumnsCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata listTableColumnsMetadata = new PaimonMetadata(listTableColumnsCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog streamTableColumnsCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata streamTableColumnsMetadata = new PaimonMetadata(streamTableColumnsCatalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("id", DataTypes.INT());
        ConnectorTableHandle wrongTableHandle = new ConnectorTableHandle() {};
        SchemaTableName tableName = new SchemaTableName("schema", "table");

        assertThatThrownBy(() -> tableMetadata.getTableMetadata(null, tableHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(tableMetadataCatalog.initialized).isFalse();

        assertThatThrownBy(() -> columnHandlesMetadata.getColumnHandles(null, tableHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(columnHandlesCatalog.initialized).isFalse();

        assertThatThrownBy(() -> schemaExistsMetadata.schemaExists(null, "schema"))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(schemaExistsCatalog.initialized).isFalse();

        assertThatThrownBy(() -> schemaExistsMetadata.schemaExists(SESSION, " "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("schemaName cannot be null or empty");
        assertThat(schemaExistsCatalog.initialized).isFalse();

        assertThatThrownBy(() -> listSchemasMetadata.listSchemaNames(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(listSchemasCatalog.initialized).isFalse();

        assertThatThrownBy(() -> versionedTableHandleMetadata.getTableHandle(null, tableName,
                Optional.empty(), Optional.empty()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(versionedTableHandleCatalog.initialized).isFalse();

        assertThatThrownBy(() -> versionedTableHandleMetadata.getTableHandle(SESSION, null,
                Optional.empty(), Optional.empty()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableName is null");
        assertThat(versionedTableHandleCatalog.initialized).isFalse();

        assertThatThrownBy(() -> versionedTableHandleMetadata.getTableHandle(SESSION, tableName,
                null, Optional.empty()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("startVersion is null");
        assertThat(versionedTableHandleCatalog.initialized).isFalse();

        assertThatThrownBy(() -> versionedTableHandleMetadata.getTableHandle(SESSION, tableName,
                Optional.empty(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("endVersion is null");
        assertThat(versionedTableHandleCatalog.initialized).isFalse();

        assertTrinoError(() -> versionedTableHandleMetadata.getTableHandle(SESSION, tableName,
                Optional.empty(), Optional.of(new ConnectorTableVersion(PointerType.TARGET_ID, VARCHAR,
                        Slices.utf8Slice(" ")))),
                INVALID_ARGUMENTS.toErrorCode(), "Paimon table version may not be blank");
        assertThat(versionedTableHandleCatalog.initialized).isFalse();

        assertThat(versionedTableHandleMetadata.getTableHandle(SESSION, tableName,
                Optional.empty(), Optional.of(new ConnectorTableVersion(PointerType.TARGET_ID, INTEGER, 1L))))
                .isEqualTo(new PaimonTableHandle("schema", "table",
                        Map.of(CoreOptions.SCAN_VERSION.key(), "1")));
        assertThat(versionedTableHandleCatalog.initialized).isTrue();

        assertThat(versionedTableHandleMetadata.getTableHandle(SESSION, tableName,
                Optional.empty(), Optional.of(new ConnectorTableVersion(PointerType.TARGET_ID, VARCHAR,
                        Slices.utf8Slice("tag-1")))))
                .isEqualTo(new PaimonTableHandle("schema", "table",
                        Map.of(CoreOptions.SCAN_VERSION.key(), "tag-1")));
        assertThat(versionedTableHandleCatalog.initialized).isTrue();

        assertThatThrownBy(() -> directTableHandleMetadata.getTableHandle(null, tableName, Map.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(directTableHandleCatalog.initialized).isFalse();

        assertThatThrownBy(() -> directTableHandleMetadata.getTableHandle(SESSION, null, Map.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableName is null");
        assertThat(directTableHandleCatalog.initialized).isFalse();

        assertThatThrownBy(() -> directTableHandleMetadata.getTableHandle(SESSION, tableName, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("dynamicOptions is null");
        assertThat(directTableHandleCatalog.initialized).isFalse();

        Map<String, String> nullDynamicOptionKey = new HashMap<>();
        nullDynamicOptionKey.put(null, "value");
        assertThatThrownBy(() -> directTableHandleMetadata.getTableHandle(SESSION, tableName, nullDynamicOptionKey))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("dynamicOptions contains null key");
        assertThat(directTableHandleCatalog.initialized).isFalse();

        assertThatThrownBy(() -> directTableHandleMetadata.getTableHandle(SESSION, tableName, Map.of(" ", "value")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("dynamicOptions contains blank key");
        assertThat(directTableHandleCatalog.initialized).isFalse();

        Map<String, String> nullDynamicOptionValue = new HashMap<>();
        nullDynamicOptionValue.put(CoreOptions.SCAN_TAG_NAME.key(), null);
        assertThatThrownBy(() -> directTableHandleMetadata.getTableHandle(SESSION, tableName, nullDynamicOptionValue))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("dynamicOptions contains null value for key 'scan.tag-name'");
        assertThat(directTableHandleCatalog.initialized).isFalse();

        assertThatThrownBy(() -> directTableHandleMetadata.getTableHandle(SESSION, tableName,
                Map.of(CoreOptions.SCAN_TAG_NAME.key(), " ")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("dynamicOptions contains blank value for key 'scan.tag-name'");
        assertThat(directTableHandleCatalog.initialized).isFalse();

        assertThatThrownBy(() -> tablePropertiesMetadata.getTableProperties(null, tableHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(tablePropertiesCatalog.initialized).isFalse();

        assertThatThrownBy(() -> tablePropertiesMetadata.getTableProperties(SESSION, wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon table properties requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThat(tablePropertiesCatalog.initialized).isFalse();

        assertThatThrownBy(() -> rowChangeMetadata.getRowChangeParadigm(null, tableHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(rowChangeCatalog.initialized).isFalse();

        assertThatThrownBy(() -> rowChangeMetadata.getRowChangeParadigm(SESSION, wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon row change paradigm requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThat(rowChangeCatalog.initialized).isFalse();

        assertThatThrownBy(() -> insertLayoutMetadata.getInsertLayout(null, tableHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(insertLayoutCatalog.initialized).isFalse();

        assertThatThrownBy(() -> mergeRowIdMetadata.getMergeRowIdColumnHandle(null, tableHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(mergeRowIdCatalog.initialized).isFalse();

        assertThatThrownBy(() -> updateLayoutMetadata.getUpdateLayout(null, tableHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(updateLayoutCatalog.initialized).isFalse();

        assertThatThrownBy(() -> beginInsertMetadata.beginInsert(null, tableHandle,
                List.of(PaimonColumnHandle.of("id", DataTypes.INT())), RetryMode.NO_RETRIES))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(beginInsertCatalog.initialized).isFalse();

        assertThatThrownBy(() -> beginInsertMetadata.beginInsert(SESSION, tableHandle,
                List.of(PaimonColumnHandle.of("id", DataTypes.INT())), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("retryMode is null");
        assertThat(beginInsertCatalog.initialized).isFalse();

        assertTrinoError(() -> beginInsertMetadata.beginInsert(SESSION, tableHandle,
                List.of(PaimonColumnHandle.of("id", DataTypes.INT())), RetryMode.RETRIES_ENABLED),
                NOT_SUPPORTED.toErrorCode(), "This connector does not support query retries");
        assertThat(beginInsertCatalog.initialized).isFalse();

        assertThatThrownBy(() -> beginMergeMetadata.beginMerge(null, tableHandle, RetryMode.NO_RETRIES))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(beginMergeCatalog.initialized).isFalse();

        assertThatThrownBy(() -> beginMergeMetadata.beginMerge(SESSION, tableHandle, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("retryMode is null");
        assertThat(beginMergeCatalog.initialized).isFalse();

        assertThatThrownBy(() -> columnMetadata.getColumnMetadata(null, tableHandle, columnHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(columnMetadataCatalog.initialized).isFalse();

        assertThatThrownBy(() -> listTableColumnsMetadata.listTableColumns(null,
                new SchemaTablePrefix("schema", "table")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(listTableColumnsCatalog.initialized).isFalse();

        assertThatThrownBy(() -> streamTableColumnsMetadata.streamTableColumns(null,
                new SchemaTablePrefix("schema", "table")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(streamTableColumnsCatalog.initialized).isFalse();
    }

    @Test
    public void testDdlEntrypointsRequirePaimonTableHandle()
    {
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED)),
                TESTING_TYPE_MANAGER);
        ConnectorTableHandle wrongTableHandle = new ConnectorTableHandle() {};

        assertThatThrownBy(() -> metadata.setTableProperties(SESSION, wrongTableHandle,
                Map.of("bucket", Optional.of("4"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon set table properties requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.setTableProperties(null, new PaimonTableHandle("schema", "table", Map.of()),
                Map.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.setTableProperties(SESSION, new PaimonTableHandle("schema", "table", Map.of()),
                null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("properties is null");
        assertThatThrownBy(() -> metadata.renameTable(SESSION, wrongTableHandle,
                new SchemaTableName("schema", "target")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon rename table requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.dropTable(SESSION, wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon drop table requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.addColumn(SESSION, wrongTableHandle,
                new ColumnMetadata("id", INTEGER)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon add column requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.setTableComment(SESSION, wrongTableHandle, Optional.of("comment")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon set table comment requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.addField(SESSION, wrongTableHandle, List.of(), "nested", INTEGER, false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon add field requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.renameField(SESSION, wrongTableHandle, List.of("row", "field"), "renamed"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon rename field requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.setFieldType(SESSION, wrongTableHandle, List.of("row", "field"), INTEGER))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon set field type requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.truncateTable(SESSION, wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon truncate table requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.applyDelete(SESSION, wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon delete requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.executeDelete(SESSION, wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon delete requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
    }

    @Test
    public void testDdlEntrypointsRejectNullSessionBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("id", DataTypes.INT());

        assertThatThrownBy(() -> metadata.renameTable(null, tableHandle, new SchemaTableName("schema", "target")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.dropTable(null, tableHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.addColumn(null, tableHandle, new ColumnMetadata("value", INTEGER)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.renameColumn(null, tableHandle, columnHandle, "renamed"))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.dropColumn(null, tableHandle, columnHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.setTableComment(null, tableHandle, Optional.of("comment")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.setColumnComment(null, tableHandle, columnHandle, Optional.of("comment")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.setColumnType(null, tableHandle, columnHandle, INTEGER))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.dropNotNullConstraint(null, tableHandle, columnHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.addField(null, tableHandle, List.of(), "nested", INTEGER, false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.dropField(null, tableHandle, columnHandle, List.of("nested")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.renameField(null, tableHandle, List.of("row", "nested"), "renamed"))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.setFieldType(null, tableHandle, List.of("row", "nested"), INTEGER))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.truncateTable(null, tableHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.applyDelete(null, tableHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.executeDelete(null, tableHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testEmptySetTablePropertiesIsNoOp()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        metadata.setTableProperties(SESSION, tableHandle, Map.of());

        assertThat(catalog.initialized).isFalse();
        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testDdlColumnEntrypointsRequirePaimonColumnHandle()
    {
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED)),
                TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        ColumnHandle wrongColumnHandle = new ColumnHandle() {};

        assertThatThrownBy(() -> metadata.renameColumn(SESSION, tableHandle, wrongColumnHandle, "renamed"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon rename column requires PaimonColumnHandle, got: %s",
                        wrongColumnHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.dropColumn(SESSION, tableHandle, wrongColumnHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon drop column requires PaimonColumnHandle, got: %s",
                        wrongColumnHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.setColumnComment(SESSION, tableHandle, wrongColumnHandle,
                Optional.of("comment")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon set column comment requires PaimonColumnHandle, got: %s",
                        wrongColumnHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.setColumnType(SESSION, tableHandle, wrongColumnHandle, INTEGER))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon set column type requires PaimonColumnHandle, got: %s",
                        wrongColumnHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.dropNotNullConstraint(SESSION, tableHandle, wrongColumnHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon drop not null constraint requires PaimonColumnHandle, got: %s",
                        wrongColumnHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.dropField(SESSION, tableHandle, wrongColumnHandle, List.of("field")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon drop field requires PaimonColumnHandle, got: %s",
                        wrongColumnHandle.getClass().getName());
    }

    @Test
    public void testNestedFieldDdlUsesExplicitFieldPaths()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle rowColumn = PaimonColumnHandle.of("payload", DataTypes.ROW(
                DataTypes.FIELD(0, "zip", DataTypes.INT())));

        metadata.addField(SESSION, tableHandle, List.of("payload"), "city", INTEGER, false);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.AddColumn.class, change ->
                        assertThat(change.fieldNames()).containsExactly("payload", "city"));

        metadata.dropField(SESSION, tableHandle, rowColumn, List.of("zip"));
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.DropColumn.class, change ->
                        assertThat(change.fieldNames()).containsExactly("payload", "zip"));

        metadata.renameField(SESSION, tableHandle, List.of("payload", "zip"), "postal_code");
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.RenameColumn.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("payload", "zip");
                    assertThat(change.newName()).isEqualTo("postal_code");
                });

        metadata.setFieldType(SESSION, tableHandle, List.of("payload", "zip"), INTEGER);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.UpdateColumnType.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("payload", "zip");
                    assertThat(change.newDataType().getTypeRoot()).isEqualTo(DataTypeRoot.INTEGER);
                });
    }

    @Test
    public void testSetColumnCommentUsesPaimonCommentSchemaChange()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle column = PaimonColumnHandle.of("payload", DataTypes.BYTES());

        metadata.setColumnComment(SESSION, tableHandle, column, Optional.of("__BLOB_FIELD; display bytes"));

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.UpdateColumnComment.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("payload");
                    assertThat(change.newDescription()).isEqualTo("__BLOB_FIELD; display bytes");
                });
    }

    @Test
    public void testNestedFieldDdlRejectsMalformedPathsBeforeCatalogAlter()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle rowColumn = PaimonColumnHandle.of("payload", DataTypes.ROW(
                DataTypes.FIELD(0, "zip", DataTypes.INT())));

        assertThatThrownBy(() -> metadata.addField(SESSION, tableHandle, List.of("payload"), " ", INTEGER, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("fieldName contains blank field");
        assertThatThrownBy(() -> metadata.addField(SESSION, tableHandle, List.of("payload", " "), "city", INTEGER,
                false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("parentPath contains blank field");
        assertThatThrownBy(() -> metadata.addField(SESSION, tableHandle, Arrays.asList("payload", (String) null), "city",
                INTEGER, false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("parentPath contains null field");
        assertThatThrownBy(() -> metadata.dropField(SESSION, tableHandle, rowColumn, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("drop field fieldPath is null");
        assertThatThrownBy(() -> metadata.dropField(SESSION, tableHandle, rowColumn, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("drop field fieldPath is empty");
        assertThatThrownBy(() -> metadata.dropField(SESSION, tableHandle, rowColumn, List.of(" ")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("drop field fieldPath contains blank field");
        assertThatThrownBy(() -> metadata.renameField(SESSION, tableHandle, null, "renamed"))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("rename field fieldPath is null");
        assertThatThrownBy(() -> metadata.renameField(SESSION, tableHandle, List.of("payload"), "renamed"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("rename field fieldPath must include a column name and nested field");
        assertThatThrownBy(() -> metadata.renameField(SESSION, tableHandle, List.of("payload", " "), "renamed"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("rename field fieldPath contains blank field");
        assertThatThrownBy(() -> metadata.renameField(SESSION, tableHandle, List.of("payload", "zip"), " "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("target contains blank field");
        assertThatThrownBy(() -> metadata.setFieldType(SESSION, tableHandle, List.of("payload"), INTEGER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("set field type fieldPath must include a column name and nested field");
        assertThatThrownBy(() -> metadata.setFieldType(SESSION, tableHandle, Arrays.asList("payload", (String) null),
                INTEGER))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("set field type fieldPath contains null field");

        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testDdlRejectsMalformedArgumentsBeforeCatalogAlter()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle rowColumn = PaimonColumnHandle.of("payload", DataTypes.ROW(
                DataTypes.FIELD(0, "zip", DataTypes.INT())));

        assertThatThrownBy(() -> metadata.renameTable(SESSION, tableHandle, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("newTableName is null");
        assertThatThrownBy(() -> metadata.addColumn(SESSION, tableHandle, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("column is null");
        assertThatThrownBy(() -> metadata.renameColumn(SESSION, tableHandle, rowColumn, " "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("target contains blank field");
        assertThatThrownBy(() -> metadata.setTableComment(SESSION, tableHandle, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("comment is null");
        assertThatThrownBy(() -> metadata.setColumnComment(SESSION, tableHandle, rowColumn, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("comment is null");
        assertThatThrownBy(() -> metadata.setColumnType(SESSION, tableHandle, rowColumn, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("type is null");
        assertThatThrownBy(() -> metadata.addField(SESSION, tableHandle, List.of("payload"), "city", null, false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("type is null");
        assertThatThrownBy(() -> metadata.setFieldType(SESSION, tableHandle, List.of("payload", "zip"), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("type is null");

        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testSchemaAndCreateTableDdlRejectsMalformedArgumentsBeforeCatalogInitialization()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("id", INTEGER)));

        assertThatThrownBy(() -> metadata.createSchema(null, "schema", Map.of(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.setSchemaAuthorization(null, "schema",
                        new TrinoPrincipal(PrincipalType.USER, "schema_owner")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.createSchema(SESSION, "schema", null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("properties is null");
        assertThatThrownBy(() -> metadata.createSchema(SESSION, " ", Map.of(), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("schemaName cannot be null or empty");
        assertThatThrownBy(() -> metadata.setSchemaAuthorization(SESSION, "schema", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("principal is null");
        assertThatThrownBy(() -> metadata.setSchemaAuthorization(SESSION, " ",
                        new TrinoPrincipal(PrincipalType.USER, "schema_owner")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("schemaName cannot be null or empty");
        assertThatThrownBy(() -> metadata.dropSchema(null, "schema", false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.dropSchema(SESSION, " ", false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("schemaName cannot be null or empty");
        assertThatThrownBy(() -> metadata.listTables(null, Optional.of("schema")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.listTables(SESSION, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("schemaName is null");
        assertThatThrownBy(() -> metadata.listTables(SESSION, Optional.of(" ")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("schemaName cannot be null or empty");
        assertThatThrownBy(() -> metadata.createTable(null, tableMetadata, io.trino.spi.connector.SaveMode.FAIL))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.createTable(SESSION, null, io.trino.spi.connector.SaveMode.FAIL))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableMetadata is null");
        assertThatThrownBy(() -> metadata.createTable(SESSION, tableMetadata, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("saveMode is null");
        assertThatThrownBy(() -> metadata.beginCreateTable(null, tableMetadata, Optional.empty(),
                RetryMode.NO_RETRIES))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.beginCreateTable(SESSION, null, Optional.empty(), RetryMode.NO_RETRIES))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableMetadata is null");
        assertThatThrownBy(() -> metadata.beginCreateTable(SESSION, tableMetadata, null, RetryMode.NO_RETRIES))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("layout is null");
        assertThatThrownBy(() -> metadata.beginCreateTable(SESSION, tableMetadata, Optional.empty(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("retryMode is null");

        CapturingDdlCatalog retryCatalog = new CapturingDdlCatalog();
        PaimonMetadata retryMetadata = new PaimonMetadata(retryCatalog, TESTING_TYPE_MANAGER);
        assertTrinoError(() -> retryMetadata.beginCreateTable(SESSION, tableMetadata, Optional.empty(),
                RetryMode.RETRIES_ENABLED), NOT_SUPPORTED.toErrorCode(),
                "This connector does not support query retries");
        assertThat(retryCatalog.initialized).isFalse();
        assertThat(retryCatalog.createdSchema).isNull();

        ConnectorTableMetadata invalidProperties = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("id", INTEGER)),
                Map.of("bucket", List.of("not a string")));
        assertThatThrownBy(() -> metadata.createTable(SESSION, invalidProperties,
                io.trino.spi.connector.SaveMode.FAIL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("properties value for property 'bucket' must be a string");

        ConnectorTableMetadata invalidPrimaryKey = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("id", INTEGER)),
                Map.of(PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, List.of(" ")));
        assertThatThrownBy(() -> metadata.createTable(SESSION, invalidPrimaryKey,
                io.trino.spi.connector.SaveMode.FAIL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("primary_key contains blank value");

        ConnectorTableMetadata invalidPartitionedBy = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("id", INTEGER)),
                Map.of(PaimonTableOptions.PARTITIONED_BY_PROPERTY, List.of(1)));
        assertThatThrownBy(() -> metadata.createTable(SESSION, invalidPartitionedBy,
                io.trino.spi.connector.SaveMode.FAIL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("partitioned_by contains non-string value");

        assertThat(catalog.initialized).isFalse();
        assertThat(catalog.createdSchema).isNull();
        assertThat(catalog.createdDatabase).isNull();
        assertThat(catalog.droppedDatabase).isNull();
    }

    @Test
    public void testDdlErrorsUseTrinoErrorCodes()
    {
        PaimonMetadata metadata = new PaimonMetadata(new FailingDdlCatalog(), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("missing", DataTypes.INT());
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("id", INTEGER)));
        ConnectorTableMetadata missingSchemaTableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("missing_schema", "table"),
                List.of(new ColumnMetadata("id", INTEGER)));

        assertTrinoError(() -> metadata.createSchema(SESSION, "schema", Map.of(), null),
                SCHEMA_ALREADY_EXISTS.toErrorCode(), "Schema 'schema' already exists");
        assertTrinoError(() -> metadata.dropSchema(SESSION, "schema", false),
                SCHEMA_NOT_EMPTY.toErrorCode(), "Schema 'schema' is not empty");
        assertTrinoError(() -> metadata.createTable(SESSION, tableMetadata, io.trino.spi.connector.SaveMode.FAIL),
                TABLE_ALREADY_EXISTS.toErrorCode(), "Table 'schema.table' already exists");
        assertThatCode(() -> metadata.createTable(SESSION, tableMetadata, io.trino.spi.connector.SaveMode.IGNORE))
                .doesNotThrowAnyException();
        assertTrinoError(() -> metadata.createTable(SESSION, tableMetadata, io.trino.spi.connector.SaveMode.REPLACE),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon create or replace table 'schema.table' is not supported: replace is not supported");
        assertTrinoError(() -> metadata.createTable(SESSION, missingSchemaTableMetadata,
                io.trino.spi.connector.SaveMode.FAIL),
                SCHEMA_NOT_FOUND.toErrorCode(), "Schema 'missing_schema' does not exist");
        assertTrinoError(() -> metadata.renameTable(SESSION, tableHandle, new SchemaTableName("schema", "target")),
                TABLE_NOT_FOUND.toErrorCode(), "Table 'schema.table' does not exist");
        assertTrinoError(() -> metadata.dropTable(SESSION, tableHandle),
                TABLE_NOT_FOUND.toErrorCode(), "Table 'schema.table' does not exist");
        assertTrinoError(() -> metadata.addColumn(SESSION, tableHandle, new ColumnMetadata("existing", INTEGER)),
                COLUMN_ALREADY_EXISTS.toErrorCode(), "Column 'existing' already exists in table 'schema.table'");
        assertTrinoError(() -> metadata.addField(SESSION, tableHandle, List.of(), "existing", INTEGER, false),
                COLUMN_ALREADY_EXISTS.toErrorCode(), "Column 'existing' already exists in table 'schema.table'");
        assertThatCode(() -> metadata.addField(SESSION, tableHandle, List.of(), "existing", INTEGER, true))
                .doesNotThrowAnyException();
        assertTrinoError(() -> metadata.addField(SESSION, tableHandle, List.of(), "missing_table", INTEGER, true),
                TABLE_NOT_FOUND.toErrorCode(), "Table 'schema.table' does not exist");
        assertTrinoError(() -> metadata.renameColumn(SESSION, tableHandle, columnHandle, "renamed"),
                COLUMN_NOT_FOUND.toErrorCode(), "Column 'missing' does not exist in table 'schema.table'");
        assertTrinoError(() -> metadata.dropColumn(SESSION, tableHandle, columnHandle),
                COLUMN_NOT_FOUND.toErrorCode(), "Column 'missing' does not exist in table 'schema.table'");
        assertTrinoError(() -> metadata.setTableProperties(SESSION, tableHandle, Map.of("bucket", Optional.of("4"))),
                TABLE_NOT_FOUND.toErrorCode(), "Table 'schema.table' does not exist");
        assertTrinoError(() -> metadata.setTableComment(SESSION, tableHandle, Optional.of("comment")),
                TABLE_NOT_FOUND.toErrorCode(), "Table 'schema.table' does not exist");
        assertTrinoError(() -> metadata.truncateTable(SESSION, tableHandle),
                TABLE_NOT_FOUND.toErrorCode(), "Table 'schema.table' does not exist");
        assertTrinoError(() -> metadata.applyDelete(SESSION, tableHandle),
                TABLE_NOT_FOUND.toErrorCode(), "Table 'schema.table' does not exist");
        assertTrinoError(() -> metadata.executeDelete(SESSION, tableHandle),
                TABLE_NOT_FOUND.toErrorCode(), "Table 'schema.table' does not exist");
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("schema", "table"),
                Optional.empty(), Optional.of(new ConnectorTableVersion(PointerType.TARGET_ID, INTEGER, 1L))))
                .isNull();
        assertTrinoError(() -> metadata.listTables(SESSION, Optional.of("schema")),
                SCHEMA_NOT_FOUND.toErrorCode(), "Schema 'schema' does not exist");
    }

    @Test
    public void testTableColumnListingSkipsMissingExplicitTable()
    {
        PaimonMetadata metadata = new PaimonMetadata(new FailingDdlCatalog(), TESTING_TYPE_MANAGER);
        SchemaTablePrefix prefix = new SchemaTablePrefix("schema", "table");

        assertThat(metadata.listTableColumns(SESSION, prefix)).isEmpty();
        assertThat(metadata.streamTableColumns(SESSION, prefix).hasNext()).isFalse();
    }

    @Test
    public void testUnknownAlterFailureIsNotReportedAsUnsupported()
    {
        IllegalStateException failure = new IllegalStateException("catalog invariant broken");
        PaimonMetadata metadata = new PaimonMetadata(new RuntimeFailingAlterCatalog(failure), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThatThrownBy(() -> metadata.setTableProperties(SESSION, tableHandle, Map.of("bucket", Optional.of("4"))))
                .isSameAs(failure);
    }

    @Test
    public void testUnsupportedAlterFailureUsesNotSupported()
    {
        UnsupportedOperationException failure = new UnsupportedOperationException("Cannot change bucket when it is -1.");
        PaimonMetadata metadata = new PaimonMetadata(new RuntimeFailingAlterCatalog(failure), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(() -> metadata.setTableProperties(SESSION, tableHandle, Map.of("bucket", Optional.of("4"))),
                NOT_SUPPORTED.toErrorCode(), "Cannot change bucket when it is -1.");
    }

    @Test
    public void testCheckedAlterFailureUsesPaimonMetadataError()
    {
        IOException failure = new IOException("metastore I/O failed");
        PaimonMetadata metadata = new PaimonMetadata(new CheckedFailingAlterCatalog(failure), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThatThrownBy(() -> metadata.setTableProperties(SESSION, tableHandle, Map.of("bucket", Optional.of("4"))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to alter Paimon table 'schema.table'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
    }

    @Test
    public void testCheckedTruncateFailureUsesPaimonMetadataError()
    {
        IOException failure = new IOException("truncate metastore I/O failed");
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        FileStoreTable table = truncateFailingFileStoreTable(copiedWithLatestSchema, failure);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.truncateTable(SESSION, new PaimonTableHandle("schema", "table", Map.of())))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to truncate Paimon table 'table'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testCheckedExecuteDeleteFailureUsesPaimonMetadataError()
    {
        IOException failure = new IOException("delete metastore I/O failed");
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        FileStoreTable table = truncateFailingFileStoreTable(copiedWithLatestSchema, failure);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.executeDelete(SESSION, new PaimonTableHandle("schema", "table", Map.of())))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to delete rows from Paimon table 'table'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testCheckedCreateSchemaFailureUsesPaimonMetadataError()
    {
        IOException failure = new IOException("schema metastore I/O failed");
        PaimonMetadata metadata = new PaimonMetadata(new CheckedFailingSchemaCatalog(failure), TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.createSchema(SESSION, "schema", Map.of(), null))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to create Paimon schema 'schema'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
    }

    @Test
    public void testCheckedDropSchemaFailureUsesPaimonMetadataError()
    {
        IOException failure = new IOException("schema delete metastore I/O failed");
        PaimonMetadata metadata = new PaimonMetadata(new CheckedFailingDropSchemaCatalog(failure), TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.dropSchema(SESSION, "schema", false))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to drop Paimon schema 'schema'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
    }

    @Test
    public void testCheckedSetSchemaAuthorizationFailureUsesPaimonMetadataError()
    {
        IOException failure = new IOException("schema authorization metastore I/O failed");
        PaimonMetadata metadata = new PaimonMetadata(new CheckedFailingSchemaAuthorizationCatalog(failure),
                TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.setSchemaAuthorization(SESSION, "schema",
                        new TrinoPrincipal(PrincipalType.USER, "schema_owner")))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to set authorization on Paimon schema 'schema'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
    }

    @Test
    public void testCheckedCreateTableFailureUsesPaimonMetadataError()
    {
        IOException failure = new IOException("table create metastore I/O failed");
        PaimonMetadata metadata = new PaimonMetadata(new CheckedFailingCreateTableCatalog(failure), TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("id", INTEGER)));

        assertThatThrownBy(() -> metadata.createTable(SESSION, tableMetadata, io.trino.spi.connector.SaveMode.FAIL))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to create Paimon table 'schema.table'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
    }

    @Test
    public void testCheckedRenameTableFailureUsesPaimonMetadataError()
    {
        IOException failure = new IOException("table rename metastore I/O failed");
        PaimonMetadata metadata = new PaimonMetadata(new CheckedFailingRenameTableCatalog(failure), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThatThrownBy(() -> metadata.renameTable(SESSION, tableHandle, new SchemaTableName("schema", "target")))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to rename Paimon table 'schema.table' to 'schema.target'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
    }

    @Test
    public void testCheckedDropTableFailureUsesPaimonMetadataError()
    {
        IOException failure = new IOException("table drop metastore I/O failed");
        PaimonMetadata metadata = new PaimonMetadata(new CheckedFailingDropTableCatalog(failure), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThatThrownBy(() -> metadata.dropTable(SESSION, tableHandle))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to drop Paimon table 'schema.table'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
    }

    @Test
    public void testSetTablePropertiesUsesPaimonOptionKeys()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        metadata.setTableProperties(SESSION, tableHandle, Map.of(
                "variant_shredding_max_schema_width", Optional.of("64"),
                "vector_file_format", Optional.empty()));

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges).hasSize(2);
        assertThat(catalog.lastAlterChanges)
                .anySatisfy(change -> {
                    assertThat(change).isInstanceOf(SchemaChange.SetOption.class);
                    SchemaChange.SetOption setOption = (SchemaChange.SetOption) change;
                    assertThat(setOption.key()).isEqualTo(CoreOptions.VARIANT_SHREDDING_MAX_SCHEMA_WIDTH.key());
                    assertThat(setOption.value()).isEqualTo("64");
                })
                .anySatisfy(change -> {
                    assertThat(change).isInstanceOf(SchemaChange.RemoveOption.class);
                    SchemaChange.RemoveOption removeOption = (SchemaChange.RemoveOption) change;
                    assertThat(removeOption.key()).isEqualTo(CoreOptions.VECTOR_FILE_FORMAT.key());
                });
    }

    @Test
    public void testSetTablePropertiesRejectsLayoutProperties()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(() -> metadata.setTableProperties(SESSION, tableHandle, Map.of(
                PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, Optional.of(List.of("id")),
                PaimonTableOptions.PARTITIONED_BY_PROPERTY, Optional.of(List.of("dt")))),
                NOT_SUPPORTED.toErrorCode(),
                "The following properties cannot be updated: partitioned_by, primary_key");
        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testSetTablePropertiesRejectsRuntimeReadSelectors()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(() -> metadata.setTableProperties(SESSION, tableHandle, Map.of(
                "incremental_between", Optional.of("1,2"),
                "scan_snapshot_id", Optional.of("7"),
                "scan_version", Optional.of("tag-1"))),
                NOT_SUPPORTED.toErrorCode(),
                "The following properties cannot be updated: incremental_between, scan_snapshot_id, scan_version");
        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testSetTablePropertiesRejectsNullPropertyEntriesBeforeCatalogAlter()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        Map<String, Optional<Object>> nullKeyProperties = new HashMap<>();
        nullKeyProperties.put(null, Optional.of("64"));
        assertThatThrownBy(() -> metadata.setTableProperties(SESSION, tableHandle, nullKeyProperties))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("properties contains null property name");

        assertThatThrownBy(() -> metadata.setTableProperties(SESSION, tableHandle,
                Map.of(" ", Optional.of((Object) "64"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("properties contains blank property name");

        Map<String, Optional<Object>> nullOptionalProperties = new HashMap<>();
        nullOptionalProperties.put("vector_file_format", null);
        assertThatThrownBy(() -> metadata.setTableProperties(SESSION, tableHandle, nullOptionalProperties))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("properties contains null value for property 'vector_file_format'");

        assertThatThrownBy(() -> metadata.setTableProperties(SESSION, tableHandle,
                Map.of("vector_file_format", Optional.of((Object) List.of("lance")))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("properties value for property 'vector_file_format' must be a string");

        assertThatThrownBy(() -> metadata.setTableProperties(SESSION, tableHandle,
                Map.of("vector_file_format", Optional.of((Object) " "))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("properties value for property 'vector_file_format' is blank");

        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testCreateSchemaDoesNotIgnoreExistingSchema()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        metadata.createSchema(SESSION, "schema", Map.of(
                        LOCATION_PROPERTY, "s3://warehouse/schema",
                        COMMENT_PROPERTY, "schema comment"),
                new TrinoPrincipal(PrincipalType.USER, "schema_owner"));

        assertThat(catalog.createdDatabase).isEqualTo("schema");
        assertThat(catalog.createDatabaseIgnoreIfExists).isFalse();
        assertThat(catalog.createdDatabaseProperties).containsExactlyInAnyOrderEntriesOf(Map.of(
                LOCATION_PROPERTY, "s3://warehouse/schema",
                COMMENT_PROPERTY, "schema comment",
                OWNER_PROPERTY, "schema_owner"));
    }

    @Test
    public void testCreateSchemaRejectsMalformedPropertiesBeforeCatalogInitialization()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.createSchema(SESSION, "schema", Map.of(" ", "value"), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("properties contains blank property name");
        assertThatThrownBy(() -> metadata.createSchema(SESSION, "schema", Map.of("location", List.of("s3://warehouse")), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("properties value for property 'location' must be a string");
        assertThatThrownBy(() -> metadata.createSchema(SESSION, "schema", Map.of("location", " "), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("properties value for property 'location' is blank");

        assertThat(catalog.initialized).isFalse();
        assertThat(catalog.createdDatabase).isNull();
    }

    @Test
    public void testGetSchemaPropertiesAndOwner()
    {
        PaimonMetadata metadata = new PaimonMetadata(new SchemaPropertiesCatalog(), TESTING_TYPE_MANAGER);

        assertThat(metadata.getSchemaProperties(SESSION, "schema")).containsExactlyInAnyOrderEntriesOf(Map.of(
                LOCATION_PROPERTY, "s3://warehouse/schema",
                COMMENT_PROPERTY, "schema comment",
                OWNER_PROPERTY, "schema_owner"));
        assertThat(metadata.getSchemaOwner(SESSION, "schema"))
                .isEmpty();

        assertTrinoError(() -> metadata.getSchemaProperties(SESSION, SYSTEM_DATABASE_NAME),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon schema properties are not supported for the system schema 'sys'");
        assertTrinoError(() -> metadata.getSchemaProperties(SESSION, "missing"),
                SCHEMA_NOT_FOUND.toErrorCode(), "Schema 'missing' does not exist");
    }

    @Test
    public void testSetSchemaAuthorizationStoresOwnerProperty()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        metadata.setSchemaAuthorization(SESSION, "schema", new TrinoPrincipal(PrincipalType.USER, "new_owner"));

        assertThat(catalog.alteredDatabase).isEqualTo("schema");
        assertThat(catalog.alterDatabaseIgnoreIfNotExists).isFalse();
        assertThat(catalog.lastDatabasePropertyChanges)
                .singleElement()
                .isInstanceOfSatisfying(PropertyChange.SetProperty.class, change -> {
                    assertThat(change.property()).isEqualTo(OWNER_PROPERTY);
                    assertThat(change.value()).isEqualTo("new_owner");
                });
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testDropSchemaPreservesCascadeFlag()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        metadata.dropSchema(SESSION, "schema", false);
        assertThat(catalog.droppedDatabase).isEqualTo("schema");
        assertThat(catalog.dropDatabaseIgnoreIfNotExists).isFalse();
        assertThat(catalog.dropDatabaseCascade).isFalse();

        metadata.dropSchema(SESSION, "schema", true);
        assertThat(catalog.dropDatabaseCascade).isTrue();
    }

    @Test
    public void testSchemaExistsReturnsTrueAndFalse()
    {
        SchemaQueryCatalog catalog = new SchemaQueryCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        assertThat(metadata.schemaExists(SESSION, "existing_schema")).isTrue();
        assertThat(metadata.schemaExists(SESSION, "missing_schema")).isFalse();
    }

    @Test
    public void testListSchemaNames()
    {
        SchemaQueryCatalog catalog = new SchemaQueryCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        assertThat(metadata.listSchemaNames(SESSION)).containsExactly("alpha", "beta", SYSTEM_DATABASE_NAME);
    }

    @Test
    public void testDropSchemaTranslatesPaimonExceptions()
    {
        SchemaQueryCatalog notExistCatalog = new SchemaQueryCatalog() {
            @Override
            public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
                    throws Catalog.DatabaseNotExistException
            {
                throw new Catalog.DatabaseNotExistException(name);
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(notExistCatalog, TESTING_TYPE_MANAGER);
        assertTrinoError(() -> metadata.dropSchema(SESSION, "missing", false),
                SCHEMA_NOT_FOUND.toErrorCode(), "Schema 'missing' does not exist");

        SchemaQueryCatalog notEmptyCatalog = new SchemaQueryCatalog() {
            @Override
            public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
                    throws Catalog.DatabaseNotEmptyException
            {
                throw new Catalog.DatabaseNotEmptyException(name);
            }
        };
        PaimonMetadata metadata2 = new PaimonMetadata(notEmptyCatalog, TESTING_TYPE_MANAGER);
        assertTrinoError(() -> metadata2.dropSchema(SESSION, "nonempty", false),
                SCHEMA_NOT_EMPTY.toErrorCode(), "Schema 'nonempty' is not empty");

        SchemaQueryCatalog alterMissingCatalog = new SchemaQueryCatalog() {
            @Override
            public void alterDatabase(String name, List<PropertyChange> changes, boolean ignoreIfNotExists)
                    throws Catalog.DatabaseNotExistException
            {
                throw new Catalog.DatabaseNotExistException(name);
            }
        };
        PaimonMetadata metadata3 = new PaimonMetadata(alterMissingCatalog, TESTING_TYPE_MANAGER);
        assertTrinoError(() -> metadata3.setSchemaAuthorization(SESSION, "missing",
                        new TrinoPrincipal(PrincipalType.USER, "schema_owner")),
                SCHEMA_NOT_FOUND.toErrorCode(), "Schema 'missing' does not exist");
    }

    @Test
    public void testRenameTableSuccess()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        SchemaTableName newName = new SchemaTableName("schema", "renamed_table");

        metadata.renameTable(SESSION, tableHandle, newName);

        assertThat(catalog.renamedFromTable.getFullName()).isEqualTo("schema.table");
        assertThat(catalog.renamedToTable.getFullName()).isEqualTo("schema.renamed_table");
        assertThat(catalog.renamedIgnoreIfNotExists).isFalse();
    }

    @Test
    public void testRenameTableNotFound()
    {
        PaimonCatalog catalog = new PaimonCatalog(new Options(), unsupportedFileSystemFactory()) {
            @Override
            public void initSession(ConnectorSession connectorSession) {}

            @Override
            public Catalog forSession(ConnectorSession connectorSession)
            {
                return this;
            }

            @Override
            public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
                    throws Catalog.TableNotExistException
            {
                throw new Catalog.TableNotExistException(fromTable);
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(
                () -> metadata.renameTable(SESSION, tableHandle, new SchemaTableName("schema", "target")),
                TABLE_NOT_FOUND.toErrorCode(),
                "Table 'schema.table' does not exist");
    }

    @Test
    public void testRenameTableAlreadyExists()
    {
        PaimonCatalog catalog = new PaimonCatalog(new Options(), unsupportedFileSystemFactory()) {
            @Override
            public void initSession(ConnectorSession connectorSession) {}

            @Override
            public Catalog forSession(ConnectorSession connectorSession)
            {
                return this;
            }

            @Override
            public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
                    throws Catalog.TableAlreadyExistException
            {
                throw new Catalog.TableAlreadyExistException(toTable);
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(
                () -> metadata.renameTable(SESSION, tableHandle, new SchemaTableName("schema", "target")),
                TABLE_ALREADY_EXISTS.toErrorCode(),
                "Table 'schema.target' already exists");
    }

    @Test
    public void testDropTableSuccess()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        metadata.dropTable(SESSION, tableHandle);

        assertThat(catalog.droppedTable.getFullName()).isEqualTo("schema.table");
        assertThat(catalog.droppedTableIgnoreIfNotExists).isFalse();
    }

    @Test
    public void testDropTableNotFound()
    {
        PaimonCatalog catalog = new PaimonCatalog(new Options(), unsupportedFileSystemFactory()) {
            @Override
            public void initSession(ConnectorSession connectorSession) {}

            @Override
            public Catalog forSession(ConnectorSession connectorSession)
            {
                return this;
            }

            @Override
            public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
                    throws Catalog.TableNotExistException
            {
                throw new Catalog.TableNotExistException(identifier);
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(
                () -> metadata.dropTable(SESSION, tableHandle),
                TABLE_NOT_FOUND.toErrorCode(),
                "Table 'schema.table' does not exist");
    }

    @Test
    public void testListTablesWithSchema()
    {
        SchemaQueryCatalog catalog = new SchemaQueryCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        List<SchemaTableName> tables = metadata.listTables(SESSION, Optional.of("alpha"));
        assertThat(tables).containsExactly(
                new SchemaTableName("alpha", "t1"),
                new SchemaTableName("alpha", "t2"));
    }

    @Test
    public void testListTablesAllSchemas()
    {
        SchemaQueryCatalog catalog = new SchemaQueryCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        List<SchemaTableName> tables = metadata.listTables(SESSION, Optional.empty());
        assertThat(tables).containsExactly(
                new SchemaTableName("alpha", "t1"),
                new SchemaTableName("alpha", "t2"),
                new SchemaTableName("beta", "t3"),
                new SchemaTableName(SYSTEM_DATABASE_NAME, "tables"),
                new SchemaTableName(SYSTEM_DATABASE_NAME, "partitions"),
                new SchemaTableName(SYSTEM_DATABASE_NAME, "all_table_options"),
                new SchemaTableName(SYSTEM_DATABASE_NAME, "catalog_options"));
    }

    @Test
    public void testListTablesSchemaNotFound()
    {
        SchemaQueryCatalog catalog = new SchemaQueryCatalog() {
            @Override
            public List<String> listTables(String databaseName)
                    throws Catalog.DatabaseNotExistException
            {
                throw new Catalog.DatabaseNotExistException(databaseName);
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        assertTrinoError(
                () -> metadata.listTables(SESSION, Optional.of("nonexistent")),
                SCHEMA_NOT_FOUND.toErrorCode(),
                "Schema 'nonexistent' does not exist");
    }

    @Test
    public void testCreateTableSchemaNotFound()
    {
        PaimonCatalog catalog = new PaimonCatalog(new Options(), unsupportedFileSystemFactory()) {
            @Override
            public void initSession(ConnectorSession connectorSession) {}

            @Override
            public Catalog forSession(ConnectorSession connectorSession)
            {
                return this;
            }

            @Override
            public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
                    throws Catalog.DatabaseNotExistException
            {
                throw new Catalog.DatabaseNotExistException(identifier.getDatabaseName());
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("missing_schema", "table"),
                List.of(ColumnMetadata.builder().setName("id").setType(BIGINT).build()));

        assertTrinoError(
                () -> metadata.createTable(SESSION, tableMetadata, false),
                SCHEMA_NOT_FOUND.toErrorCode(),
                "Schema 'missing_schema' does not exist");
    }

    @Test
    public void testCreateTableAlreadyExists()
    {
        PaimonCatalog catalog = new PaimonCatalog(new Options(), unsupportedFileSystemFactory()) {
            @Override
            public void initSession(ConnectorSession connectorSession) {}

            @Override
            public Catalog forSession(ConnectorSession connectorSession)
            {
                return this;
            }

            @Override
            public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
                    throws Catalog.TableAlreadyExistException
            {
                throw new Catalog.TableAlreadyExistException(identifier);
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(ColumnMetadata.builder().setName("id").setType(BIGINT).build()));

        assertTrinoError(
                () -> metadata.createTable(SESSION, tableMetadata, false),
                TABLE_ALREADY_EXISTS.toErrorCode(),
                "Table 'schema.table' already exists");
    }

    @Test
    public void testCreateTableIgnoreIfExists()
    {
        PaimonCatalog catalog = new PaimonCatalog(new Options(), unsupportedFileSystemFactory()) {
            @Override
            public void initSession(ConnectorSession connectorSession) {}

            @Override
            public Catalog forSession(ConnectorSession connectorSession)
            {
                return this;
            }

            @Override
            public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
                    throws Catalog.TableAlreadyExistException
            {
                throw new Catalog.TableAlreadyExistException(identifier);
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(ColumnMetadata.builder().setName("id").setType(BIGINT).build()));

        // SaveMode.IGNORE should not throw when table already exists
        metadata.createTable(SESSION, tableMetadata, io.trino.spi.connector.SaveMode.IGNORE);
    }

    @Test
    public void testCreateTableReplaceModeUsesPaimonReplaceTable()
    {
        java.util.concurrent.atomic.AtomicBoolean replaced = new java.util.concurrent.atomic.AtomicBoolean();
        PaimonCatalog catalog = new PaimonCatalog(new Options(), unsupportedFileSystemFactory()) {
            @Override
            public void initSession(ConnectorSession connectorSession) {}

            @Override
            public Catalog forSession(ConnectorSession connectorSession)
            {
                return this;
            }

            @Override
            public void replaceTable(Identifier identifier, Schema newSchema, boolean ignoreIfNotExists)
            {
                assertThat(identifier.getFullName()).isEqualTo("schema.table");
                assertThat(ignoreIfNotExists).isFalse();
                replaced.set(true);
            }

            @Override
            public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            {
                throw new AssertionError("CREATE OR REPLACE TABLE should use Paimon replaceTable");
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(ColumnMetadata.builder().setName("id").setType(BIGINT).build()));

        metadata.createTable(SESSION, tableMetadata, io.trino.spi.connector.SaveMode.REPLACE);
        assertThat(replaced).isTrue();
    }

    @Test
    public void testCreateTableReplaceModeCreatesMissingTable()
    {
        java.util.concurrent.atomic.AtomicBoolean created = new java.util.concurrent.atomic.AtomicBoolean();
        PaimonCatalog catalog = new PaimonCatalog(new Options(), unsupportedFileSystemFactory()) {
            @Override
            public void initSession(ConnectorSession connectorSession) {}

            @Override
            public Catalog forSession(ConnectorSession connectorSession)
            {
                return this;
            }

            @Override
            public void replaceTable(Identifier identifier, Schema newSchema, boolean ignoreIfNotExists)
                    throws Catalog.TableNotExistException
            {
                assertThat(identifier.getFullName()).isEqualTo("schema.table");
                assertThat(ignoreIfNotExists).isFalse();
                throw new Catalog.TableNotExistException(identifier);
            }

            @Override
            public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            {
                assertThat(identifier.getFullName()).isEqualTo("schema.table");
                assertThat(ignoreIfExists).isFalse();
                created.set(true);
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(ColumnMetadata.builder().setName("id").setType(BIGINT).build()));

        metadata.createTable(SESSION, tableMetadata, io.trino.spi.connector.SaveMode.REPLACE);
        assertThat(created).isTrue();
    }

    @Test
    public void testSetTablePropertiesSuccess()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        metadata.setTableProperties(SESSION, tableHandle,
                Map.of("bucket", Optional.of((Object) "4"),
                        "removed_prop", Optional.empty()));

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges).hasSize(2);

        SchemaChange setChange = catalog.lastAlterChanges.stream()
                .filter(c -> c instanceof SchemaChange.SetOption).findFirst().orElseThrow();
        assertThat(setChange)
                .isInstanceOfSatisfying(SchemaChange.SetOption.class, change -> {
                    assertThat(change.key()).isEqualTo("bucket");
                    assertThat(change.value()).isEqualTo("4");
                });

        SchemaChange removeChange = catalog.lastAlterChanges.stream()
                .filter(c -> c instanceof SchemaChange.RemoveOption).findFirst().orElseThrow();
        assertThat(removeChange)
                .isInstanceOfSatisfying(SchemaChange.RemoveOption.class, change ->
                        assertThat(change.key()).isEqualTo("removed_prop"));
    }

    @Test
    public void testGetTableHandleRejectsStartVersion()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        assertTrinoError(
                () -> metadata.getTableHandle(SESSION, new SchemaTableName("schema", "table"),
                        Optional.of(new ConnectorTableVersion(PointerType.TARGET_ID, INTEGER, 1L)),
                        Optional.empty()),
                NOT_SUPPORTED.toErrorCode(),
                "Read paimon table with start version is not supported");
    }

    @Test
    public void testBeginCreateTableUsesCreatedPaimonSchemaForWriteColumns()
    {
        AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions = new AtomicReference<>();
        CreatedSchemaCatalog catalog = new CreatedSchemaCatalog(createdVectorAndBlobTable(copyWithoutTimeTravelOptions));
        PaimonMetadata metadata = new PaimonMetadata(catalog,
                TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(
                        ColumnMetadata.builder()
                                .setName("embedding")
                                .setType(new ArrayType(REAL))
                                .setComment(Optional.of("__VECTOR_FIELD;3; embedding"))
                                .build(),
                        ColumnMetadata.builder()
                                .setName("picture")
                                .setType(VarbinaryType.VARBINARY)
                                .setComment(Optional.of("__BLOB_FIELD; profile picture"))
                                .build()));

        ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(SESSION, tableMetadata, Optional.empty(),
                RetryMode.NO_RETRIES);

        PaimonTableHandle handle = (PaimonTableHandle) outputHandle;
        assertThat(handle.getWriteColumns()).hasValueSatisfying(writeColumns -> {
            assertThat(writeColumns).extracting(PaimonColumnHandle::getColumnName)
                    .containsExactly("embedding", "picture");
            assertThat(writeColumns).extracting(column -> column.logicalType().getTypeRoot())
                    .containsExactly(DataTypeRoot.VECTOR, DataTypeRoot.BLOB);
        });
        assertThat(copyWithoutTimeTravelOptions.get()).isNull();
        assertThat(catalog.createdSchema.fields()).extracting(field -> field.description())
                .containsExactly("__VECTOR_FIELD;3; embedding", "__BLOB_FIELD; profile picture");
    }

    @Test
    public void testBeginCreateTableMatchesCreatedPaimonSchemaCaseInsensitively()
    {
        CreatedSchemaCatalog catalog = new CreatedSchemaCatalog(createdLowerCaseTable());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(
                        new ColumnMetadata("ID", INTEGER),
                        new ColumnMetadata("VALUE", INTEGER)));

        ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(SESSION, tableMetadata, Optional.empty(),
                RetryMode.NO_RETRIES);

        PaimonTableHandle handle = (PaimonTableHandle) outputHandle;
        assertThat(handle.getWriteColumns()).hasValueSatisfying(writeColumns ->
                assertThat(writeColumns).extracting(PaimonColumnHandle::getColumnName)
                        .containsExactly("id", "value"));
    }

    @Test
    public void testBeginCreateTableRejectsDuplicateCaseInsensitiveCreatedFields()
    {
        CreatedSchemaCatalog catalog = new CreatedSchemaCatalog(createdDuplicateCaseTable());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("id", INTEGER)));

        assertThatThrownBy(() -> metadata.beginCreateTable(SESSION, tableMetadata, Optional.empty(),
                RetryMode.NO_RETRIES))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Created Paimon table 'schema.table' schema contains case-insensitive duplicate field name 'id'");
    }

    @Test
    public void testBeginCreateTablePreservesExternalStorageBlobDirective()
    {
        CreatedSchemaCatalog catalog = new CreatedSchemaCatalog(createdExternalStorageBlobTable());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(
                        ColumnMetadata.builder()
                                .setName("picture")
                                .setType(VarbinaryType.VARBINARY)
                                .setComment(Optional.of("__BLOB_EXTERNAL_STORAGE_FIELD; external picture"))
                                .build()),
                Map.of("blob_external_storage_path", "file:/tmp/blob-external"));

        ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(SESSION, tableMetadata, Optional.empty(),
                RetryMode.NO_RETRIES);

        PaimonTableHandle handle = (PaimonTableHandle) outputHandle;
        assertThat(catalog.createdSchema.options())
                .containsEntry(CoreOptions.BLOB_EXTERNAL_STORAGE_PATH.key(), "file:/tmp/blob-external");
        assertThat(catalog.createdSchema.fields()).extracting(field -> field.description())
                .containsExactly("__BLOB_EXTERNAL_STORAGE_FIELD; external picture");
        assertThat(handle.getWriteColumns()).hasValueSatisfying(writeColumns -> {
            assertThat(writeColumns).extracting(PaimonColumnHandle::getColumnName)
                    .containsExactly("picture");
            assertThat(writeColumns).extracting(column -> column.logicalType().getTypeRoot())
                    .containsExactly(DataTypeRoot.BLOB);
        });
    }

    @Test
    public void testCreateTablePreservesColumnNullability()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(
                        ColumnMetadata.builder()
                                .setName("nullable_col")
                                .setType(INTEGER)
                                .setNullable(true)
                                .build(),
                        ColumnMetadata.builder()
                                .setName("not_null_col")
                                .setType(INTEGER)
                                .setNullable(false)
                                .build()));

        metadata.createTable(SESSION, tableMetadata, io.trino.spi.connector.SaveMode.FAIL);

        assertThat(catalog.createdSchema.fields()).extracting(field -> field.type().isNullable())
                .containsExactly(true, false);
    }

    @Test
    public void testAddColumnRejectsNotNullBeforeCatalogAlter()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        ColumnMetadata column = ColumnMetadata.builder()
                .setName("required_value")
                .setType(INTEGER)
                .setNullable(false)
                .build();

        assertTrinoError(() -> metadata.addColumn(SESSION, tableHandle, column),
                NOT_SUPPORTED.toErrorCode(), "This connector does not support adding not null columns");
        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testAddColumnPreservesNullableTypeAndComment()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        ColumnMetadata column = ColumnMetadata.builder()
                .setName("embedding")
                .setType(new ArrayType(REAL))
                .setNullable(true)
                .setComment(Optional.of("__VECTOR_FIELD;3; added embedding"))
                .build();

        metadata.addColumn(SESSION, tableHandle, column);

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.AddColumn.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("embedding");
                    assertThat(change.dataType().isNullable()).isTrue();
                    assertThat(change.description()).isEqualTo("__VECTOR_FIELD;3; added embedding");
                });
    }

    @Test
    public void testRenameColumnUsesPaimonRenameSchemaChange()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("old_name", DataTypes.STRING());

        metadata.renameColumn(SESSION, tableHandle, columnHandle, "new_name");

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.RenameColumn.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("old_name");
                    assertThat(change.newName()).isEqualTo("new_name");
                });
    }

    @Test
    public void testDropColumnUsesPaimonDropSchemaChange()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("obsolete_col", DataTypes.STRING());

        metadata.dropColumn(SESSION, tableHandle, columnHandle);

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.DropColumn.class, change ->
                        assertThat(change.fieldNames()).containsExactly("obsolete_col"));
    }

    @Test
    public void testSetTableCommentUsesPaimonUpdateCommentSchemaChange()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        metadata.setTableComment(SESSION, tableHandle, Optional.of("table description"));
        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.UpdateComment.class, change ->
                        assertThat(change.comment()).isEqualTo("table description"));

        metadata.setTableComment(SESSION, tableHandle, Optional.empty());
        assertThat(catalog.alterCalls).isEqualTo(2);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.UpdateComment.class, change ->
                        assertThat(change.comment()).isNull());
    }

    @Test
    public void testSetColumnTypePreservesExistingPaimonNullability()
    {
        assertSetColumnTypePreservesExistingPaimonNullability(DataTypes.INT(), true);
        assertSetColumnTypePreservesExistingPaimonNullability(DataTypes.INT().notNull(), false);
    }

    private static void assertSetColumnTypePreservesExistingPaimonNullability(org.apache.paimon.types.DataType existingType,
            boolean expectedNullable)
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        metadata.setColumnType(SESSION, tableHandle, PaimonColumnHandle.of("id", existingType),
                io.trino.spi.type.BigintType.BIGINT);

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.UpdateColumnType.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("id");
                    assertThat(change.newDataType().getTypeRoot()).isEqualTo(DataTypeRoot.BIGINT);
                    assertThat(change.newDataType().isNullable()).isEqualTo(expectedNullable);
                    assertThat(change.keepNullability()).isTrue();
                });
    }

    @Test
    public void testDropNotNullConstraint()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("id", DataTypes.INT().notNull());

        metadata.dropNotNullConstraint(SESSION, tableHandle, columnHandle);

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.UpdateColumnNullability.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("id");
                    assertThat(change.newNullability()).isTrue();
                });
    }

    @Test
    public void testAddFieldSuccess()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        metadata.addField(SESSION, tableHandle, List.of("address"), "street", VARCHAR, false);

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.AddColumn.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("address", "street");
                    assertThat(change.dataType().getTypeRoot()).isEqualTo(DataTypeRoot.VARCHAR);
                });
    }

    @Test
    public void testAddFieldIgnoreExisting()
    {
        PaimonCatalog catalog = new PaimonCatalog(new Options(), unsupportedFileSystemFactory()) {
            @Override
            public void initSession(ConnectorSession connectorSession) {}

            @Override
            public Catalog forSession(ConnectorSession connectorSession)
            {
                return this;
            }

            @Override
            public void alterTable(Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
                    throws Catalog.ColumnAlreadyExistException
            {
                throw new Catalog.ColumnAlreadyExistException(identifier, ((SchemaChange.AddColumn) changes.get(0)).fieldNames()[0]);
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        // Should not throw when ignoreExisting=true
        metadata.addField(SESSION, tableHandle, List.of("address"), "street", VARCHAR, true);
    }

    @Test
    public void testDropFieldSuccess()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("address", DataTypes.ROW(DataTypes.FIELD(0, "street", DataTypes.STRING())));

        metadata.dropField(SESSION, tableHandle, columnHandle, List.of("street"));

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.DropColumn.class, change ->
                        assertThat(change.fieldNames()).containsExactly("address", "street"));
    }

    @Test
    public void testRenameFieldSuccess()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        metadata.renameField(SESSION, tableHandle, List.of("address", "street"), "road");

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.RenameColumn.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("address", "street");
                    assertThat(change.newName()).isEqualTo("road");
                });
    }

    @Test
    public void testSetFieldTypeSuccess()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        metadata.setFieldType(SESSION, tableHandle, List.of("address", "zip"), BIGINT);

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.UpdateColumnType.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("address", "zip");
                    assertThat(change.newDataType().getTypeRoot()).isEqualTo(DataTypeRoot.BIGINT);
                });
    }

    private static void assertUnsupportedFileStoreTable(Runnable call, String message)
    {
        assertThatThrownBy(call::run)
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessageContaining(message);
                });
    }

    private static void assertTrinoError(Runnable call, io.trino.spi.ErrorCode errorCode, String message)
    {
        assertThatThrownBy(call::run)
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(errorCode);
                    assertThat(exception).hasMessage(message);
                });
    }

    private static Map<String, ColumnHandle> assignments(PaimonColumnHandle first, PaimonColumnHandle second)
    {
        Map<String, ColumnHandle> assignments = new LinkedHashMap<>();
        assignments.put(first.getColumnName(), first);
        assignments.put(second.getColumnName(), second);
        return assignments;
    }

    private static FileStoreTable fileStoreTable(BucketMode bucketMode)
    {
        org.apache.paimon.types.RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
        return fileStoreTable(bucketMode, new AtomicBoolean(), rowType, rowType, List.of("id"));
    }

    private static FileStoreTable fileStoreTable(BucketMode bucketMode, AtomicBoolean copiedWithLatestSchema,
            org.apache.paimon.types.RowType rowType, org.apache.paimon.types.RowType latestRowType)
    {
        return fileStoreTable(bucketMode, copiedWithLatestSchema, rowType, latestRowType, List.of("id"));
    }

    private static FileStoreTable fileStoreTable(BucketMode bucketMode, AtomicBoolean copiedWithLatestSchema,
            org.apache.paimon.types.RowType rowType, org.apache.paimon.types.RowType latestRowType,
            List<String> primaryKeys)
    {
        return fileStoreTable(bucketMode, copiedWithLatestSchema, rowType, latestRowType, List.of("id"), primaryKeys,
                "id");
    }

    private static FileStoreTable fileStoreTable(BucketMode bucketMode, AtomicBoolean copiedWithLatestSchema,
            org.apache.paimon.types.RowType rowType, org.apache.paimon.types.RowType latestRowType,
            List<String> partitionKeys, List<String> primaryKeys)
    {
        return fileStoreTable(bucketMode, copiedWithLatestSchema, rowType, latestRowType, partitionKeys, primaryKeys,
                "id");
    }

    private static FileStoreTable fileStoreTable(BucketMode bucketMode, AtomicBoolean copiedWithLatestSchema,
            org.apache.paimon.types.RowType rowType, org.apache.paimon.types.RowType latestRowType,
            List<String> primaryKeys, String bucketKey)
    {
        return fileStoreTable(bucketMode, copiedWithLatestSchema, rowType, latestRowType, List.of("id"), primaryKeys,
                bucketKey);
    }

    private static FileStoreTable fileStoreTable(BucketMode bucketMode, AtomicBoolean copiedWithLatestSchema,
            org.apache.paimon.types.RowType rowType, org.apache.paimon.types.RowType latestRowType,
            List<String> partitionKeys, List<String> primaryKeys, String bucketKey)
    {
        return fileStoreTable(bucketMode, copiedWithLatestSchema, rowType, latestRowType, partitionKeys, primaryKeys,
                bucketKey, Map.of());
    }

    private static FileStoreTable fileStoreTable(BucketMode bucketMode, AtomicBoolean copiedWithLatestSchema,
            org.apache.paimon.types.RowType rowType, org.apache.paimon.types.RowType latestRowType,
            List<String> partitionKeys, List<String> primaryKeys, String bucketKey, Map<String, String> options)
    {
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "bucketMode" -> bucketMode;
                    case "name" -> "testing_file_store_table";
                    case "rowType" -> rowType;
                    case "partitionKeys" -> partitionKeys;
                    case "primaryKeys" -> primaryKeys;
                    case "comment" -> Optional.empty();
                    case "options" -> options;
                    case "coreOptions" -> new CoreOptions(new Options(options));
                    case "schema" -> TableSchema.create(1, new Schema(
                            rowType.getFields(),
                            partitionKeys,
                            primaryKeys,
                            mergeOptions(options, Map.of(
                                    CoreOptions.BUCKET.key(), "7",
                                    CoreOptions.BUCKET_KEY.key(), bucketKey)),
                            ""));
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield fileStoreTable(bucketMode, copiedWithLatestSchema, latestRowType, latestRowType,
                                partitionKeys, primaryKeys, bucketKey, options);
                    }
                    case "copy", "copyWithoutTimeTravel" -> proxy;
                    case "toString" -> "testing-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Map<String, String> mergeOptions(Map<String, String> first, Map<String, String> second)
    {
        Map<String, String> result = new HashMap<>();
        result.putAll(first);
        result.putAll(second);
        return Map.copyOf(result);
    }

    private static FileStoreTable nonSerializableSchemaFileStoreTable(IOException failure)
    {
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "bucketMode" -> BucketMode.HASH_FIXED;
                    case "name" -> "non-serializable-schema-file-store-table";
                    case "rowType" -> DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
                    case "partitionKeys" -> List.of();
                    case "primaryKeys" -> List.of("id");
                    case "comment" -> Optional.empty();
                    case "options" -> Map.of();
                    case "coreOptions" -> new CoreOptions(new Options(Map.of()));
                    case "schema" -> {
                        TableSchema schema = TableSchema.create(1, new Schema(
                                List.of(new DataField(0, "id", DataTypes.INT())),
                                List.of(),
                                List.of("id"),
                                Map.of(CoreOptions.BUCKET.key(), "7", CoreOptions.BUCKET_KEY.key(), "id"),
                                ""));
                        yield new TableSchema(
                                schema.version(),
                                schema.id(),
                                schema.fields(),
                                schema.highestFieldId(),
                                schema.partitionKeys(),
                                schema.primaryKeys(),
                                schema.options(),
                                schema.comment(),
                                schema.timeMillis()) {
                            private Object writeReplace()
                                    throws IOException
                            {
                                throw failure;
                            }
                        };
                    }
                    case "copyWithLatestSchema", "copy", "copyWithoutTimeTravel" -> proxy;
                    case "toString" -> "non-serializable-schema-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static FileStoreTable rowTrackingFileStoreTable(AtomicBoolean copiedWithLatestSchema,
            org.apache.paimon.types.RowType rowType)
    {
        org.apache.paimon.types.RowType latestRowType = rowType;
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "rowType" -> rowType;
                    case "partitionKeys" -> List.of();
                    case "primaryKeys" -> List.of();
                    case "schema" -> TableSchema.create(1, new Schema(
                            rowType.getFields(),
                            List.of(),
                            List.of(),
                            Map.of(CoreOptions.ROW_TRACKING_ENABLED.key(), "true"),
                            ""));
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield rowTrackingFileStoreTable(copiedWithLatestSchema, latestRowType);
                    }
                    case "copy", "copyWithoutTimeTravel" -> proxy;
                    case "options" -> Map.of(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
                    case "coreOptions" -> new CoreOptions(new Options(Map.of(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")));
                    case "toString" -> "testing-row-tracking-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static FileStoreTable sequenceNumberEnabledFileStoreTable(AtomicBoolean copiedWithLatestSchema,
            org.apache.paimon.types.RowType rowType)
    {
        org.apache.paimon.types.RowType latestRowType = rowType;
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.TABLE_READ_SEQUENCE_NUMBER_ENABLED.key(), "true");
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "rowType" -> rowType;
                    case "partitionKeys" -> List.of("pt");
                    case "primaryKeys" -> List.of("pk", "pt");
                    case "schema" -> TableSchema.create(1, new Schema(
                            rowType.getFields(),
                            List.of("pt"),
                            List.of("pk", "pt"),
                            Map.of(CoreOptions.TABLE_READ_SEQUENCE_NUMBER_ENABLED.key(), "true"),
                            ""));
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield sequenceNumberEnabledFileStoreTable(copiedWithLatestSchema, latestRowType);
                    }
                    case "copy", "copyWithoutTimeTravel" -> proxy;
                    case "options" -> options;
                    case "coreOptions" -> new CoreOptions(new Options(
                            Map.of(CoreOptions.TABLE_READ_SEQUENCE_NUMBER_ENABLED.key(), "true")));
                    case "toString" -> "testing-sequence-number-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static FileStoreTable commitFileStoreTable(AtomicBoolean copiedWithLatestSchema, AtomicBoolean committed)
    {
        return commitFileStoreTable(copiedWithLatestSchema, committed, new AtomicReference<>(), null);
    }

    private static FileStoreTable writePlanningFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions)
    {
        org.apache.paimon.types.RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "pt", DataTypes.STRING()));
        AtomicReference<FileStoreTable> latestTableRef = new AtomicReference<>();
        FileStoreTable latestTable = (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "bucketMode" -> BucketMode.HASH_FIXED;
                    case "name" -> "latest-write-planning-file-store-table";
                    case "rowType" -> rowType;
                    case "partitionKeys" -> List.of("pt");
                    case "primaryKeys" -> List.of("id");
                    case "comment" -> Optional.empty();
                    case "options" -> Map.of();
                    case "coreOptions" -> new CoreOptions(new Options(Map.of()));
                    case "schema" -> TableSchema.create(1, new Schema(
                            rowType.getFields(),
                            List.of("pt"),
                            List.of("id"),
                            Map.of(
                                    CoreOptions.BUCKET.key(), "7",
                                    CoreOptions.BUCKET_KEY.key(), "id"),
                            ""));
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield proxy;
                    }
                    case "copy" -> proxy;
                    case "copyWithoutTimeTravel" -> {
                        copyWithoutTimeTravelOptions.set(Map.copyOf((Map<String, String>) args[0]));
                        yield proxy;
                    }
                    case "toString" -> "latest-write-planning-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        latestTableRef.set(latestTable);
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield latestTableRef.get();
                    }
                    case "copy" -> proxy;
                    case "copyWithoutTimeTravel" -> {
                        copyWithoutTimeTravelOptions.set(Map.copyOf((Map<String, String>) args[0]));
                        yield latestTableRef.get();
                    }
                    case "bucketMode" -> BucketMode.HASH_FIXED;
                    case "name" -> "stale-write-planning-file-store-table";
                    case "rowType" -> rowType;
                    case "partitionKeys" -> List.of("pt");
                    case "primaryKeys" -> List.of("id");
                    case "comment" -> Optional.empty();
                    case "options" -> Map.of();
                    case "coreOptions" -> new CoreOptions(new Options(Map.of()));
                    case "schema" -> TableSchema.create(1, new Schema(
                            rowType.getFields(),
                            List.of("pt"),
                            List.of("id"),
                            Map.of(
                                    CoreOptions.BUCKET.key(), "7",
                                    CoreOptions.BUCKET_KEY.key(), "id"),
                            ""));
                    case "toString" -> "stale-write-planning-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static FileStoreTable commitFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicBoolean committed,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions)
    {
        return commitFileStoreTable(copiedWithLatestSchema, committed, copyWithoutTimeTravelOptions, null);
    }

    private static FileStoreTable commitFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicBoolean committed,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions,
            RuntimeException commitFailure)
    {
        return commitFileStoreTable(copiedWithLatestSchema, committed, copyWithoutTimeTravelOptions, commitFailure,
                new AtomicBoolean());
    }

    private static FileStoreTable commitFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicBoolean committed,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions,
            RuntimeException commitFailure,
            AtomicBoolean overwriteEnabled)
    {
        return commitFileStoreTable(
                copiedWithLatestSchema,
                committed,
                copyWithoutTimeTravelOptions,
                commitFailure,
                overwriteEnabled,
                List.of(),
                List.of(),
                Map.of());
    }

    private static FileStoreTable commitFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicBoolean committed,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions,
            RuntimeException commitFailure,
            AtomicBoolean overwriteEnabled,
            List<PartitionEntry> existingPartitions,
            List<String> partitionKeys,
            Map<String, String> options)
    {
        AtomicReference<FileStoreTable> latestTableRef = new AtomicReference<>();
        Object snapshotReader = Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {org.apache.paimon.table.source.snapshot.SnapshotReader.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "partitionEntries" -> existingPartitions;
                    case "toString" -> "testing-snapshot-reader";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        BatchTableCommit commit = (BatchTableCommit) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {BatchTableCommit.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "commit" -> {
                        assertThat(args).hasSize(1);
                        assertThat(args[0]).isInstanceOf(List.class);
                        if (commitFailure != null) {
                            throw commitFailure;
                        }
                        committed.set(true);
                        yield null;
                    }
                    case "close", "abort", "withMetricRegistry" -> proxy;
                    case "toString" -> "testing-batch-table-commit";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        BatchWriteBuilder batchWriteBuilder = (BatchWriteBuilder) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {BatchWriteBuilder.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "newCommit" -> commit;
                    case "withOverwrite" -> {
                        overwriteEnabled.set(true);
                        yield proxy;
                    }
                    case "tableName" -> "testing";
                    case "rowType" -> DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
                    case "newWriteSelector" -> Optional.empty();
                    case "toString" -> "testing-batch-write-builder";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        FileStoreTable latestTable = (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "newBatchWriteBuilder" -> batchWriteBuilder;
                    case "newSnapshotReader" -> snapshotReader;
                    case "partitionKeys" -> partitionKeys;
                    case "coreOptions" -> new CoreOptions(new Options(options));
                    case "schema" -> TableSchema.create(1, new Schema(
                            DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())).getFields(),
                            partitionKeys,
                            List.of(),
                            options,
                            ""));
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield proxy;
                    }
                    case "copy" -> proxy;
                    case "copyWithoutTimeTravel" -> {
                        copyWithoutTimeTravelOptions.set(Map.copyOf((Map<String, String>) args[0]));
                        yield proxy;
                    }
                    case "toString" -> "latest-testing-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        latestTableRef.set(latestTable);
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield latestTableRef.get();
                    }
                    case "copy" -> proxy;
                    case "copyWithoutTimeTravel" -> {
                        copyWithoutTimeTravelOptions.set(Map.copyOf((Map<String, String>) args[0]));
                        yield latestTableRef.get();
                    }
                    case "partitionKeys" -> partitionKeys;
                    case "coreOptions" -> new CoreOptions(new Options(options));
                    case "schema" -> TableSchema.create(1, new Schema(
                            DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())).getFields(),
                            partitionKeys,
                            List.of(),
                            options,
                            ""));
                    case "newSnapshotReader" -> snapshotReader;
                    case "newBatchWriteBuilder" -> throw new AssertionError(
                            "stale FileStoreTable should not create BatchWriteBuilder before latest-schema refresh");
                    case "toString" -> "stale-testing-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static FileStoreTable truncateFileStoreTable(AtomicBoolean copiedWithLatestSchema, AtomicBoolean truncated)
    {
        AtomicReference<FileStoreTable> latestTableRef = new AtomicReference<>();
        BatchTableCommit commit = (BatchTableCommit) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {BatchTableCommit.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "truncateTable" -> {
                        truncated.set(true);
                        yield null;
                    }
                    case "close", "abort", "withMetricRegistry" -> proxy;
                    case "toString" -> "testing-truncate-batch-table-commit";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        BatchWriteBuilder batchWriteBuilder = (BatchWriteBuilder) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {BatchWriteBuilder.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "newCommit" -> commit;
                    case "withOverwrite" -> proxy;
                    case "tableName" -> "testing";
                    case "rowType" -> DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
                    case "newWriteSelector" -> Optional.empty();
                    case "toString" -> "testing-truncate-batch-write-builder";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        FileStoreTable latestTable = (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "newBatchWriteBuilder" -> batchWriteBuilder;
                    case "copyWithLatestSchema", "copy", "copyWithoutTimeTravel" -> proxy;
                    case "toString" -> "latest-truncate-testing-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        latestTableRef.set(latestTable);
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield latestTableRef.get();
                    }
                    case "copy", "copyWithoutTimeTravel" -> proxy;
                    case "newBatchWriteBuilder" -> throw new AssertionError(
                            "stale FileStoreTable should not create BatchWriteBuilder before latest-schema refresh");
                    case "toString" -> "stale-truncate-testing-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static FileStoreTable truncateFailingFileStoreTable(AtomicBoolean copiedWithLatestSchema, IOException failure)
    {
        AtomicReference<FileStoreTable> latestTableRef = new AtomicReference<>();
        BatchTableCommit commit = (BatchTableCommit) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {BatchTableCommit.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "truncateTable" -> throw new RuntimeException(failure);
                    case "close", "abort", "withMetricRegistry" -> proxy;
                    case "toString" -> "testing-failing-truncate-batch-table-commit";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        BatchWriteBuilder batchWriteBuilder = (BatchWriteBuilder) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {BatchWriteBuilder.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "newCommit" -> commit;
                    case "withOverwrite" -> proxy;
                    case "tableName" -> "testing";
                    case "rowType" -> DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
                    case "newWriteSelector" -> Optional.empty();
                    case "toString" -> "testing-failing-truncate-batch-write-builder";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        FileStoreTable latestTable = (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "newBatchWriteBuilder" -> batchWriteBuilder;
                    case "copyWithLatestSchema", "copy", "copyWithoutTimeTravel" -> proxy;
                    case "toString" -> "latest-failing-truncate-testing-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        latestTableRef.set(latestTable);
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield latestTableRef.get();
                    }
                    case "copy", "copyWithoutTimeTravel" -> proxy;
                    case "newBatchWriteBuilder" -> throw new AssertionError(
                            "stale FileStoreTable should not create BatchWriteBuilder before latest-schema refresh");
                    case "toString" -> "stale-failing-truncate-testing-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static TableSchema partitioningSchema(ConnectorPartitioningHandle partitioningHandle)
    {
        assertThat(partitioningHandle).isInstanceOf(PaimonPartitioningHandle.class);
        return ((PaimonPartitioningHandle) partitioningHandle).getOriginalSchema();
    }

    private static FileStoreTable createdVectorAndBlobTable(
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions)
    {
        org.apache.paimon.types.RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "embedding", DataTypes.VECTOR(3, DataTypes.FLOAT())),
                DataTypes.FIELD(1, "picture", DataTypes.BLOB()));
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "copyWithoutTimeTravel" -> {
                        copyWithoutTimeTravelOptions.set(Map.copyOf((Map<String, String>) args[0]));
                        yield proxy;
                    }
                    case "rowType" -> rowType;
                    case "toString" -> "created-vector-and-blob-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Table createdLowerCaseTable()
    {
        org.apache.paimon.types.RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "value", DataTypes.INT()));
        return (Table) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "rowType" -> rowType;
                    case "toString" -> "created-lower-case-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Table createdDuplicateCaseTable()
    {
        org.apache.paimon.types.RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "ID", DataTypes.INT()));
        return (Table) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "rowType" -> rowType;
                    case "toString" -> "created-duplicate-case-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Table createdExternalStorageBlobTable()
    {
        org.apache.paimon.types.RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "picture", DataTypes.BLOB()));
        return (Table) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "rowType" -> rowType;
                    case "toString" -> "created-external-storage-blob-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Slice commitFragment()
            throws IOException
    {
        return commitFragment(BinaryRow.EMPTY_ROW);
    }

    private static Slice commitFragment(BinaryRow partition)
            throws IOException
    {
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        return Slices.wrappedBuffer(serializer.serialize(new CommitMessageImpl(
                partition,
                0,
                null,
                DataIncrement.emptyIncrement(),
                CompactIncrement.emptyIncrement())));
    }

    private static BinaryRow partitionRow(String value)
    {
        return new InternalRowSerializer(DataTypes.ROW(DataTypes.FIELD(0, "pt", DataTypes.STRING())))
                .toBinaryRow(org.apache.paimon.data.GenericRow.of(org.apache.paimon.data.BinaryString.fromString(value)));
    }

    private static ConnectorMergeTableHandle mergeTableHandle(ConnectorTableHandle tableHandle)
    {
        return () -> tableHandle;
    }

    private static Table table()
    {
        return (Table) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "toString" -> "testing-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Table statisticsTable(org.apache.paimon.types.RowType rowType, Optional<Statistics> statistics)
    {
        return (Table) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "rowType" -> rowType;
                    case "statistics" -> statistics;
                    case "toString" -> "statistics-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Table failingStatisticsTable(org.apache.paimon.types.RowType rowType)
    {
        return (Table) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "rowType" -> rowType;
                    case "statistics" -> throw new RuntimeException("stats file is unreadable");
                    case "toString" -> "failing-statistics-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static InnerTable innerTable()
    {
        return (InnerTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {InnerTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "toString" -> "testing-inner-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static class TestingPaimonCatalog
            extends PaimonCatalog
    {
        private final Table table;
        private boolean initialized;

        private TestingPaimonCatalog(Table table)
        {
            super(new Options(), unsupportedFileSystemFactory());
            this.table = table;
        }

        @Override
        public void initSession(ConnectorSession connectorSession)
        {
            assertThat(connectorSession).isNotNull();
            initialized = true;
        }

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            assertThat(connectorSession).isNotNull();
            initialized = true;
            return this;
        }

        @Override
        public Table getTable(Identifier identifier)
        {
            assertThat(initialized).isTrue();
            assertThat(identifier.getDatabaseName()).isEqualTo("schema");
            assertThat(identifier.getObjectName()).isEqualTo("table");
            return table;
        }
    }

    private static class FailingDdlCatalog
            extends PaimonCatalog
    {
        private FailingDdlCatalog()
        {
            super(new Options(), unsupportedFileSystemFactory());
        }

        @Override
        public void initSession(ConnectorSession connectorSession) {}

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            return this;
        }

        @Override
        public void createDatabase(String name, boolean ignoreIfExists)
                throws Catalog.DatabaseAlreadyExistException
        {
            assertThat(name).isEqualTo("schema");
            assertThat(ignoreIfExists).isFalse();
            throw new Catalog.DatabaseAlreadyExistException(name);
        }

        @Override
        public void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
                throws Catalog.DatabaseAlreadyExistException
        {
            assertThat(properties).isEmpty();
            createDatabase(name, ignoreIfExists);
        }

        @Override
        public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
                throws Catalog.DatabaseNotEmptyException
        {
            assertThat(name).isEqualTo("schema");
            assertThat(ignoreIfNotExists).isFalse();
            assertThat(cascade).isFalse();
            throw new Catalog.DatabaseNotEmptyException(name);
        }

        @Override
        public List<String> listTables(String databaseName)
                throws Catalog.DatabaseNotExistException
        {
            assertThat(databaseName).isEqualTo("schema");
            throw new Catalog.DatabaseNotExistException(databaseName);
        }

        @Override
        public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
                throws Catalog.TableAlreadyExistException, Catalog.DatabaseNotExistException
        {
            assertThat(identifier.getObjectName()).isEqualTo("table");
            if (identifier.getDatabaseName().equals("missing_schema")) {
                throw new Catalog.DatabaseNotExistException(identifier.getDatabaseName());
            }
            assertThat(identifier.getDatabaseName()).isEqualTo("schema");
            throw new Catalog.TableAlreadyExistException(identifier);
        }

        @Override
        public void replaceTable(Identifier identifier, Schema newSchema, boolean ignoreIfNotExists)
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            assertThat(ignoreIfNotExists).isFalse();
            throw new UnsupportedOperationException("replace is not supported");
        }

        @Override
        public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
                throws Catalog.TableNotExistException
        {
            assertThat(fromTable.getFullName()).isEqualTo("schema.table");
            assertThat(toTable.getFullName()).isEqualTo("schema.target");
            throw new Catalog.TableNotExistException(fromTable);
        }

        @Override
        public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
                throws Catalog.TableNotExistException
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            throw new Catalog.TableNotExistException(identifier);
        }

        @Override
        public Table getTable(Identifier identifier)
                throws Catalog.TableNotExistException
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            throw new Catalog.TableNotExistException(identifier);
        }

        @Override
        public void alterTable(Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
                throws Catalog.TableNotExistException, Catalog.ColumnAlreadyExistException,
                Catalog.ColumnNotExistException
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            SchemaChange change = changes.get(0);
            if (change instanceof SchemaChange.AddColumn addColumn) {
                if (addColumn.fieldNames()[0].equals("missing_table")) {
                    throw new Catalog.TableNotExistException(identifier);
                }
                throw new Catalog.ColumnAlreadyExistException(identifier, addColumn.fieldNames()[0]);
            }
            if (change instanceof SchemaChange.RenameColumn renameColumn) {
                throw new Catalog.ColumnNotExistException(identifier, renameColumn.fieldNames()[0]);
            }
            if (change instanceof SchemaChange.DropColumn dropColumn) {
                throw new Catalog.ColumnNotExistException(identifier, dropColumn.fieldNames()[0]);
            }
            if (change instanceof SchemaChange.SetOption) {
                throw new Catalog.TableNotExistException(identifier);
            }
            if (change instanceof SchemaChange.UpdateComment) {
                throw new Catalog.TableNotExistException(identifier);
            }
            throw new AssertionError("Unexpected schema change: " + change);
        }
    }

    private static class CreatedSchemaCatalog
            extends PaimonCatalog
    {
        private final Table table;
        private Schema createdSchema;

        private CreatedSchemaCatalog(Table table)
        {
            super(new Options(), unsupportedFileSystemFactory());
            this.table = table;
        }

        @Override
        public void initSession(ConnectorSession connectorSession) {}

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            return this;
        }

        @Override
        public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            this.createdSchema = schema;
        }

        @Override
        public Table getTable(Identifier identifier)
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            return table;
        }
    }

    private static class CapturingDdlCatalog
            extends PaimonCatalog
    {
        private String createdDatabase;
        private Boolean createDatabaseIgnoreIfExists;
        private Map<String, String> createdDatabaseProperties;
        private String droppedDatabase;
        private Boolean dropDatabaseIgnoreIfNotExists;
        private Boolean dropDatabaseCascade;
        private String alteredDatabase;
        private Boolean alterDatabaseIgnoreIfNotExists;
        private List<PropertyChange> lastDatabasePropertyChanges = List.of();
        private Schema createdSchema;
        private boolean initialized;
        private int alterCalls;
        private List<SchemaChange> lastAlterChanges = List.of();
        private Identifier renamedFromTable;
        private Identifier renamedToTable;
        private boolean renamedIgnoreIfNotExists;
        private Identifier droppedTable;
        private boolean droppedTableIgnoreIfNotExists;

        private CapturingDdlCatalog()
        {
            super(new Options(), unsupportedFileSystemFactory());
        }

        @Override
        public void initSession(ConnectorSession connectorSession)
        {
            initialized = true;
        }

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            initialized = true;
            return this;
        }

        @Override
        public void createDatabase(String name, boolean ignoreIfExists)
        {
            this.createdDatabase = name;
            this.createDatabaseIgnoreIfExists = ignoreIfExists;
            this.createdDatabaseProperties = Map.of();
        }

        @Override
        public void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
        {
            this.createdDatabase = name;
            this.createDatabaseIgnoreIfExists = ignoreIfExists;
            this.createdDatabaseProperties = Map.copyOf(properties);
        }

        @Override
        public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
        {
            this.droppedDatabase = name;
            this.dropDatabaseIgnoreIfNotExists = ignoreIfNotExists;
            this.dropDatabaseCascade = cascade;
        }

        @Override
        public void alterDatabase(String name, List<PropertyChange> changes, boolean ignoreIfNotExists)
        {
            this.alteredDatabase = name;
            this.alterDatabaseIgnoreIfNotExists = ignoreIfNotExists;
            this.lastDatabasePropertyChanges = List.copyOf(changes);
        }

        @Override
        public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            this.createdSchema = schema;
        }

        @Override
        public void alterTable(Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            assertThat(ignoreIfNotExists).isFalse();
            alterCalls++;
            lastAlterChanges = List.copyOf(changes);
        }

        @Override
        public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
        {
            renamedFromTable = fromTable;
            renamedToTable = toTable;
            renamedIgnoreIfNotExists = ignoreIfNotExists;
        }

        @Override
        public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
        {
            droppedTable = identifier;
            droppedTableIgnoreIfNotExists = ignoreIfNotExists;
        }
    }

    private static class RuntimeFailingAlterCatalog
            extends PaimonCatalog
    {
        private final RuntimeException failure;

        private RuntimeFailingAlterCatalog(RuntimeException failure)
        {
            super(new Options(), unsupportedFileSystemFactory());
            this.failure = failure;
        }

        @Override
        public void initSession(ConnectorSession connectorSession) {}

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            return this;
        }

        @Override
        public void alterTable(Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            throw failure;
        }
    }

    private static class CheckedFailingAlterCatalog
            extends PaimonCatalog
    {
        private final IOException failure;

        private CheckedFailingAlterCatalog(IOException failure)
        {
            super(new Options(), unsupportedFileSystemFactory());
            this.failure = failure;
        }

        @Override
        public void initSession(ConnectorSession connectorSession) {}

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            return this;
        }

        @Override
        public void alterTable(Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            throw new RuntimeException(failure);
        }
    }

    private static class CheckedFailingSchemaCatalog
            extends PaimonCatalog
    {
        private final IOException failure;

        private CheckedFailingSchemaCatalog(IOException failure)
        {
            super(new Options(), unsupportedFileSystemFactory());
            this.failure = failure;
        }

        @Override
        public void initSession(ConnectorSession connectorSession) {}

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            return this;
        }

        @Override
        public void createDatabase(String name, boolean ignoreIfExists)
        {
            assertThat(name).isEqualTo("schema");
            throw new RuntimeException(failure);
        }

        @Override
        public void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
        {
            assertThat(properties).isEmpty();
            createDatabase(name, ignoreIfExists);
        }
    }

    private static class CheckedFailingDropSchemaCatalog
            extends PaimonCatalog
    {
        private final IOException failure;

        private CheckedFailingDropSchemaCatalog(IOException failure)
        {
            super(new Options(), unsupportedFileSystemFactory());
            this.failure = failure;
        }

        @Override
        public void initSession(ConnectorSession connectorSession) {}

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            return this;
        }

        @Override
        public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
        {
            assertThat(name).isEqualTo("schema");
            assertThat(ignoreIfNotExists).isFalse();
            assertThat(cascade).isFalse();
            throw new RuntimeException(failure);
        }
    }

    private static class CheckedFailingSchemaAuthorizationCatalog
            extends PaimonCatalog
    {
        private final IOException failure;

        private CheckedFailingSchemaAuthorizationCatalog(IOException failure)
        {
            super(new Options(), unsupportedFileSystemFactory());
            this.failure = failure;
        }

        @Override
        public void initSession(ConnectorSession connectorSession) {}

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            return this;
        }

        @Override
        public void alterDatabase(String name, List<PropertyChange> changes, boolean ignoreIfNotExists)
        {
            assertThat(name).isEqualTo("schema");
            assertThat(ignoreIfNotExists).isFalse();
            assertThat(changes).singleElement().isInstanceOf(PropertyChange.SetProperty.class);
            throw new RuntimeException(failure);
        }
    }

    private static class CheckedFailingCreateTableCatalog
            extends PaimonCatalog
    {
        private final IOException failure;

        private CheckedFailingCreateTableCatalog(IOException failure)
        {
            super(new Options(), unsupportedFileSystemFactory());
            this.failure = failure;
        }

        @Override
        public void initSession(ConnectorSession connectorSession) {}

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            return this;
        }

        @Override
        public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            assertThat(ignoreIfExists).isFalse();
            throw new RuntimeException(failure);
        }
    }

    private static class CheckedFailingRenameTableCatalog
            extends PaimonCatalog
    {
        private final IOException failure;

        private CheckedFailingRenameTableCatalog(IOException failure)
        {
            super(new Options(), unsupportedFileSystemFactory());
            this.failure = failure;
        }

        @Override
        public void initSession(ConnectorSession connectorSession) {}

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            return this;
        }

        @Override
        public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
        {
            assertThat(fromTable.getFullName()).isEqualTo("schema.table");
            assertThat(toTable.getFullName()).isEqualTo("schema.target");
            assertThat(ignoreIfNotExists).isFalse();
            throw new RuntimeException(failure);
        }
    }

    private static class CheckedFailingDropTableCatalog
            extends PaimonCatalog
    {
        private final IOException failure;

        private CheckedFailingDropTableCatalog(IOException failure)
        {
            super(new Options(), unsupportedFileSystemFactory());
            this.failure = failure;
        }

        @Override
        public void initSession(ConnectorSession connectorSession) {}

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            return this;
        }

        @Override
        public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            assertThat(ignoreIfNotExists).isFalse();
            throw new RuntimeException(failure);
        }
    }

    private static class SchemaQueryCatalog
            extends PaimonCatalog
    {
        private SchemaQueryCatalog()
        {
            super(new Options(), unsupportedFileSystemFactory());
        }

        @Override
        public void initSession(ConnectorSession connectorSession) {}

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            return this;
        }

        @Override
        public Database getDatabase(String name)
                throws Catalog.DatabaseNotExistException
        {
            if (name.equals(SYSTEM_DATABASE_NAME)) {
                return Database.of(name);
            }
            if (name.equals("existing_schema")) {
                return Database.of(name);
            }
            throw new Catalog.DatabaseNotExistException(name);
        }

        @Override
        public List<String> listDatabases()
        {
            return List.of("alpha", "beta");
        }

        @Override
        public List<String> listTables(String databaseName)
                throws Catalog.DatabaseNotExistException
        {
            return switch (databaseName) {
                case "alpha" -> List.of("t1", "t2");
                case "beta" -> List.of("t3");
                case SYSTEM_DATABASE_NAME -> SystemTableLoader.loadGlobalTableNames();
                default -> List.of();
            };
        }
    }

    private static class SchemaPropertiesCatalog
            extends PaimonCatalog
    {
        private SchemaPropertiesCatalog()
        {
            super(new Options(), unsupportedFileSystemFactory());
        }

        @Override
        public void initSession(ConnectorSession connectorSession) {}

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            return this;
        }

        @Override
        public Database getDatabase(String name)
                throws Catalog.DatabaseNotExistException
        {
            if (name.equals("schema")) {
                return Database.of(name, Map.of(
                        LOCATION_PROPERTY, "s3://warehouse/schema",
                        COMMENT_PROPERTY, "schema comment",
                        OWNER_PROPERTY, "schema_owner",
                        "unregistered-paimon-property", "hidden"), "schema comment");
            }
            throw new Catalog.DatabaseNotExistException(name);
        }
    }

    private static TrinoFileSystemFactory unsupportedFileSystemFactory()
    {
        return identity -> {
            throw new UnsupportedOperationException("filesystem is not used by this test");
        };
    }
}
