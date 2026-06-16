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

import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FullTextSearch;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FullTextSearchTable;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.VectorSearchTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_WRITER_CLOSE_ERROR;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_WRITER_DATA_ERROR;
import static io.trino.plugin.paimon.PaimonSessionProperties.SCAN_SNAPSHOT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PaimonPageSinkProviderTest
{
    @Test
    public void testSupportedWriteBucketModes()
    {
        assertThatCode(() -> PaimonPageSinkProvider.validateWriteBucketMode(fileStoreTable(BucketMode.HASH_FIXED)))
                .doesNotThrowAnyException();
        assertThatCode(() -> PaimonPageSinkProvider.validateWriteBucketMode(fileStoreTable(BucketMode.BUCKET_UNAWARE)))
                .doesNotThrowAnyException();
    }

    @Test
    public void testUnsupportedWriteBucketModesFailFast()
    {
        assertUnsupportedWriteBucketMode(BucketMode.HASH_DYNAMIC);
        assertUnsupportedWriteBucketMode(BucketMode.KEY_DYNAMIC);
        assertUnsupportedWriteBucketMode(BucketMode.POSTPONE_MODE);
    }

    @Test
    public void testMergeRequiresHashFixedBucketMode()
    {
        assertThatCode(() -> PaimonPageSinkProvider.validateMergeBucketMode(fileStoreTable(BucketMode.HASH_FIXED)))
                .doesNotThrowAnyException();

        assertUnsupportedMergeBucketMode(BucketMode.BUCKET_UNAWARE);
        assertUnsupportedMergeBucketMode(BucketMode.HASH_DYNAMIC);
        assertUnsupportedMergeBucketMode(BucketMode.KEY_DYNAMIC);
        assertUnsupportedMergeBucketMode(BucketMode.POSTPONE_MODE);
    }

    @Test
    public void testNonFileStoreTableFailsFast()
    {
        assertThatThrownBy(() -> PaimonPageSinkProvider.validateWriteBucketMode(table()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessageContaining("Paimon writes requires FileStoreTable, but got:");
                });

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateMergeBucketMode(table()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessageContaining("Paimon merge writes requires FileStoreTable, but got:");
                });
    }

    @Test
    public void testSearchWrapperTablesFailFast()
    {
        assertThatThrownBy(() -> PaimonPageSinkProvider.validateWriteBucketMode(VectorSearchTable.create(
                innerTable(),
                new VectorSearch(new float[] {1.0f}, 1, "embedding"))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon vector search tables are not supported by the Trino connector");
                });

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateMergeBucketMode(FullTextSearchTable.create(
                innerTable(),
                new FullTextSearch("paimon", 1, "content"))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon full-text search tables are not supported by the Trino connector");
                });
    }

    @Test
    public void testPageSinkUsesLatestFileStoreTableSchema()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, copiedWithLatestSchema);

        assertThat(PaimonPageSinkProvider.latestFileStoreTable(table, "writes"))
                .isSameAs(table);
        assertThat(copiedWithLatestSchema).isTrue();
    }

    @Test
    public void testWriteColumnsAreRequired()
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThatThrownBy(() -> PaimonPageSinkProvider.getWriteColumns(handle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon page sink requires explicit write columns");
    }

    @Test
    public void testGetWriteColumnsRejectsNullTableHandle()
    {
        assertThatThrownBy(() -> PaimonPageSinkProvider.getWriteColumns(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableHandle is null");
    }

    @Test
    public void testPageSinkProviderRejectsNullSessionBeforeCatalogInitialization()
    {
        PaimonPageSinkProvider provider = new PaimonPageSinkProvider(failingInitMetadataFactory());
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of())
                .withWriteColumns(List.of(PaimonColumnHandle.of("id", DataTypes.INT())));

        assertThatThrownBy(() -> provider.createPageSink(null, null, (ConnectorOutputTableHandle) tableHandle, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> provider.createPageSink(null, null, (ConnectorInsertTableHandle) tableHandle, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> provider.createMergeSink(null, null, new PaimonMergeTableHandle(tableHandle), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
    }

    @Test
    public void testPageSinkProviderRejectsMissingWriteColumnsBeforeCatalogInitialization()
    {
        PaimonPageSinkProvider provider = new PaimonPageSinkProvider(failingInitMetadataFactory());
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThatThrownBy(() -> provider.createPageSink(null, SESSION, (ConnectorOutputTableHandle) tableHandle, null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon page sink requires explicit write columns");
        assertThatThrownBy(() -> provider.createPageSink(null, SESSION, (ConnectorInsertTableHandle) tableHandle, null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon page sink requires explicit write columns");
        assertThatThrownBy(() -> provider.createMergeSink(null, SESSION, new PaimonMergeTableHandle(tableHandle), null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon page sink requires explicit write columns");
    }

    @Test
    public void testCreatePageSinkIgnoresSessionScanSnapshotAndHandleStartupSelections()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions = new AtomicReference<>();
        PaimonPageSinkProvider provider = new PaimonPageSinkProvider(metadataFactory(
                writeReadyFileStoreTable(copiedWithLatestSchema, copyWithoutTimeTravelOptions)));
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(
                        "custom.option", "value",
                        CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2",
                        CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key(), "delta",
                        CoreOptions.INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT.key(), "true"))
                .withWriteColumns(List.of(PaimonColumnHandle.of("id", DataTypes.INT())));
        ConnectorSession session = io.trino.testing.TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(SCAN_SNAPSHOT, 9L))
                .build();

        ConnectorPageSink pageSink = provider.createPageSink(null, session, (ConnectorInsertTableHandle) tableHandle,
                null);

        assertThat(pageSink).isNotNull();
        assertThat(copyWithoutTimeTravelOptions.get()).containsExactlyEntriesOf(Map.of("custom.option", "value"));
        assertThat(copiedWithLatestSchema).isTrue();
    }

    @Test
    public void testInsertOverwriteAppliesToInsertPageSinkOnly()
    {
        AtomicBoolean overwriteEnabled = new AtomicBoolean();
        PaimonPageSinkProvider provider = new PaimonPageSinkProvider(metadataFactory(
                writeReadyFileStoreTable(new AtomicBoolean(), new AtomicReference<>(), overwriteEnabled)));
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of())
                .withWriteColumns(List.of(PaimonColumnHandle.of("id", DataTypes.INT())));
        ConnectorSession overwriteSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR,
                        PaimonSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE.name()))
                .build();

        ConnectorPageSink pageSink = provider.createPageSink(null, overwriteSession, (ConnectorInsertTableHandle) tableHandle, null);

        assertThat(pageSink).isNotNull();
        assertThat(overwriteEnabled).isTrue();
    }

    @Test
    public void testInsertOverwriteDoesNotApplyToMergePageSink()
    {
        AtomicBoolean overwriteEnabled = new AtomicBoolean();
        PaimonPageSinkProvider provider = new PaimonPageSinkProvider(metadataFactory(
                writeReadyFileStoreTable(new AtomicBoolean(), new AtomicReference<>(), overwriteEnabled)));
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of())
                .withWriteColumns(List.of(PaimonColumnHandle.of("id", DataTypes.INT())));
        ConnectorSession overwriteSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR,
                        PaimonSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE.name()))
                .build();

        ConnectorMergeSink pageSink = provider.createMergeSink(null, overwriteSession, new PaimonMergeTableHandle(tableHandle), null);

        assertThat(pageSink).isNotNull();
        assertThat(overwriteEnabled).isFalse();
    }

    @Test
    public void testInsertOverwriteRejectsPartitionedTableWithoutDynamicPartitionOverwrite()
    {
        AtomicBoolean overwriteEnabled = new AtomicBoolean();
        PaimonPageSinkProvider provider = new PaimonPageSinkProvider(metadataFactory(
                writeReadyPartitionedFileStoreTable(new AtomicBoolean(), new AtomicReference<>(), overwriteEnabled,
                        false)));
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of())
                .withWriteColumns(List.of(PaimonColumnHandle.of("id", DataTypes.INT())));
        ConnectorSession overwriteSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR,
                        PaimonSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE.name()))
                .build();

        assertThatThrownBy(() -> provider.createPageSink(null, overwriteSession, (ConnectorInsertTableHandle) tableHandle, null))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage(
                            "Paimon insert overwrite requires dynamic-partition-overwrite=true for partitioned tables");
                });
        assertThat(overwriteEnabled).isFalse();
    }

    @Test
    public void testMergeSinkRejectsMalformedHandleBeforeCatalogInitialization()
    {
        PaimonPageSinkProvider provider = new PaimonPageSinkProvider(failingInitMetadataFactory());

        assertThatThrownBy(() -> provider.createMergeSink(null, SESSION, null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("mergeHandle is null");
        assertThatThrownBy(() -> provider.createMergeSink(null, SESSION, mergeTableHandle(null), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("mergeHandle tableHandle is null");

        ConnectorTableHandle wrongTableHandle = new ConnectorTableHandle() {};
        assertThatThrownBy(() -> provider.createMergeSink(null, SESSION, mergeTableHandle(wrongTableHandle), null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon merge sink requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
    }

    @Test
    public void testPageSinkCreateTableRequiresPaimonTableHandle()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThat(PaimonPageSinkProvider.getOutputTableHandle(tableHandle)).isSameAs(tableHandle);

        assertThatThrownBy(() -> PaimonPageSinkProvider.getOutputTableHandle(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("outputTableHandle is null");

        ConnectorOutputTableHandle wrongTableHandle = new ConnectorOutputTableHandle() {};
        assertThatThrownBy(() -> PaimonPageSinkProvider.getOutputTableHandle(wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon create table page sink requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
    }

    @Test
    public void testPageSinkInsertRequiresPaimonTableHandle()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThat(PaimonPageSinkProvider.getInsertTableHandle(tableHandle)).isSameAs(tableHandle);

        assertThatThrownBy(() -> PaimonPageSinkProvider.getInsertTableHandle(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("insertTableHandle is null");

        ConnectorInsertTableHandle wrongTableHandle = new ConnectorInsertTableHandle() {};
        assertThatThrownBy(() -> PaimonPageSinkProvider.getInsertTableHandle(wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon insert page sink requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
    }

    @Test
    public void testEmptyExplicitWriteColumnsFailFast()
    {
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT())));

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateWriteColumns(table, List.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon page sink requires non-empty write columns");
    }

    @Test
    public void testValidateWriteColumnsRejectsNullInputs()
    {
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT())));

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateWriteColumns(null,
                List.of(PaimonColumnHandle.of("id", DataTypes.INT()))))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("table is null");

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateWriteColumns(table, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("writeColumns is null");

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateWriteColumns(table,
                Collections.singletonList(null)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("writeColumns contains null column");
    }

    @Test
    public void testValidateLatestTableFieldsRejectsNulls()
    {
        assertThatThrownBy(() -> PaimonPageSinkProvider.validateNoCaseInsensitiveDuplicateFieldNames(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fields is null");

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateNoCaseInsensitiveDuplicateFieldNames(
                Collections.singletonList(null)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fields contains null field");
    }

    @Test
    public void testWriteColumnsPreserveExplicitOrder()
    {
        List<ColumnHandle> writeColumns = List.of(
                PaimonColumnHandle.of("new_column", DataTypes.STRING()),
                PaimonColumnHandle.of("id", DataTypes.INT()));
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty())
                .withWriteColumns(writeColumns);

        assertThat(PaimonPageSinkProvider.getWriteColumns(handle))
                .extracting(PaimonColumnHandle::getColumnName)
                .containsExactly("new_column", "id");
    }

    @Test
    public void testWriteColumnsAreValidatedAgainstLatestTableSchema()
    {
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "name", DataTypes.STRING())));
        List<PaimonColumnHandle> writeColumns = List.of(
                PaimonColumnHandle.of("name", DataTypes.STRING()),
                PaimonColumnHandle.of("id", DataTypes.INT()));

        assertThatCode(() -> PaimonPageSinkProvider.validateWriteColumns(table, writeColumns))
                .doesNotThrowAnyException();
    }

    @Test
    public void testWriteColumnsMatchLatestTableSchemaCaseInsensitively()
    {
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, DataTypes.ROW(
                DataTypes.FIELD(0, "ID", DataTypes.INT()),
                DataTypes.FIELD(1, "Name", DataTypes.STRING())));
        List<PaimonColumnHandle> writeColumns = List.of(
                PaimonColumnHandle.of("id", DataTypes.INT()),
                PaimonColumnHandle.of("name", DataTypes.STRING()));

        assertThatCode(() -> PaimonPageSinkProvider.validateWriteColumns(table, writeColumns))
                .doesNotThrowAnyException();
    }

    @Test
    public void testWriteColumnMissingFromLatestTableSchemaFailsFast()
    {
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT())));

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateWriteColumns(table,
                List.of(PaimonColumnHandle.of("zip", DataTypes.STRING()))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Write column 'zip' is not present in latest Paimon table schema [id]");
    }

    @Test
    public void testDuplicateWriteColumnFailsFast()
    {
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT())));

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateWriteColumns(table,
                List.of(
                        PaimonColumnHandle.of("id", DataTypes.INT()),
                        PaimonColumnHandle.of("id", DataTypes.INT()))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Write column 'id' appears more than once");
    }

    @Test
    public void testCaseInsensitiveDuplicateWriteColumnFailsFast()
    {
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT())));

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateWriteColumns(table,
                List.of(
                        PaimonColumnHandle.of("id", DataTypes.INT()),
                        PaimonColumnHandle.of("ID", DataTypes.INT()))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Write column 'ID' appears more than once");
    }

    @Test
    public void testCaseInsensitiveDuplicateLatestTableFieldFailsFast()
    {
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "ID", DataTypes.INT())));

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateWriteColumns(table,
                List.of(PaimonColumnHandle.of("id", DataTypes.INT()))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Latest Paimon table schema contains case-insensitive duplicate field name 'id'");
    }

    @Test
    public void testWriteColumnTypeMismatchWithLatestTableSchemaFailsFast()
    {
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT())));

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateWriteColumns(table,
                List.of(PaimonColumnHandle.of("id", DataTypes.STRING()))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Write column 'id' type STRING does not match latest Paimon table schema type INT");
    }

    @Test
    public void testMergeWriteColumnsMustMatchLatestTableSchemaOrder()
    {
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "name", DataTypes.STRING())));

        assertThatCode(() -> PaimonPageSinkProvider.validateMergeWriteColumns(table,
                List.of(
                        PaimonColumnHandle.of("id", DataTypes.INT()),
                        PaimonColumnHandle.of("name", DataTypes.STRING()))))
                .doesNotThrowAnyException();

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateMergeWriteColumns(table,
                List.of(PaimonColumnHandle.of("id", DataTypes.INT()))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Merge write columns [id] must match latest Paimon table schema columns [id, name]");

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateMergeWriteColumns(table,
                List.of(
                        PaimonColumnHandle.of("name", DataTypes.STRING()),
                        PaimonColumnHandle.of("id", DataTypes.INT()))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Merge write columns [name, id] must match latest Paimon table schema columns [id, name]");

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateMergeWriteColumns(table,
                List.of(
                        PaimonColumnHandle.of("ID", DataTypes.INT()),
                        PaimonColumnHandle.of("name", DataTypes.STRING()))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Merge write columns [ID, name] must match latest Paimon table schema columns [id, name]");
    }

    @Test
    public void testMergeSinkRequiresPaimonTableHandle()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThat(PaimonPageSinkProvider.getMergeTableHandle(new PaimonMergeTableHandle(tableHandle)))
                .isSameAs(tableHandle);

        assertThatThrownBy(() -> PaimonPageSinkProvider.getMergeTableHandle(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("mergeHandle is null");

        assertThatThrownBy(() -> PaimonPageSinkProvider.getMergeTableHandle(mergeTableHandle(null)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("mergeHandle tableHandle is null");

        ConnectorTableHandle wrongTableHandle = new ConnectorTableHandle() {};
        assertThatThrownBy(() -> PaimonPageSinkProvider.getMergeTableHandle(mergeTableHandle(wrongTableHandle)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon merge sink requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
    }

    @Test
    public void testPageSinkRequiresMatchingTrinoAndPaimonTypeMetadata()
    {
        assertThatThrownBy(() -> new PaimonPageSink(null, List.of(INTEGER), List.of()))
                .hasMessage("writer is null");

        assertThatThrownBy(() -> new PaimonPageSink(writer(), Collections.singletonList(null), List.of(DataTypes.INT())))
                .hasMessage("columnTypes contains null type");

        assertThatThrownBy(() -> new PaimonPageSink(writer(), List.of(INTEGER), Collections.singletonList(null)))
                .hasMessage("logicalTypes contains null type");

        assertThatThrownBy(() -> new PaimonPageSink(writer(), List.of(INTEGER), List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("columnTypes and logicalTypes size mismatch: 1 != 0");
    }

    @Test
    public void testPageSinkRequiresPageShapeToMatchExplicitWriteColumns()
    {
        PaimonPageSink pageSink = new PaimonPageSink(writer(), List.of(INTEGER), List.of(DataTypes.INT()));

        assertThatThrownBy(() -> pageSink.appendPage(new io.trino.spi.Page(
                1,
                writeNativeValue(INTEGER, 1L),
                writeNativeValue(INTEGER, 2L))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("page channel count (2) must match write column count (1)");
    }

    @Test
    public void testPageSinkRequiresRowKind()
    {
        PaimonPageSink pageSink = new PaimonPageSink(writer(), List.of(INTEGER), List.of(DataTypes.INT()));

        assertThatThrownBy(() -> pageSink.writePage(new io.trino.spi.Page(1, writeNativeValue(INTEGER, 1L)), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("rowKind is null");
    }

    @Test
    public void testPageSinkWriteExceptionsUsePaimonErrorCodes()
    {
        IllegalArgumentException contractViolation = new IllegalArgumentException("metadata mismatch");
        IOException writeFailure = new IOException("write failed");
        TrinoException alreadyMapped = new TrinoException(PAIMON_WRITER_DATA_ERROR, "already mapped");
        UnsupportedOperationException unsupported = new UnsupportedOperationException("unsupported nested type");
        RuntimeException runtimeFailure = new RuntimeException("runtime write failed");

        assertThat(PaimonPageSink.wrapWriteException(contractViolation)).isSameAs(contractViolation);
        assertThat(PaimonPageSink.wrapWriteException(alreadyMapped)).isSameAs(alreadyMapped);
        assertThat(PaimonPageSink.wrapWriteException(unsupported))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon write uses features which are not supported by the Trino connector");
                    assertThat(exception.getCause()).isSameAs(unsupported);
                });
        assertThat(PaimonPageSink.wrapWriteException(runtimeFailure))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_DATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to write data to Paimon");
                    assertThat(exception.getCause()).isSameAs(runtimeFailure);
                });
        assertThat(PaimonPageSink.wrapWriteException(writeFailure))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_DATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to write data to Paimon");
                    assertThat(exception.getCause()).isSameAs(writeFailure);
                });

        assertThat(PaimonPageSink.wrapWriterCloseException(contractViolation)).isSameAs(contractViolation);
        assertThat(PaimonPageSink.wrapWriterCloseException(alreadyMapped)).isSameAs(alreadyMapped);
        assertThat(PaimonPageSink.wrapWriterCloseException(writeFailure))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_CLOSE_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to close Paimon writer");
                    assertThat(exception.getCause()).isSameAs(writeFailure);
                });
    }

    @Test
    public void testPageSinkCloseFailureDoesNotHideCommitFailure()
    {
        IllegalStateException commitFailure = new IllegalStateException("commit failed");
        IllegalArgumentException closeFailure = new IllegalArgumentException("close failed");

        RuntimeException actual = PaimonPageSink.closeWriter(writer(closeFailure), commitFailure);

        assertThat(actual).isSameAs(commitFailure);
        assertThat(actual.getSuppressed()).containsExactly(closeFailure);
    }

    @Test
    public void testPageSinkCloseFailureIsThrownWhenCommitSucceeds()
    {
        IllegalArgumentException closeFailure = new IllegalArgumentException("close failed");

        RuntimeException actual = PaimonPageSink.closeWriter(writer(closeFailure), null);

        assertThat(actual).isSameAs(closeFailure);
    }

    @Test
    public void testPageSinkAbortWrapsCheckedCloseFailures()
    {
        PaimonPageSink pageSink = new PaimonPageSink(writer(List.of(), null, null, new IOException("close failed")),
                List.of(INTEGER),
                List.of(DataTypes.INT()));

        assertThatThrownBy(pageSink::abort)
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_CLOSE_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to close Paimon writer");
                    assertThat(exception.getCause()).isInstanceOf(IOException.class)
                            .hasMessage("close failed");
                });
    }

    @Test
    public void testPageSinkWriteAndFinishWrapCheckedFailures()
    {
        IOException writeFailure = new IOException("write failed");
        PaimonPageSink failingWriteSink = new PaimonPageSink(writer(List.of(), writeFailure, null, null),
                List.of(INTEGER),
                List.of(DataTypes.INT()));

        assertThatThrownBy(() -> failingWriteSink.appendPage(new io.trino.spi.Page(1, writeNativeValue(INTEGER, 1L))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_DATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to write data to Paimon");
                    assertThat(exception.getCause()).isSameAs(writeFailure);
                });

        IOException prepareFailure = new IOException("prepare failed");
        PaimonPageSink failingFinishSink = new PaimonPageSink(writer(List.of(), null, prepareFailure, null),
                List.of(INTEGER),
                List.of(DataTypes.INT()));

        assertThatThrownBy(() -> failingFinishSink.finish().join())
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_DATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to write data to Paimon");
                    assertThat(exception.getCause()).isSameAs(prepareFailure);
                });
    }

    @Test
    public void testVariantWriteFailuresUseStableConnectorErrors()
    {
        io.trino.spi.type.Type jsonType = TESTING_TYPE_MANAGER.getType(new io.trino.spi.type.TypeSignature(JSON));
        PaimonPageSink pageSink = new PaimonPageSink(variantValidatingWriter(), List.of(jsonType), List.of(DataTypes.VARIANT()));

        assertThatThrownBy(() -> pageSink.appendPage(new io.trino.spi.Page(1,
                writeNativeValue(jsonType, Slices.utf8Slice("{broken")))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_DATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to write data to Paimon");
                    assertThat(exception.getCause()).isInstanceOf(RuntimeException.class)
                            .hasMessage("Failed to parse Variant from JSON");
                    assertThat(exception.getCause().getCause()).isInstanceOf(IOException.class);
                });

        PaimonPageSink unsupportedVariantSink = new PaimonPageSink(variantValidatingWriter(), List.of(INTEGER), List.of(DataTypes.VARIANT()));
        assertThatThrownBy(() -> unsupportedVariantSink.appendPage(new io.trino.spi.Page(1, writeNativeValue(INTEGER, 1L))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon write uses features which are not supported by the Trino connector");
                    assertThat(exception.getCause()).isInstanceOf(UnsupportedOperationException.class)
                            .hasMessage("Paimon VARIANT requires Trino JSON type metadata");
                });
    }

    @Test
    public void testPageSinkFinishRejectsNullCommitMessages()
    {
        PaimonPageSink pageSink = new PaimonPageSink(writer(List.of()), List.of(INTEGER), List.of(DataTypes.INT()));
        assertThat(pageSink.finish().join()).isEmpty();

        assertThatThrownBy(() -> new PaimonPageSink(writer((List<CommitMessage>) null), List.of(INTEGER),
                List.of(DataTypes.INT())).finish())
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Paimon writer returned null commit messages");

        assertThatThrownBy(() -> new PaimonPageSink(writer(Collections.singletonList(null)), List.of(INTEGER),
                List.of(DataTypes.INT())).finish())
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Paimon writer returned null commit message");
    }

    private static void assertUnsupportedWriteBucketMode(BucketMode bucketMode)
    {
        assertThatThrownBy(() -> PaimonPageSinkProvider.validateWriteBucketMode(fileStoreTable(bucketMode)))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessageContaining("Unsupported table bucket mode: " + bucketMode);
                });
    }

    private static void assertUnsupportedMergeBucketMode(BucketMode bucketMode)
    {
        assertThatThrownBy(() -> PaimonPageSinkProvider.validateMergeBucketMode(fileStoreTable(bucketMode)))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessageContaining("Unsupported table bucket mode: " + bucketMode);
                });
    }

    private static FileStoreTable fileStoreTable(BucketMode bucketMode)
    {
        return fileStoreTable(bucketMode, new AtomicBoolean());
    }

    private static FileStoreTable fileStoreTable(BucketMode bucketMode, RowType rowType)
    {
        return fileStoreTable(bucketMode, new AtomicBoolean(), rowType);
    }

    private static FileStoreTable fileStoreTable(BucketMode bucketMode, AtomicBoolean copiedWithLatestSchema)
    {
        return fileStoreTable(bucketMode, copiedWithLatestSchema, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT())));
    }

    private static FileStoreTable fileStoreTable(BucketMode bucketMode, AtomicBoolean copiedWithLatestSchema,
            RowType rowType)
    {
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonPageSinkProviderTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "bucketMode" -> bucketMode;
                    case "rowType" -> rowType;
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield proxy;
                    }
                    case "toString" -> "testing-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Table table()
    {
        return (Table) Proxy.newProxyInstance(
                PaimonPageSinkProviderTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "toString" -> "testing-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static InnerTable innerTable()
    {
        return (InnerTable) Proxy.newProxyInstance(
                PaimonPageSinkProviderTest.class.getClassLoader(),
                new Class<?>[] {InnerTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "toString" -> "testing-inner-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static ConnectorMergeTableHandle mergeTableHandle(ConnectorTableHandle tableHandle)
    {
        return () -> tableHandle;
    }

    private static PaimonMetadataFactory failingInitMetadataFactory()
    {
        return new PaimonMetadataFactory(new Options(), session -> {
            throw new AssertionError("filesystem should not be used");
        }, TESTING_TYPE_MANAGER)
        {
            @Override
            public PaimonMetadata create()
            {
                return new PaimonMetadata(new FailingInitCatalog(), TESTING_TYPE_MANAGER);
            }
        };
    }

    private static PaimonMetadataFactory metadataFactory(FileStoreTable table)
    {
        return new PaimonMetadataFactory(new Options(), session -> {
            throw new AssertionError("filesystem should not be used");
        }, TESTING_TYPE_MANAGER)
        {
            @Override
            public PaimonMetadata create()
            {
                return new PaimonMetadata(new TestingCatalog(table), TESTING_TYPE_MANAGER);
            }
        };
    }

    private static FileStoreTable writeReadyFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions)
    {
        return writeReadyFileStoreTable(copiedWithLatestSchema, copyWithoutTimeTravelOptions, new AtomicBoolean());
    }

    private static FileStoreTable writeReadyFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions,
            AtomicBoolean overwriteEnabled)
    {
        return writeReadyFileStoreTable(copiedWithLatestSchema, copyWithoutTimeTravelOptions, overwriteEnabled,
                List.of(), Map.of());
    }

    private static FileStoreTable writeReadyPartitionedFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions,
            AtomicBoolean overwriteEnabled,
            boolean dynamicPartitionOverwrite)
    {
        return writeReadyFileStoreTable(
                copiedWithLatestSchema,
                copyWithoutTimeTravelOptions,
                overwriteEnabled,
                List.of("pt"),
                Map.of(CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key(), String.valueOf(dynamicPartitionOverwrite)));
    }

    private static FileStoreTable writeReadyFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions,
            AtomicBoolean overwriteEnabled,
            List<String> partitionKeys,
            Map<String, String> options)
    {
        org.apache.paimon.table.sink.BatchTableWrite writer = writer();
        org.apache.paimon.table.sink.BatchWriteBuilder batchWriteBuilder = (org.apache.paimon.table.sink.BatchWriteBuilder) Proxy
                .newProxyInstance(
                        PaimonPageSinkProviderTest.class.getClassLoader(),
                        new Class<?>[] {org.apache.paimon.table.sink.BatchWriteBuilder.class},
                        (proxy, method, args) -> switch (method.getName()) {
                            case "newWrite" -> writer;
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
        AtomicReference<FileStoreTable> latestTableRef = new AtomicReference<>();
        FileStoreTable latestTable = (FileStoreTable) Proxy.newProxyInstance(
                PaimonPageSinkProviderTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "bucketMode" -> BucketMode.HASH_FIXED;
                    case "rowType" -> DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
                    case "partitionKeys" -> partitionKeys;
                    case "coreOptions" -> new CoreOptions(new Options(options));
                    case "schema" -> TableSchema.create(1, new Schema(
                            DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())).getFields(),
                            partitionKeys,
                            List.of(),
                            options,
                            ""));
                    case "newBatchWriteBuilder" -> batchWriteBuilder;
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
                PaimonPageSinkProviderTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "bucketMode" -> BucketMode.HASH_FIXED;
                    case "rowType" -> DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
                    case "partitionKeys" -> partitionKeys;
                    case "coreOptions" -> new CoreOptions(new Options(options));
                    case "schema" -> TableSchema.create(1, new Schema(
                            DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())).getFields(),
                            partitionKeys,
                            List.of(),
                            options,
                            ""));
                    case "copyWithoutTimeTravel" -> {
                        copyWithoutTimeTravelOptions.set(Map.copyOf((Map<String, String>) args[0]));
                        yield latestTableRef.get();
                    }
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield latestTableRef.get();
                    }
                    case "copy" -> proxy;
                    case "newBatchWriteBuilder" -> throw new AssertionError(
                            "stale FileStoreTable should not create BatchWriteBuilder before latest-schema refresh");
                    case "toString" -> "stale-testing-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static class TestingCatalog
            extends io.trino.plugin.paimon.catalog.PaimonCatalog
    {
        private final FileStoreTable table;

        private TestingCatalog(FileStoreTable table)
        {
            super(new Options(), session -> {
                throw new AssertionError("filesystem should not be used");
            });
            this.table = table;
        }

        @Override
        public void initSession(ConnectorSession connectorSession)
        {
        }

        @Override
        public org.apache.paimon.catalog.Catalog forSession(ConnectorSession connectorSession)
        {
            return this;
        }

        @Override
        public Table getTable(org.apache.paimon.catalog.Identifier identifier)
        {
            return table;
        }
    }

    private static class FailingInitCatalog
            extends io.trino.plugin.paimon.catalog.PaimonCatalog
    {
        private FailingInitCatalog()
        {
            super(new Options(), session -> {
                throw new AssertionError("filesystem should not be used");
            });
        }

        @Override
        public void initSession(ConnectorSession connectorSession)
        {
            throw new AssertionError("catalog should not be initialized for malformed page-sink session");
        }

        @Override
        public org.apache.paimon.catalog.Catalog forSession(ConnectorSession connectorSession)
        {
            throw new AssertionError("catalog should not be initialized for malformed page-sink session");
        }
    }

    private static org.apache.paimon.table.sink.BatchTableWrite writer()
    {
        return writer(List.of(), null, null, null);
    }

    private static org.apache.paimon.table.sink.BatchTableWrite writer(RuntimeException closeFailure)
    {
        return writer(List.of(), null, null, closeFailure);
    }

    private static org.apache.paimon.table.sink.BatchTableWrite writer(List<CommitMessage> commitMessages)
    {
        return writer(commitMessages, null, null, null);
    }

    private static org.apache.paimon.table.sink.BatchTableWrite writer(List<CommitMessage> commitMessages,
            RuntimeException closeFailure)
    {
        return writer(commitMessages, null, null, closeFailure);
    }

    private static org.apache.paimon.table.sink.BatchTableWrite writer(List<CommitMessage> commitMessages,
            Exception writeFailure, Exception prepareFailure, Exception closeFailure)
    {
        return (org.apache.paimon.table.sink.BatchTableWrite) Proxy.newProxyInstance(
                PaimonPageSinkProviderTest.class.getClassLoader(),
                new Class<?>[] {org.apache.paimon.table.sink.BatchTableWrite.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "write" -> {
                        if (writeFailure != null) {
                            throw writeFailure;
                        }
                        yield null;
                    }
                    case "prepareCommit" -> {
                        if (prepareFailure != null) {
                            throw prepareFailure;
                        }
                        yield commitMessages;
                    }
                    case "close" -> {
                        if (closeFailure != null) {
                            throw closeFailure;
                        }
                        yield null;
                    }
                    case "toString" -> "testing-writer";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static org.apache.paimon.table.sink.BatchTableWrite variantValidatingWriter()
    {
        return (org.apache.paimon.table.sink.BatchTableWrite) Proxy.newProxyInstance(
                PaimonPageSinkProviderTest.class.getClassLoader(),
                new Class<?>[] {org.apache.paimon.table.sink.BatchTableWrite.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "write" -> {
                        ((org.apache.paimon.data.InternalRow) args[0]).getVariant(0);
                        yield null;
                    }
                    case "prepareCommit" -> List.of();
                    case "close" -> null;
                    case "toString" -> "variant-validating-writer";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
