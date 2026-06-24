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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.filesystem.memory.MemoryInputFile;
import io.trino.orc.OrcColumn;
import io.trino.orc.OrcDataSourceId;
import io.trino.orc.metadata.OrcColumnId;
import io.trino.orc.metadata.OrcType.OrcTypeKind;
import io.trino.parquet.Column;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.parquet.ParquetPageSource;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeUtils;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FullTextQuery;
import org.apache.paimon.predicate.FullTextSearch;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FullTextSearchTable;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.VectorSearchTable;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_BAD_DATA;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_CURSOR_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Objects.requireNonNull;
import static org.apache.paimon.fileindex.FileIndexCommon.toMapKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PaimonPageSourceTest
{
    private static final Type JSON_TYPE = TESTING_TYPE_MANAGER.getType(new TypeSignature(JSON));

    @Test
    void testHighPrecisionTemporalValues()
    {
        GenericRow row = new GenericRow(3);
        row.setField(0, 12_345);
        row.setField(1, Timestamp.fromEpochMillis(1_695_645_403_123L, 456_789));
        row.setField(2, Timestamp.fromEpochMillis(1_695_645_403_123L, 456_000));

        PaimonPageSource pageSource = new PaimonPageSource(new TestingRecordReader(row), List.of(
                PaimonColumnHandle.of("t", new org.apache.paimon.types.TimeType(6)),
                PaimonColumnHandle.of("ts", new org.apache.paimon.types.TimestampType(9)),
                PaimonColumnHandle.of("tz", new org.apache.paimon.types.LocalZonedTimestampType(6))),
                OptionalLong.empty());

        Page page = pageSource.getNextPage();

        assertThat(page.getPositionCount()).isEqualTo(1);
        assertThat(TypeUtils.readNativeValue(TIME_MICROS, page.getBlock(0), 0))
                .isEqualTo(12_345L * PICOSECONDS_PER_MILLISECOND);
        assertThat(TypeUtils.readNativeValue(TIMESTAMP_NANOS, page.getBlock(1), 0))
                .isEqualTo(new LongTimestamp(1_695_645_403_123_456L, 789_000));
        assertThat(TypeUtils.readNativeValue(TIMESTAMP_TZ_MICROS, page.getBlock(2), 0))
                .isEqualTo(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1_695_645_403_123L,
                        456_000_000, UTC_KEY));
        assertThat(pageSource.getNextPage()).isNull();
    }

    @Test
    void testPaimonRowKindCanBeUpdatedByPaimonReaderWrappers()
    {
        PaimonRow row = new PaimonRow(new Page(1, writeNativeValue(INTEGER, 7L)), RowKind.INSERT,
                List.of(INTEGER), List.of(DataTypes.INT()));
        row.setRowKind(RowKind.UPDATE_AFTER);

        PaimonPageSource pageSource = new PaimonPageSource(new TestingRecordReader(row), List.of(
                PaimonColumnHandle.of("id", DataTypes.INT())),
                OptionalLong.empty());

        Page page = pageSource.getNextPage();

        assertThat(row.getRowKind()).isEqualTo(RowKind.UPDATE_AFTER);
        assertThatThrownBy(() -> row.setRowKind(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("rowKind is null");
        assertThat(page.getPositionCount()).isEqualTo(1);
        assertThat(TypeUtils.readNativeValue(INTEGER, page.getBlock(0), 0)).isEqualTo(7L);
        assertThat(pageSource.getNextPage()).isNull();
    }

    @Test
    void testBlobValuesReadAsVarbinary()
    {
        byte[] bytes = "paimon-blob-data".getBytes(StandardCharsets.UTF_8);
        GenericRow row = new GenericRow(1);
        row.setField(0, Blob.fromData(bytes));

        PaimonPageSource pageSource = new PaimonPageSource(new TestingRecordReader(row), List.of(
                PaimonColumnHandle.of("blob_value", DataTypes.BLOB())),
                OptionalLong.empty());

        Page page = pageSource.getNextPage();

        assertThat(page.getPositionCount()).isEqualTo(1);
        assertThat(((Slice) TypeUtils.readNativeValue(VARBINARY, page.getBlock(0), 0)).getBytes()).isEqualTo(bytes);
        assertThat(pageSource.getNextPage()).isNull();
    }

    @Test
    void testVariantValuesReadAsJson()
    {
        String json = "[1,\"two\",true]";
        GenericRow row = new GenericRow(1);
        row.setField(0, GenericVariant.fromJson(json));

        PaimonPageSource pageSource = new PaimonPageSource(new TestingRecordReader(row), List.of(
                PaimonColumnHandle.of("variant_value", DataTypes.VARIANT(), TESTING_TYPE_MANAGER)),
                OptionalLong.empty());

        Page page = pageSource.getNextPage();

        assertThat(page.getPositionCount()).isEqualTo(1);
        assertThat(TypeUtils.readNativeValue(JSON_TYPE, page.getBlock(0), 0)).isEqualTo(jsonParse(Slices.utf8Slice(json)));
        assertThat(pageSource.getNextPage()).isNull();
    }

    @Test
    void testVectorValuesReadAsArray()
    {
        GenericRow row = new GenericRow(1);
        row.setField(0, BinaryVector.fromPrimitiveArray(new float[] {1.0f, 2.5f, 3.75f}));

        PaimonPageSource pageSource = new PaimonPageSource(new TestingRecordReader(row), List.of(
                PaimonColumnHandle.of("embedding", DataTypes.VECTOR(3, DataTypes.FLOAT()))),
                OptionalLong.empty());

        Page page = pageSource.getNextPage();
        Block vectorBlock = new io.trino.spi.type.ArrayType(REAL).getObject(page.getBlock(0), 0);

        assertThat(page.getPositionCount()).isEqualTo(1);
        assertThat(TypeUtils.readNativeValue(REAL, vectorBlock, 0)).isEqualTo((long) Float.floatToIntBits(1.0f));
        assertThat(TypeUtils.readNativeValue(REAL, vectorBlock, 1)).isEqualTo((long) Float.floatToIntBits(2.5f));
        assertThat(TypeUtils.readNativeValue(REAL, vectorBlock, 2)).isEqualTo((long) Float.floatToIntBits(3.75f));
        assertThat(pageSource.getNextPage()).isNull();
    }

    @Test
    void testVectorValuesReadAsArrayForAllPaimonPrimitiveElementTypes()
    {
        assertVectorRead(DataTypes.BOOLEAN(), BinaryVector.fromPrimitiveArray(new boolean[] {true, false, true}),
                BOOLEAN,
                vectorBlock -> assertThat(vectorValues(BOOLEAN, vectorBlock))
                        .containsExactly(true, false, true));
        assertVectorRead(DataTypes.TINYINT(), BinaryVector.fromPrimitiveArray(new byte[] {1, 2, 3}),
                TINYINT,
                vectorBlock -> assertThat(vectorValues(TINYINT, vectorBlock))
                        .containsExactly(1L, 2L, 3L));
        assertVectorRead(DataTypes.SMALLINT(), BinaryVector.fromPrimitiveArray(new short[] {10, 20, 30}),
                SMALLINT,
                vectorBlock -> assertThat(vectorValues(SMALLINT, vectorBlock))
                        .containsExactly(10L, 20L, 30L));
        assertVectorRead(DataTypes.INT(), BinaryVector.fromPrimitiveArray(new int[] {100, 200, 300}),
                INTEGER,
                vectorBlock -> assertThat(vectorValues(INTEGER, vectorBlock))
                        .containsExactly(100L, 200L, 300L));
        assertVectorRead(DataTypes.BIGINT(), BinaryVector.fromPrimitiveArray(new long[] {1_000L, 2_000L, 3_000L}),
                BIGINT,
                vectorBlock -> assertThat(vectorValues(BIGINT, vectorBlock))
                        .containsExactly(1_000L, 2_000L, 3_000L));
        assertVectorRead(DataTypes.FLOAT(), BinaryVector.fromPrimitiveArray(new float[] {1.0f, 2.5f, 3.75f}),
                REAL,
                vectorBlock -> assertThat(vectorValues(REAL, vectorBlock))
                        .containsExactly((long) Float.floatToIntBits(1.0f), (long) Float.floatToIntBits(2.5f),
                                (long) Float.floatToIntBits(3.75f)));
        assertVectorRead(DataTypes.DOUBLE(), BinaryVector.fromPrimitiveArray(new double[] {1.0d, 2.5d, 3.75d}),
                DOUBLE,
                vectorBlock -> assertThat(vectorValues(DOUBLE, vectorBlock))
                        .containsExactly(1.0d, 2.5d, 3.75d));
    }

    @Test
    void testMultisetValuesReadAsMap()
    {
        GenericRow row = new GenericRow(1);
        row.setField(0, new GenericMap(Map.of(BinaryString.fromString("red"), 2)));

        PaimonPageSource pageSource = new PaimonPageSource(new TestingRecordReader(row), List.of(
                PaimonColumnHandle.of("tags", DataTypes.MULTISET(DataTypes.STRING()))),
                OptionalLong.empty());

        Page page = pageSource.getNextPage();
        MapType multisetType = new MapType(VARCHAR, INTEGER, new TypeOperators());
        SqlMap multiset = multisetType.getObject(page.getBlock(0), 0);

        assertThat(page.getPositionCount()).isEqualTo(1);
        assertThat(multiset.getSize()).isEqualTo(1);
        assertThat(((Slice) TypeUtils.readNativeValue(VARCHAR, multiset.getRawKeyBlock(), multiset.getRawOffset()))
                .toStringUtf8()).isEqualTo("red");
        assertThat(TypeUtils.readNativeValue(INTEGER, multiset.getRawValueBlock(), multiset.getRawOffset()))
                .isEqualTo(2L);
        assertThat(pageSource.getNextPage()).isNull();
    }

    @Test
    void testSemiStructuredColumnsFallBackFromDirectPageSource()
    {
        List<RawFile> rawFiles = List.of(rawFile("orc"));

        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(rawFiles, List.of(
                PaimonColumnHandle.of("bytes", DataTypes.VARBINARY(10))))).isTrue();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(rawFiles, List.of(
                PaimonColumnHandle.of("blob_value", DataTypes.BLOB())))).isFalse();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(rawFiles, List.of(
                PaimonColumnHandle.of("nested_blob", DataTypes.ARRAY(DataTypes.BLOB()))))).isFalse();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(rawFiles, List.of(
                PaimonColumnHandle.of("variant_value", DataTypes.VARIANT(), TESTING_TYPE_MANAGER)))).isFalse();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(rawFiles, List.of(
                PaimonColumnHandle.of("nested_variant", DataTypes.ARRAY(DataTypes.VARIANT()),
                        TESTING_TYPE_MANAGER)))).isFalse();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(rawFiles, List.of(
                PaimonColumnHandle.of("embedding", DataTypes.VECTOR(3, DataTypes.FLOAT()))))).isFalse();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(rawFiles, List.of(
                PaimonColumnHandle.of("nested_vector", DataTypes.ARRAY(DataTypes.VECTOR(3, DataTypes.FLOAT()))))))
                .isFalse();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(rawFiles, List.of(
                PaimonColumnHandle.of("tags", DataTypes.MULTISET(DataTypes.STRING()))))).isFalse();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(rawFiles, List.of(
                PaimonColumnHandle.of("nested_tags", DataTypes.ROW(
                        DataTypes.FIELD(0, "tags", DataTypes.MULTISET(DataTypes.STRING()))))))).isFalse();
    }

    @Test
    void testDirectReaderSchemaEvolutionRequiresProjectedFieldTypesToMatchDataFiles()
    {
        List<DataField> currentFields = List.of(
                new DataField(1, "renamed_id", DataTypes.INT()),
                new DataField(2, "payload", DataTypes.STRING()),
                new DataField(3, "new_field", DataTypes.BIGINT()));
        List<DataField> renamedDataFields = List.of(
                new DataField(1, "old_id", DataTypes.INT()),
                new DataField(2, "payload", DataTypes.STRING()));

        assertThat(PaimonPageSourceProvider.directReaderSupportsSchemaEvolution(
                List.of("renamed_id", "payload", "new_field"), currentFields, renamedDataFields))
                .isTrue();
        assertThat(PaimonPageSourceProvider.directReaderSupportsSchemaEvolution(
                List.of("payload"), currentFields, List.of(
                        new DataField(1, "old_id", DataTypes.INT()),
                        new DataField(2, "payload", DataTypes.VARCHAR(10)))))
                .isFalse();
        assertThat(PaimonPageSourceProvider.directReaderSupportsSchemaEvolution(
                List.of("payload"), List.of(
                        new DataField(2, "payload", DataTypes.ROW(
                                DataTypes.FIELD(10, "nested", DataTypes.STRING())))),
                        List.of(new DataField(2, "payload", DataTypes.ROW(
                                DataTypes.FIELD(10, "nested", DataTypes.INT()))))))
                .isFalse();
    }

    @Test
    void testOrcTimeColumnsFallBackFromDirectPageSource()
    {
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(List.of(rawFile("orc")), List.of(
                PaimonColumnHandle.of("event_time", DataTypes.TIME(3))))).isFalse();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(List.of(rawFile("orc")), List.of(
                PaimonColumnHandle.of("nested_time", DataTypes.ROW(
                        DataTypes.FIELD(0, "event_time", DataTypes.TIME(3))))))).isFalse();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(List.of(rawFile("parquet")), List.of(
                PaimonColumnHandle.of("event_time", DataTypes.TIME(3))))).isTrue();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(List.of(rawFile("parquet"), rawFile("orc")),
                List.of(PaimonColumnHandle.of("event_time", DataTypes.TIME(3))))).isFalse();
    }

    @Test
    void testSystemFieldsFallBackFromDirectPageSource()
    {
        List<RawFile> rawFiles = List.of(rawFile("orc"));

        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(rawFiles, List.of(
                PaimonColumnHandle.of("id", DataTypes.BIGINT())))).isTrue();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(rawFiles, List.of(
                PaimonColumnHandle.of("_ROW_ID", DataTypes.BIGINT())))).isFalse();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(rawFiles, List.of(
                PaimonColumnHandle.of("_SEQUENCE_NUMBER", DataTypes.BIGINT())))).isFalse();
    }

    @Test
    void testIncrementalWindowReadsFallBackFromDirectPageSource()
    {
        List<RawFile> rawFiles = List.of(rawFile("orc"));
        List<PaimonColumnHandle> columns = List.of(PaimonColumnHandle.of("id", DataTypes.BIGINT()));
        PaimonTableHandle scanHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonTableHandle incrementalHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(
                        org.apache.paimon.CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2",
                        org.apache.paimon.CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key(), "delta"));
        PaimonTableHandle incrementalTimestampHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(
                        org.apache.paimon.CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key(), "1000,2000",
                        org.apache.paimon.CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key(), "auto"));

        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(scanHandle, rawFiles, columns)).isTrue();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(incrementalHandle, rawFiles, columns)).isFalse();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(incrementalTimestampHandle, rawFiles, columns)).isFalse();
    }

    @Test
    void testIncrementalAutoTagReadsFallBackFromDirectPageSource()
    {
        List<RawFile> rawFiles = List.of(rawFile("orc"));
        List<PaimonColumnHandle> columns = List.of(PaimonColumnHandle.of("id", DataTypes.BIGINT()));
        PaimonTableHandle incrementalAutoTagHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(org.apache.paimon.CoreOptions.INCREMENTAL_TO_AUTO_TAG.key(), "2024-12-04"));

        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(incrementalAutoTagHandle, rawFiles, columns)).isFalse();
    }

    @Test
    void testUnsupportedRawFileFormatsFallBackFromDirectPageSource()
    {
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(List.of(), List.of(
                PaimonColumnHandle.of("bytes", DataTypes.VARBINARY(10))))).isFalse();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(List.of(rawFile("avro")), List.of(
                PaimonColumnHandle.of("bytes", DataTypes.VARBINARY(10))))).isFalse();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(List.of(rawFile("csv")), List.of(
                PaimonColumnHandle.of("bytes", DataTypes.VARBINARY(10))))).isFalse();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(List.of(rawFile("json")), List.of(
                PaimonColumnHandle.of("bytes", DataTypes.VARBINARY(10))))).isFalse();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(List.of(rawFile("lance")), List.of(
                PaimonColumnHandle.of("bytes", DataTypes.VARBINARY(10))))).isFalse();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(List.of(rawFile("vortex")), List.of(
                PaimonColumnHandle.of("bytes", DataTypes.VARBINARY(10))))).isFalse();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(List.of(rawFile("mosaic")), List.of(
                PaimonColumnHandle.of("bytes", DataTypes.VARBINARY(10))))).isFalse();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(List.of(rawFile("row")), List.of(
                PaimonColumnHandle.of("bytes", DataTypes.VARBINARY(10))))).isFalse();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(List.of(rawFile("blob")), List.of(
                PaimonColumnHandle.of("bytes", DataTypes.VARBINARY(10))))).isFalse();
    }

    @Test
    void testDirectRawFileFastPathRejectsHiddenAndSystemColumnsCaseInsensitively()
    {
        List<RawFile> rawFiles = List.of(rawFile("orc"));

        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(rawFiles, List.of(
                PaimonColumnHandle.of("_row_id", org.apache.paimon.table.SpecialFields.ROW_ID.type())))).isFalse();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(rawFiles, List.of(
                PaimonColumnHandle.of("_sequence_number",
                        org.apache.paimon.table.SpecialFields.SEQUENCE_NUMBER.type())))).isFalse();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(rawFiles, List.of(
                PaimonColumnHandle.of("_ROW_ID", org.apache.paimon.table.SpecialFields.ROW_ID.type())))).isFalse();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(rawFiles, List.of(
                PaimonColumnHandle.of("_key_id", DataTypes.BIGINT())))).isFalse();
    }

    @Test
    void testDirectRawFileSelectionRejectsMalformedInputs()
    {
        List<RawFile> rawFiles = List.of(rawFile("orc"));

        assertThatThrownBy(() -> PaimonPageSourceProvider.canUseTrinoPageSource(null, List.of(
                PaimonColumnHandle.of("id", DataTypes.BIGINT()))))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("rawFiles is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.canUseTrinoPageSource(Arrays.asList(rawFile("orc"), null),
                List.of(PaimonColumnHandle.of("id", DataTypes.BIGINT()))))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("rawFiles contains null file");
        assertThatThrownBy(() -> PaimonPageSourceProvider.canUseTrinoPageSource(List.of(rawFile(null)),
                List.of(PaimonColumnHandle.of("id", DataTypes.BIGINT()))))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("rawFiles contains file with null format");
        assertThatThrownBy(() -> PaimonPageSourceProvider.canUseTrinoPageSource(List.of(rawFile(" ")),
                List.of(PaimonColumnHandle.of("id", DataTypes.BIGINT()))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("rawFiles contains file with blank format");
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(List.of(rawFile(null, "avro")), List.of(
                PaimonColumnHandle.of("id", DataTypes.BIGINT())))).isFalse();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(List.of(rawFile(null, "orc")), List.of(
                PaimonColumnHandle.of("blob_value", DataTypes.BLOB())))).isFalse();
        assertThatThrownBy(() -> PaimonPageSourceProvider.canUseTrinoPageSource(List.of(rawFile(null, "orc")),
                List.of(PaimonColumnHandle.of("id", DataTypes.BIGINT()))))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("rawFiles contains file with null path");
        assertThatThrownBy(() -> PaimonPageSourceProvider.canUseTrinoPageSource(List.of(rawFile(" ", "parquet")),
                List.of(PaimonColumnHandle.of("id", DataTypes.BIGINT()))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("rawFiles contains file with blank path");
        assertThatThrownBy(() -> PaimonPageSourceProvider.canUseTrinoPageSource(rawFiles, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("columns is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.canUseTrinoPageSource(rawFiles, Arrays.asList(
                PaimonColumnHandle.of("id", DataTypes.BIGINT()), null)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("columns contains null column");

        ColumnHandle wrongColumn = new ColumnHandle() {};
        assertThatThrownBy(() -> PaimonPageSourceProvider.canUseTrinoPageSource(rawFiles, List.of(wrongColumn)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon page source requires PaimonColumnHandle, got: %s",
                        wrongColumn.getClass().getName());
    }

    @Test
    void testPageSourceProviderRequiresPaimonHandles()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonSplit split = new PaimonSplit("serialized-split", 1.0);
        PaimonColumnHandle column = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        PaimonColumnHandle rowId = PaimonColumnHandle.of(PaimonColumnHandle.TRINO_ROW_ID_NAME,
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT())),
                RowType.from(List.of(RowType.field("id", BIGINT))));

        assertThat(PaimonPageSourceProvider.getTableHandle(tableHandle)).isSameAs(tableHandle);
        assertThat(PaimonPageSourceProvider.getSplit(split)).isSameAs(split);
        assertThat(PaimonPageSourceProvider.getColumnHandles(List.of(column))).containsExactly(column);
        assertThat(PaimonPageSourceProvider.rowIdColumn(List.of(column))).isEmpty();
        assertThat(PaimonPageSourceProvider.rowIdColumn(List.of(column, rowId))).contains(rowId);

        assertThatThrownBy(() -> PaimonPageSourceProvider.getTableHandle(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableHandle is null");
        ConnectorTableHandle wrongTableHandle = new ConnectorTableHandle() {};
        assertThatThrownBy(() -> PaimonPageSourceProvider.getTableHandle(wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon page source requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());

        assertThatThrownBy(() -> PaimonPageSourceProvider.getSplit(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("split is null");
        ConnectorSplit wrongSplit = new ConnectorSplit()
        {
            @Override
            public Object getInfo()
            {
                return Map.of();
            }
        };
        assertThatThrownBy(() -> PaimonPageSourceProvider.getSplit(wrongSplit))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon page source requires PaimonSplit, got: %s",
                        wrongSplit.getClass().getName());
        assertThatThrownBy(() -> PaimonPageSourceProvider.rowIdColumn(Arrays.asList(column, null)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("columns contains null column");
        assertThatThrownBy(() -> PaimonPageSourceProvider.rowIdColumn(Arrays.asList(rowId, rowId)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon page source expected at most one row id column, got: 2");
    }

    @Test
    void testDirectRawFileFormatsAreCaseInsensitiveForHardenedFormatsOnly()
    {
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(List.of(rawFile("ORC")), List.of(
                PaimonColumnHandle.of("bytes", DataTypes.VARBINARY(10))))).isTrue();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(List.of(rawFile("PARQUET")), List.of(
                PaimonColumnHandle.of("bytes", DataTypes.VARBINARY(10))))).isTrue();
        assertThat(PaimonPageSourceProvider.canUseTrinoPageSource(List.of(rawFile("AVRO")), List.of(
                PaimonColumnHandle.of("bytes", DataTypes.VARBINARY(10))))).isFalse();
    }

    @Test
    void testTrinoFormatProviderUnsupportedTypeDetection()
    {
        assertThat(PaimonTypeUtils.containsUnsupportedTrinoFormatProviderReadType(
                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))).isFalse();
        assertThat(PaimonTypeUtils.containsUnsupportedTrinoFormatProviderWriteType(
                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))).isFalse();
        assertThat(PaimonTypeUtils.containsUnsupportedTrinoFormatProviderReadType(DataTypes.BLOB())).isTrue();
        assertThat(PaimonTypeUtils.containsUnsupportedTrinoFormatProviderWriteType(DataTypes.BLOB())).isTrue();
        assertThat(PaimonTypeUtils.containsUnsupportedTrinoFormatProviderReadType(DataTypes.VARIANT())).isTrue();
        assertThat(PaimonTypeUtils.containsUnsupportedTrinoFormatProviderWriteType(DataTypes.VARIANT())).isTrue();
        assertThat(PaimonTypeUtils.containsUnsupportedTrinoFormatProviderReadType(
                DataTypes.VECTOR(3, DataTypes.FLOAT()))).isTrue();
        assertThat(PaimonTypeUtils.containsUnsupportedTrinoFormatProviderWriteType(
                DataTypes.VECTOR(3, DataTypes.FLOAT()))).isTrue();
        assertThat(PaimonTypeUtils.containsUnsupportedTrinoFormatProviderReadType(
                DataTypes.ARRAY(DataTypes.BLOB()))).isTrue();
        assertThat(PaimonTypeUtils.containsUnsupportedTrinoFormatProviderWriteType(
                DataTypes.ARRAY(DataTypes.BLOB()))).isTrue();
        assertThatThrownBy(() -> PaimonTypeUtils.containsUnsupportedTrinoFormatProviderReadType(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("type is null");
        assertThatThrownBy(() -> PaimonTypeUtils.containsUnsupportedTrinoFormatProviderWriteType(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("type is null");
    }

    @Test
    void testPaimonReaderFallbackReadsFilterColumns()
    {
        PaimonColumnHandle categoryColumn = PaimonColumnHandle.of("category", DataTypes.STRING());
        org.apache.paimon.types.MapType propertiesType = DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING());
        PaimonColumnHandle propertiesRegionColumn = PaimonColumnHandle.of(toMapKey("properties", "region"), propertiesType);
        TupleDomain<PaimonColumnHandle> filter = TupleDomain.withColumnDomains(Map.of(
                categoryColumn, Domain.singleValue(VARCHAR, Slices.utf8Slice("keep")),
                propertiesRegionColumn, Domain.singleValue(VARCHAR, Slices.utf8Slice("ap-south"))));

        assertThat(PaimonPageSourceProvider.readerFields(List.of("id", "category", "payload", "properties"), List.of("id", "payload"), filter))
                .containsExactly("id", "payload", "category", "properties");
        assertThat(PaimonPageSourceProvider.readerFields(List.of("id", "category", "payload", "properties"), List.of("id", "CATEGORY", "properties"), filter))
                .containsExactly("id", "CATEGORY", "properties");
        assertThat(PaimonPageSourceProvider.readerFields(List.of("id", "category", "payload", "properties"), List.of("id"), TupleDomain.all()))
                .containsExactly("id");
        assertThatThrownBy(() -> PaimonPageSourceProvider.readerFields(null, List.of("id"), filter))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fieldNames is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.readerFields(List.of("id"), null, filter))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("projectedFields is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.readerFields(List.of("id"), List.of("id"), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("readerFilter is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.readerFields(List.of("id"), Arrays.asList("id", null), filter))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("projectedFields contains null field");
        assertThatThrownBy(() -> PaimonPageSourceProvider.readerFields(Arrays.asList("id", null), List.of("id"), filter))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fieldNames contains null field");
    }

    @Test
    void testDirectReaderDomainsAreDisabledWhenDeletionVectorsArePresent()
    {
        PaimonColumnHandle idColumn = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        TupleDomain<PaimonColumnHandle> filter = TupleDomain.withColumnDomains(Map.of(
                idColumn, Domain.singleValue(BIGINT, 2L)));

        List<Domain> domainsWithoutDeletionVectors = PaimonPageSourceProvider.directReaderDomains(
                List.of("id", "payload"), filter, false);
        List<Domain> domainsWithDeletionVectors = PaimonPageSourceProvider.directReaderDomains(
                List.of("id", "payload"), filter, true);

        assertThat(domainsWithoutDeletionVectors).containsExactly(Domain.singleValue(BIGINT, 2L), null);
        assertThat(domainsWithDeletionVectors).containsExactly(null, null);
    }

    @Test
    void testDirectReaderDomainsMatchProjectedFieldsCaseInsensitively()
    {
        PaimonColumnHandle idColumn = PaimonColumnHandle.of("ID", DataTypes.BIGINT());
        TupleDomain<PaimonColumnHandle> filter = TupleDomain.withColumnDomains(Map.of(
                idColumn, Domain.singleValue(BIGINT, 2L)));

        assertThat(PaimonPageSourceProvider.directReaderDomains(List.of("id"), filter, false))
                .containsExactly(Domain.singleValue(BIGINT, 2L));
    }

    @Test
    void testDirectReaderSupportsFilterOnlyWhenProjectedFieldsCoverFilterColumns()
    {
        PaimonColumnHandle categoryColumn = PaimonColumnHandle.of("CATEGORY", DataTypes.STRING());
        org.apache.paimon.types.MapType propertiesType = DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING());
        PaimonColumnHandle propertiesRegionColumn = PaimonColumnHandle.of(toMapKey("properties", "region"), propertiesType);
        TupleDomain<PaimonColumnHandle> categoryFilter = TupleDomain.withColumnDomains(Map.of(
                categoryColumn, Domain.singleValue(VARCHAR, Slices.utf8Slice("keep"))));
        TupleDomain<PaimonColumnHandle> propertiesRegionFilter = TupleDomain.withColumnDomains(Map.of(
                propertiesRegionColumn, Domain.singleValue(VARCHAR, Slices.utf8Slice("ap-south"))));

        assertThat(PaimonPageSourceProvider.directReaderSupportsFilter(List.of("id", "category"), categoryFilter))
                .isTrue();
        assertThat(PaimonPageSourceProvider.directReaderSupportsFilter(List.of("id"), categoryFilter))
                .isFalse();
        assertThat(PaimonPageSourceProvider.directReaderSupportsFilter(List.of("id", "properties"), propertiesRegionFilter))
                .isTrue();
        assertThat(PaimonPageSourceProvider.directReaderSupportsFilter(List.of("id"), propertiesRegionFilter))
                .isFalse();
        assertThat(PaimonPageSourceProvider.directReaderSupportsFilter(List.of("id"), TupleDomain.all()))
                .isTrue();
        assertThat(PaimonPageSourceProvider.directReaderSupportsFilter(List.of("id"), TupleDomain.none()))
                .isFalse();
        assertThatThrownBy(() -> PaimonPageSourceProvider.directReaderSupportsFilter(null, categoryFilter))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("projectedFields is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.directReaderSupportsFilter(List.of("id"), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("filter is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.directReaderSupportsFilter(Arrays.asList("id", null), categoryFilter))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("projectedFields contains null field");
    }

    @Test
    void testReaderFilterDropsHiddenRowIdPredicate()
    {
        PaimonColumnHandle rowIdColumn = PaimonColumnHandle.of("_row_id", org.apache.paimon.table.SpecialFields.ROW_ID.type());
        PaimonColumnHandle idColumn = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        TupleDomain<PaimonColumnHandle> filter = TupleDomain.withColumnDomains(Map.of(
                rowIdColumn, Domain.singleValue(BIGINT, 7L),
                idColumn, Domain.singleValue(BIGINT, 11L)));

        assertThat(PaimonPageSourceProvider.readerFilter(filter).getDomains().orElseThrow())
                .containsOnly(Map.entry(idColumn, Domain.singleValue(BIGINT, 11L)));
    }

    @Test
    void testReaderFilterLeavesNonRowIdHiddenColumnsUntouched()
    {
        PaimonColumnHandle sequenceNumberColumn = PaimonColumnHandle.of("_sequence_number",
                org.apache.paimon.table.SpecialFields.SEQUENCE_NUMBER.type());
        TupleDomain<PaimonColumnHandle> filter = TupleDomain.withColumnDomains(Map.of(
                sequenceNumberColumn, Domain.singleValue(BIGINT, 3L)));

        assertThat(PaimonPageSourceProvider.readerFilter(filter)).isEqualTo(filter);
    }

    @Test
    void testCanSkipDirectReadFileWhenMissingFilteredColumnCannotMatchNull()
    {
        List<DataField> dataSchemaFields = List.of(
                new DataField(1, "id", DataTypes.BIGINT()));
        List<String> dataFileColumns = Arrays.asList("id", null);
        List<Domain> filterDomains = Arrays.asList(
                null,
                Domain.singleValue(VARCHAR, Slices.utf8Slice("keep")));

        assertThat(PaimonPageSourceProvider.canSkipDirectReadFile(dataFileColumns, filterDomains, dataSchemaFields))
                .isTrue();
        assertThat(PaimonPageSourceProvider.canSkipDirectReadFile(dataFileColumns,
                Arrays.asList(null, Domain.onlyNull(VARCHAR)), dataSchemaFields))
                .isFalse();
        assertThat(PaimonPageSourceProvider.canSkipDirectReadFile(
                List.of("id", "category"),
                filterDomains,
                List.of(
                        new DataField(1, "id", DataTypes.BIGINT()),
                        new DataField(2, "category", DataTypes.STRING()))))
                .isFalse();
        assertThatThrownBy(() -> PaimonPageSourceProvider.canSkipDirectReadFile(null, filterDomains, dataSchemaFields))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("dataFileColumns is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.canSkipDirectReadFile(dataFileColumns, null, dataSchemaFields))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("filterDomains is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.canSkipDirectReadFile(dataFileColumns, filterDomains, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("dataSchemaFields is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.canSkipDirectReadFile(dataFileColumns, List.of(), dataSchemaFields))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("filterDomains count (0) must match dataFileColumns count (2)");
        assertThatThrownBy(() -> PaimonPageSourceProvider.canSkipDirectReadFile(dataFileColumns, filterDomains,
                Collections.singletonList(null)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("dataSchemaFields contains null field");
    }

    @Test
    void testDirectReaderDomainsRejectCaseInsensitiveDomainConflicts()
    {
        PaimonColumnHandle upperIdColumn = PaimonColumnHandle.of("ID", DataTypes.BIGINT());
        PaimonColumnHandle lowerIdColumn = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        TupleDomain<PaimonColumnHandle> filter = TupleDomain.withColumnDomains(Map.of(
                upperIdColumn, Domain.singleValue(BIGINT, 2L),
                lowerIdColumn, Domain.singleValue(BIGINT, 3L)));

        assertThatThrownBy(() -> PaimonPageSourceProvider.directReaderDomains(List.of("id"), filter, false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Filter contains conflicting domains for field 'id'");
    }

    @Test
    void testDirectReaderDomainsRejectCaseInsensitiveDuplicateDomainHandles()
    {
        PaimonColumnHandle upperIdColumn = PaimonColumnHandle.of("ID", DataTypes.BIGINT());
        PaimonColumnHandle lowerIdColumn = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        Domain idDomain = Domain.singleValue(BIGINT, 2L);
        TupleDomain<PaimonColumnHandle> filter = TupleDomain.withColumnDomains(Map.of(
                upperIdColumn, idDomain,
                lowerIdColumn, idDomain));

        assertThatThrownBy(() -> PaimonPageSourceProvider.directReaderDomains(List.of("id"), filter, false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Filter contains conflicting domains for field 'id'");
    }

    @Test
    void testNoneFilterUsesEmptyPageSourceInsteadOfDirectReaderDomains()
    {
        ConnectorPageSource pageSource = PaimonPageSourceProvider.emptyPageSource();

        assertThat(pageSource.isFinished()).isTrue();
        assertThat(pageSource.getNextPage()).isNull();

        assertThatThrownBy(() -> PaimonPageSourceProvider.directReaderDomains(
                List.of("id"), TupleDomain.none(), false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Direct raw-file reads must not receive TupleDomain.none()");
    }

    @Test
    void testPageSourceProviderShortCircuitsNoneFilterBeforeCatalogAndRowIdMapping()
    {
        PaimonPageSourceProvider provider = new PaimonPageSourceProvider(
                session -> {
                    throw new AssertionError("filesystem should not be used by TupleDomain.none()");
                },
                new PaimonMetadataFactory(new Options(),
                        session -> {
                            throw new AssertionError("catalog should not be initialized by TupleDomain.none()");
                        },
                        TESTING_TYPE_MANAGER),
                new OrcReaderConfig(),
                new ParquetReaderConfig());
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of(),
                TupleDomain.none(), Optional.empty(), Optional.empty(), OptionalLong.empty());
        PaimonColumnHandle rowId = PaimonColumnHandle.of(PaimonColumnHandle.TRINO_ROW_ID_NAME,
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT())),
                RowType.from(List.of(RowType.field("id", BIGINT))));

        ConnectorPageSource pageSource = provider.createPageSource(null, SESSION,
                new PaimonSplit("serialized-split", 1.0), tableHandle, List.of(rowId), DynamicFilter.EMPTY);

        assertThat(pageSource.isFinished()).isTrue();
        assertThat(pageSource.getNextPage()).isNull();
    }

    @Test
    void testPageSourceProviderShortCircuitsLateDynamicNoneFilterBeforeCatalog()
    {
        PaimonPageSourceProvider provider = new PaimonPageSourceProvider(
                session -> {
                    throw new AssertionError("filesystem should not be used by late dynamic TupleDomain.none()");
                },
                new PaimonMetadataFactory(new Options(),
                        session -> {
                            throw new AssertionError("catalog should not be initialized by late dynamic TupleDomain.none()");
                        },
                        TESTING_TYPE_MANAGER),
                new OrcReaderConfig(),
                new ParquetReaderConfig());
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of(),
                TupleDomain.all(), Optional.empty(), Optional.empty(), OptionalLong.empty());

        ConnectorPageSource pageSource = provider.createPageSource(null, SESSION,
                new PaimonSplit("serialized-split", 1.0), tableHandle, List.of(), dynamicFilter(TupleDomain.none()));

        assertThat(pageSource.isFinished()).isTrue();
        assertThat(pageSource.getNextPage()).isNull();
    }

    @Test
    void testPaimonReaderFallbackUsesTrinoFileFormatForUnsupportedTypes()
    {
        AtomicReference<Map<String, String>> copyOptions = new AtomicReference<>();
        UnsupportedOperationException readFailure = new UnsupportedOperationException(
                "Trino Paimon file format does not support Paimon BLOB, VARIANT, VECTOR, or MULTISET reads");
        FileStoreTable table = readFailingFileStoreTable(copyOptions, DataTypes.ROW(
                DataTypes.FIELD(0, "payload", DataTypes.BLOB())), readFailure);
        PaimonPageSourceProvider provider = new PaimonPageSourceProvider(
                session -> {
                    throw new AssertionError("filesystem should not be used by Paimon reader fallback");
                },
                new PaimonMetadataFactory(new Options(),
                        session -> {
                            throw new AssertionError("filesystem should not be used by Paimon reader fallback catalog");
                        },
                        TESTING_TYPE_MANAGER)
                {
                    @Override
                    public PaimonMetadata create()
                    {
                        return new PaimonMetadata(new TestingCatalog(table), TESTING_TYPE_MANAGER);
                    }
                },
                new OrcReaderConfig(),
                new ParquetReaderConfig());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.of(List.of(PaimonColumnHandle.of("payload", DataTypes.BLOB()))),
                Optional.empty(),
                OptionalLong.empty());
        ConnectorSession session = io.trino.testing.TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .build();

        assertThatThrownBy(() -> provider.createPageSource(null, session,
                PaimonSplit.fromSplit(testingSplit(1), 1.0),
                tableHandle,
                List.of(PaimonColumnHandle.of("payload", DataTypes.BLOB())),
                DynamicFilter.EMPTY))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage(
                            "Paimon page read uses features which are not supported by the Trino connector");
                    assertThat(exception.getCause()).isSameAs(readFailure);
                });
        assertThat(copyOptions.get()).isNull();
    }

    @Test
    void testPageSourceProviderRejectsNullSessionAndDynamicFilterBeforeNoneFilterShortCircuit()
    {
        PaimonPageSourceProvider provider = new PaimonPageSourceProvider(
                session -> {
                    throw new AssertionError("filesystem should not be used by malformed input");
                },
                new PaimonMetadataFactory(new Options(),
                        session -> {
                            throw new AssertionError("catalog should not be initialized by malformed input");
                        },
                        TESTING_TYPE_MANAGER),
                new OrcReaderConfig(),
                new ParquetReaderConfig());
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of(),
                TupleDomain.none(), Optional.empty(), Optional.empty(), OptionalLong.empty());
        PaimonSplit split = new PaimonSplit("serialized-split", 1.0);

        assertThatThrownBy(() -> provider.createPageSource(null, null, split, tableHandle, List.of(), DynamicFilter.EMPTY))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> provider.createPageSource(null, SESSION, split, tableHandle, List.of(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("dynamicFilter is null");
    }

    @Test
    void testPageSourceEffectiveFilterAppliesLateDynamicFilterWithoutLimit()
    {
        PaimonColumnHandle idColumn = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        PaimonColumnHandle regionColumn = PaimonColumnHandle.of("region", DataTypes.BIGINT());
        TupleDomain<PaimonColumnHandle> staticFilter = TupleDomain.withColumnDomains(Map.of(
                regionColumn, Domain.singleValue(BIGINT, 7L)));
        TupleDomain<ColumnHandle> lateDynamicFilter = TupleDomain.withColumnDomains(Map.of(
                (ColumnHandle) idColumn, Domain.singleValue(BIGINT, 11L)));
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                staticFilter,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThat(PaimonPageSourceProvider.effectiveFilter(tableHandle, dynamicFilter(lateDynamicFilter)).getDomains().orElseThrow())
                .containsEntry(regionColumn, Domain.singleValue(BIGINT, 7L))
                .containsEntry(idColumn, Domain.singleValue(BIGINT, 11L));
    }

    @Test
    void testPageSourceEffectiveFilterIgnoresLateDynamicFilterAfterAcceptedLimit()
    {
        PaimonColumnHandle idColumn = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        PaimonColumnHandle regionColumn = PaimonColumnHandle.of("region", DataTypes.BIGINT());
        TupleDomain<PaimonColumnHandle> staticFilter = TupleDomain.withColumnDomains(Map.of(
                regionColumn, Domain.singleValue(BIGINT, 7L)));
        TupleDomain<ColumnHandle> lateDynamicFilter = TupleDomain.withColumnDomains(Map.of(
                (ColumnHandle) idColumn, Domain.singleValue(BIGINT, 11L)));
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                staticFilter,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(5));

        assertThat(PaimonPageSourceProvider.effectiveFilter(tableHandle, dynamicFilter(lateDynamicFilter)))
                .isEqualTo(staticFilter);
    }

    @Test
    void testPageSourceProviderRejectsNullConstructorDependencies()
    {
        PaimonMetadataFactory metadataFactory = new PaimonMetadataFactory(new Options(),
                session -> {
                    throw new UnsupportedOperationException("not used");
                },
                TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> new PaimonPageSourceProvider(
                null, metadataFactory, new OrcReaderConfig(), new ParquetReaderConfig()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fileSystemFactory is null");
        assertThatThrownBy(() -> new PaimonPageSourceProvider(
                session -> {
                    throw new UnsupportedOperationException("not used");
                },
                null,
                new OrcReaderConfig(),
                new ParquetReaderConfig()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("trinoMetadataFactory is null");
        assertThatThrownBy(() -> new PaimonPageSourceProvider(
                session -> {
                    throw new UnsupportedOperationException("not used");
                },
                metadataFactory,
                null,
                new ParquetReaderConfig()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("orcReaderConfig is null");
        assertThatThrownBy(() -> new PaimonPageSourceProvider(
                session -> {
                    throw new UnsupportedOperationException("not used");
                },
                metadataFactory,
                new OrcReaderConfig(),
                null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("parquetReaderConfig is null");
    }

    @Test
    void testDirectReaderDomainsAreDisabledOnlyForFilesWithDeletionVectors()
    {
        PaimonColumnHandle idColumn = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        TupleDomain<PaimonColumnHandle> filter = TupleDomain.withColumnDomains(Map.of(
                idColumn, Domain.singleValue(BIGINT, 2L)));
        Optional<List<DeletionFile>> deletionFiles = Optional.of(Arrays.asList(null, new DeletionFile("dv", 0, 10, 1L)));

        assertThat(PaimonPageSourceProvider.deletionFileAt(deletionFiles, 0)).isEmpty();
        assertThat(PaimonPageSourceProvider.directReaderDomains(
                List.of("id"), filter, PaimonPageSourceProvider.deletionFileAt(deletionFiles, 0).isPresent()))
                .containsExactly(Domain.singleValue(BIGINT, 2L));

        assertThat(PaimonPageSourceProvider.deletionFileAt(deletionFiles, 1)).isPresent();
        assertThat(PaimonPageSourceProvider.directReaderDomains(
                List.of("id"), filter, PaimonPageSourceProvider.deletionFileAt(deletionFiles, 1).isPresent()))
                .containsExactly((Domain) null);
    }

    @Test
    void testDeletionFileAtRejectsMalformedInputs()
    {
        assertThatThrownBy(() -> PaimonPageSourceProvider.deletionFileAt(null, 0))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("deletionFiles is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.deletionFileAt(Optional.empty(), -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("fileIndex is negative: -1");
        assertThatThrownBy(() -> PaimonPageSourceProvider.deletionFileAt(Optional.of(List.of()), 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("fileIndex 0 is out of range for deletionFiles count 0");
    }

    @Test
    void testProjectionIndexesUseCaseInsensitiveIdentity()
    {
        int[] projectionIndexes = PaimonPageSourceProvider.projectionIndexes(
                List.of("ID", "Payload"),
                List.of("id", "payload"));

        assertThat(projectionIndexes).containsExactly(0, 1);
        assertThat(PaimonPageSourceProvider.isIdentityProjection(projectionIndexes, 2)).isTrue();
    }

    @Test
    void testProjectionIndexesDetectReorderedAndMissingFields()
    {
        int[] projectionIndexes = PaimonPageSourceProvider.projectionIndexes(
                List.of("id", "payload", "extra"),
                List.of("payload", "id"));

        assertThat(projectionIndexes).containsExactly(1, 0);
        assertThat(PaimonPageSourceProvider.isIdentityProjection(projectionIndexes, 3)).isFalse();

        assertThatThrownBy(() -> PaimonPageSourceProvider.projectionIndexes(
                List.of("id", "payload"),
                List.of("missing")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Projected field 'missing' does not exist in table fields [id, payload]");
    }

    @Test
    void testProjectionIndexesRejectCaseInsensitiveDuplicateTableFields()
    {
        assertThatThrownBy(() -> PaimonPageSourceProvider.projectionIndexes(
                List.of("id", "ID"),
                List.of("Id")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Table fields contain case-insensitive duplicate field name 'Id': [id, ID]");
    }

    @Test
    void testDirectRawFileMetadataFilesMustAlign()
    {
        PaimonPageSourceProvider.validateAlignedMetadataFiles("indexFiles", Optional.empty(), 2);
        PaimonPageSourceProvider.validateAlignedMetadataFiles("indexFiles", Optional.of(List.of(new Object(), new Object())), 2);

        assertThatThrownBy(() -> PaimonPageSourceProvider.validateAlignedMetadataFiles(null, Optional.empty(), 2))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("name is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.validateAlignedMetadataFiles("indexFiles", null, 2))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("files is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.validateAlignedMetadataFiles("indexFiles", Optional.empty(), -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("rawFileCount is negative: -1");
        assertThatThrownBy(() -> PaimonPageSourceProvider.validateAlignedMetadataFiles(
                "deletionFiles", Optional.of(List.of(new Object())), 2))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("deletionFiles count (1) must match raw file count (2)");
    }

    @Test
    void testDirectPageSourceInputsRejectMalformedInputs()
    {
        MemoryInputFile inputFile = new MemoryInputFile(Location.of("memory:///data.orc"), EMPTY_SLICE);

        PaimonPageSourceProvider.validateDirectPageSourceInputs("orc", inputFile, Arrays.asList("id", null),
                List.of(BIGINT, VARCHAR), Arrays.asList(Domain.all(BIGINT), null));

        assertThatThrownBy(() -> PaimonPageSourceProvider.validateDirectPageSourceInputs(null, inputFile,
                List.of("id"), List.of(BIGINT), List.of(Domain.all(BIGINT))))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("format is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.validateDirectPageSourceInputs(" ", inputFile,
                List.of("id"), List.of(BIGINT), List.of(Domain.all(BIGINT))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("format is blank");
        assertThatThrownBy(() -> PaimonPageSourceProvider.validateDirectPageSourceInputs("orc", null,
                List.of("id"), List.of(BIGINT), List.of(Domain.all(BIGINT))))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("inputFile is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.validateDirectPageSourceInputs("orc", inputFile,
                null, List.of(BIGINT), List.of(Domain.all(BIGINT))))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("columns is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.validateDirectPageSourceInputs("orc", inputFile,
                List.of("id"), null, List.of(Domain.all(BIGINT))))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("types is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.validateDirectPageSourceInputs("orc", inputFile,
                List.of("id"), List.of(BIGINT), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("domains is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.validateDirectPageSourceInputs("orc", inputFile,
                List.of("id"), List.of(BIGINT, VARCHAR), List.of(Domain.all(BIGINT))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("types count (2) must match columns count (1)");
        assertThatThrownBy(() -> PaimonPageSourceProvider.validateDirectPageSourceInputs("orc", inputFile,
                List.of("id"), List.of(BIGINT), Arrays.asList(Domain.all(BIGINT), null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("domains count (2) must match columns count (1)");
        assertThatThrownBy(() -> PaimonPageSourceProvider.validateDirectPageSourceInputs("orc", inputFile,
                List.of(" "), List.of(BIGINT), List.of(Domain.all(BIGINT))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("columns contains blank column");
        assertThatThrownBy(() -> PaimonPageSourceProvider.validateDirectPageSourceInputs("orc", inputFile,
                List.of("id"), Collections.singletonList(null), List.of(Domain.all(BIGINT))))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("types contains null type");
    }

    @Test
    void testOrcFooterFieldsRejectCaseInsensitiveDuplicates()
    {
        assertThat(PaimonPageSourceProvider.orcFieldsByLowercaseName(List.of(orcColumn("ID", 1))))
                .containsOnlyKeys("id");

        assertThatThrownBy(() -> PaimonPageSourceProvider.orcFieldsByLowercaseName(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("columns is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.orcFieldsByLowercaseName(Collections.singletonList(null)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("columns contains null column");
        assertThatThrownBy(() -> PaimonPageSourceProvider.orcFieldsByLowercaseName(List.of(
                orcColumn("ID", 1),
                orcColumn("id", 2))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("ORC file schema contains case-insensitive duplicate field name 'id'");
    }

    @Test
    void testParquetFooterFieldsRejectCaseInsensitiveDuplicates()
    {
        assertThat(PaimonPageSourceProvider.parquetFieldsByLowercaseName(List.of(parquetField("ID"))))
                .containsOnlyKeys("id");

        assertThatThrownBy(() -> PaimonPageSourceProvider.parquetFieldsByLowercaseName(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fields is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.parquetFieldsByLowercaseName(Collections.singletonList(null)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fields contains null field");
        assertThatThrownBy(() -> PaimonPageSourceProvider.parquetFieldsByLowercaseName(List.of(
                parquetField("ID"),
                parquetField("id"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Parquet file schema contains case-insensitive duplicate field name 'id'");
    }

    @Test
    void testParquetColumnAdaptationDoesNotHideExistingUnreadableColumns()
    {
        ParquetPageSource.Builder pageSourceBuilder = ParquetPageSource.builder();
        ImmutableList.Builder<Column> parquetColumnsBuilder = ImmutableList.builder();

        int nextChannel = PaimonPageSourceProvider.addParquetColumn("missing", BIGINT, Optional.empty(),
                Optional.empty(), pageSourceBuilder, parquetColumnsBuilder, 0);

        assertThat(nextChannel).isEqualTo(0);
        assertThat(parquetColumnsBuilder.build()).isEmpty();
        assertThatThrownBy(() -> PaimonPageSourceProvider.addParquetColumn("payload", BIGINT,
                Optional.of("payload"), Optional.empty(), ParquetPageSource.builder(), ImmutableList.builder(), 0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Parquet file column 'payload' exists but cannot be read as bigint");
    }

    @Test
    void testSchemaEvolutionFieldNamesRequireCurrentTableFields()
    {
        List<org.apache.paimon.types.DataField> tableFields = List.of(
                new org.apache.paimon.types.DataField(1, "ID", DataTypes.BIGINT()),
                new org.apache.paimon.types.DataField(2, "New_Value", DataTypes.STRING()));
        List<org.apache.paimon.types.DataField> dataFields = List.of(
                new org.apache.paimon.types.DataField(1, "OLD_ID", DataTypes.BIGINT()));

        assertThat(PaimonPageSourceProvider.schemaEvolutionFieldNames(
                List.of("id", "new_value"), tableFields, dataFields))
                .containsExactly("old_id", "new_value");

        List<org.apache.paimon.types.DataField> readdedTableFields = List.of(
                new org.apache.paimon.types.DataField(1, "id", DataTypes.BIGINT()),
                new org.apache.paimon.types.DataField(3, "readded", DataTypes.STRING()));
        List<org.apache.paimon.types.DataField> oldDataFields = List.of(
                new org.apache.paimon.types.DataField(1, "id", DataTypes.BIGINT()),
                new org.apache.paimon.types.DataField(2, "readded", DataTypes.STRING()));
        assertThat(PaimonPageSourceProvider.schemaEvolutionFieldNames(
                List.of("id", "readded"), readdedTableFields, oldDataFields))
                .containsExactly("id", null);

        assertThatThrownBy(() -> PaimonPageSourceProvider.schemaEvolutionFieldNames(
                List.of("missing"), tableFields, dataFields))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Projected field 'missing' does not exist in current Paimon table fields [ID, New_Value]");
    }

    @Test
    void testSchemaEvolutionFieldNamesRejectMalformedInputs()
    {
        List<org.apache.paimon.types.DataField> tableFields = List.of(
                new org.apache.paimon.types.DataField(1, "ID", DataTypes.BIGINT()));
        List<org.apache.paimon.types.DataField> dataFields = List.of(
                new org.apache.paimon.types.DataField(1, "ID", DataTypes.BIGINT()));

        assertThatThrownBy(() -> PaimonPageSourceProvider.schemaEvolutionFieldNames(null, tableFields, dataFields))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fieldNames is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.schemaEvolutionFieldNames(List.of("id"), null, dataFields))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableFields is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.schemaEvolutionFieldNames(List.of("id"), tableFields, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("dataFields is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.schemaEvolutionFieldNames(
                List.of("id"), Collections.singletonList(null), dataFields))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableFields contains null field");
        assertThatThrownBy(() -> PaimonPageSourceProvider.schemaEvolutionFieldNames(
                List.of("id"), tableFields, Collections.singletonList(null)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("dataFields contains null field");
        assertThatThrownBy(() -> PaimonPageSourceProvider.schemaEvolutionFieldNames(
                Arrays.asList("id", null), tableFields, dataFields))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fieldName is null");

        assertThatThrownBy(() -> PaimonPageSourceProvider.directReaderSupportsSchemaEvolution(null, tableFields, dataFields))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("projectedFields is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.directReaderSupportsSchemaEvolution(List.of("id"), null, dataFields))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableFields is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.directReaderSupportsSchemaEvolution(List.of("id"), tableFields, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("dataFields is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.directReaderSupportsSchemaEvolution(
                List.of("id"), Collections.singletonList(null), dataFields))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableFields contains null field");
        assertThatThrownBy(() -> PaimonPageSourceProvider.directReaderSupportsSchemaEvolution(
                List.of("id"), tableFields, Collections.singletonList(null)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("dataFields contains null field");
        assertThatThrownBy(() -> PaimonPageSourceProvider.directReaderSupportsSchemaEvolution(
                Arrays.asList("id", null), tableFields, dataFields))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fieldName is null");
    }

    @Test
    void testCurrentSchemaFieldNamesRequireCurrentTableFields()
    {
        List<org.apache.paimon.types.DataField> tableFields = List.of(
                new org.apache.paimon.types.DataField(1, "ID", DataTypes.BIGINT()),
                new org.apache.paimon.types.DataField(2, "Payload", DataTypes.STRING()));

        assertThat(PaimonPageSourceProvider.currentSchemaFieldNames(
                List.of("id", "payload"), tableFields))
                .containsExactly("id", "payload");

        assertThatThrownBy(() -> PaimonPageSourceProvider.currentSchemaFieldNames(
                List.of("stale_column"), tableFields))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Projected field 'stale_column' does not exist in current Paimon table fields [ID, Payload]");

        assertThatThrownBy(() -> PaimonPageSourceProvider.currentSchemaFieldNames(null, tableFields))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fieldNames is null");
        assertThatThrownBy(() -> PaimonPageSourceProvider.currentSchemaFieldNames(List.of("id"), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableFields is null");
    }

    @Test
    void testSchemaEvolutionFieldNamesRejectDuplicateCurrentTableFields()
    {
        List<org.apache.paimon.types.DataField> tableFields = List.of(
                new org.apache.paimon.types.DataField(1, "ID", DataTypes.BIGINT()),
                new org.apache.paimon.types.DataField(2, "id", DataTypes.BIGINT()));
        List<org.apache.paimon.types.DataField> dataFields = List.of(
                new org.apache.paimon.types.DataField(1, "ID", DataTypes.BIGINT()));

        assertThatThrownBy(() -> PaimonPageSourceProvider.schemaEvolutionFieldNames(
                List.of("id"), tableFields, dataFields))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Current Paimon table schema contains case-insensitive duplicate field name 'id'");
    }

    @Test
    void testSchemaEvolutionFieldNamesRejectDuplicateDataFileFieldIds()
    {
        List<org.apache.paimon.types.DataField> tableFields = List.of(
                new org.apache.paimon.types.DataField(1, "ID", DataTypes.BIGINT()));
        List<org.apache.paimon.types.DataField> dataFields = List.of(
                new org.apache.paimon.types.DataField(1, "ID", DataTypes.BIGINT()),
                new org.apache.paimon.types.DataField(1, "old_id", DataTypes.BIGINT()));

        assertThatThrownBy(() -> PaimonPageSourceProvider.schemaEvolutionFieldNames(
                List.of("id"), tableFields, dataFields))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon data file schema contains duplicate field id 1");
    }

    @Test
    void testSchemaEvolutionFieldNamesRejectDuplicateDataFileFieldNames()
    {
        List<org.apache.paimon.types.DataField> tableFields = List.of(
                new org.apache.paimon.types.DataField(1, "ID", DataTypes.BIGINT()),
                new org.apache.paimon.types.DataField(2, "payload", DataTypes.STRING()));
        List<org.apache.paimon.types.DataField> dataFields = List.of(
                new org.apache.paimon.types.DataField(1, "VALUE", DataTypes.BIGINT()),
                new org.apache.paimon.types.DataField(2, "value", DataTypes.STRING()));

        assertThatThrownBy(() -> PaimonPageSourceProvider.schemaEvolutionFieldNames(
                List.of("id", "payload"), tableFields, dataFields))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon data file schema contains case-insensitive duplicate field name 'value'");
    }

    @Test
    void testDirectRawFileContractExceptionsAreNotRewrapped()
    {
        TrinoException unsupported = new TrinoException(NOT_SUPPORTED, "unsupported direct read");
        IllegalStateException contractViolation = new IllegalStateException("metadata mismatch");
        UnsupportedOperationException unsupportedRead = new UnsupportedOperationException("unsupported logical type");
        IOException ioException = new IOException("cannot read");
        ParquetCorruptionException parquetCorruption = new ParquetCorruptionException(
                new ParquetDataSourceId("memory://broken.parquet"),
                "bad parquet");
        RuntimeException wrappedIoException = new RuntimeException(ioException);
        RuntimeException wrappedParquetCorruption = new RuntimeException(parquetCorruption);

        assertThat(PaimonPageSourceProvider.wrapPaimonReadException(unsupported)).isSameAs(unsupported);
        assertThat(PaimonPageSourceProvider.wrapPaimonReadException(contractViolation)).isSameAs(contractViolation);
        assertThat(PaimonPageSourceProvider.wrapPaimonReadException(unsupportedRead))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon page read uses features which are not supported by the Trino connector");
                    assertThat(exception.getCause()).isSameAs(unsupportedRead);
                });
        assertThat(PaimonPageSourceProvider.wrapPaimonReadException(parquetCorruption))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_BAD_DATA.toErrorCode());
                    assertThat(exception.getCause()).isSameAs(parquetCorruption);
                });
        assertThat(PaimonPageSourceProvider.wrapPaimonReadException(ioException))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_CANNOT_OPEN_SPLIT.toErrorCode());
                    assertThat(exception).hasMessage("Failed to open or read Paimon split");
                    assertThat(exception.getCause()).isSameAs(ioException);
                });
        assertThat(PaimonPageSourceProvider.wrapPaimonReadException(wrappedParquetCorruption))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_BAD_DATA.toErrorCode());
                    assertThat(exception.getCause()).isSameAs(parquetCorruption);
                });
        assertThat(PaimonPageSourceProvider.wrapPaimonReadException(wrappedIoException))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_CANNOT_OPEN_SPLIT.toErrorCode());
                    assertThat(exception).hasMessage("Failed to open or read Paimon split");
                    assertThat(exception.getCause()).isSameAs(ioException);
                });
        assertThat(PaimonPageSourceProvider.wrapPaimonReadException("reader failed", unsupported)).isSameAs(unsupported);
        assertThat(PaimonPageSourceProvider.wrapPaimonReadException("reader failed", contractViolation))
                .isSameAs(contractViolation);
        assertThat(PaimonPageSourceProvider.wrapPaimonReadException("reader failed", unsupportedRead))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("reader failed");
                    assertThat(exception.getCause()).isSameAs(unsupportedRead);
                });
        assertThat(PaimonPageSourceProvider.wrapPaimonReadException("reader failed", parquetCorruption))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_BAD_DATA.toErrorCode());
                    assertThat(exception.getCause()).isSameAs(parquetCorruption);
                });
        assertThat(PaimonPageSourceProvider.wrapPaimonReadException("reader failed", ioException))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_CANNOT_OPEN_SPLIT.toErrorCode());
                    assertThat(exception).hasMessage("reader failed");
                    assertThat(exception.getCause()).isSameAs(ioException);
                });
        assertThat(PaimonPageSourceProvider.wrapPaimonReadException("reader failed", wrappedParquetCorruption))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_BAD_DATA.toErrorCode());
                    assertThat(exception.getCause()).isSameAs(parquetCorruption);
                });
        assertThat(PaimonPageSourceProvider.wrapPaimonReadException("reader failed", wrappedIoException))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_CANNOT_OPEN_SPLIT.toErrorCode());
                    assertThat(exception).hasMessage("reader failed");
                    assertThat(exception.getCause()).isSameAs(ioException);
                });
    }

    @Test
    void testDirectReaderRuntimeExceptionsUsePaimonErrorCodes()
    {
        OrcDataSourceId orcDataSourceId = new OrcDataSourceId("memory://broken.orc");
        IOException orcIo = new IOException("orc cursor failed");
        IOException parquetIo = new IOException("parquet cursor failed");
        TrinoException alreadyMapped = new TrinoException(NOT_SUPPORTED, "unsupported direct read");

        assertThat(PaimonPageSourceProvider.handleOrcException(
                orcDataSourceId,
                new io.trino.orc.OrcCorruptionException(orcDataSourceId, "bad stripe")))
                .hasFieldOrPropertyWithValue("errorCode", PAIMON_BAD_DATA.toErrorCode());
        assertThat(PaimonPageSourceProvider.handleOrcException(orcDataSourceId, orcIo))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_CURSOR_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to read ORC file: " + orcDataSourceId);
                    assertThat(exception.getCause()).isSameAs(orcIo);
                });
        assertThat(PaimonPageSourceProvider.handleOrcException(orcDataSourceId, alreadyMapped)).isSameAs(alreadyMapped);

        ParquetDataSourceId parquetDataSourceId = new ParquetDataSourceId("memory://broken.parquet");
        assertThat(PaimonPageSourceProvider.handleParquetException(
                parquetDataSourceId,
                new ParquetCorruptionException(parquetDataSourceId, "bad row group")))
                .hasFieldOrPropertyWithValue("errorCode", PAIMON_BAD_DATA.toErrorCode());
        assertThat(PaimonPageSourceProvider.handleParquetException(parquetDataSourceId, parquetIo))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_CURSOR_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to read Parquet file: " + parquetDataSourceId);
                    assertThat(exception.getCause()).isSameAs(parquetIo);
                });
        assertThat(PaimonPageSourceProvider.handleParquetException(parquetDataSourceId, alreadyMapped)).isSameAs(alreadyMapped);
    }

    @Test
    void testPaimonPageSourceRequiresPaimonColumnHandles()
    {
        assertThatThrownBy(() -> new PaimonPageSource(null, List.of(), OptionalLong.empty()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("reader is null");
        assertThatThrownBy(() -> new PaimonPageSource(new TestingRecordReader(new GenericRow(0)), null,
                OptionalLong.empty()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("projectedColumns is null");
        assertThatThrownBy(() -> new PaimonPageSource(new TestingRecordReader(new GenericRow(0)),
                Arrays.asList(PaimonColumnHandle.of("id", DataTypes.BIGINT()), null), OptionalLong.empty()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("projectedColumns contains null column");
        assertThatThrownBy(() -> new PaimonPageSource(new TestingRecordReader(new GenericRow(0)), List.of(
                new ColumnHandle() {}), OptionalLong.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Paimon page source requires PaimonColumnHandle, got:");
        assertThatThrownBy(() -> new PaimonPageSource(new TestingRecordReader(new GenericRow(0)), List.of(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("limit is null");
        assertThatThrownBy(() -> new PaimonPageSource(new TestingRecordReader(new GenericRow(0)), List.of(),
                OptionalLong.of(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("limit must be non-negative");
    }

    @Test
    void testNestedPageSourceConversionExceptionsAreNotRewrapped()
    {
        RowType rowType = RowType.anonymous(List.of(INTEGER));

        assertThatThrownBy(() -> appendSingleColumn(rowType,
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(1, "name", DataTypes.STRING())),
                GenericRow.of(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Paimon ROW field count mismatch: expected 2, got 1");
    }

    @Test
    void testPaimonPageSourceWrappedReaderIoUsesCannotOpenSplit()
    {
        IOException failure = new IOException("reader batch failed");
        AtomicBoolean readerClosed = new AtomicBoolean();
        PaimonPageSource pageSource = new PaimonPageSource(
                new FailingRecordReader(new RuntimeException(failure), null, readerClosed),
                List.of(PaimonColumnHandle.of("id", DataTypes.BIGINT())),
                OptionalLong.empty());

        assertThatThrownBy(pageSource::getNextPage)
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_CANNOT_OPEN_SPLIT.toErrorCode());
                    assertThat(exception).hasMessage("Failed to open or read Paimon split");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
        assertThat(readerClosed).isTrue();
    }

    @Test
    void testPaimonPageSourceUnsupportedReaderFailureUsesNotSupported()
    {
        UnsupportedOperationException failure = new UnsupportedOperationException("vector wrapper unsupported");
        AtomicBoolean readerClosed = new AtomicBoolean();
        PaimonPageSource pageSource = new PaimonPageSource(
                new FailingRecordReader(failure, null, readerClosed),
                List.of(PaimonColumnHandle.of("id", DataTypes.BIGINT())),
                OptionalLong.empty());

        assertThatThrownBy(pageSource::getNextPage)
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage(
                            "Paimon page read uses features which are not supported by the Trino connector");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
        assertThat(readerClosed).isTrue();
    }

    @Test
    void testPageSourceArrayConversionRequiresArrayOrVectorLogicalType()
    {
        io.trino.spi.type.ArrayType arrayType = new io.trino.spi.type.ArrayType(INTEGER);

        assertThatThrownBy(() -> appendSingleColumn(arrayType, DataTypes.MAP(DataTypes.INT(), DataTypes.INT()),
                new GenericArray(new int[] {1})))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Paimon ARRAY or VECTOR logical type metadata is required");
    }

    @Test
    void testPageSourceRowConversionRequiresRowLogicalTypeAndMatchingFieldCount()
    {
        RowType rowType = RowType.anonymous(List.of(INTEGER));
        GenericRow row = GenericRow.of(1);

        assertThatThrownBy(() -> appendSingleColumn(rowType, DataTypes.ARRAY(DataTypes.INT()), row))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Paimon ROW logical type metadata is required");

        assertThatThrownBy(() -> appendSingleColumn(rowType,
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(1, "name", DataTypes.STRING())),
                row))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Paimon ROW field count mismatch: expected 2, got 1");
    }

    @Test
    void testPageSourceMultisetConversionRequiresIntegerCountType()
    {
        MapType multisetType = new MapType(VARCHAR, BIGINT, new TypeOperators());

        assertThatThrownBy(() -> appendSingleColumn(multisetType, DataTypes.MULTISET(DataTypes.STRING()),
                new GenericMap(Map.of(BinaryString.fromString("red"), 2))))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Paimon MULTISET requires Trino integer count type metadata");
    }

    @Test
    void testGetNextPageMapsUnsupportedReadFeaturesToNotSupported()
    {
        UnsupportedOperationException failure =
                new UnsupportedOperationException("Paimon MULTISET requires Trino integer count type metadata");
        AtomicBoolean readerClosed = new AtomicBoolean();
        PaimonPageSource pageSource = new PaimonPageSource(
                new FailingRecordReader(failure, null, readerClosed),
                List.of(PaimonColumnHandle.of("payload", DataTypes.INT())),
                OptionalLong.empty());

        assertThatThrownBy(pageSource::getNextPage)
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon page read uses features which are not supported by the Trino connector");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
        assertThat(readerClosed).isTrue();
    }

    @Test
    void testDirectRawFileReadsRequireFileStoreTable()
    {
        FileStoreTable fileStoreTable = fileStoreTable();
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        FileStoreTable latestFileStoreTable = fileStoreTable(copiedWithLatestSchema);

        assertThat(PaimonPageSourceProvider.requireFileStoreTableForDirectRead(fileStoreTable))
                .isSameAs(fileStoreTable);
        assertThat(PaimonPageSourceProvider.fileStoreTableForDirectRead(latestFileStoreTable, true))
                .isSameAs(latestFileStoreTable);
        assertThat(copiedWithLatestSchema).isTrue();
        assertThatThrownBy(() -> PaimonPageSourceProvider.requireFileStoreTableForDirectRead(table()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessageContaining("Paimon direct raw-file reads requires FileStoreTable, but got:");
                });
    }

    @Test
    void testDirectRawFileReadsRejectSearchWrapperTables()
    {
        assertThatThrownBy(() -> PaimonPageSourceProvider.requireFileStoreTableForDirectRead(VectorSearchTable.create(
                innerTable(),
                new VectorSearch(new float[] {1.0f}, 1, "embedding"))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon vector search tables are not supported by the Trino connector");
                });

        assertThatThrownBy(() -> PaimonPageSourceProvider.requireFileStoreTableForDirectRead(FullTextSearchTable.create(
                innerTable(),
                new FullTextSearch(FullTextQuery.match("paimon", "content"), 1))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon full-text search tables are not supported by the Trino connector");
                });
    }

    @Test
    void testPaimonReaderFallbackUsesLatestFileStoreTableSchema()
    {
        FileStoreTable latestTable = fileStoreTable(DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.BIGINT()),
                DataTypes.FIELD(1, "payload", DataTypes.STRING())));
        FileStoreTable staleTable = staleFileStoreTable(latestTable);

        assertThat(PaimonTableHandle.schemaAwareReadTable(staleTable, true))
                .isSameAs(latestTable);
    }

    @Test
    void testPaimonReaderFallbackKeepsNonFileStoreTable()
    {
        Table table = table();

        assertThat(PaimonTableHandle.schemaAwareReadTable(table, true))
                .isSameAs(table);
    }

    @Test
    void testPaimonReaderFallbackKeepsHistoricalSchemaWhenNotRefreshing()
    {
        FileStoreTable latestTable = fileStoreTable(DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.BIGINT()),
                DataTypes.FIELD(1, "payload", DataTypes.STRING())));
        FileStoreTable staleTable = staleFileStoreTable(latestTable);

        assertThat(PaimonTableHandle.schemaAwareReadTable(staleTable, false))
                .isSameAs(staleTable);
    }

    @Test
    void testDirectRawFileFileIndexPredicateUsesLatestSchema()
    {
        org.apache.paimon.types.MapType latestMapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING());
        org.apache.paimon.types.RowType latestRowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.BIGINT()),
                DataTypes.FIELD(1, "properties", latestMapType));
        FileStoreTable latestTable = fileStoreTable(latestRowType);
        FileStoreTable staleTable = staleFileStoreTable(latestTable);
        PaimonColumnHandle mapElement = PaimonColumnHandle.of(toMapKey("properties", "region"), latestMapType);
        TupleDomain<PaimonColumnHandle> filter = TupleDomain.withColumnDomains(Map.of(
                mapElement, Domain.singleValue(VARCHAR, Slices.utf8Slice("ap-south"))));

        PaimonPageSourceProvider.DirectReadTableContext context =
                PaimonPageSourceProvider.directReadTableContext(staleTable, filter, true);

        assertThat(context.table()).isSameAs(latestTable);
        assertThat(context.rowType()).isSameAs(latestRowType);
        assertThat(context.fileIndexFilter()).hasValueSatisfying(predicate -> {
            assertThat(predicate).isInstanceOf(LeafPredicate.class);
            assertThat(((LeafPredicate) predicate).fieldNames()).containsExactly(toMapKey("properties", "region"));
        });
    }

    @Test
    void testDirectRawFileContextKeepsHistoricalSchemaWhenNotRefreshing()
    {
        org.apache.paimon.types.MapType staleMapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING());
        org.apache.paimon.types.RowType staleRowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.BIGINT()),
                DataTypes.FIELD(1, "properties", staleMapType));
        FileStoreTable staleTable = fileStoreTable(staleRowType);
        PaimonColumnHandle mapElement = PaimonColumnHandle.of(toMapKey("properties", "region"), staleMapType);
        TupleDomain<PaimonColumnHandle> filter = TupleDomain.withColumnDomains(Map.of(
                mapElement, Domain.singleValue(VARCHAR, Slices.utf8Slice("ap-south"))));

        PaimonPageSourceProvider.DirectReadTableContext context =
                PaimonPageSourceProvider.directReadTableContext(staleTable, filter, false);

        assertThat(context.table()).isSameAs(staleTable);
        assertThat(context.rowType()).isEqualTo(staleRowType);
    }

    @Test
    void testDirectPageSourceEnforcesLimitAcrossSources()
    {
        TestingPageSource first = new TestingPageSource(new Page(3, bigintBlock(1, 2, 3)));
        TestingPageSource second = new TestingPageSource(new Page(3, bigintBlock(4, 5, 6)));
        DirectTrinoPageSource pageSource = new DirectTrinoPageSource(new LinkedList<>(List.of(first, second)),
                OptionalLong.of(4));

        Page firstPage = pageSource.getNextPage();
        Page secondPage = pageSource.getNextPage();

        assertThat(firstPage.getPositionCount()).isEqualTo(3);
        assertThat(secondPage.getPositionCount()).isEqualTo(1);
        assertThat(TypeUtils.readNativeValue(BIGINT, secondPage.getBlock(0), 0)).isEqualTo(4L);
        assertThat(pageSource.getNextPage()).isNull();
        assertThat(first.closed()).isTrue();
        assertThat(second.closed()).isTrue();
    }

    @Test
    void testDirectPageSourceLoadsLimitedPageBeforeClosingSource()
    {
        AtomicBoolean sourceClosed = new AtomicBoolean();
        ClosableLazyPageSource source = new ClosableLazyPageSource(new Page(3,
                new LazyBlock(3, () -> {
                    if (sourceClosed.get()) {
                        throw new IllegalStateException("source already closed");
                    }
                    return bigintBlock(1, 2, 3);
                })), sourceClosed);
        DirectTrinoPageSource pageSource = new DirectTrinoPageSource(new LinkedList<>(List.of(source)),
                OptionalLong.of(2));

        Page page = pageSource.getNextPage();

        assertThat(source.closed()).isTrue();
        assertThat(page.getBlock(0).isLoaded()).isTrue();
        assertThat(page.getPositionCount()).isEqualTo(2);
        assertThat(TypeUtils.readNativeValue(BIGINT, page.getBlock(0), 0)).isEqualTo(1L);
        assertThat(TypeUtils.readNativeValue(BIGINT, page.getBlock(0), 1)).isEqualTo(2L);
    }

    @Test
    void testDirectPageSourceDoesNotAdvanceWhenCurrentSourceIsBlocked()
    {
        BlockingPageSource first = new BlockingPageSource(new Page(1, bigintBlock(1)));
        TestingPageSource second = new TestingPageSource(new Page(1, bigintBlock(2)));
        DirectTrinoPageSource pageSource = new DirectTrinoPageSource(new LinkedList<>(List.of(first, second)),
                OptionalLong.empty());
        CompletableFuture<?> blocked = first.blockedFuture();

        assertThat(pageSource.getNextPage()).isNull();
        assertThat(pageSource.isBlocked()).isSameAs(blocked);

        Page page = pageSource.getNextPage();
        assertThat(TypeUtils.readNativeValue(BIGINT, page.getBlock(0), 0)).isEqualTo(1L);
        assertThat(pageSource.isBlocked()).isSameAs(ConnectorPageSource.NOT_BLOCKED);
        assertThat(second.closed()).isFalse();
    }

    @Test
    void testDirectPageSourceAccumulatesProgressAcrossSources()
    {
        DelegatingStatePageSource first = new DelegatingStatePageSource(
                new Page(2, bigintBlock(10, 11)),
                OptionalLong.of(0),
                123L,
                456L);
        DelegatingStatePageSource second = new DelegatingStatePageSource(
                new Page(3, bigintBlock(20, 21, 22)),
                OptionalLong.of(0),
                321L,
                654L);
        DirectTrinoPageSource pageSource = new DirectTrinoPageSource(new LinkedList<>(List.of(first, second)),
                OptionalLong.empty());

        assertThat(pageSource.getCompletedPositions()).hasValue(0L);
        assertThat(pageSource.getCompletedBytes()).isEqualTo(123L);
        assertThat(pageSource.getReadTimeNanos()).isEqualTo(456L);

        Page firstPage = pageSource.getNextPage();
        assertThat(firstPage.getPositionCount()).isEqualTo(2);
        assertThat(pageSource.getCompletedPositions()).hasValue(2L);
        assertThat(pageSource.getCompletedBytes()).isEqualTo(123L);
        assertThat(pageSource.getReadTimeNanos()).isEqualTo(456L);

        Page secondPage = pageSource.getNextPage();
        assertThat(secondPage.getPositionCount()).isEqualTo(3);
        assertThat(pageSource.getCompletedPositions()).hasValue(5L);
        assertThat(pageSource.getCompletedBytes()).isEqualTo(444L);
        assertThat(pageSource.getReadTimeNanos()).isEqualTo(1110L);
        assertThat(pageSource.getMetrics()).isEqualTo(new Metrics(Map.of("merge-wrapper", new LongCount(22))));
    }

    @Test
    void testDirectPageSourceCloseAccumulatesQueuedSourceState()
    {
        DelegatingStatePageSource first = new DelegatingStatePageSource(
                new Page(1, bigintBlock(1)),
                OptionalLong.of(0),
                10L,
                20L);
        DelegatingStatePageSource second = new DelegatingStatePageSource(
                new Page(1, bigintBlock(2)),
                OptionalLong.of(0),
                30L,
                40L);
        DirectTrinoPageSource pageSource = new DirectTrinoPageSource(new LinkedList<>(List.of(first, second)),
                OptionalLong.empty());

        pageSource.close();

        assertThat(pageSource.getCompletedPositions()).hasValue(0L);
        assertThat(pageSource.getCompletedBytes()).isEqualTo(40L);
        assertThat(pageSource.getReadTimeNanos()).isEqualTo(60L);
        assertThat(pageSource.getMetrics()).isEqualTo(new Metrics(Map.of("merge-wrapper", new LongCount(22))));
    }

    @Test
    void testDirectPageSourceLazilyOpensQueuedSources()
    {
        AtomicInteger firstOpens = new AtomicInteger();
        AtomicInteger secondOpens = new AtomicInteger();
        LinkedList<Supplier<ConnectorPageSource>> suppliers = new LinkedList<>(List.of(
                countingPageSourceSupplier(firstOpens, new TestingPageSource(new Page(1, bigintBlock(1)))),
                countingPageSourceSupplier(secondOpens, new TestingPageSource(new Page(1, bigintBlock(2))))));

        DirectTrinoPageSource pageSource = DirectTrinoPageSource.lazyPageSources(suppliers, OptionalLong.empty());

        assertThat(firstOpens).hasValue(0);
        assertThat(secondOpens).hasValue(0);

        Page firstPage = pageSource.getNextPage();
        assertThat(firstPage.getPositionCount()).isEqualTo(1);
        assertThat(firstOpens).hasValue(1);
        assertThat(secondOpens).hasValue(0);

        Page secondPage = pageSource.getNextPage();
        assertThat(secondPage.getPositionCount()).isEqualTo(1);
        assertThat(firstOpens).hasValue(1);
        assertThat(secondOpens).hasValue(1);
    }

    @Test
    void testDirectPageSourceLimitDoesNotOpenUnusedQueuedSources()
    {
        AtomicInteger firstOpens = new AtomicInteger();
        AtomicInteger secondOpens = new AtomicInteger();
        LinkedList<Supplier<ConnectorPageSource>> suppliers = new LinkedList<>(List.of(
                countingPageSourceSupplier(firstOpens, new TestingPageSource(new Page(2, bigintBlock(1, 2)))),
                countingPageSourceSupplier(secondOpens, new TestingPageSource(new Page(2, bigintBlock(3, 4))))));

        DirectTrinoPageSource pageSource = DirectTrinoPageSource.lazyPageSources(suppliers, OptionalLong.of(1));

        Page page = pageSource.getNextPage();

        assertThat(page.getPositionCount()).isEqualTo(1);
        assertThat(firstOpens).hasValue(1);
        assertThat(secondOpens).hasValue(0);
        assertThat(pageSource.getNextPage()).isNull();
    }

    @Test
    void testDirectPageSourceWrappedIoUsesCannotOpenSplit()
    {
        IOException failure = new IOException("direct page read failed");
        AtomicBoolean sourceClosed = new AtomicBoolean();
        DirectTrinoPageSource pageSource = new DirectTrinoPageSource(new LinkedList<>(List.of(
                new FailingPageSource(new RuntimeException(failure), sourceClosed))), OptionalLong.empty());

        assertThatThrownBy(pageSource::getNextPage)
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_CANNOT_OPEN_SPLIT.toErrorCode());
                    assertThat(exception).hasMessage("Failed to open or read Paimon split");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
        assertThat(sourceClosed).isTrue();
    }

    @Test
    void testDirectPageSourceAdvanceCloseFailureUsesCannotOpenSplit()
    {
        IOException failure = new IOException("closing exhausted source failed");
        AtomicBoolean firstClosed = new AtomicBoolean();
        AtomicBoolean secondClosed = new AtomicBoolean();
        DirectTrinoPageSource pageSource = new DirectTrinoPageSource(new LinkedList<>(List.of(
                new CloseFailingFinishedPageSource(firstClosed, failure),
                new FailingPageSource(new RuntimeException("should be closed during suppression"), secondClosed))),
                OptionalLong.empty());

        assertThatThrownBy(pageSource::getNextPage)
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_CANNOT_OPEN_SPLIT.toErrorCode());
                    assertThat(exception).hasMessage("Failed to open or read Paimon split");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
        assertThat(firstClosed).isTrue();
        assertThat(secondClosed).isTrue();
    }

    @Test
    void testDirectPageSourceRejectsMalformedInputs()
    {
        assertThatThrownBy(() -> new DirectTrinoPageSource(null, OptionalLong.empty()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("pageSourceQueue is null");
        assertThatThrownBy(() -> new DirectTrinoPageSource(new LinkedList<>(Arrays.asList(
                new TestingPageSource(new Page(1, bigintBlock(1))),
                null)), OptionalLong.empty()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("pageSourceQueue contains null source");
        assertThatThrownBy(() -> new DirectTrinoPageSource(new LinkedList<>(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("limit is null");
        assertThatThrownBy(() -> new DirectTrinoPageSource(new LinkedList<>(), OptionalLong.of(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("limit must be non-negative");
    }

    @Test
    void testMergePageSourceWrapperPreservesRowIdFieldOrder()
    {
        TestingPageSource source = new TestingPageSource(new Page(1, bigintBlock(10), bigintBlock(20)));
        HashMap<String, Integer> fieldToIndex = new HashMap<>();
        fieldToIndex.put("a", 0);
        fieldToIndex.put("b", 1);
        PaimonMergePageSourceWrapper wrapper = PaimonMergePageSourceWrapper.wrap(source, List.of("b", "a"),
                fieldToIndex);
        RowType rowIdType = RowType.from(List.of(RowType.field("b", BIGINT), RowType.field("a", BIGINT)));

        Page page = wrapper.getNextPage();
        SqlRow rowId = rowIdType.getObject(page.getBlock(2), 0);

        assertThat(BIGINT.getLong(rowId.getRawFieldBlock(0), rowId.getRawIndex())).isEqualTo(20);
        assertThat(BIGINT.getLong(rowId.getRawFieldBlock(1), rowId.getRawIndex())).isEqualTo(10);
    }

    @Test
    void testMergePageSourceWrapperDelegatesProgressBlockingAndMetrics()
    {
        DelegatingStatePageSource source = new DelegatingStatePageSource(new Page(1, bigintBlock(10)),
                OptionalLong.of(7), 123L, 456L);
        PaimonMergePageSourceWrapper wrapper = PaimonMergePageSourceWrapper.wrap(source, List.of("a"),
                Map.of("a", 0));

        assertThat(wrapper.getCompletedBytes()).isEqualTo(123L);
        assertThat(wrapper.getCompletedPositions()).hasValue(7L);
        assertThat(wrapper.getReadTimeNanos()).isEqualTo(456L);
        assertThat(wrapper.isBlocked()).isSameAs(source.blockedFuture());
        assertThat(wrapper.getMetrics()).isSameAs(source.metrics());
    }

    @Test
    void testMergePageSourceWrapperClosesSourceWhenRowIdConstructionFails()
    {
        AtomicBoolean closed = new AtomicBoolean();
        PaimonMergePageSourceWrapper wrapper = PaimonMergePageSourceWrapper.wrap(
                new FailingClosePageSource(new Page(1, bigintBlock(10)), closed, null),
                List.of("row_id"),
                Map.of("row_id", 1));

        assertThatThrownBy(wrapper::getNextPage)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Row id field 'row_id' maps to channel 1, but page has 1 channels");
        assertThat(closed).isTrue();
    }

    @Test
    void testDeletionVectorWrapperDoesNotRequireCompletedPositionsWithoutDeletionVector()
    {
        PaimonPageSourceWrapper wrapper = new PaimonPageSourceWrapper(
                new TestingPageSource(new Page(1, bigintBlock(10))),
                Optional.empty());

        Page page = wrapper.getNextPage();

        assertThat(TypeUtils.readNativeValue(BIGINT, page.getBlock(0), 0)).isEqualTo(10L);
    }

    @Test
    void testDeletionVectorWrapperRequiresCompletedPositionsWithDeletionVector()
    {
        PaimonPageSourceWrapper wrapper = new PaimonPageSourceWrapper(
                new TestingPageSource(new Page(1, bigintBlock(10))),
                Optional.of(emptyDeletionVector()));

        assertThatThrownBy(wrapper::getNextPage)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Deletion-vector page source requires completed positions");
    }

    @Test
    void testDeletionVectorWrapperReadsStartPositionBeforePage()
    {
        PositionTrackingPageSource source = new PositionTrackingPageSource(new Page(3, bigintBlock(10, 20, 30)),
                5);
        PaimonPageSourceWrapper wrapper = new PaimonPageSourceWrapper(source,
                Optional.of(deletionVectorDeleting(6)));

        Page page = wrapper.getNextPage();

        assertThat(source.completedPositionsReadBeforePage()).isTrue();
        assertThat(page.getPositionCount()).isEqualTo(2);
        assertThat(TypeUtils.readNativeValue(BIGINT, page.getBlock(0), 0)).isEqualTo(10L);
        assertThat(TypeUtils.readNativeValue(BIGINT, page.getBlock(0), 1)).isEqualTo(30L);
    }

    @Test
    void testDeletionVectorWrapperClosesSourceWhenDeletionFilteringFails()
    {
        AtomicBoolean closed = new AtomicBoolean();
        PaimonPageSourceWrapper wrapper = new PaimonPageSourceWrapper(
                new FailingClosePageSource(new Page(1, bigintBlock(10)), closed, null),
                Optional.of(deletionVectorDeleting(0)));

        assertThatThrownBy(wrapper::getNextPage)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Deletion-vector page source requires completed positions");
        assertThat(closed).isTrue();
    }

    @Test
    void testMergePageSourceWrapperRejectsInvalidRowIdMappings()
    {
        TestingPageSource source = new TestingPageSource(new Page(1, bigintBlock(10)));
        HashMap<String, Integer> fieldToIndex = new HashMap<>();
        fieldToIndex.put("a", 0);

        assertThatThrownBy(() -> PaimonMergePageSourceWrapper.wrap(null, List.of("a"), fieldToIndex))
                .hasMessage("pageSource is null");
        assertThatThrownBy(() -> PaimonMergePageSourceWrapper.wrap(source, null, fieldToIndex))
                .hasMessage("rowIdFields is null");
        assertThatThrownBy(() -> PaimonMergePageSourceWrapper.wrap(source, List.of(), fieldToIndex))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("rowIdFields is empty");
        assertThatThrownBy(() -> PaimonMergePageSourceWrapper.wrap(source, List.of(" "), fieldToIndex))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("rowIdFields contains blank field");
        assertThatThrownBy(() -> PaimonMergePageSourceWrapper.wrap(source, List.of("a", "a"), fieldToIndex))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("rowIdFields contains duplicate field: a");
        assertThatThrownBy(() -> PaimonMergePageSourceWrapper.wrap(source, List.of("b"), fieldToIndex))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Missing row id field: b");
        assertThatThrownBy(() -> PaimonMergePageSourceWrapper.wrap(source, List.of("a"), null))
                .hasMessage("fieldToIndex is null");
        assertThatThrownBy(() -> PaimonMergePageSourceWrapper.wrap(source, List.of("a"), mapWithNullKey()))
                .hasMessage("fieldToIndex contains null field");
        assertThatThrownBy(() -> PaimonMergePageSourceWrapper.wrap(source, List.of("a"), mapWithBlankKey()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("fieldToIndex contains blank field");
        assertThatThrownBy(() -> PaimonMergePageSourceWrapper.wrap(source, List.of("a"), mapWithNullValue("a")))
                .hasMessage("fieldToIndex contains null index for field 'a'");
        assertThatThrownBy(() -> PaimonMergePageSourceWrapper.wrap(source, List.of("a"), Map.of("a", -1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("fieldToIndex contains negative index for field 'a': -1");
    }

    @Test
    void testMergePageSourceWrapperRejectsOutOfRangeRowIdChannel()
    {
        TestingPageSource source = new TestingPageSource(new Page(1, bigintBlock(10)));
        PaimonMergePageSourceWrapper wrapper = PaimonMergePageSourceWrapper.wrap(source, List.of("a"),
                Map.of("a", 1));

        assertThatThrownBy(wrapper::getNextPage)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Row id field 'a' maps to channel 1, but page has 1 channels");
    }

    @Test
    void testRowIdFieldNamesMustBeNamed()
    {
        assertThatThrownBy(() -> PaimonPageSourceProvider.rowIdFieldNames(BIGINT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Paimon row id column must be ROW, got: bigint");

        assertThatThrownBy(() -> PaimonPageSourceProvider.rowIdFieldNames(RowType.anonymous(List.of(BIGINT))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Paimon row id field at index 0 must be named");

        assertThatThrownBy(() -> PaimonPageSourceProvider.rowIdFieldNames(RowType.from(List.of(
                RowType.field(" ", BIGINT)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Paimon row id field at index 0 is blank");

        assertThatThrownBy(() -> PaimonPageSourceProvider.rowIdFieldNames(RowType.from(List.of(
                RowType.field("id", BIGINT),
                RowType.field("id", BIGINT)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Paimon row id field 'id' appears more than once");
    }

    private static Block bigintBlock(long... values)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(values.length);
        for (long value : values) {
            BIGINT.writeLong(builder, value);
        }
        return builder.build();
    }

    private static DeletionVector emptyDeletionVector()
    {
        return deletionVectorDeleting(-1);
    }

    private static DeletionVector deletionVectorDeleting(long deletedPosition)
    {
        return new DeletionVector()
        {
            @Override
            public void delete(long position)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void merge(DeletionVector deletionVector)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isEmpty()
            {
                return true;
            }

            @Override
            public long getCardinality()
            {
                return 0;
            }

            @Override
            public int serializeTo(DataOutputStream out)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isDeleted(long position)
            {
                return position == deletedPosition;
            }
        };
    }

    private static Map<String, Integer> mapWithNullKey()
    {
        Map<String, Integer> map = new HashMap<>();
        map.put(null, 0);
        return map;
    }

    private static Map<String, Integer> mapWithBlankKey()
    {
        Map<String, Integer> map = new HashMap<>();
        map.put(" ", 0);
        return map;
    }

    private static Map<String, Integer> mapWithNullValue(String key)
    {
        Map<String, Integer> map = new HashMap<>();
        map.put(key, null);
        return map;
    }

    private static RawFile rawFile(String format)
    {
        return new RawFile("memory://file." + format, 1, 0, 1, format, 0, 1);
    }

    private static RawFile rawFile(String path, String format)
    {
        return new RawFile(path, 1, 0, 1, format, 0, 1);
    }

    private static OrcColumn orcColumn(String name, int id)
    {
        return new OrcColumn(name, new OrcColumnId(id), name, OrcTypeKind.INT,
                new OrcDataSourceId("testing.orc"), List.of(), Map.of());
    }

    private static org.apache.parquet.schema.Type parquetField(String name)
    {
        return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, org.apache.parquet.schema.Type.Repetition.OPTIONAL)
                .named(name);
    }

    private static void assertVectorRead(DataType elementLogicalType, BinaryVector vector, Type elementType,
            java.util.function.Consumer<Block> assertVectorBlock)
    {
        GenericRow row = new GenericRow(1);
        row.setField(0, vector);

        PaimonPageSource pageSource = new PaimonPageSource(new TestingRecordReader(row), List.of(
                PaimonColumnHandle.of("embedding", DataTypes.VECTOR(3, elementLogicalType))),
                OptionalLong.empty());

        Page page = pageSource.getNextPage();
        Block vectorBlock = new io.trino.spi.type.ArrayType(elementType).getObject(page.getBlock(0), 0);

        assertThat(page.getPositionCount()).isEqualTo(1);
        assertVectorBlock.accept(vectorBlock);
        assertThat(pageSource.getNextPage()).isNull();
    }

    private static List<Object> vectorValues(Type elementType, Block vectorBlock)
    {
        return java.util.stream.IntStream.range(0, vectorBlock.getPositionCount())
                .mapToObj(position -> TypeUtils.readNativeValue(elementType, vectorBlock, position))
                .toList();
    }

    private static void appendSingleColumn(Type type, DataType logicalType, Object value)
    {
        PaimonPageBuilder pageBuilder = new PaimonPageBuilder(List.of(type), List.of(logicalType));
        pageBuilder.appendRow(GenericRow.of(value));
    }

    private static FileStoreTable fileStoreTable()
    {
        return fileStoreTable(new AtomicBoolean());
    }

    private static FileStoreTable fileStoreTable(AtomicBoolean copiedWithLatestSchema)
    {
        return fileStoreTable(copiedWithLatestSchema, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.BIGINT())));
    }

    private static FileStoreTable fileStoreTable(org.apache.paimon.types.RowType rowType)
    {
        return fileStoreTable(new AtomicBoolean(), rowType);
    }

    private static FileStoreTable fileStoreTable(AtomicBoolean copiedWithLatestSchema,
            org.apache.paimon.types.RowType rowType)
    {
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonPageSourceTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield proxy;
                    }
                    case "rowType" -> rowType;
                    case "toString" -> "testing-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static FileStoreTable readFailingFileStoreTable(
            AtomicReference<Map<String, String>> copyOptions,
            org.apache.paimon.types.RowType rowType,
            UnsupportedOperationException readFailure)
    {
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonPageSourceTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copyWithLatestSchema" -> proxy;
                    case "copy" -> {
                        copyOptions.set(Map.copyOf((Map<String, String>) args[0]));
                        yield proxy;
                    }
                    case "rowType" -> rowType;
                    case "coreOptions" -> new org.apache.paimon.CoreOptions(new Options());
                    case "newReadBuilder" -> readBuilder(readFailure);
                    case "toString" -> "read-failing-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static ReadBuilder readBuilder(UnsupportedOperationException readFailure)
    {
        return (ReadBuilder) Proxy.newProxyInstance(
                PaimonPageSourceTest.class.getClassLoader(),
                new Class<?>[] {ReadBuilder.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "newRead" -> throw readFailure;
                    case "tableName" -> "testing";
                    case "toString" -> "read-failing-read-builder";
                    default -> proxy;
                });
    }

    private static FileStoreTable staleFileStoreTable(FileStoreTable latestTable)
    {
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonPageSourceTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copyWithLatestSchema" -> latestTable;
                    case "rowType" -> throw new AssertionError("stale rowType should not be used for direct reads");
                    case "toString" -> "stale-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Table table()
    {
        return (Table) Proxy.newProxyInstance(
                PaimonPageSourceTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "toString" -> "testing-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static org.apache.paimon.table.source.Split testingSplit(long rowCount)
    {
        return new org.apache.paimon.table.source.Split()
        {
            @Override
            public long rowCount()
            {
                return rowCount;
            }

            @Override
            public OptionalLong mergedRowCount()
            {
                return OptionalLong.empty();
            }
        };
    }

    private static class TestingCatalog
            extends io.trino.plugin.paimon.catalog.PaimonCatalog
    {
        private final Table table;

        private TestingCatalog(Table table)
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

    private static DynamicFilter dynamicFilter(TupleDomain<ColumnHandle> predicate)
    {
        return new DynamicFilter()
        {
            @Override
            public Set<ColumnHandle> getColumnsCovered()
            {
                return predicate.getDomains()
                        .map(Map::keySet)
                        .orElse(Set.of());
            }

            @Override
            public CompletableFuture<?> isBlocked()
            {
                return DynamicFilter.NOT_BLOCKED;
            }

            @Override
            public boolean isComplete()
            {
                return true;
            }

            @Override
            public boolean isAwaitable()
            {
                return false;
            }

            @Override
            public TupleDomain<ColumnHandle> getCurrentPredicate()
            {
                return predicate;
            }
        };
    }

    private static InnerTable innerTable()
    {
        return (InnerTable) Proxy.newProxyInstance(
                PaimonPageSourceTest.class.getClassLoader(),
                new Class<?>[] {InnerTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "toString" -> "testing-inner-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static class TestingRecordReader
            implements RecordReader<InternalRow>
    {
        private final InternalRow row;
        private boolean returned;

        private TestingRecordReader(InternalRow row)
        {
            this.row = requireNonNull(row, "row is null");
        }

        @Override
        public RecordIterator<InternalRow> readBatch()
        {
            if (returned) {
                return null;
            }
            returned = true;
            return new RecordIterator<>()
            {
                private boolean hasNext = true;

                @Override
                public InternalRow next()
                {
                    if (!hasNext) {
                        return null;
                    }
                    hasNext = false;
                    return row;
                }

                @Override
                public void releaseBatch() {}
            };
        }

        @Override
        public void close() {}
    }

    private static class FailingRecordReader
            implements RecordReader<InternalRow>
    {
        private final RuntimeException readFailure;
        private final IOException closeFailure;
        private final AtomicBoolean closed;

        private FailingRecordReader(RuntimeException readFailure, IOException closeFailure, AtomicBoolean closed)
        {
            this.readFailure = requireNonNull(readFailure, "readFailure is null");
            this.closeFailure = closeFailure;
            this.closed = requireNonNull(closed, "closed is null");
        }

        @Override
        public RecordIterator<InternalRow> readBatch()
        {
            return new RecordIterator<>()
            {
                @Override
                public InternalRow next()
                {
                    throw readFailure;
                }

                @Override
                public void releaseBatch() {}
            };
        }

        @Override
        public void close()
                throws IOException
        {
            closed.set(true);
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static class TestingPageSource
            implements ConnectorPageSource
    {
        private final Page page;
        private boolean returned;
        private boolean closed;

        private TestingPageSource(Page page)
        {
            this.page = page;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public boolean isFinished()
        {
            return returned;
        }

        @Override
        public Page getNextPage()
        {
            if (returned) {
                return null;
            }
            returned = true;
            return page;
        }

        @Override
        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public void close()
                throws IOException
        {
            closed = true;
        }

        private boolean closed()
        {
            return closed;
        }
    }

    private static Supplier<ConnectorPageSource> countingPageSourceSupplier(
            AtomicInteger openCount,
            ConnectorPageSource pageSource)
    {
        requireNonNull(openCount, "openCount is null");
        requireNonNull(pageSource, "pageSource is null");
        return () -> {
            openCount.incrementAndGet();
            return pageSource;
        };
    }

    private static class ClosableLazyPageSource
            implements ConnectorPageSource
    {
        private final Page page;
        private final AtomicBoolean closedMarker;
        private boolean returned;
        private boolean closed;

        private ClosableLazyPageSource(Page page, AtomicBoolean closedMarker)
        {
            this.page = page;
            this.closedMarker = closedMarker;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public boolean isFinished()
        {
            return returned;
        }

        @Override
        public Page getNextPage()
        {
            if (returned) {
                return null;
            }
            returned = true;
            return page;
        }

        @Override
        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public void close()
        {
            closed = true;
            closedMarker.set(true);
        }

        private boolean closed()
        {
            return closed;
        }
    }

    private static class BlockingPageSource
            implements ConnectorPageSource
    {
        private final Page page;
        private final CompletableFuture<?> blockedFuture = new CompletableFuture<>();
        private boolean blocked = true;
        private boolean returned;

        private BlockingPageSource(Page page)
        {
            this.page = page;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public boolean isFinished()
        {
            return returned;
        }

        @Override
        public Page getNextPage()
        {
            if (blocked) {
                blocked = false;
                return null;
            }
            if (returned) {
                return null;
            }
            returned = true;
            return page;
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            return blockedFuture;
        }

        @Override
        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public void close() {}

        private CompletableFuture<?> blockedFuture()
        {
            return blockedFuture;
        }
    }

    private static class PositionTrackingPageSource
            implements ConnectorPageSource
    {
        private final Page page;
        private final long completedPositionsBeforePage;
        private boolean returned;
        private boolean completedPositionsReadBeforePage;

        private PositionTrackingPageSource(Page page, long completedPositionsBeforePage)
        {
            this.page = page;
            this.completedPositionsBeforePage = completedPositionsBeforePage;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public OptionalLong getCompletedPositions()
        {
            if (!returned) {
                completedPositionsReadBeforePage = true;
                return OptionalLong.of(completedPositionsBeforePage);
            }
            return OptionalLong.of(completedPositionsBeforePage + page.getPositionCount());
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public boolean isFinished()
        {
            return returned;
        }

        @Override
        public Page getNextPage()
        {
            if (returned) {
                return null;
            }
            returned = true;
            return page;
        }

        @Override
        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public void close() {}

        private boolean completedPositionsReadBeforePage()
        {
            return completedPositionsReadBeforePage;
        }
    }

    private static class DelegatingStatePageSource
            implements ConnectorPageSource
    {
        private final Page page;
        private final OptionalLong completedPositionsBeforePage;
        private final long completedBytes;
        private final long readTimeNanos;
        private final CompletableFuture<?> blockedFuture = new CompletableFuture<>();
        private final Metrics metrics = new Metrics(Map.of("merge-wrapper", new LongCount(11)));
        private boolean returned;

        private DelegatingStatePageSource(Page page, OptionalLong completedPositionsBeforePage, long completedBytes,
                long readTimeNanos)
        {
            this.page = requireNonNull(page, "page is null");
            this.completedPositionsBeforePage = requireNonNull(completedPositionsBeforePage, "completedPositionsBeforePage is null");
            this.completedBytes = completedBytes;
            this.readTimeNanos = readTimeNanos;
        }

        @Override
        public long getCompletedBytes()
        {
            return completedBytes;
        }

        @Override
        public OptionalLong getCompletedPositions()
        {
            if (completedPositionsBeforePage.isEmpty()) {
                return OptionalLong.empty();
            }
            if (!returned) {
                return completedPositionsBeforePage;
            }
            return OptionalLong.of(completedPositionsBeforePage.getAsLong() + page.getPositionCount());
        }

        @Override
        public long getReadTimeNanos()
        {
            return readTimeNanos;
        }

        @Override
        public boolean isFinished()
        {
            return returned;
        }

        @Override
        public Page getNextPage()
        {
            if (returned) {
                return null;
            }
            returned = true;
            return page;
        }

        @Override
        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            return blockedFuture;
        }

        @Override
        public Metrics getMetrics()
        {
            return metrics;
        }

        @Override
        public void close() {}

        private CompletableFuture<?> blockedFuture()
        {
            return blockedFuture;
        }

        private Metrics metrics()
        {
            return metrics;
        }
    }

    private static class FailingClosePageSource
            implements ConnectorPageSource
    {
        private final Page page;
        private final AtomicBoolean closed;
        private final IOException closeFailure;

        private FailingClosePageSource(Page page, AtomicBoolean closed, IOException closeFailure)
        {
            this.page = requireNonNull(page, "page is null");
            this.closed = requireNonNull(closed, "closed is null");
            this.closeFailure = closeFailure;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public boolean isFinished()
        {
            return false;
        }

        @Override
        public Page getNextPage()
        {
            return page;
        }

        @Override
        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public void close()
                throws IOException
        {
            closed.set(true);
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static class FailingPageSource
            implements ConnectorPageSource
    {
        private final RuntimeException failure;
        private final AtomicBoolean closed;

        private FailingPageSource(RuntimeException failure, AtomicBoolean closed)
        {
            this.failure = requireNonNull(failure, "failure is null");
            this.closed = requireNonNull(closed, "closed is null");
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public boolean isFinished()
        {
            return false;
        }

        @Override
        public Page getNextPage()
        {
            throw failure;
        }

        @Override
        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public void close()
        {
            closed.set(true);
        }
    }

    private static class CloseFailingFinishedPageSource
            implements ConnectorPageSource
    {
        private final AtomicBoolean closed;
        private final IOException closeFailure;

        private CloseFailingFinishedPageSource(AtomicBoolean closed, IOException closeFailure)
        {
            this.closed = requireNonNull(closed, "closed is null");
            this.closeFailure = requireNonNull(closeFailure, "closeFailure is null");
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public boolean isFinished()
        {
            return true;
        }

        @Override
        public Page getNextPage()
        {
            return null;
        }

        @Override
        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public void close()
                throws IOException
        {
            closed.set(true);
            throw closeFailure;
        }
    }
}
