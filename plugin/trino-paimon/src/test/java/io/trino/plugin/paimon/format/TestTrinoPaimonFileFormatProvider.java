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
package io.trino.plugin.paimon.format;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.orc.MemoryOrcDataSource;
import io.trino.orc.OrcDataSourceId;
import io.trino.orc.OrcReader;
import io.trino.orc.OrcReaderOptions;
import io.trino.parquet.AbstractParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.reader.MetadataReader;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatProvider;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileRecordReader;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FormatReaderMapping;
import org.apache.paimon.utils.RoaringBitmap32;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTrinoPaimonFileFormatProvider
{
    private static final String UNSUPPORTED_PROVIDER_READ_MESSAGE = "Trino Paimon file format provider does not support Paimon BLOB, VARIANT, VECTOR, or MULTISET reads";
    private static final String UNSUPPORTED_PROVIDER_WRITE_MESSAGE = "Trino Paimon file format provider does not support Paimon BLOB, VARIANT, VECTOR, or MULTISET writes";

    @TempDir
    java.nio.file.Path tempDir;

    @Test
    void testParquetWriterRoundTripWithPaimonReader()
            throws Exception
    {
        assertRoundTrip("parquet", "snappy");
    }

    @Test
    void testOrcWriterRoundTripWithPaimonReader()
            throws Exception
    {
        assertRoundTrip("orc", "zstd");
    }

    @Test
    void testWriterCloseLeavesPaimonOutputStreamOpen()
            throws Exception
    {
        assertWriterCloseLeavesPaimonOutputStreamOpen("parquet", "snappy");
        assertWriterCloseLeavesPaimonOutputStreamOpen("orc", "zstd");
    }

    @Test
    void testTrinoReaderPreservesFilePositionsForPaimonSelection()
            throws Exception
    {
        assertTrinoReaderPreservesFilePositionsForPaimonSelection("parquet", "snappy");
        assertTrinoReaderPreservesFilePositionsForPaimonSelection("orc", "zstd");
    }

    @Test
    void testTrinoReaderWorksWithPaimonSchemaEvolutionMapping()
            throws Exception
    {
        assertTrinoReaderWorksWithPaimonSchemaEvolutionMapping("parquet", "snappy");
        assertTrinoReaderWorksWithPaimonSchemaEvolutionMapping("orc", "zstd");
    }

    @Test
    void testParquetWriterUsesPaimonFileBlockSize()
            throws Exception
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "payload", DataTypes.STRING()));
        Path file = new Path(tempDir.resolve("block-size-data.parquet").toUri().toString());
        LocalFileIO fileIO = LocalFileIO.create();
        FileFormat trinoWriteFormat = FileFormat.writerFromIdentifier("parquet", trinoProviderOptionsWithBlockSize(2 * 1024));
        try (PositionOutputStream out = fileIO.newOutputStream(file, false);
                FormatWriter writer = trinoWriteFormat.createWriterFactory(rowType).create(out, "snappy")) {
            for (GenericRow row : largeRows(200)) {
                writer.addElement(row);
            }
        }

        Slice data = Slices.wrappedBuffer(java.nio.file.Files.readAllBytes(java.nio.file.Path.of(file.toUri())));
        ParquetMetadata metadata = MetadataReader.readFooter(
                new SliceParquetDataSource(data, new ParquetReaderOptions()),
                java.util.Optional.empty());
        assertThat(metadata.getBlocks())
                .hasSizeGreaterThan(1)
                .allSatisfy(block -> assertThat(block.getRowCount()).isPositive());
    }

    @Test
    void testOrcWriterUsesPaimonFileBlockSize()
            throws Exception
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "payload", DataTypes.STRING()));
        Path file = new Path(tempDir.resolve("block-size-data.orc").toUri().toString());
        LocalFileIO fileIO = LocalFileIO.create();
        FileFormat trinoWriteFormat = FileFormat.writerFromIdentifier("orc", trinoProviderOptionsWithBlockSize(2 * 1024));
        try (PositionOutputStream out = fileIO.newOutputStream(file, false);
                FormatWriter writer = trinoWriteFormat.createWriterFactory(rowType).create(out, "zstd")) {
            for (GenericRow row : largeRows(200)) {
                writer.addElement(row);
            }
        }

        Slice data = Slices.wrappedBuffer(java.nio.file.Files.readAllBytes(java.nio.file.Path.of(file.toUri())));
        assertThat(OrcReader.createOrcReader(
                        new MemoryOrcDataSource(new OrcDataSourceId(file.toString()), data),
                        new OrcReaderOptions())
                .orElseThrow()
                .getFooter()
                .getStripes())
                .hasSizeGreaterThan(1)
                .allSatisfy(stripe -> assertThat(stripe.getNumberOfRows()).isPositive());
    }

    @Test
    void testWriterRejectsNonPositivePaimonFileBlockSize()
    {
        FileFormat trinoWriteFormat = FileFormat.writerFromIdentifier("parquet", trinoProviderOptionsWithBlockSize(0));

        assertThatThrownBy(() -> trinoWriteFormat.createWriterFactory(rowType()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("file.block-size must be greater than 0 bytes");
    }

    @Test
    void testTrinoReaderRejectsPaimonSpecialTypes()
    {
        FileFormat trinoReadFormat = FileFormat.readerFromIdentifier("parquet", trinoReadProviderOptions());

        for (RowType rowType : unsupportedTrinoProviderTypes()) {
            assertThatThrownBy(() -> trinoReadFormat.createReaderFactory(rowType, rowType, new ArrayList<>()))
                    .as("read provider should reject %s", rowType)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessage(UNSUPPORTED_PROVIDER_READ_MESSAGE);
        }
    }

    @Test
    void testTrinoWriterRejectsPaimonSpecialTypes()
    {
        FileFormat trinoWriteFormat = FileFormat.writerFromIdentifier("parquet", trinoProviderOptions());

        for (RowType rowType : unsupportedTrinoProviderTypes()) {
            assertThatThrownBy(() -> trinoWriteFormat.createWriterFactory(rowType))
                    .as("write provider should reject %s", rowType)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessage(UNSUPPORTED_PROVIDER_WRITE_MESSAGE);
        }
    }

    private void assertRoundTrip(String formatIdentifier, String compression)
            throws Exception
    {
        RowType rowType = rowType();
        List<GenericRow> rows = rows();
        Path file = new Path(tempDir.resolve("data." + formatIdentifier).toUri().toString());
        LocalFileIO fileIO = LocalFileIO.create();

        FileFormat trinoFormat = FileFormat.writerFromIdentifier(formatIdentifier, trinoProviderOptions());
        try (PositionOutputStream out = fileIO.newOutputStream(file, false);
                FormatWriter writer = trinoFormat.createWriterFactory(rowType).create(out, compression)) {
            for (GenericRow row : rows) {
                writer.addElement(row);
            }
        }

        FileFormat paimonFormat = FileFormat.fromIdentifier(formatIdentifier, new Options());
        assertThat(readRows(paimonFormat, rowType, fileIO, file))
                .containsExactlyElementsOf(canonicalizeRows(rowType, rows));

        FileFormat trinoReadFormat = FileFormat.readerFromIdentifier(formatIdentifier, trinoReadProviderOptions());
        assertThat(readRows(trinoReadFormat, rowType, fileIO, file))
                .containsExactlyElementsOf(canonicalizeRows(rowType, rows));
    }

    private static void assertWriterCloseLeavesPaimonOutputStreamOpen(String formatIdentifier, String compression)
            throws IOException
    {
        TrackingPositionOutputStream out = new TrackingPositionOutputStream();
        FileFormat trinoFormat = FileFormat.writerFromIdentifier(formatIdentifier, trinoProviderOptions());

        trinoFormat.createWriterFactory(rowType()).create(out, compression).close();

        assertThat(out.closed()).isFalse();
        assertThatCode(out::flush).doesNotThrowAnyException();
        assertThatCode(out::close).doesNotThrowAnyException();
        assertThat(out.closed()).isTrue();
    }

    private void assertTrinoReaderPreservesFilePositionsForPaimonSelection(String formatIdentifier, String compression)
            throws Exception
    {
        RowType rowType = rowType();
        List<GenericRow> rows = rows();
        List<InternalRow> canonicalRows = canonicalizeRows(rowType, rows);
        Path file = new Path(tempDir.resolve("selection-data." + formatIdentifier).toUri().toString());
        LocalFileIO fileIO = LocalFileIO.create();
        FileFormat trinoWriteFormat = FileFormat.writerFromIdentifier(formatIdentifier, trinoProviderOptions());
        try (PositionOutputStream out = fileIO.newOutputStream(file, false);
                FormatWriter writer = trinoWriteFormat.createWriterFactory(rowType).create(out, compression)) {
            for (GenericRow row : rows) {
                writer.addElement(row);
            }
        }

        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.add(1);
        selection.add(2);
        FileFormat trinoReadFormat = FileFormat.readerFromIdentifier(formatIdentifier, trinoReadProviderOptions());

        assertThat(readRowsWithPositions(trinoReadFormat, rowType, fileIO, file, selection))
                .containsExactly(
                        new PositionedRow(1, canonicalRows.get(1)),
                        new PositionedRow(2, canonicalRows.get(2)));
    }

    private void assertTrinoReaderWorksWithPaimonSchemaEvolutionMapping(String formatIdentifier, String compression)
            throws Exception
    {
        TableSchema dataSchema = tableSchema(
                1,
                DataTypes.ROW(
                        DataTypes.FIELD(0, "old_name", DataTypes.STRING()),
                        DataTypes.FIELD(1, "old_amount", DataTypes.INT())));
        TableSchema tableSchema = tableSchema(
                2,
                DataTypes.ROW(
                        DataTypes.FIELD(1, "amount", DataTypes.BIGINT()),
                        DataTypes.FIELD(2, "new_comment", DataTypes.STRING()),
                        DataTypes.FIELD(0, "name", DataTypes.STRING())));
        RowType tableRowType = tableSchema.logicalRowType();
        Path file = new Path(tempDir.resolve("schema-evolution-data." + formatIdentifier).toUri().toString());
        LocalFileIO fileIO = LocalFileIO.create();

        FileFormat trinoWriteFormat = FileFormat.writerFromIdentifier(formatIdentifier, trinoProviderOptions());
        try (PositionOutputStream out = fileIO.newOutputStream(file, false);
                FormatWriter writer = trinoWriteFormat.createWriterFactory(dataSchema.logicalRowType()).create(out, compression)) {
            writer.addElement(GenericRow.of(BinaryString.fromString("alpha"), 12));
            writer.addElement(GenericRow.of(BinaryString.fromString("beta"), 34));
        }

        FormatReaderMapping mapping = new FormatReaderMapping.Builder(
                identifier -> FileFormat.readerFromIdentifier(identifier, trinoReadProviderOptions()),
                tableSchema.fields(),
                TableSchema::fields,
                new ArrayList<>(),
                null,
                null)
                .build(formatIdentifier, tableSchema, dataSchema);
        InternalRowSerializer serializer = new InternalRowSerializer(tableRowType);

        try (FileRecordReader<InternalRow> reader = new DataFileRecordReader(
                tableRowType,
                mapping.getReaderFactory(),
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)),
                false,
                false,
                mapping.getIndexMapping(),
                mapping.getCastMapping(),
                null,
                false,
                null,
                0,
                mapping.getSystemFields())) {
            List<InternalRow> rows = new ArrayList<>();
            reader.forEachRemaining(row -> rows.add(serializer.toBinaryRow(row).copy()));

            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getLong(0)).isEqualTo(12L);
            assertThat(rows.get(0).isNullAt(1)).isTrue();
            assertThat(rows.get(0).getString(2)).isEqualTo(BinaryString.fromString("alpha"));
            assertThat(rows.get(1).getLong(0)).isEqualTo(34L);
            assertThat(rows.get(1).isNullAt(1)).isTrue();
            assertThat(rows.get(1).getString(2)).isEqualTo(BinaryString.fromString("beta"));
        }

        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.add(1);
        try (FileRecordReader<InternalRow> reader = new DataFileRecordReader(
                tableRowType,
                mapping.getReaderFactory(),
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file), selection),
                false,
                false,
                mapping.getIndexMapping(),
                mapping.getCastMapping(),
                null,
                false,
                null,
                0,
                mapping.getSystemFields())) {
            List<InternalRow> rows = new ArrayList<>();
            reader.forEachRemaining(row -> rows.add(serializer.toBinaryRow(row).copy()));

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getLong(0)).isEqualTo(34L);
            assertThat(rows.get(0).isNullAt(1)).isTrue();
            assertThat(rows.get(0).getString(2)).isEqualTo(BinaryString.fromString("beta"));
        }
    }

    private static Options trinoProviderOptions()
    {
        Options options = new Options();
        options.setString(FileFormatProvider.WRITE_FORMAT_PROVIDER, TrinoPaimonFileFormatProvider.IDENTIFIER);
        options.set(CoreOptions.WRITE_BATCH_SIZE, 1);
        return options;
    }

    private static Options trinoProviderOptionsWithBlockSize(long blockSizeBytes)
    {
        Options options = trinoProviderOptions();
        options.set(CoreOptions.FILE_BLOCK_SIZE, MemorySize.ofBytes(blockSizeBytes));
        return options;
    }

    private static Options trinoReadProviderOptions()
    {
        Options options = new Options();
        options.setString(FileFormatProvider.READ_FORMAT_PROVIDER, TrinoPaimonFileFormatProvider.IDENTIFIER);
        return options;
    }

    private static RowType rowType()
    {
        return DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "name", DataTypes.STRING()),
                DataTypes.FIELD(2, "amount", DataTypes.DECIMAL(12, 2)),
                DataTypes.FIELD(3, "created_at", DataTypes.TIMESTAMP(6)),
                DataTypes.FIELD(4, "scores", DataTypes.ARRAY(DataTypes.INT())),
                DataTypes.FIELD(5, "attributes", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                DataTypes.FIELD(
                        6,
                        "payload",
                        DataTypes.ROW(
                                DataTypes.FIELD(7, "flag", DataTypes.BOOLEAN()),
                                DataTypes.FIELD(8, "note", DataTypes.STRING()))));
    }

    private static List<RowType> unsupportedTrinoProviderTypes()
    {
        return List.of(
                rowTypeWith(DataTypes.BLOB()),
                rowTypeWith(DataTypes.VARIANT()),
                rowTypeWith(DataTypes.VECTOR(3, DataTypes.FLOAT())),
                rowTypeWith(DataTypes.MULTISET(DataTypes.STRING())),
                rowTypeWith(DataTypes.ARRAY(DataTypes.VARIANT())),
                rowTypeWith(DataTypes.MAP(DataTypes.STRING(), DataTypes.BLOB())),
                rowTypeWith(
                        DataTypes.ROW(
                                DataTypes.FIELD(10, "nested_vector", DataTypes.VECTOR(3, DataTypes.FLOAT())))));
    }

    private static RowType rowTypeWith(DataType type)
    {
        return DataTypes.ROW(DataTypes.FIELD(0, "payload", type));
    }

    private static TableSchema tableSchema(long id, RowType rowType)
    {
        return new TableSchema(
                id,
                rowType.getFields(),
                rowType.getFields().stream()
                        .mapToInt(DataField::id)
                        .max()
                        .orElse(0),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyMap(),
                null);
    }

    private static List<GenericRow> rows()
    {
        return List.of(
                GenericRow.of(
                        1,
                        BinaryString.fromString("alpha"),
                        Decimal.fromBigDecimal(new BigDecimal("12.34"), 12, 2),
                        Timestamp.fromEpochMillis(1_695_645_403_123L, 456_000),
                        new GenericArray(new int[] {1, 2, 3}),
                        new GenericMap(
                                Map.of(
                                        BinaryString.fromString("red"), 7,
                                        BinaryString.fromString("blue"), 11)),
                        GenericRow.of(true, BinaryString.fromString("nested-alpha"))),
                GenericRow.of(
                        2,
                        BinaryString.fromString("beta"),
                        Decimal.fromBigDecimal(new BigDecimal("56.78"), 12, 2),
                        Timestamp.fromEpochMillis(1_695_645_404_000L, 0),
                        new GenericArray(new int[] {4, 5}),
                        new GenericMap(Map.of(BinaryString.fromString("green"), 13)),
                        GenericRow.of(false, BinaryString.fromString("nested-beta"))),
                GenericRow.of(
                        3,
                        BinaryString.fromString("gamma"),
                        Decimal.fromBigDecimal(new BigDecimal("90.12"), 12, 2),
                        Timestamp.fromEpochMillis(1_695_645_405_000L, 123_000),
                        new GenericArray(new int[] {6}),
                        new GenericMap(Map.of(BinaryString.fromString("yellow"), 17)),
                        GenericRow.of(false, BinaryString.fromString("nested-beta"))));
    }

    private static List<GenericRow> largeRows(int count)
    {
        String payload = "x".repeat(1024);
        List<GenericRow> rows = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            rows.add(GenericRow.of(index, BinaryString.fromString(payload + index)));
        }
        return rows;
    }

    private static List<InternalRow> readRows(
            FileFormat format,
            RowType rowType,
            LocalFileIO fileIO,
            Path file)
            throws IOException
    {
        InternalRowSerializer serializer = new InternalRowSerializer(rowType);
        List<InternalRow> rows = new ArrayList<>();
        try (FileRecordReader<InternalRow> reader =
                format.createReaderFactory(rowType, rowType, new ArrayList<>())
                        .createReader(new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)))) {
            reader.forEachRemaining(row -> rows.add(serializer.toBinaryRow(row).copy()));
        }
        return rows;
    }

    private static List<PositionedRow> readRowsWithPositions(
            FileFormat format,
            RowType rowType,
            LocalFileIO fileIO,
            Path file,
            RoaringBitmap32 selection)
            throws IOException
    {
        InternalRowSerializer serializer = new InternalRowSerializer(rowType);
        List<PositionedRow> rows = new ArrayList<>();
        try (FileRecordReader<InternalRow> reader =
                format.createReaderFactory(rowType, rowType, new ArrayList<>())
                        .createReader(new FormatReaderContext(fileIO, file, fileIO.getFileSize(file), selection))) {
            reader.forEachRemainingWithPosition(
                    (position, row) ->
                            rows.add(new PositionedRow(position, serializer.toBinaryRow(row).copy())));
        }
        return rows;
    }

    private static List<InternalRow> canonicalizeRows(RowType rowType, List<GenericRow> rows)
    {
        InternalRowSerializer serializer = new InternalRowSerializer(rowType);
        return rows.stream()
                .map(row -> (InternalRow) serializer.toBinaryRow(row).copy())
                .toList();
    }

    private record PositionedRow(long position, InternalRow row) {}

    private static class TrackingPositionOutputStream
            extends PositionOutputStream
    {
        private long position;
        private boolean closed;

        @Override
        public long getPos()
        {
            return position;
        }

        @Override
        public void write(int b)
        {
            position++;
        }

        @Override
        public void write(byte[] b)
                throws IOException
        {
            write(b, 0, b.length);
        }

        @Override
        public void write(byte[] b, int off, int len)
        {
            position += len;
        }

        @Override
        public void flush()
                throws IOException
        {
            if (closed) {
                throw new IOException("Already closed");
            }
        }

        @Override
        public void close()
        {
            closed = true;
        }

        boolean closed()
        {
            return closed;
        }
    }

    private static class SliceParquetDataSource
            extends AbstractParquetDataSource
    {
        private final Slice data;

        private SliceParquetDataSource(Slice data, ParquetReaderOptions options)
        {
            super(new ParquetDataSourceId("slice"), data.length(), options);
            this.data = data;
        }

        @Override
        protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
        {
            data.getBytes((int) position, buffer, bufferOffset, bufferLength);
        }
    }
}
