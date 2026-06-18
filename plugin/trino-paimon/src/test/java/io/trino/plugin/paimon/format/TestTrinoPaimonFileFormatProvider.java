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
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

public class TestTrinoPaimonFileFormatProvider
{
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

    private void assertRoundTrip(String formatIdentifier, String compression)
            throws Exception
    {
        RowType rowType = rowType();
        List<GenericRow> rows = rows();
        Path file = new Path(tempDir.resolve("data." + formatIdentifier).toUri().toString());
        LocalFileIO fileIO = LocalFileIO.create();

        FileFormat trinoFormat = FileFormat.fromIdentifier(formatIdentifier, trinoProviderOptions());
        try (PositionOutputStream out = fileIO.newOutputStream(file, false);
                FormatWriter writer = trinoFormat.createWriterFactory(rowType).create(out, compression)) {
            for (GenericRow row : rows) {
                writer.addElement(row);
            }
        }

        FileFormat paimonFormat = FileFormat.fromIdentifier(formatIdentifier, new Options());
        assertThat(readRows(paimonFormat, rowType, fileIO, file))
                .containsExactlyElementsOf(canonicalizeRows(rowType, rows));
    }

    private static void assertWriterCloseLeavesPaimonOutputStreamOpen(String formatIdentifier, String compression)
            throws IOException
    {
        TrackingPositionOutputStream out = new TrackingPositionOutputStream();
        FileFormat trinoFormat = FileFormat.fromIdentifier(formatIdentifier, trinoProviderOptions());

        trinoFormat.createWriterFactory(rowType()).create(out, compression).close();

        assertThat(out.closed()).isFalse();
        assertThatCode(out::flush).doesNotThrowAnyException();
        assertThatCode(out::close).doesNotThrowAnyException();
        assertThat(out.closed()).isTrue();
    }

    private static Options trinoProviderOptions()
    {
        Options options = new Options();
        options.setString(FileFormatProvider.FORMAT_PROVIDER, TrinoPaimonFileFormatProvider.IDENTIFIER);
        options.set(CoreOptions.WRITE_BATCH_SIZE, 1);
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
                        GenericRow.of(false, BinaryString.fromString("nested-beta"))));
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

    private static List<InternalRow> canonicalizeRows(RowType rowType, List<GenericRow> rows)
    {
        InternalRowSerializer serializer = new InternalRowSerializer(rowType);
        return rows.stream()
                .map(row -> (InternalRow) serializer.toBinaryRow(row).copy())
                .toList();
    }

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
}
