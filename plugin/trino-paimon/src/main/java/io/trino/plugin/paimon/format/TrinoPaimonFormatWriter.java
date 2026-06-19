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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.orc.OrcWriter;
import io.trino.orc.OrcWriterOptions;
import io.trino.orc.OrcWriterStats;
import io.trino.orc.OutputStreamOrcDataSink;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.OrcType;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.paimon.PaimonPageBuilder;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.CloseShieldOutputStream;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.DataType;
import org.joda.time.DateTimeZone;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static io.trino.orc.OrcWriteValidation.OrcWriteValidationMode.BOTH;
import static io.trino.plugin.paimon.format.TrinoPaimonFileFormat.ORC;
import static io.trino.plugin.paimon.format.TrinoPaimonFileFormat.PARQUET;
import static java.util.Objects.requireNonNull;

class TrinoPaimonFormatWriter
        implements FormatWriter
{
    private static final String TRINO_PAIMON_WRITER_VERSION = "trino-paimon";

    private final PaimonPageBuilder pageBuilder;
    private final int writeBatchSize;
    private final WriterAdapter writer;
    private boolean closed;

    TrinoPaimonFormatWriter(
            String formatIdentifier,
            List<String> columnNames,
            List<Type> columnTypes,
            List<DataType> logicalTypes,
            int writeBatchSize,
            Optional<Long> blockSizeBytes,
            PositionOutputStream out,
            String compression)
            throws IOException
    {
        this.pageBuilder = new PaimonPageBuilder(columnTypes, logicalTypes);
        this.writeBatchSize = writeBatchSize;
        this.writer = switch (requireNonNull(formatIdentifier, "formatIdentifier is null")) {
            case PARQUET -> createParquetWriter(out, columnNames, columnTypes, blockSizeBytes, compression);
            case ORC -> createOrcWriter(out, columnNames, columnTypes, blockSizeBytes, compression);
            default -> throw new UnsupportedOperationException(
                    "Unsupported Trino Paimon file format: " + formatIdentifier);
        };
    }

    @Override
    public void addElement(InternalRow element)
            throws IOException
    {
        pageBuilder.appendRow(element);
        if (pageBuilder.isFull()
                || (writeBatchSize > 0 && pageBuilder.getPositionCount() >= writeBatchSize)) {
            flush();
        }
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize)
    {
        return suggestedCheck && writer.getWrittenBytes() + writer.getBufferedBytes() >= targetSize;
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;
        flush();
        writer.close();
    }

    private void flush()
            throws IOException
    {
        if (pageBuilder.isEmpty()) {
            return;
        }
        Page page = pageBuilder.build();
        writer.write(page);
    }

    private static WriterAdapter createParquetWriter(
            PositionOutputStream out,
            List<String> columnNames,
            List<Type> columnTypes,
            Optional<Long> blockSizeBytes,
            String compression)
    {
        ParquetSchemaConverter schemaConverter =
                new ParquetSchemaConverter(columnTypes, columnNames, true, true);
        ParquetWriterOptions.Builder writerOptions = ParquetWriterOptions.builder();
        blockSizeBytes.map(DataSize::ofBytes).ifPresent(writerOptions::setMaxBlockSize);
        ParquetWriter parquetWriter =
                new ParquetWriter(
                        new CloseShieldOutputStream(out),
                        schemaConverter.getMessageType(),
                        schemaConverter.getPrimitiveTypes(),
                        writerOptions.build(),
                        parquetCompressionCodec(compression),
                        TRINO_PAIMON_WRITER_VERSION,
                        Optional.of(DateTimeZone.UTC),
                        Optional.empty());
        return new ParquetWriterAdapter(parquetWriter);
    }

    private static WriterAdapter createOrcWriter(
            PositionOutputStream out,
            List<String> columnNames,
            List<Type> columnTypes,
            Optional<Long> blockSizeBytes,
            String compression)
            throws IOException
    {
        OrcWriterOptions writerOptions = new OrcWriterOptions();
        if (blockSizeBytes.isPresent()) {
            DataSize stripeSize = DataSize.ofBytes(blockSizeBytes.get());
            writerOptions = writerOptions
                    .withStripeMinSize(stripeSize)
                    .withStripeMaxSize(stripeSize);
        }
        OrcWriter orcWriter =
                new OrcWriter(
                        OutputStreamOrcDataSink.create(new CloseShieldOutputStream(out)),
                        columnNames,
                        columnTypes,
                        OrcType.createRootOrcType(columnNames, columnTypes),
                        orcCompressionKind(compression),
                        writerOptions,
                        ImmutableMap.of(),
                        true,
                        BOTH,
                        new OrcWriterStats());
        return new OrcWriterAdapter(orcWriter);
    }

    private static org.apache.parquet.format.CompressionCodec parquetCompressionCodec(
            String compression)
    {
        return switch (normalizeCompression(compression)) {
            case "none", "uncompressed" -> org.apache.parquet.format.CompressionCodec.UNCOMPRESSED;
            case "snappy" -> org.apache.parquet.format.CompressionCodec.SNAPPY;
            case "gzip", "zlib" -> org.apache.parquet.format.CompressionCodec.GZIP;
            case "lz4" -> org.apache.parquet.format.CompressionCodec.LZ4;
            case "zstd" -> org.apache.parquet.format.CompressionCodec.ZSTD;
            default -> throw new UnsupportedOperationException(
                    "Unsupported Parquet compression codec: " + compression);
        };
    }

    private static CompressionKind orcCompressionKind(String compression)
    {
        return switch (normalizeCompression(compression)) {
            case "none", "uncompressed" -> CompressionKind.NONE;
            case "snappy" -> CompressionKind.SNAPPY;
            case "gzip", "zlib" -> CompressionKind.ZLIB;
            case "lz4" -> CompressionKind.LZ4;
            case "zstd" -> CompressionKind.ZSTD;
            default -> throw new UnsupportedOperationException(
                    "Unsupported ORC compression codec: " + compression);
        };
    }

    private static String normalizeCompression(String compression)
    {
        return requireNonNull(compression, "compression is null").toLowerCase(Locale.ENGLISH);
    }

    private interface WriterAdapter
            extends Closeable
    {
        void write(Page page)
                throws IOException;

        long getWrittenBytes();

        long getBufferedBytes();
    }

    private record ParquetWriterAdapter(ParquetWriter writer)
            implements WriterAdapter
    {
        @Override
        public void write(Page page)
                throws IOException
        {
            writer.write(page);
        }

        @Override
        public long getWrittenBytes()
        {
            return writer.getWrittenBytes();
        }

        @Override
        public long getBufferedBytes()
        {
            return writer.getBufferedBytes();
        }

        @Override
        public void close()
                throws IOException
        {
            writer.close();
        }
    }

    private record OrcWriterAdapter(OrcWriter writer)
            implements WriterAdapter
    {
        @Override
        public void write(Page page)
                throws IOException
        {
            writer.write(page);
        }

        @Override
        public long getWrittenBytes()
        {
            return writer.getWrittenBytes();
        }

        @Override
        public long getBufferedBytes()
        {
            return writer.getBufferedBytes();
        }

        @Override
        public void close()
                throws IOException
        {
            writer.close();
        }
    }
}
