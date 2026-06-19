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

import io.trino.orc.metadata.OrcType;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.plugin.paimon.PaimonTypeUtils;
import io.trino.spi.type.Type;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.paimon.format.TrinoPaimonFormatWriterOptions.empty;
import static java.util.Objects.requireNonNull;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.convertToParquetMessageType;

public class TrinoPaimonFileFormat
        extends FileFormat
{
    static final String ORC = "orc";
    static final String PARQUET = "parquet";
    private static final String ORC_STRIPE_SIZE = "orc.stripe.size";
    private static final String PARQUET_BLOCK_SIZE = "parquet.block.size";
    private static final String PARQUET_PAGE_SIZE = "parquet.page.size";
    private static final String PARQUET_PAGE_ROW_COUNT_LIMIT = "parquet.page.row.count.limit";

    private final FormatContext context;

    TrinoPaimonFileFormat(String formatIdentifier, FormatContext context)
    {
        super(formatIdentifier);
        this.context = requireNonNull(context, "context is null");
    }

    @Override
    public FormatReaderFactory createReaderFactory(RowType dataSchemaRowType, RowType projectedRowType,
            @Nullable List<Predicate> filters)
    {
        validateSupportedReadType(projectedRowType);
        return new TrinoPaimonFormatReaderFactory(formatIdentifier, projectedRowType);
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type)
    {
        validateSupportedWriteType(type);
        return new TrinoPaimonFormatWriterFactory(
                formatIdentifier,
                type,
                context.writeBatchSize(),
                writerOptions());
    }

    @Override
    public void validateDataFields(RowType rowType)
    {
        if (PARQUET.equals(formatIdentifier)) {
            if (rowType.getFields().stream()
                    .map(field -> field.type())
                    .anyMatch(PaimonTypeUtils::containsVariant)) {
                convertToParquetMessageType(rowType);
                return;
            }
            new ParquetSchemaConverter(trinoTypes(rowType), rowType.getFieldNames(), true, true);
            return;
        }
        if (ORC.equals(formatIdentifier)) {
            validateSupportedWriteType(rowType);
            OrcType.createRootOrcType(rowType.getFieldNames(), trinoTypes(rowType));
            return;
        }
        throw new UnsupportedOperationException("Unsupported Trino Paimon file format: " + formatIdentifier);
    }

    @Override
    public Optional<SimpleStatsExtractor> createStatsExtractor(
            RowType type, SimpleColStatsCollector.Factory[] statsCollectors)
    {
        return Optional.of(new TrinoPaimonSimpleStatsExtractor(type, statsCollectors));
    }

    static List<Type> trinoTypes(RowType rowType)
    {
        return rowType.getFields().stream()
                .map(field -> PaimonTypeUtils.fromPaimonType(field.type()))
                .toList();
    }

    private TrinoPaimonFormatWriterOptions writerOptions()
    {
        if (PARQUET.equals(formatIdentifier)) {
            return new TrinoPaimonFormatWriterOptions(
                    blockSizeBytes(PARQUET_BLOCK_SIZE),
                    positiveIntegerOption(PARQUET_PAGE_SIZE),
                    positiveIntegerOption(PARQUET_PAGE_ROW_COUNT_LIMIT));
        }
        if (ORC.equals(formatIdentifier)) {
            return new TrinoPaimonFormatWriterOptions(
                    blockSizeBytes(ORC_STRIPE_SIZE),
                    Optional.empty(),
                    Optional.empty());
        }
        return empty();
    }

    private Optional<Long> blockSizeBytes(String formatSpecificKey)
    {
        Optional<Long> blockSizeBytes = Optional.ofNullable(context.blockSize())
                .map(blockSize -> blockSize.getBytes());
        if (blockSizeBytes.isEmpty()) {
            blockSizeBytes = positiveLongOption(formatSpecificKey);
        }
        blockSizeBytes.ifPresent(size -> checkArgument(size > 0, "file.block-size must be greater than 0 bytes"));
        return blockSizeBytes;
    }

    private Optional<Long> positiveLongOption(String key)
    {
        if (!context.options().containsKey(key)) {
            return Optional.empty();
        }
        long value = context.options().getLong(key, -1);
        checkArgument(value > 0, "%s must be greater than 0", key);
        return Optional.of(value);
    }

    private Optional<Integer> positiveIntegerOption(String key)
    {
        if (!context.options().containsKey(key)) {
            return Optional.empty();
        }
        int value = context.options().getInteger(key, -1);
        checkArgument(value > 0, "%s must be greater than 0", key);
        return Optional.of(value);
    }

    private static void validateSupportedWriteType(RowType rowType)
    {
        if (rowType.getFields().stream()
                .map(field -> field.type())
                .anyMatch(PaimonTypeUtils::containsUnsupportedTrinoFormatProviderWriteType)) {
            throw new UnsupportedOperationException(
                    "Trino Paimon file format provider does not support Paimon BLOB, VARIANT, VECTOR, or MULTISET writes");
        }
    }

    private static void validateSupportedReadType(RowType rowType)
    {
        if (rowType.getFields().stream()
                .map(field -> field.type())
                .anyMatch(PaimonTypeUtils::containsUnsupportedTrinoFormatProviderReadType)) {
            throw new UnsupportedOperationException(
                    "Trino Paimon file format provider does not support Paimon BLOB, VARIANT, VECTOR, or MULTISET reads");
        }
    }
}
