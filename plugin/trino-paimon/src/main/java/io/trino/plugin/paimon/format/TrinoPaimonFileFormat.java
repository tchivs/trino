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
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.convertToParquetMessageType;

public class TrinoPaimonFileFormat
        extends FileFormat
{
    static final String ORC = "orc";
    static final String PARQUET = "parquet";

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
        throw new UnsupportedOperationException("Trino Paimon file format provider only supports writes");
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type)
    {
        validateSupportedWriteType(type);
        return new TrinoPaimonFormatWriterFactory(formatIdentifier, type, context.writeBatchSize());
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
        return Optional.empty();
    }

    static List<Type> trinoTypes(RowType rowType)
    {
        return rowType.getFields().stream()
                .map(field -> PaimonTypeUtils.fromPaimonType(field.type()))
                .toList();
    }

    private static void validateSupportedWriteType(RowType rowType)
    {
        rowType.getFields().forEach(field -> validateSupportedWriteType(field.type()));
    }

    private static void validateSupportedWriteType(DataType type)
    {
        if (type.getTypeRoot() == DataTypeRoot.VARIANT) {
            throw new UnsupportedOperationException(
                    "Trino Paimon file format provider does not support Paimon VARIANT writes");
        }
        if (type instanceof org.apache.paimon.types.ArrayType arrayType) {
            validateSupportedWriteType(arrayType.getElementType());
            return;
        }
        if (type instanceof org.apache.paimon.types.MapType mapType) {
            validateSupportedWriteType(mapType.getKeyType());
            validateSupportedWriteType(mapType.getValueType());
            return;
        }
        if (type instanceof org.apache.paimon.types.MultisetType multisetType) {
            validateSupportedWriteType(multisetType.getElementType());
            return;
        }
        if (type instanceof org.apache.paimon.types.RowType rowType) {
            validateSupportedWriteType(rowType);
            return;
        }
        if (type instanceof org.apache.paimon.types.VectorType vectorType) {
            validateSupportedWriteType(vectorType.getElementType());
        }
    }
}
