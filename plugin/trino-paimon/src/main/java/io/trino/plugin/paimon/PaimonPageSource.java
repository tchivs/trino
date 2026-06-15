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
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.ArrayValueBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.MapValueBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.RowValueBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.InternalRowUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.trino.plugin.paimon.PaimonTrinoTypeConversions.paimonTimeMillisToTrinoPicos;
import static io.trino.plugin.paimon.PaimonTrinoTypeConversions.paimonTimestampToTrino;
import static io.trino.plugin.paimon.PaimonTrinoTypeConversions.paimonTimestampToTrinoTimestampWithTimeZone;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PaimonPageSource
        implements
        ConnectorPageSource
{
    private static final int ROWS_PER_REQUEST = 4096;

    private final CloseableIterator<InternalRow> iterator;
    private final OptionalLong limit;
    private final PageBuilder pageBuilder;
    private final List<Type> columnTypes;
    private final List<DataType> logicalTypes;

    private boolean isFinished;
    private long numReturn;

    public PaimonPageSource(RecordReader<InternalRow> reader, List<? extends ColumnHandle> projectedColumns,
            OptionalLong limit)
    {
        this.limit = requireNonNull(limit, "limit is null");
        checkArgument(this.limit.isEmpty() || this.limit.getAsLong() >= 0, "limit must be non-negative");
        this.iterator = requireNonNull(reader, "reader is null").toCloseableIterator();
        this.columnTypes = new ArrayList<>();
        this.logicalTypes = new ArrayList<>();
        requireNonNull(projectedColumns, "projectedColumns is null");
        for (ColumnHandle handle : projectedColumns) {
            if (!(requireNonNull(handle, "projectedColumns contains null column") instanceof PaimonColumnHandle paimonColumnHandle)) {
                throw new IllegalArgumentException("Paimon page source requires PaimonColumnHandle, got: "
                        + handle.getClass().getName());
            }
            columnTypes.add(paimonColumnHandle.getTrinoType());
            logicalTypes.add(paimonColumnHandle.logicalType());
        }

        this.pageBuilder = new PageBuilder(columnTypes);
    }

    private static void writeSlice(BlockBuilder output, Type type, Object value)
    {
        if (type.getBaseName().equals(JSON)) {
            type.writeSlice(output, jsonParse(utf8Slice(((Variant) value).toJson())));
        }
        else if (type instanceof VarcharType || type instanceof io.trino.spi.type.CharType) {
            type.writeSlice(output, wrappedBuffer(((BinaryString) value).toBytes()));
        }
        else if (type instanceof VarbinaryType) {
            if (value instanceof Blob blob) {
                type.writeSlice(output, wrappedBuffer(blob.toData()));
            }
            else {
                type.writeSlice(output, wrappedBuffer((byte[]) value));
            }
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeSignature());
        }
    }

    private static void writeObject(BlockBuilder output, Type type, Object value)
    {
        if (type instanceof DecimalType decimalType) {
            BigDecimal decimal = ((Decimal) value).toBigDecimal();
            type.writeObject(output, Decimals.encodeScaledValue(decimal, decimalType.getScale()));
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Object: " + type.getTypeSignature());
        }
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
        return isFinished;
    }

    @Override
    public Page getNextPage()
    {
        return ClassLoaderUtils.runWithContextClassLoader(() -> {
            try {
                return nextPage();
            }
            catch (TrinoException e) {
                closeAllSuppress(e, this);
                throw e;
            }
            catch (IOException e) {
                closeAllSuppress(e, this);
                throw PaimonPageSourceProvider.wrapPaimonReadException(e);
            }
            catch (UnsupportedOperationException e) {
                closeAllSuppress(e, this);
                throw PaimonPageSourceProvider.wrapPaimonReadException(e);
            }
            catch (RuntimeException e) {
                closeAllSuppress(e, this);
                throw PaimonPageSourceProvider.wrapPaimonReadException(e);
            }
        }, PaimonPageSource.class.getClassLoader());
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Nullable
    private Page nextPage()
            throws IOException
    {
        int count = 0;
        while (count < ROWS_PER_REQUEST && !pageBuilder.isFull()) {
            if (limit.isPresent() && numReturn + count >= limit.getAsLong()) {
                isFinished = true;
                return returnPage(count);
            }

            if (!iterator.hasNext()) {
                isFinished = true;
                return returnPage(count);
            }

            InternalRow row = iterator.next();
            pageBuilder.declarePosition();
            count++;
            for (int i = 0; i < columnTypes.size(); i++) {
                BlockBuilder output = pageBuilder.getBlockBuilder(i);
                appendTo(columnTypes.get(i), logicalTypes.get(i), InternalRowUtils.get(row, i, logicalTypes.get(i)),
                        output);
            }
        }

        return returnPage(count);
    }

    private Page returnPage(int count)
    {
        if (count == 0) {
            return null;
        }
        numReturn += count;
        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    @Override
    public void close()
            throws IOException
    {
        try {
            this.iterator.close();
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }

    protected void appendTo(Type type, DataType logicalType, Object value, BlockBuilder output)
    {
        if (value == null) {
            output.appendNull();
            return;
        }

        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            type.writeBoolean(output, (Boolean) value);
        }
        else if (javaType == long.class) {
            if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(TINYINT) || type.equals(SMALLINT)
                    || type.equals(DATE)) {
                type.writeLong(output, ((Number) value).longValue());
            }
            else if (type.equals(REAL)) {
                type.writeLong(output, Float.floatToIntBits((Float) value));
            }
            else if (type instanceof DecimalType decimalType) {
                BigDecimal decimal = ((Decimal) value).toBigDecimal();
                type.writeLong(output, encodeShortScaledValue(decimal, decimalType.getScale()));
            }
            else if (type instanceof TimestampType) {
                type.writeLong(output, (long) paimonTimestampToTrino(type, (Timestamp) value));
            }
            else if (type instanceof TimestampWithTimeZoneType) {
                type.writeLong(output, (long) paimonTimestampToTrinoTimestampWithTimeZone(type, (Timestamp) value));
            }
            else if (type instanceof TimeType) {
                type.writeLong(output, paimonTimeMillisToTrinoPicos((int) value));
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR,
                        format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
            }
        }
        else if (javaType == double.class) {
            type.writeDouble(output, ((Number) value).doubleValue());
        }
        else if (type instanceof DecimalType) {
            writeObject(output, type, value);
        }
        else if (javaType == Slice.class) {
            writeSlice(output, type, value);
        }
        else if (javaType == LongTimestamp.class) {
            type.writeObject(output, paimonTimestampToTrino(type, (Timestamp) value));
        }
        else if (javaType == LongTimestampWithTimeZone.class) {
            type.writeObject(output, paimonTimestampToTrinoTimestampWithTimeZone(type, (Timestamp) value));
        }
        else if (type instanceof ArrayType || type instanceof MapType || type instanceof RowType) {
            writeBlock(output, type, logicalType, value);
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
        }
    }

    protected void writeBlock(BlockBuilder output, Type type, DataType logicalType, Object value)
    {
        if (type instanceof ArrayType) {
            ArrayBlockBuilder arrayBlockBuilder = (ArrayBlockBuilder) output;
            try {
                arrayBlockBuilder.buildEntry((ArrayValueBuilder<Throwable>) elementBuilder -> {
                    InternalArray arrayData = (InternalArray) value;
                    DataType elementType = arrayElementLogicalType(logicalType);
                    for (int i = 0; i < arrayData.size(); i++) {
                        appendTo(type.getTypeParameters().get(0), elementType,
                                InternalRowUtils.get(arrayData, i, elementType), elementBuilder);
                    }
                });
            }
            catch (Throwable e) {
                throw propagateBlockBuilderFailure(e);
            }
            return;
        }
        if (type instanceof RowType) {
            RowBlockBuilder rowBlockBuilder = (RowBlockBuilder) output;
            org.apache.paimon.types.RowType rowLogicalType = rowLogicalType(logicalType);
            validateRowFieldCount(type.getTypeParameters().size(), rowLogicalType.getFieldCount());
            try {
                rowBlockBuilder.buildEntry((RowValueBuilder<Throwable>) fieldBuilders -> {
                    InternalRow rowData = (InternalRow) value;
                    for (int index = 0; index < type.getTypeParameters().size(); index++) {
                        Type fieldType = type.getTypeParameters().get(index);
                        DataType fieldLogicalType = rowLogicalType.getTypeAt(index);
                        appendTo(fieldType, fieldLogicalType, InternalRowUtils.get(rowData, index, fieldLogicalType),
                                fieldBuilders.get(index));
                    }
                });
            }
            catch (Throwable e) {
                throw propagateBlockBuilderFailure(e);
            }
            return;
        }
        if (type instanceof MapType) {
            InternalMap mapData = (InternalMap) value;
            InternalArray keyArray = mapData.keyArray();
            InternalArray valueArray = mapData.valueArray();
            DataType keyType;
            DataType valueType;
            if (logicalType instanceof org.apache.paimon.types.MapType mapType) {
                keyType = mapType.getKeyType();
                valueType = mapType.getValueType();
            }
            else if (logicalType instanceof MultisetType multisetType) {
                if (!type.getTypeParameters().get(1).equals(INTEGER)) {
                    throw new UnsupportedOperationException("Paimon MULTISET requires Trino integer count type metadata");
                }
                keyType = multisetType.getElementType();
                valueType = new IntType(false);
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled Paimon logical type for Map: " + logicalType);
            }
            MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) output;
            try {
                mapBlockBuilder.buildEntry((MapValueBuilder<Throwable>) (keyBuilder, valueBuilder) -> {
                    for (int i = 0; i < keyArray.size(); i++) {
                        appendTo(type.getTypeParameters().get(0), keyType, InternalRowUtils.get(keyArray, i, keyType),
                                keyBuilder);
                        appendTo(type.getTypeParameters().get(1), valueType,
                                InternalRowUtils.get(valueArray, i, valueType), valueBuilder);
                    }
                });
            }
            catch (Throwable e) {
                throw propagateBlockBuilderFailure(e);
            }
            return;
        }
        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Block: " + type.getTypeSignature());
    }

    private static DataType arrayElementLogicalType(DataType logicalType)
    {
        if (logicalType instanceof VectorType vectorType) {
            return vectorType.getElementType();
        }
        if (logicalType instanceof org.apache.paimon.types.ArrayType arrayType) {
            return arrayType.getElementType();
        }
        throw new UnsupportedOperationException("Paimon ARRAY or VECTOR logical type metadata is required");
    }

    private static org.apache.paimon.types.RowType rowLogicalType(DataType logicalType)
    {
        if (logicalType instanceof org.apache.paimon.types.RowType rowType) {
            return rowType;
        }
        throw new UnsupportedOperationException("Paimon ROW logical type metadata is required");
    }

    private static void validateRowFieldCount(int trinoFieldCount, int paimonFieldCount)
    {
        if (trinoFieldCount != paimonFieldCount) {
            throw new IllegalArgumentException("Paimon ROW field count mismatch: expected "
                    + paimonFieldCount + ", got " + trinoFieldCount);
        }
    }

    private static RuntimeException propagateBlockBuilderFailure(Throwable failure)
    {
        if (failure instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        return new RuntimeException(failure);
    }
}
