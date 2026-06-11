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
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeUtils;
import io.trino.spi.type.VarcharType;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.GenericVariantBuilder;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.RowKind;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.trino.plugin.paimon.PaimonTrinoTypeConversions.trinoTimePicosToPaimonMillis;
import static io.trino.plugin.paimon.PaimonTrinoTypeConversions.trinoTimestampToPaimon;
import static io.trino.plugin.paimon.PaimonTrinoTypeConversions.trinoTimestampWithTimeZoneToPaimon;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.toIntExact;
import static org.apache.paimon.shade.guava30.com.google.common.base.Verify.verify;

public class PaimonRow
        implements
        InternalRow,
        Serializable
{
    private final RowKind rowKind;
    private final Page singlePage;
    private final List<Type> types;

    public PaimonRow(Page singlePage, RowKind rowKind)
    {
        this(singlePage, rowKind, Collections.nCopies(singlePage.getChannelCount(), null));
    }

    public PaimonRow(Page singlePage, RowKind rowKind, List<Type> types)
    {
        verify(singlePage.getPositionCount() == 1, "singlePage must have only one row");
        verify(types.size() == singlePage.getChannelCount(), "types size must match page channel count");
        this.singlePage = singlePage;
        this.rowKind = rowKind;
        this.types = Collections.unmodifiableList(new ArrayList<>(types));
    }

    /** Helper method to parse Variant from JSON stored in VARCHAR block. */
    private static Variant parseVariantFromBlock(Block block, int position)
    {
        try {
            Slice slice = (Slice) TypeUtils.readNativeValue(VarcharType.VARCHAR, block, position);
            String json = slice.toStringUtf8();
            return GenericVariantBuilder.parseJson(json, true);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to parse Variant from JSON", e);
        }
    }

    private static byte readByte(Block block, int position)
    {
        long value = (long) TypeUtils.readNativeValue(TINYINT, block, position);
        return (byte) value;
    }

    private static int readInt(Block block, int position, Type type)
    {
        if (type instanceof TimeType) {
            return trinoTimePicosToPaimonMillis((long) TypeUtils.readNativeValue(type, block, position));
        }
        return toIntExact((long) TypeUtils.readNativeValue(INTEGER, block, position));
    }

    private static Timestamp readTimestamp(Block block, int position, Type type)
    {
        if (type instanceof TimestampType) {
            return trinoTimestampToPaimon(TypeUtils.readNativeValue(type, block, position));
        }
        if (type instanceof TimestampWithTimeZoneType) {
            return trinoTimestampWithTimeZoneToPaimon(TypeUtils.readNativeValue(type, block, position));
        }
        long value = (long) TypeUtils.readNativeValue(TIMESTAMP_MICROS, block, position);
        return Timestamp.fromMicros(value);
    }

    @Override
    public int getFieldCount()
    {
        return singlePage.getChannelCount();
    }

    @Override
    public RowKind getRowKind()
    {
        return rowKind;
    }

    @Override
    public void setRowKind(RowKind rowKind)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNullAt(int i)
    {
        return singlePage.getBlock(i).isNull(0);
    }

    @Override
    public boolean getBoolean(int i)
    {
        return (boolean) TypeUtils.readNativeValue(BOOLEAN, singlePage.getBlock(i), 0);
    }

    @Override
    public byte getByte(int i)
    {
        return readByte(singlePage.getBlock(i), 0);
    }

    @Override
    public short getShort(int i)
    {
        long value = (long) TypeUtils.readNativeValue(SMALLINT, singlePage.getBlock(i), 0);
        if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Value out of range for short: " + value);
        }
        return (short) value;
    }

    @Override
    public int getInt(int i)
    {
        long value = readInt(singlePage.getBlock(i), 0, types.get(i));
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Value out of range for int: " + value);
        }
        return toIntExact(value);
    }

    @Override
    public long getLong(int i)
    {
        return (long) TypeUtils.readNativeValue(BIGINT, singlePage.getBlock(i), 0);
    }

    @Override
    public float getFloat(int i)
    {
        return Float.intBitsToFloat(toIntExact((long) TypeUtils.readNativeValue(REAL, singlePage.getBlock(i), 0)));
    }

    @Override
    public double getDouble(int i)
    {
        return (double) TypeUtils.readNativeValue(DOUBLE, singlePage.getBlock(i), 0);
    }

    @Override
    public BinaryString getString(int i)
    {
        return BinaryString.fromBytes(getBinary(i));
    }

    @Override
    public Decimal getDecimal(int i, int decimalPrecision, int decimalScale)
    {
        Object value = TypeUtils.readNativeValue(DecimalType.createDecimalType(decimalPrecision, decimalScale),
                singlePage.getBlock(i), 0);
        if (decimalPrecision <= MAX_SHORT_PRECISION) {
            return Decimal.fromUnscaledLong((Long) value, decimalPrecision, decimalScale);
        }
        else {
            BigDecimal bigDecimalValue = new BigDecimal(DecimalUtils.toBigInteger(value), decimalScale);
            return Decimal.fromBigDecimal(bigDecimalValue, decimalPrecision, decimalScale);
        }
    }

    @Override
    public Timestamp getTimestamp(int i, int timestampPrecision)
    {
        return readTimestamp(singlePage.getBlock(i), 0, types.get(i));
    }

    @Override
    public byte[] getBinary(int i)
    {
        Slice slice = (Slice) TypeUtils.readNativeValue(VARBINARY, singlePage.getBlock(i), 0);
        return slice.getBytes();
    }

    @Override
    public Variant getVariant(int i)
    {
        if (isNullAt(i)) {
            return null;
        }
        return parseVariantFromBlock(singlePage.getBlock(i), 0);
    }

    @Override
    public Blob getBlob(int i)
    {
        if (isNullAt(i)) {
            return null;
        }
        return Blob.fromData(getBinary(i));
    }

    @Override
    public InternalArray getArray(int i)
    {
        if (isNullAt(i)) {
            return null;
        }
        Type type = types.get(i);
        if (type instanceof ArrayType arrayType) {
            return new TrinoArray(arrayType.getObject(singlePage.getBlock(i), 0), arrayType.getElementType());
        }
        ArrayBlock arrayBlock = (ArrayBlock) singlePage.getBlock(i);
        return new TrinoArray(arrayBlock.getArray(0), null);
    }

    @Override
    public InternalVector getVector(int i)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalMap getMap(int i)
    {
        if (isNullAt(i)) {
            return null;
        }
        Type type = types.get(i);
        if (type instanceof MapType mapType) {
            SqlMap sqlMap = mapType.getObject(singlePage.getBlock(i), 0);
            return new TrinoMap(sqlMap, mapType.getKeyType(), mapType.getValueType());
        }
        throw new UnsupportedOperationException("Map type metadata is required");
    }

    @Override
    public InternalRow getRow(int i, int i1)
    {
        if (isNullAt(i)) {
            return null;
        }
        Type type = types.get(i);
        if (type instanceof io.trino.spi.type.RowType rowType) {
            return new TrinoNestedRow(rowType.getObject(singlePage.getBlock(i), 0), rowKind, rowType.getTypeParameters());
        }
        return new TrinoNestedRow((RowBlock) singlePage.getBlock(i).getSingleValueBlock(0), rowKind, null);
    }

    /** Base class for InternalArray implementations wrapping Trino Block. */
    private abstract static class AbstractTrinoArray
            implements
            InternalArray
    {
        protected final Block block;
        protected final Type type;

        AbstractTrinoArray(Block block, Type type)
        {
            this.block = block;
            this.type = type;
        }

        /** Get the actual position in the block for a logical position. */
        protected abstract int getPosition(int pos);

        @Override
        public boolean isNullAt(int pos)
        {
            return block.isNull(getPosition(pos));
        }

        @Override
        public boolean getBoolean(int pos)
        {
            return (boolean) TypeUtils.readNativeValue(BOOLEAN, block, getPosition(pos));
        }

        @Override
        public byte getByte(int pos)
        {
            return readByte(block, getPosition(pos));
        }

        @Override
        public short getShort(int pos)
        {
            long value = (long) TypeUtils.readNativeValue(SMALLINT, block, getPosition(pos));
            return (short) value;
        }

        @Override
        public int getInt(int pos)
        {
            return readInt(block, getPosition(pos), type);
        }

        @Override
        public long getLong(int pos)
        {
            return (long) TypeUtils.readNativeValue(BIGINT, block, getPosition(pos));
        }

        @Override
        public float getFloat(int pos)
        {
            return Float.intBitsToFloat(toIntExact((long) TypeUtils.readNativeValue(REAL, block, getPosition(pos))));
        }

        @Override
        public double getDouble(int pos)
        {
            return (double) TypeUtils.readNativeValue(DOUBLE, block, getPosition(pos));
        }

        @Override
        public BinaryString getString(int pos)
        {
            return BinaryString.fromBytes(getBinary(pos));
        }

        @Override
        public Decimal getDecimal(int pos, int precision, int scale)
        {
            Object value = TypeUtils.readNativeValue(DecimalType.createDecimalType(precision, scale), block,
                    getPosition(pos));
            if (precision <= MAX_SHORT_PRECISION) {
                return Decimal.fromUnscaledLong((Long) value, precision, scale);
            }
            else {
                BigDecimal bigDecimalValue = new BigDecimal(DecimalUtils.toBigInteger(value), scale);
                return Decimal.fromBigDecimal(bigDecimalValue, precision, scale);
            }
        }

        @Override
        public Timestamp getTimestamp(int pos, int precision)
        {
            return readTimestamp(block, getPosition(pos), type);
        }

        @Override
        public byte[] getBinary(int pos)
        {
            Slice slice = (Slice) TypeUtils.readNativeValue(VARBINARY, block, getPosition(pos));
            return slice.getBytes();
        }

        @Override
        public Variant getVariant(int pos)
        {
            if (isNullAt(pos)) {
                return null;
            }
            return parseVariantFromBlock(block, getPosition(pos));
        }

        @Override
        public Blob getBlob(int pos)
        {
            if (isNullAt(pos)) {
                return null;
            }
            return Blob.fromData(getBinary(pos));
        }

        @Override
        public InternalArray getArray(int pos)
        {
            if (isNullAt(pos)) {
                return null;
            }
            if (type instanceof ArrayType arrayType) {
                return new TrinoArray(arrayType.getObject(block, getPosition(pos)), arrayType.getElementType());
            }
            ArrayBlock nestedBlock = (ArrayBlock) block;
            return new TrinoArray(nestedBlock.getArray(getPosition(pos)), null);
        }

        @Override
        public InternalVector getVector(int pos)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public InternalMap getMap(int pos)
        {
            if (isNullAt(pos)) {
                return null;
            }
            if (type instanceof MapType mapType) {
                SqlMap sqlMap = mapType.getObject(block, getPosition(pos));
                return new TrinoMap(sqlMap, mapType.getKeyType(), mapType.getValueType());
            }
            throw new UnsupportedOperationException("Map type metadata is required");
        }

        @Override
        public InternalRow getRow(int pos, int numFields)
        {
            if (isNullAt(pos)) {
                return null;
            }
            if (type instanceof io.trino.spi.type.RowType rowType) {
                return new TrinoNestedRow(rowType.getObject(block, getPosition(pos)), RowKind.INSERT,
                        rowType.getTypeParameters());
            }
            return new TrinoNestedRow((RowBlock) block.getSingleValueBlock(getPosition(pos)), RowKind.INSERT, null);
        }

        @Override
        public boolean[] toBooleanArray()
        {
            boolean[] result = new boolean[size()];
            for (int i = 0; i < size(); i++) {
                result[i] = getBoolean(i);
            }
            return result;
        }

        @Override
        public byte[] toByteArray()
        {
            byte[] result = new byte[size()];
            for (int i = 0; i < size(); i++) {
                result[i] = getByte(i);
            }
            return result;
        }

        @Override
        public short[] toShortArray()
        {
            short[] result = new short[size()];
            for (int i = 0; i < size(); i++) {
                result[i] = getShort(i);
            }
            return result;
        }

        @Override
        public int[] toIntArray()
        {
            int[] result = new int[size()];
            for (int i = 0; i < size(); i++) {
                result[i] = getInt(i);
            }
            return result;
        }

        @Override
        public long[] toLongArray()
        {
            long[] result = new long[size()];
            for (int i = 0; i < size(); i++) {
                result[i] = getLong(i);
            }
            return result;
        }

        @Override
        public float[] toFloatArray()
        {
            float[] result = new float[size()];
            for (int i = 0; i < size(); i++) {
                result[i] = getFloat(i);
            }
            return result;
        }

        @Override
        public double[] toDoubleArray()
        {
            double[] result = new double[size()];
            for (int i = 0; i < size(); i++) {
                result[i] = getDouble(i);
            }
            return result;
        }
    }

    /** TrinoArray implementation for {@link InternalArray}. */
    private static class TrinoArray
            extends
            AbstractTrinoArray
    {
        TrinoArray(Block block)
        {
            this(block, null);
        }

        TrinoArray(Block block, Type type)
        {
            super(block, type);
        }

        @Override
        protected int getPosition(int pos)
        {
            return pos;
        }

        @Override
        public int size()
        {
            return block.getPositionCount();
        }
    }

    /** TrinoMap implementation for {@link InternalMap}. */
    private record TrinoMap(SqlMap sqlMap, Type keyType, Type valueType) implements InternalMap
    {
        @Override
        public int size()
        {
            return sqlMap.getSize();
        }

        @Override
        public InternalArray keyArray()
        {
            Block keyBlock = sqlMap.getRawKeyBlock();
            int offset = sqlMap.getRawOffset();
            int count = sqlMap.getSize();
            return new TrinoArrayView(keyBlock, offset, count, keyType);
        }

        @Override
        public InternalArray valueArray()
        {
            Block valueBlock = sqlMap.getRawValueBlock();
            int offset = sqlMap.getRawOffset();
            int count = sqlMap.getSize();
            return new TrinoArrayView(valueBlock, offset, count, valueType);
        }
    }

    /** TrinoNestedRow implementation for nested {@link InternalRow}. */
    private static class TrinoNestedRow
            implements
            InternalRow
    {
        private final SqlRow sqlRow;
        private final RowKind rowKind;
        private final List<Type> types;

        TrinoNestedRow(RowBlock rowBlock, RowKind rowKind, List<Type> types)
        {
            this(rowBlock.getRow(0), rowKind, types);
        }

        TrinoNestedRow(SqlRow sqlRow, RowKind rowKind, List<Type> types)
        {
            this.sqlRow = sqlRow;
            this.rowKind = rowKind;
            this.types = types == null ? Collections.nCopies(sqlRow.getFieldCount(), null) : types;
        }

        @Override
        public int getFieldCount()
        {
            return sqlRow.getFieldCount();
        }

        @Override
        public RowKind getRowKind()
        {
            return rowKind;
        }

        @Override
        public void setRowKind(RowKind rowKind)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isNullAt(int pos)
        {
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            return fieldBlock.isNull(sqlRow.getRawIndex());
        }

        @Override
        public boolean getBoolean(int pos)
        {
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            return (boolean) TypeUtils.readNativeValue(BOOLEAN, fieldBlock, sqlRow.getRawIndex());
        }

        @Override
        public byte getByte(int pos)
        {
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            return readByte(fieldBlock, sqlRow.getRawIndex());
        }

        @Override
        public short getShort(int pos)
        {
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            long value = (long) TypeUtils.readNativeValue(SMALLINT, fieldBlock, sqlRow.getRawIndex());
            return (short) value;
        }

        @Override
        public int getInt(int pos)
        {
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            return readInt(fieldBlock, sqlRow.getRawIndex(), types.get(pos));
        }

        @Override
        public long getLong(int pos)
        {
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            return (long) TypeUtils.readNativeValue(BIGINT, fieldBlock, sqlRow.getRawIndex());
        }

        @Override
        public float getFloat(int pos)
        {
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            return Float.intBitsToFloat(toIntExact((long) TypeUtils.readNativeValue(REAL, fieldBlock, sqlRow.getRawIndex())));
        }

        @Override
        public double getDouble(int pos)
        {
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            return (double) TypeUtils.readNativeValue(DOUBLE, fieldBlock, sqlRow.getRawIndex());
        }

        @Override
        public BinaryString getString(int pos)
        {
            return BinaryString.fromBytes(getBinary(pos));
        }

        @Override
        public Decimal getDecimal(int pos, int precision, int scale)
        {
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            Object value = TypeUtils.readNativeValue(DecimalType.createDecimalType(precision, scale), fieldBlock,
                    sqlRow.getRawIndex());
            if (precision <= MAX_SHORT_PRECISION) {
                return Decimal.fromUnscaledLong((Long) value, precision, scale);
            }
            else {
                BigDecimal bigDecimalValue = new BigDecimal(DecimalUtils.toBigInteger(value), scale);
                return Decimal.fromBigDecimal(bigDecimalValue, precision, scale);
            }
        }

        @Override
        public Timestamp getTimestamp(int pos, int precision)
        {
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            return readTimestamp(fieldBlock, sqlRow.getRawIndex(), types.get(pos));
        }

        @Override
        public byte[] getBinary(int pos)
        {
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            Slice slice = (Slice) TypeUtils.readNativeValue(VARBINARY, fieldBlock, sqlRow.getRawIndex());
            return slice.getBytes();
        }

        @Override
        public Variant getVariant(int pos)
        {
            if (isNullAt(pos)) {
                return null;
            }
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            return parseVariantFromBlock(fieldBlock, sqlRow.getRawIndex());
        }

        @Override
        public Blob getBlob(int pos)
        {
            if (isNullAt(pos)) {
                return null;
            }
            return Blob.fromData(getBinary(pos));
        }

        @Override
        public InternalArray getArray(int pos)
        {
            if (isNullAt(pos)) {
                return null;
            }
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            Type type = types.get(pos);
            if (type instanceof ArrayType arrayType) {
                return new TrinoArray(arrayType.getObject(fieldBlock, sqlRow.getRawIndex()), arrayType.getElementType());
            }
            ArrayBlock arrayBlock = (ArrayBlock) fieldBlock;
            return new TrinoArray(arrayBlock.getArray(sqlRow.getRawIndex()), null);
        }

        @Override
        public InternalVector getVector(int pos)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public InternalMap getMap(int pos)
        {
            if (isNullAt(pos)) {
                return null;
            }
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            Type type = types.get(pos);
            if (type instanceof MapType mapType) {
                SqlMap sqlMap = mapType.getObject(fieldBlock, sqlRow.getRawIndex());
                return new TrinoMap(sqlMap, mapType.getKeyType(), mapType.getValueType());
            }
            throw new UnsupportedOperationException("Map type metadata is required");
        }

        @Override
        public InternalRow getRow(int pos, int numFields)
        {
            if (isNullAt(pos)) {
                return null;
            }
            Block fieldBlock = sqlRow.getRawFieldBlock(pos);
            Type type = types.get(pos);
            if (type instanceof io.trino.spi.type.RowType rowType) {
                return new TrinoNestedRow(rowType.getObject(fieldBlock, sqlRow.getRawIndex()), rowKind,
                        rowType.getTypeParameters());
            }
            return new TrinoNestedRow((RowBlock) fieldBlock.getSingleValueBlock(sqlRow.getRawIndex()), rowKind, null);
        }
    }

    /**
     * TrinoArrayView implementation with offset and length for viewing part of a
     * Block. Used for Map key/value arrays.
     */
    private static class TrinoArrayView
            extends
            AbstractTrinoArray
    {
        private final int offset;
        private final int length;

        TrinoArrayView(Block block, int offset, int length)
        {
            this(block, offset, length, null);
        }

        TrinoArrayView(Block block, int offset, int length, Type type)
        {
            super(block, type);
            this.offset = offset;
            this.length = length;
        }

        @Override
        protected int getPosition(int pos)
        {
            return offset + pos;
        }

        @Override
        public int size()
        {
            return length;
        }
    }
}
