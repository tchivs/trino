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
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.SqlMap;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TypeUtils;
import io.trino.spi.type.VarcharType;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.GenericVariantBuilder;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.RowKind;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
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

    public PaimonRow(Page singlePage, RowKind rowKind)
    {
        verify(singlePage.getPositionCount() == 1, "singlePage must have only one row");
        this.singlePage = singlePage;
        this.rowKind = rowKind;
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
        long value = (long) TypeUtils.readNativeValue(TINYINT, singlePage.getBlock(i), 0);
        return (byte) value;
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
        Block block = singlePage.getBlock(i);
        try {
            // Try reading as INTEGER first (regular int columns)
            long value = (long) TypeUtils.readNativeValue(INTEGER, block, 0);
            if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Value out of range for int: " + value);
            }
            return (int) value;
        }
        catch (ClassCastException e) {
            // Block is LongArrayBlock, could be TIME column
            // TIME is stored as picoseconds in Trino but as int milliseconds in Paimon
            long picoseconds = (long) TypeUtils.readNativeValue(TIME_MILLIS, block, 0);
            if (picoseconds < 0 || picoseconds >= PICOSECONDS_PER_DAY) {
                throw new IllegalArgumentException("Time value out of range: " + picoseconds);
            }
            return toIntExact(picoseconds / PICOSECONDS_PER_MILLISECOND);
        }
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
            long high = ((Int128) value).getHigh();
            long low = ((Int128) value).getLow();
            BigInteger bigIntegerValue = BigInteger.valueOf(high).shiftLeft(64).add(BigInteger.valueOf(low));
            BigDecimal bigDecimalValue = new BigDecimal(bigIntegerValue, decimalScale);
            return Decimal.fromBigDecimal(bigDecimalValue, decimalPrecision, decimalScale);
        }
    }

    @Override
    public Timestamp getTimestamp(int i, int timestampPrecision)
    {
        long value = (long) TypeUtils.readNativeValue(TIMESTAMP_MICROS, singlePage.getBlock(i), 0);
        return Timestamp.fromMicros(value);
    }

    @Override
    public byte[] getBinary(int i)
    {
        Slice slice = (Slice) TypeUtils.readNativeValue(VARBINARY, singlePage.getBlock(i), 0);
        return slice.getBytes();
    }

    @Override
    public Blob getBlob(int i)
    {
        return Blob.fromData(getBinary(i));
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
    public InternalArray getArray(int i)
    {
        if (isNullAt(i)) {
            return null;
        }
        ArrayBlock arrayBlock = (ArrayBlock) singlePage.getBlock(i).getSingleValueBlock(0);
        return new TrinoArray(arrayBlock);
    }

    @Override
    public InternalMap getMap(int i)
    {
        if (isNullAt(i)) {
            return null;
        }
        MapBlock mapBlock = (MapBlock) singlePage.getBlock(i);
        SqlMap sqlMap = mapBlock.getMap(0);
        return new TrinoMap(sqlMap);
    }

    @Override
    public InternalRow getRow(int i, int i1)
    {
        if (isNullAt(i)) {
            return null;
        }
        RowBlock rowBlock = (RowBlock) singlePage.getBlock(i).getSingleValueBlock(0);
        return new TrinoNestedRow(rowBlock, rowKind);
    }

    /** Base class for InternalArray implementations wrapping Trino Block. */
    private abstract static class AbstractTrinoArray
            implements
            InternalArray
    {
        protected final Block block;

        AbstractTrinoArray(Block block)
        {
            this.block = block;
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
            long value = (long) TypeUtils.readNativeValue(TINYINT, block, getPosition(pos));
            return (byte) value;
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
            int position = getPosition(pos);
            try {
                // Try reading as INTEGER first (regular int columns)
                long value = (long) TypeUtils.readNativeValue(INTEGER, block, position);
                return (int) value;
            }
            catch (ClassCastException e) {
                // Block is LongArrayBlock, could be TIME column
                // TIME is stored as picoseconds in Trino but as int milliseconds in Paimon
                long picoseconds = (long) TypeUtils.readNativeValue(TIME_MILLIS, block, position);
                if (picoseconds < 0 || picoseconds >= PICOSECONDS_PER_DAY) {
                    throw new IllegalArgumentException("Time value out of range: " + picoseconds);
                }
                return toIntExact(picoseconds / PICOSECONDS_PER_MILLISECOND);
            }
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
                long high = ((Int128) value).getHigh();
                long low = ((Int128) value).getLow();
                BigInteger bigIntegerValue = BigInteger.valueOf(high).shiftLeft(64).add(BigInteger.valueOf(low));
                BigDecimal bigDecimalValue = new BigDecimal(bigIntegerValue, scale);
                return Decimal.fromBigDecimal(bigDecimalValue, precision, scale);
            }
        }

        @Override
        public Timestamp getTimestamp(int pos, int precision)
        {
            long value = (long) TypeUtils.readNativeValue(TIMESTAMP_MICROS, block, getPosition(pos));
            return Timestamp.fromMicros(value);
        }

        @Override
        public byte[] getBinary(int pos)
        {
            Slice slice = (Slice) TypeUtils.readNativeValue(VARBINARY, block, getPosition(pos));
            return slice.getBytes();
        }

        @Override
        public Blob getBlob(int pos)
        {
            return Blob.fromData(getBinary(pos));
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
        public InternalArray getArray(int pos)
        {
            if (isNullAt(pos)) {
                return null;
            }
            ArrayBlock nestedBlock = (ArrayBlock) block.getSingleValueBlock(getPosition(pos));
            return new TrinoArray(nestedBlock);
        }

        @Override
        public InternalMap getMap(int pos)
        {
            if (isNullAt(pos)) {
                return null;
            }
            MapBlock mapBlock = (MapBlock) block;
            SqlMap sqlMap = mapBlock.getMap(getPosition(pos));
            return new TrinoMap(sqlMap);
        }

        @Override
        public InternalRow getRow(int pos, int numFields)
        {
            if (isNullAt(pos)) {
                return null;
            }
            RowBlock rowBlock = (RowBlock) block.getSingleValueBlock(getPosition(pos));
            return new TrinoNestedRow(rowBlock, RowKind.INSERT);
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
            super(block);
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
    private record TrinoMap(SqlMap sqlMap) implements InternalMap
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
            return new TrinoArrayView(keyBlock, offset, count);
        }

        @Override
        public InternalArray valueArray()
        {
            Block valueBlock = sqlMap.getRawValueBlock();
            int offset = sqlMap.getRawOffset();
            int count = sqlMap.getSize();
            return new TrinoArrayView(valueBlock, offset, count);
        }
    }

    /** TrinoNestedRow implementation for nested {@link InternalRow}. */
    private static class TrinoNestedRow
            implements
            InternalRow
    {
        private final RowBlock rowBlock;
        private final RowKind rowKind;
        private final int position;

        TrinoNestedRow(RowBlock rowBlock, RowKind rowKind)
        {
            this.rowBlock = rowBlock;
            this.rowKind = rowKind;
            this.position = 0;
        }

        @Override
        public int getFieldCount()
        {
            // Count field blocks by iterating until we get an exception
            int count = 0;
            try {
                while (true) {
                    rowBlock.getFieldBlock(count);
                    count++;
                }
            }
            catch (Exception e) {
                return count;
            }
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
            Block fieldBlock = rowBlock.getFieldBlock(pos);
            return fieldBlock.isNull(position);
        }

        @Override
        public boolean getBoolean(int pos)
        {
            Block fieldBlock = rowBlock.getFieldBlock(pos);
            return (boolean) TypeUtils.readNativeValue(BOOLEAN, fieldBlock, position);
        }

        @Override
        public byte getByte(int pos)
        {
            Block fieldBlock = rowBlock.getFieldBlock(pos);
            long value = (long) TypeUtils.readNativeValue(TINYINT, fieldBlock, position);
            return (byte) value;
        }

        @Override
        public short getShort(int pos)
        {
            Block fieldBlock = rowBlock.getFieldBlock(pos);
            long value = (long) TypeUtils.readNativeValue(SMALLINT, fieldBlock, position);
            return (short) value;
        }

        @Override
        public int getInt(int pos)
        {
            Block fieldBlock = rowBlock.getFieldBlock(pos);
            try {
                // Try reading as INTEGER first (regular int columns)
                long value = (long) TypeUtils.readNativeValue(INTEGER, fieldBlock, position);
                return (int) value;
            }
            catch (ClassCastException e) {
                // Block is LongArrayBlock, could be TIME column
                // TIME is stored as picoseconds in Trino but as int milliseconds in Paimon
                long picoseconds = (long) TypeUtils.readNativeValue(TIME_MILLIS, fieldBlock, position);
                if (picoseconds < 0 || picoseconds >= PICOSECONDS_PER_DAY) {
                    throw new IllegalArgumentException("Time value out of range: " + picoseconds);
                }
                return toIntExact(picoseconds / PICOSECONDS_PER_MILLISECOND);
            }
        }

        @Override
        public long getLong(int pos)
        {
            Block fieldBlock = rowBlock.getFieldBlock(pos);
            return (long) TypeUtils.readNativeValue(BIGINT, fieldBlock, position);
        }

        @Override
        public float getFloat(int pos)
        {
            Block fieldBlock = rowBlock.getFieldBlock(pos);
            return Float.intBitsToFloat(toIntExact((long) TypeUtils.readNativeValue(REAL, fieldBlock, position)));
        }

        @Override
        public double getDouble(int pos)
        {
            Block fieldBlock = rowBlock.getFieldBlock(pos);
            return (double) TypeUtils.readNativeValue(DOUBLE, fieldBlock, position);
        }

        @Override
        public BinaryString getString(int pos)
        {
            return BinaryString.fromBytes(getBinary(pos));
        }

        @Override
        public Decimal getDecimal(int pos, int precision, int scale)
        {
            Block fieldBlock = rowBlock.getFieldBlock(pos);
            Object value = TypeUtils.readNativeValue(DecimalType.createDecimalType(precision, scale), fieldBlock,
                    position);
            if (precision <= MAX_SHORT_PRECISION) {
                return Decimal.fromUnscaledLong((Long) value, precision, scale);
            }
            else {
                long high = ((Int128) value).getHigh();
                long low = ((Int128) value).getLow();
                BigInteger bigIntegerValue = BigInteger.valueOf(high).shiftLeft(64).add(BigInteger.valueOf(low));
                BigDecimal bigDecimalValue = new BigDecimal(bigIntegerValue, scale);
                return Decimal.fromBigDecimal(bigDecimalValue, precision, scale);
            }
        }

        @Override
        public Timestamp getTimestamp(int pos, int precision)
        {
            Block fieldBlock = rowBlock.getFieldBlock(pos);
            long value = (long) TypeUtils.readNativeValue(TIMESTAMP_MICROS, fieldBlock, position);
            return Timestamp.fromMicros(value);
        }

        @Override
        public byte[] getBinary(int pos)
        {
            Block fieldBlock = rowBlock.getFieldBlock(pos);
            Slice slice = (Slice) TypeUtils.readNativeValue(VARBINARY, fieldBlock, position);
            return slice.getBytes();
        }

        @Override
        public Blob getBlob(int pos)
        {
            return Blob.fromData(getBinary(pos));
        }

        @Override
        public Variant getVariant(int pos)
        {
            if (isNullAt(pos)) {
                return null;
            }
            Block fieldBlock = rowBlock.getFieldBlock(pos);
            return parseVariantFromBlock(fieldBlock, position);
        }

        @Override
        public InternalArray getArray(int pos)
        {
            if (isNullAt(pos)) {
                return null;
            }
            Block fieldBlock = rowBlock.getFieldBlock(pos);
            ArrayBlock arrayBlock = (ArrayBlock) fieldBlock.getSingleValueBlock(position);
            return new TrinoArray(arrayBlock);
        }

        @Override
        public InternalMap getMap(int pos)
        {
            if (isNullAt(pos)) {
                return null;
            }
            Block fieldBlock = rowBlock.getFieldBlock(pos);
            MapBlock mapBlock = (MapBlock) fieldBlock;
            SqlMap sqlMap = mapBlock.getMap(position);
            return new TrinoMap(sqlMap);
        }

        @Override
        public InternalRow getRow(int pos, int numFields)
        {
            if (isNullAt(pos)) {
                return null;
            }
            Block fieldBlock = rowBlock.getFieldBlock(pos);
            RowBlock nestedRowBlock = (RowBlock) fieldBlock.getSingleValueBlock(position);
            return new TrinoNestedRow(nestedRowBlock, rowKind);
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
            super(block);
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
