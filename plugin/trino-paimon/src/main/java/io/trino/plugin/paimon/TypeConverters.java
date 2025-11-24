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
import io.trino.spi.block.Block;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TypeUtils;
import org.apache.paimon.data.Decimal;

import java.math.BigDecimal;
import java.math.BigInteger;

import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.toIntExact;

/**
 * Utility class for converting between Trino and Paimon data types.
 *
 * <p>Key differences:
 * <ul>
 *   <li>TIME: Trino stores as long (picoseconds), Paimon expects int (milliseconds)</li>
 *   <li>DECIMAL: Trino uses Long/Int128, Paimon uses Decimal class</li>
 *   <li>BINARY: Trino uses Slice, Paimon uses byte[]</li>
 * </ul>
 */
final class TypeConverters
{
    private TypeConverters() {}

    /**
     * Converts an int column from Trino Block to Paimon int.
     * Handles both INTEGER and TIME types.
     *
     * <p>For TIME columns: Trino stores as long (picoseconds since midnight),
     * Paimon expects int (milliseconds since midnight).
     *
     * @param block the Trino block containing the value
     * @param position the position in the block
     * @return the int value, with TIME values converted from picoseconds to milliseconds
     */
    static int readInt(Block block, int position)
    {
        try {
            // Try reading as INTEGER first (most common case)
            long value = (long) TypeUtils.readNativeValue(INTEGER, block, position);
            if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Value out of range for int: " + value);
            }
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

    /**
     * Converts a Decimal column from Trino Block to Paimon Decimal.
     *
     * <p>Trino uses Long for short decimals (precision <= 18) and Int128 for long decimals.
     * Paimon uses its own Decimal class for both.
     *
     * @param block the Trino block containing the decimal value
     * @param position the position in the block
     * @param precision the decimal precision
     * @param scale the decimal scale
     * @return the Paimon Decimal value
     */
    static Decimal readDecimal(Block block, int position, int precision, int scale)
    {
        Object value = TypeUtils.readNativeValue(DecimalType.createDecimalType(precision, scale), block, position);
        if (precision <= MAX_SHORT_PRECISION) {
            // Short decimal: stored as Long
            return Decimal.fromUnscaledLong((Long) value, precision, scale);
        }
        else {
            // Long decimal: stored as Int128
            long high = ((Int128) value).getHigh();
            long low = ((Int128) value).getLow();
            BigInteger bigIntegerValue = BigInteger.valueOf(high).shiftLeft(64).add(BigInteger.valueOf(low));
            BigDecimal bigDecimalValue = new BigDecimal(bigIntegerValue, scale);
            return Decimal.fromBigDecimal(bigDecimalValue, precision, scale);
        }
    }

    /**
     * Converts a Binary/Varbinary column from Trino Block to byte array.
     *
     * @param block the Trino block containing the binary value
     * @param position the position in the block
     * @return the byte array
     */
    static byte[] readBinary(Block block, int position)
    {
        Slice slice = (Slice) TypeUtils.readNativeValue(VARBINARY, block, position);
        return slice.getBytes();
    }
}
