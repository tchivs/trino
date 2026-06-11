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

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;

import static io.trino.spi.block.ArrayValueBuilder.buildArrayValue;
import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochMillisAndFraction;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.DateTimes.MICROSECONDS_PER_MILLISECOND;
import static org.assertj.core.api.Assertions.assertThat;

public class TrinoRowTest
{
    @Test
    void test()
    {
        Page singlePage = new Page(1, writeNativeValue(BOOLEAN, null), writeNativeValue(BOOLEAN, false),
                writeNativeValue(TINYINT, 22L), writeNativeValue(SMALLINT, 356L),
                writeNativeValue(INTEGER, 4L), writeNativeValue(BIGINT, 23567222L),
                writeNativeValue(REAL, (long) Float.floatToIntBits(1213.33f)), writeNativeValue(DOUBLE, 121.3d),
                writeNativeValue(VARCHAR, Slices.wrappedBuffer("rfyu".getBytes(StandardCharsets.UTF_8))),
                writeNativeValue(DecimalType.createDecimalType(2, 2),
                        encodeShortScaledValue(BigDecimal.valueOf(0.21), 2)),
                writeNativeValue(DecimalType.createDecimalType(38, 2),
                        encodeScaledValue(BigDecimal.valueOf(65782123123.01), 2)),
                writeNativeValue(DecimalType.createDecimalType(10, 1),
                        encodeShortScaledValue(BigDecimal.valueOf(62123123.5), 1)),
                writeNativeValue(TIMESTAMP_MICROS,
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2007-12-03T10:15:30")).getMillisecond()
                                * MICROSECONDS_PER_MILLISECOND),
                writeNativeValue(VARBINARY, Slices.wrappedBuffer("varbinary_v".getBytes(StandardCharsets.UTF_8))));
        List<Type> types = List.of(BOOLEAN, BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, VARCHAR,
                DecimalType.createDecimalType(2, 2), DecimalType.createDecimalType(38, 2),
                DecimalType.createDecimalType(10, 1), TIMESTAMP_MICROS, VARBINARY);
        PaimonRow trinoRow = new PaimonRow(singlePage, RowKind.INSERT, types);

        assertThat(trinoRow.getRowKind()).isEqualTo(RowKind.INSERT);
        assertThat(trinoRow.isNullAt(0)).isEqualTo(true);
        assertThat(trinoRow.getBoolean(1)).isEqualTo(false);
        assertThat(trinoRow.getByte(2)).isEqualTo((byte) 22);
        assertThat(trinoRow.getShort(3)).isEqualTo((short) 356);
        assertThat(trinoRow.getInt(4)).isEqualTo(4);
        assertThat(trinoRow.getLong(5)).isEqualTo(23567222L);
        assertThat(trinoRow.getFloat(6)).isEqualTo(1213.33f);
        assertThat(trinoRow.getDouble(7)).isEqualTo(121.3d);
        assertThat(trinoRow.getString(8)).isEqualTo(BinaryString.fromString("rfyu"));
        assertThat(trinoRow.getDecimal(9, 2, 2)).isEqualTo(Decimal.fromBigDecimal(BigDecimal.valueOf(0.21), 2, 2));
        assertThat(trinoRow.getDecimal(10, 38, 2))
                .isEqualTo(Decimal.fromBigDecimal(BigDecimal.valueOf(65782123123.01), 38, 2));
        assertThat(trinoRow.getDecimal(11, 10, 1))
                .isEqualTo(Decimal.fromBigDecimal(BigDecimal.valueOf(62123123.5), 10, 1));
        assertThat(trinoRow.getTimestamp(12, 6))
                .isEqualTo(Timestamp.fromLocalDateTime(LocalDateTime.parse("2007-12-03T10:15:30")));
        assertThat(trinoRow.getBinary(13)).isEqualTo("varbinary_v".getBytes(StandardCharsets.UTF_8));
        assertThat(trinoRow.getBlob(13).toData()).isEqualTo("varbinary_v".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void testTimeAndHighPrecisionTimestampConversions()
    {
        LongTimestamp timestamp = new LongTimestamp(1_695_645_403_123_456L, 789_000);
        LongTimestampWithTimeZone timestampWithTimeZone = fromEpochMillisAndFraction(1_695_645_403_123L,
                456_000_000, UTC_KEY);
        Page singlePage = new Page(1,
                writeNativeValue(TIME_MICROS, 12_345L * PICOSECONDS_PER_MILLISECOND),
                writeNativeValue(TIMESTAMP_NANOS, timestamp),
                writeNativeValue(TIMESTAMP_TZ_MICROS, timestampWithTimeZone));
        PaimonRow trinoRow = new PaimonRow(singlePage, RowKind.INSERT,
                List.of(TIME_MICROS, TIMESTAMP_NANOS, TIMESTAMP_TZ_MICROS));

        assertThat(trinoRow.getInt(0)).isEqualTo(12_345);
        assertThat(trinoRow.getTimestamp(1, 9)).isEqualTo(Timestamp.fromEpochMillis(1_695_645_403_123L, 456_789));
        assertThat(trinoRow.getTimestamp(2, 6)).isEqualTo(Timestamp.fromEpochMillis(1_695_645_403_123L, 456_000));
    }

    @Test
    void testLongDecimalWithUnsignedLowBits()
    {
        DecimalType type = DecimalType.createDecimalType(38, 0);
        BigDecimal value = new BigDecimal("18446744073709551615");
        Page singlePage = new Page(1, writeNativeValue(type, encodeScaledValue(value, type.getScale())));
        PaimonRow trinoRow = new PaimonRow(singlePage, RowKind.INSERT, List.of(type));

        assertThat(trinoRow.getDecimal(0, type.getPrecision(), type.getScale()))
                .isEqualTo(Decimal.fromBigDecimal(value, type.getPrecision(), type.getScale()));
    }

    @Test
    void testNestedComplexTypeConversionsUseElementTypes()
    {
        ArrayType timestampArrayType = new ArrayType(TIMESTAMP_NANOS);
        MapType timestampMapType = new MapType(INTEGER, TIMESTAMP_TZ_MICROS, new TypeOperators());
        DecimalType longDecimalType = DecimalType.createDecimalType(38, 0);
        RowType rowType = RowType.anonymous(List.of(TIME_MICROS, longDecimalType));

        LongTimestamp timestamp = new LongTimestamp(1_695_645_403_123_456L, 789_000);
        LongTimestampWithTimeZone timestampWithTimeZone = fromEpochMillisAndFraction(1_695_645_403_123L,
                456_000_000, UTC_KEY);
        BigDecimal decimalValue = new BigDecimal("18446744073709551615");

        Block timestampArray = buildArrayValue(timestampArrayType, 1,
                elementBuilder -> writeNativeValue(TIMESTAMP_NANOS, elementBuilder, timestamp));
        SqlMap timestampMap = buildMapValue(timestampMapType, 1, (keyBuilder, valueBuilder) -> {
            writeNativeValue(INTEGER, keyBuilder, 7L);
            writeNativeValue(TIMESTAMP_TZ_MICROS, valueBuilder, timestampWithTimeZone);
        });
        SqlRow row = buildRowValue(rowType, fieldBuilders -> {
            writeNativeValue(TIME_MICROS, fieldBuilders.get(0), 12_345L * PICOSECONDS_PER_MILLISECOND);
            writeNativeValue(longDecimalType, fieldBuilders.get(1),
                    encodeScaledValue(decimalValue, longDecimalType.getScale()));
        });

        PaimonRow trinoRow = new PaimonRow(new Page(1,
                writeNativeValue(timestampArrayType, timestampArray),
                writeNativeValue(timestampMapType, timestampMap),
                writeNativeValue(rowType, row)),
                RowKind.INSERT,
                List.of(timestampArrayType, timestampMapType, rowType));

        InternalArray array = trinoRow.getArray(0);
        assertThat(array.size()).isEqualTo(1);
        assertThat(array.getTimestamp(0, 9)).isEqualTo(Timestamp.fromEpochMillis(1_695_645_403_123L, 456_789));

        InternalMap map = trinoRow.getMap(1);
        assertThat(map.size()).isEqualTo(1);
        assertThat(map.keyArray().getInt(0)).isEqualTo(7);
        assertThat(map.valueArray().getTimestamp(0, 6))
                .isEqualTo(Timestamp.fromEpochMillis(1_695_645_403_123L, 456_000));

        InternalRow nestedRow = trinoRow.getRow(2, 2);
        assertThat(nestedRow.getInt(0)).isEqualTo(12_345);
        assertThat(nestedRow.getDecimal(1, longDecimalType.getPrecision(), longDecimalType.getScale()))
                .isEqualTo(Decimal.fromBigDecimal(decimalValue, longDecimalType.getPrecision(),
                        longDecimalType.getScale()));
    }
}
