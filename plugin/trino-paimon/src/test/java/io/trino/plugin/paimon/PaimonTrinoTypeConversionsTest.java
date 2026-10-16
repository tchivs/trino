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

import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import org.apache.paimon.data.LocalZoneTimestamp;
import org.apache.paimon.data.Timestamp;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PaimonTrinoTypeConversionsTest
{
    private static final TimestampType TIMESTAMP_9 = TimestampType.createTimestampType(9);
    private static final TimestampType TIMESTAMP_6 = TimestampType.createTimestampType(6);
    private static final TimestampWithTimeZoneType TIMESTAMP_TZ_9 = TimestampWithTimeZoneType.createTimestampWithTimeZoneType(9);
    private static final TimestampWithTimeZoneType TIMESTAMP_TZ_3 = TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3);

    // Trino represents a short timestamp as epoch micros and a long timestamp as epoch micros plus picos of micro
    private static final List<Long> MILLIS = List.of(
            -9_000_000_000_000_000L, -86_400_001L, -1_000L, -2L, -1L, 0L, 1L, 2L, 1_000L, 86_400_001L, 4_102_444_800_000L, 9_000_000_000_000_000L);
    // Paimon stores the nanoseconds within a millisecond, so the fraction is bounded by 999_999
    private static final List<Integer> NANOS = List.of(0, 1, 999, 1_000, 456_789, 999_000, 999_999);

    @Test
    public void testPaimonTimestampToTrinoLongTypeKeepsNanoseconds()
    {
        for (long millis : MILLIS) {
            for (int nanos : NANOS) {
                Object converted = PaimonTrinoTypeConversions.paimonTimestampToTrino(
                        TIMESTAMP_9, Timestamp.fromEpochMillis(millis, nanos));

                assertThat(converted).isInstanceOf(LongTimestamp.class);
                assertThat(((LongTimestamp) converted).getEpochMicros()).isEqualTo(millis * 1_000 + nanos / 1_000);
                assertThat(((LongTimestamp) converted).getPicosOfMicro()).isEqualTo((nanos % 1_000) * PICOSECONDS_PER_NANOSECOND);
            }
        }
    }

    @Test
    public void testPaimonTimestampToTrinoShortTypeUsesEpochMicros()
    {
        for (long millis : MILLIS) {
            for (int nanos : NANOS) {
                Object converted = PaimonTrinoTypeConversions.paimonTimestampToTrino(
                        TIMESTAMP_6, Timestamp.fromEpochMillis(millis, nanos));

                assertThat(converted).isEqualTo(millis * 1_000 + nanos / 1_000);
            }
        }
    }

    @Test
    public void testTrinoTimestampToPaimonRoundTrip()
    {
        for (long millis : MILLIS) {
            for (int nanos : NANOS) {
                LongTimestamp trino = (LongTimestamp) PaimonTrinoTypeConversions.paimonTimestampToTrino(
                        TIMESTAMP_9, Timestamp.fromEpochMillis(millis, nanos));

                Timestamp converted = PaimonTrinoTypeConversions.trinoTimestampToPaimon(trino);

                assertThat(converted.getMillisecond()).isEqualTo(millis);
                assertThat(converted.getNanoOfMillisecond()).isEqualTo(nanos);
            }
        }
    }

    @Test
    public void testTrinoTimestampToPaimonTruncatesSubNanosecondPicos()
    {
        Timestamp converted = PaimonTrinoTypeConversions.trinoTimestampToPaimon(new LongTimestamp(1_000L, 1));

        assertThat(converted.getMillisecond()).isEqualTo(1L);
        assertThat(converted.getNanoOfMillisecond()).isZero();
    }

    @Test
    public void testPaimonTimestampToTrinoTimestampWithTimeZone()
    {
        for (long millis : MILLIS) {
            for (int nanos : NANOS) {
                Timestamp paimon = Timestamp.fromEpochMillis(millis, nanos);
                LocalZoneTimestamp localZone = LocalZoneTimestamp.fromEpochMillis(millis, nanos);

                Object fromTimestamp = PaimonTrinoTypeConversions.paimonTimestampToTrinoTimestampWithTimeZone(TIMESTAMP_TZ_9, paimon);
                Object fromLocalZone = PaimonTrinoTypeConversions.paimonTimestampToTrinoTimestampWithTimeZone(TIMESTAMP_TZ_9, localZone);

                assertThat(fromLocalZone).isEqualTo(fromTimestamp).isInstanceOf(LongTimestampWithTimeZone.class);
                assertThat(((LongTimestampWithTimeZone) fromTimestamp).getEpochMillis()).isEqualTo(millis);
                assertThat(((LongTimestampWithTimeZone) fromTimestamp).getPicosOfMilli()).isEqualTo(nanos * PICOSECONDS_PER_NANOSECOND);
                assertThat(((LongTimestampWithTimeZone) fromTimestamp).getTimeZoneKey()).isEqualTo(UTC_KEY.getKey());
            }
        }
    }

    @Test
    public void testPaimonTimestampToTrinoTimestampWithTimeZoneShortTypeUsesMillis()
    {
        for (long millis : List.of(-4_102_444_800_000L, -86_400_001L, -1_000L, -1L, 0L, 1L, 1_000L, 4_102_444_800_000L)) {
            Object converted = PaimonTrinoTypeConversions.paimonTimestampToTrinoTimestampWithTimeZone(
                    TIMESTAMP_TZ_3, Timestamp.fromEpochMillis(millis, 999_999));

            assertThat(converted).isEqualTo(packDateTimeWithZone(millis, UTC_KEY));
            assertThat(unpackMillisUtc((long) converted)).isEqualTo(millis);
        }
    }

    @Test
    public void testTrinoTimestampWithTimeZoneToPaimon()
    {
        assertThat(PaimonTrinoTypeConversions.trinoTimestampWithTimeZoneToPaimon(
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(-86_400_001L, 456_789 * PICOSECONDS_PER_NANOSECOND, UTC_KEY)))
                .isEqualTo(Timestamp.fromEpochMillis(-86_400_001L, 456_789));

        assertThat(PaimonTrinoTypeConversions.trinoTimestampWithTimeZoneToPaimon(packDateTimeWithZone(-86_400_001L, UTC_KEY)))
                .isEqualTo(Timestamp.fromEpochMillis(-86_400_001L));
    }

    @Test
    public void testTimeOfDayRoundTrip()
    {
        for (int millisOfDay : List.of(0, 1, 999, 1_000, 43_200_000, 86_399_999)) {
            long picos = PaimonTrinoTypeConversions.paimonTimeMillisToTrinoPicos(millisOfDay);

            assertThat(picos).isEqualTo(millisOfDay * 1_000_000_000L);
            assertThat(PaimonTrinoTypeConversions.trinoTimePicosToPaimonMillis(picos)).isEqualTo(millisOfDay);
        }
    }

    @Test
    public void testTrinoTimeRejectsSubMillisecondPicos()
    {
        assertThatThrownBy(() -> PaimonTrinoTypeConversions.trinoTimePicosToPaimonMillis(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Paimon stores TIME values with millisecond precision");
    }
}
