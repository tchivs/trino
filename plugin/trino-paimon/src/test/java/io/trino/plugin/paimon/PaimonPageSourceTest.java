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

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeUtils;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.reader.RecordReader;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.OptionalLong;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static org.assertj.core.api.Assertions.assertThat;

public class PaimonPageSourceTest
{
    @Test
    void testHighPrecisionTemporalValues()
    {
        GenericRow row = new GenericRow(3);
        row.setField(0, 12_345);
        row.setField(1, Timestamp.fromEpochMillis(1_695_645_403_123L, 456_789));
        row.setField(2, Timestamp.fromEpochMillis(1_695_645_403_123L, 456_000));

        PaimonPageSource pageSource = new PaimonPageSource(new TestingRecordReader(row), List.of(
                PaimonColumnHandle.of("t", new org.apache.paimon.types.TimeType(6)),
                PaimonColumnHandle.of("ts", new org.apache.paimon.types.TimestampType(9)),
                PaimonColumnHandle.of("tz", new org.apache.paimon.types.LocalZonedTimestampType(6))),
                OptionalLong.empty());

        Page page = pageSource.getNextPage();

        assertThat(page.getPositionCount()).isEqualTo(1);
        assertThat(TypeUtils.readNativeValue(TIME_MICROS, page.getBlock(0), 0))
                .isEqualTo(12_345L * PICOSECONDS_PER_MILLISECOND);
        assertThat(TypeUtils.readNativeValue(TIMESTAMP_NANOS, page.getBlock(1), 0))
                .isEqualTo(new LongTimestamp(1_695_645_403_123_456L, 789_000));
        assertThat(TypeUtils.readNativeValue(TIMESTAMP_TZ_MICROS, page.getBlock(2), 0))
                .isEqualTo(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1_695_645_403_123L,
                        456_000_000, UTC_KEY));
        assertThat(pageSource.getNextPage()).isNull();
    }

    @Test
    void testDirectPageSourceEnforcesLimitAcrossSources()
    {
        TestingPageSource first = new TestingPageSource(new Page(3, bigintBlock(1, 2, 3)));
        TestingPageSource second = new TestingPageSource(new Page(3, bigintBlock(4, 5, 6)));
        DirectTrinoPageSource pageSource = new DirectTrinoPageSource(new LinkedList<>(List.of(first, second)),
                OptionalLong.of(4));

        Page firstPage = pageSource.getNextPage();
        Page secondPage = pageSource.getNextPage();

        assertThat(firstPage.getPositionCount()).isEqualTo(3);
        assertThat(secondPage.getPositionCount()).isEqualTo(1);
        assertThat(TypeUtils.readNativeValue(BIGINT, secondPage.getBlock(0), 0)).isEqualTo(4L);
        assertThat(pageSource.getNextPage()).isNull();
        assertThat(first.closed()).isTrue();
        assertThat(second.closed()).isTrue();
    }

    @Test
    void testDirectPageSourceDoesNotAdvanceWhenCurrentSourceIsBlocked()
    {
        BlockingPageSource first = new BlockingPageSource(new Page(1, bigintBlock(1)));
        TestingPageSource second = new TestingPageSource(new Page(1, bigintBlock(2)));
        DirectTrinoPageSource pageSource = new DirectTrinoPageSource(new LinkedList<>(List.of(first, second)),
                OptionalLong.empty());

        assertThat(pageSource.getNextPage()).isNull();

        Page page = pageSource.getNextPage();
        assertThat(TypeUtils.readNativeValue(BIGINT, page.getBlock(0), 0)).isEqualTo(1L);
        assertThat(second.closed()).isFalse();
    }

    @Test
    void testMergePageSourceWrapperPreservesRowIdFieldOrder()
    {
        TestingPageSource source = new TestingPageSource(new Page(1, bigintBlock(10), bigintBlock(20)));
        HashMap<String, Integer> fieldToIndex = new HashMap<>();
        fieldToIndex.put("a", 0);
        fieldToIndex.put("b", 1);
        PaimonMergePageSourceWrapper wrapper = PaimonMergePageSourceWrapper.wrap(source, List.of("b", "a"),
                fieldToIndex);
        RowType rowIdType = RowType.from(List.of(RowType.field("b", BIGINT), RowType.field("a", BIGINT)));

        Page page = wrapper.getNextPage();
        SqlRow rowId = rowIdType.getObject(page.getBlock(2), 0);

        assertThat(BIGINT.getLong(rowId.getRawFieldBlock(0), rowId.getRawIndex())).isEqualTo(20);
        assertThat(BIGINT.getLong(rowId.getRawFieldBlock(1), rowId.getRawIndex())).isEqualTo(10);
    }

    private static Block bigintBlock(long... values)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(values.length);
        for (long value : values) {
            BIGINT.writeLong(builder, value);
        }
        return builder.build();
    }

    private static class TestingRecordReader
            implements RecordReader<InternalRow>
    {
        private final GenericRow row;
        private boolean returned;

        private TestingRecordReader(GenericRow row)
        {
            this.row = row;
        }

        @Override
        public RecordIterator<InternalRow> readBatch()
        {
            if (returned) {
                return null;
            }
            returned = true;
            return new RecordIterator<>()
            {
                private boolean hasNext = true;

                @Override
                public InternalRow next()
                {
                    if (!hasNext) {
                        return null;
                    }
                    hasNext = false;
                    return row;
                }

                @Override
                public void releaseBatch() {}
            };
        }

        @Override
        public void close() {}
    }

    private static class TestingPageSource
            implements ConnectorPageSource
    {
        private final Page page;
        private boolean returned;
        private boolean closed;

        private TestingPageSource(Page page)
        {
            this.page = page;
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
            return returned;
        }

        @Override
        public Page getNextPage()
        {
            if (returned) {
                return null;
            }
            returned = true;
            return page;
        }

        @Override
        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public void close()
                throws IOException
        {
            closed = true;
        }

        private boolean closed()
        {
            return closed;
        }
    }

    private static class BlockingPageSource
            implements ConnectorPageSource
    {
        private final Page page;
        private boolean blocked = true;
        private boolean returned;

        private BlockingPageSource(Page page)
        {
            this.page = page;
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
            return returned;
        }

        @Override
        public Page getNextPage()
        {
            if (blocked) {
                blocked = false;
                return null;
            }
            if (returned) {
                return null;
            }
            returned = true;
            return page;
        }

        @Override
        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public void close() {}
    }
}
