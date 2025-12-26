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
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A page source that returns pre-computed aggregation results.
 * This is used when aggregation pushdown is successful and the
 * aggregation values have been computed from manifest statistics.
 */
public class PaimonAggregationPageSource
        implements ConnectorPageSource
{
    private final PaimonAggregationResult aggregationResult;
    private boolean finished;
    private long completedBytes;
    private long readTimeNanos;

    public PaimonAggregationPageSource(PaimonAggregationResult aggregationResult)
    {
        this.aggregationResult = requireNonNull(aggregationResult, "aggregationResult is null");
        this.finished = false;
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (finished) {
            return null;
        }

        long startTime = System.nanoTime();

        List<PaimonAggregationResult.AggregationColumn> columns = aggregationResult.getAggregationColumns();
        Block[] blocks;
        int rowCount;

        if (aggregationResult.isMultiRow()) {
            // Multi-row result (GROUP BY partition key)
            List<List<Object>> rows = aggregationResult.getAggregationRows();
            rowCount = rows.size();
            blocks = new Block[columns.size()];

            for (int colIdx = 0; colIdx < columns.size(); colIdx++) {
                Type type = columns.get(colIdx).getType();
                blocks[colIdx] = createMultiValueBlock(type, rows, colIdx);
            }
        }
        else {
            // Single-row result (global aggregation)
            List<Object> values = aggregationResult.getAggregationValues();
            rowCount = 1;
            blocks = new Block[columns.size()];

            for (int i = 0; i < columns.size(); i++) {
                Type type = columns.get(i).getType();
                Object value = values.get(i);
                blocks[i] = createSingleValueBlock(type, value);
            }
        }

        finished = true;
        readTimeNanos = System.nanoTime() - startTime;
        completedBytes = 8L * columns.size() * rowCount;

        return SourcePage.create(new Page(rowCount, blocks));
    }

    private Block createSingleValueBlock(Type type, Object value)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);

        if (value == null) {
            blockBuilder.appendNull();
        }
        else if (type instanceof BigintType) {
            // Handle both Integer and Long values (JSON deserialization may return Integer for small numbers)
            long longValue = ((Number) value).longValue();
            BigintType.BIGINT.writeLong(blockBuilder, longValue);
        }
        else if (type instanceof IntegerType) {
            long longValue = ((Number) value).longValue();
            IntegerType.INTEGER.writeLong(blockBuilder, longValue);
        }
        else if (type instanceof SmallintType) {
            long longValue = ((Number) value).longValue();
            SmallintType.SMALLINT.writeLong(blockBuilder, longValue);
        }
        else if (type instanceof TinyintType) {
            long longValue = ((Number) value).longValue();
            TinyintType.TINYINT.writeLong(blockBuilder, longValue);
        }
        else if (type instanceof DoubleType) {
            double doubleValue = ((Number) value).doubleValue();
            DoubleType.DOUBLE.writeDouble(blockBuilder, doubleValue);
        }
        else if (type instanceof RealType) {
            // Trino stores REAL as long bits of float
            long longBits = ((Number) value).longValue();
            RealType.REAL.writeLong(blockBuilder, longBits);
        }
        else if (type instanceof BooleanType) {
            BooleanType.BOOLEAN.writeBoolean(blockBuilder, (Boolean) value);
        }
        else if (type instanceof DateType) {
            // Date is stored as days since epoch
            long longValue = ((Number) value).longValue();
            DateType.DATE.writeLong(blockBuilder, longValue);
        }
        else {
            throw new UnsupportedOperationException("Unsupported aggregation result type: " + type);
        }

        return blockBuilder.build();
    }

    private Block createMultiValueBlock(Type type, List<List<Object>> rows, int colIdx)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, rows.size());

        for (List<Object> row : rows) {
            Object value = row.get(colIdx);
            writeValue(blockBuilder, type, value);
        }

        return blockBuilder.build();
    }

    private void writeValue(BlockBuilder blockBuilder, Type type, Object value)
    {
        if (value == null) {
            blockBuilder.appendNull();
        }
        else if (type instanceof BigintType) {
            BigintType.BIGINT.writeLong(blockBuilder, ((Number) value).longValue());
        }
        else if (type instanceof IntegerType) {
            IntegerType.INTEGER.writeLong(blockBuilder, ((Number) value).longValue());
        }
        else if (type instanceof SmallintType) {
            SmallintType.SMALLINT.writeLong(blockBuilder, ((Number) value).longValue());
        }
        else if (type instanceof TinyintType) {
            TinyintType.TINYINT.writeLong(blockBuilder, ((Number) value).longValue());
        }
        else if (type instanceof DoubleType) {
            DoubleType.DOUBLE.writeDouble(blockBuilder, ((Number) value).doubleValue());
        }
        else if (type instanceof RealType) {
            RealType.REAL.writeLong(blockBuilder, ((Number) value).longValue());
        }
        else if (type instanceof BooleanType) {
            BooleanType.BOOLEAN.writeBoolean(blockBuilder, (Boolean) value);
        }
        else if (type instanceof DateType) {
            DateType.DATE.writeLong(blockBuilder, ((Number) value).longValue());
        }
        else if (type instanceof io.trino.spi.type.VarcharType) {
            io.trino.spi.type.VarcharType.VARCHAR.writeSlice(blockBuilder,
                    io.airlift.slice.Slices.utf8Slice((String) value));
        }
        else {
            throw new UnsupportedOperationException("Unsupported aggregation result type: " + type);
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        finished = true;
    }
}
