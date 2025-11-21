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
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

/**
 * Wraps a page source to handle schema evolution by filling in null blocks
 * for columns that don't exist in the underlying data file.
 */
public class SchemaEvolutionPageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource delegate;
    private final int totalColumns;
    private final int[] columnMapping; // maps output channel to delegate channel, -1 for missing
    private final List<Type> types;

    /**
     * @param delegate      The underlying page source
     * @param totalColumns  Total number of output columns expected
     * @param columnMapping Maps each output column index to delegate column index, -1 if missing
     * @param types         Types for all columns (used to create null blocks for missing columns)
     */
    public SchemaEvolutionPageSource(
            ConnectorPageSource delegate,
            int totalColumns,
            int[] columnMapping,
            List<Type> types)
    {
        this.delegate = delegate;
        this.totalColumns = totalColumns;
        this.columnMapping = columnMapping;
        this.types = types;
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return delegate.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        SourcePage sourcePage = delegate.getNextSourcePage();
        if (sourcePage == null) {
            return null;
        }

        Page delegatePage = sourcePage.getPage();
        int positionCount = delegatePage.getPositionCount();

        Block[] blocks = new Block[totalColumns];
        for (int i = 0; i < totalColumns; i++) {
            int delegateChannel = columnMapping[i];
            if (delegateChannel >= 0) {
                blocks[i] = delegatePage.getBlock(delegateChannel);
            }
            else {
                // Create a null block for missing column
                blocks[i] = RunLengthEncodedBlock.create(types.get(i), null, positionCount);
            }
        }

        return SourcePage.create(new Page(positionCount, blocks));
    }

    @Override
    public long getMemoryUsage()
    {
        return delegate.getMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return delegate.isBlocked();
    }

    @Override
    public Metrics getMetrics()
    {
        return delegate.getMetrics();
    }
}
