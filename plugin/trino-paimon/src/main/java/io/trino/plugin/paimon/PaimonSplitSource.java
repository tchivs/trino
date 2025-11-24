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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.metrics.Metrics;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

public class PaimonSplitSource
        implements
        ConnectorSplitSource
{
    private final Queue<PaimonSplit> splits;
    private final OptionalLong limit;
    private final long totalSplitCount;
    private final long totalRowCount;
    private long count;
    private long processedSplitCount;

    public PaimonSplitSource(List<PaimonSplit> splits, OptionalLong limit)
    {
        this.splits = new LinkedList<>(splits);
        this.limit = limit;
        this.totalSplitCount = splits.size();
        this.totalRowCount = splits.stream().mapToLong(split -> split.decodeSplit().rowCount()).sum();
    }

    protected CompletableFuture<ConnectorSplitBatch> innerGetNextBatch(int maxSize)
    {
        List<ConnectorSplit> batch = new ArrayList<>();
        for (int i = 0; i < maxSize; i++) {
            PaimonSplit split = splits.poll();
            if (split == null || (limit.isPresent() && count >= limit.getAsLong())) {
                break;
            }
            count += split.decodeSplit().rowCount();
            processedSplitCount++;
            batch.add(split);
        }
        return CompletableFuture.completedFuture(new ConnectorSplitBatch(batch, isFinished()));
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        return innerGetNextBatch(maxSize);
    }

    @Override
    public void close()
    {
    }

    @Override
    public boolean isFinished()
    {
        return splits.isEmpty() || (limit.isPresent() && count >= limit.getAsLong());
    }

    @Override
    public Metrics getMetrics()
    {
        return new Metrics(ImmutableMap.of(
                "totalSplits", new LongCount(totalSplitCount),
                "processedSplits", new LongCount(processedSplitCount),
                "totalRows", new LongCount(totalRowCount)));
    }
}
