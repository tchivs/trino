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

import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.TupleDomain;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DynamicFilteringTrinoSplitSource
        implements
        ConnectorSplitSource
{
    private static final Logger LOG = Logger.get(DynamicFilteringTrinoSplitSource.class);
    private static final int DOMAIN_COMPACTION_THRESHOLD = 1000;
    private static final ConnectorSplitBatch EMPTY_BATCH = new ConnectorSplitBatch(ImmutableList.of(), false);

    private final PaimonTableHandle tableHandle;
    private final ConnectorSession session;
    private final PaimonCatalog paimonCatalog;
    private final DynamicFilter dynamicFilter;
    private final Duration dynamicFilteringWaitTimeout;
    private final long dynamicFilteringWaitStartMillis;

    @GuardedBy("this")
    private boolean splitsPlanningStarted;

    @GuardedBy("this")
    private PaimonSplitSource delegateSplitSource;

    public DynamicFilteringTrinoSplitSource(PaimonTableHandle tableHandle, ConnectorSession session,
            PaimonCatalog paimonCatalog, DynamicFilter dynamicFilter, Duration dynamicFilteringWaitTimeout)
    {
        this.tableHandle = tableHandle;
        this.session = session;
        this.paimonCatalog = paimonCatalog;
        this.dynamicFilter = dynamicFilter;
        this.dynamicFilteringWaitTimeout = dynamicFilteringWaitTimeout;
        this.dynamicFilteringWaitStartMillis = System.currentTimeMillis();
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        long timeLeft = computeTimeLeft();

        synchronized (this) {
            // Wait for dynamic filters if not yet started planning
            if (!splitsPlanningStarted && dynamicFilter.isAwaitable() && timeLeft > 0) {
                LOG.debug("Waiting for dynamic filters, time left: {}ms", timeLeft);
                return dynamicFilter.isBlocked().thenApply(ignored -> EMPTY_BATCH).completeOnTimeout(EMPTY_BATCH,
                        timeLeft, MILLISECONDS);
            }

            // Start split planning if not yet started
            if (!splitsPlanningStarted) {
                delegateSplitSource = planSplits();
                splitsPlanningStarted = true;
            }
        }

        // Delegate to actual split source
        return delegateSplitSource.getNextBatch(maxSize);
    }

    @Override
    public void close()
    {
        synchronized (this) {
            if (delegateSplitSource != null) {
                delegateSplitSource.close();
            }
        }
    }

    @Override
    public boolean isFinished()
    {
        synchronized (this) {
            if (!splitsPlanningStarted) {
                return false;
            }
            return delegateSplitSource.isFinished();
        }
    }

    @Override
    public Metrics getMetrics()
    {
        synchronized (this) {
            if (delegateSplitSource != null) {
                return delegateSplitSource.getMetrics();
            }
            return Metrics.EMPTY;
        }
    }

    private long computeTimeLeft()
    {
        if (dynamicFilteringWaitTimeout.toMillis() == 0) {
            return 0;
        }
        long elapsedMillis = System.currentTimeMillis() - dynamicFilteringWaitStartMillis;
        return Math.max(0, dynamicFilteringWaitTimeout.toMillis() - elapsedMillis);
    }

    private PaimonSplitSource planSplits()
    {
        // Combine dynamic filter with static filters
        TupleDomain<PaimonColumnHandle> combinedPredicate = combinePredicates(tableHandle.getFilter(), dynamicFilter);

        // Apply combined predicate to table scan
        Table table = tableHandle.tableWithDynamicOptions(paimonCatalog, session);
        ReadBuilder readBuilder = table.newReadBuilder();

        // Convert combined predicate to Paimon predicate
        Optional<Predicate> paimonPredicate = new PaimonFilterConverter(table.rowType()).convert(combinedPredicate);
        paimonPredicate.ifPresent(readBuilder::withFilter);

        // Apply limit if present
        tableHandle.getLimit().ifPresent(limit -> readBuilder.withLimit((int) limit));

        // Plan splits
        List<Split> splits = readBuilder.dropStats().newScan().plan().splits();

        LOG.info("Planned {} splits after applying dynamic filters", splits.size());

        // Calculate split weights
        long maxRowCount = splits.stream().mapToLong(Split::rowCount).max().orElse(0L);
        double minimumSplitWeight = PaimonSessionProperties.getMinimumSplitWeight(session);

        return new PaimonSplitSource(splits.stream()
                .map(split -> PaimonSplit.fromSplit(split,
                        Math.min(Math.max((double) split.rowCount() / maxRowCount, minimumSplitWeight), 1.0)))
                .collect(Collectors.toList()), tableHandle.getLimit());
    }

    private TupleDomain<PaimonColumnHandle> combinePredicates(TupleDomain<PaimonColumnHandle> staticPredicate,
            DynamicFilter dynamicFilter)
    {
        // Extract dynamic filter predicate
        TupleDomain<PaimonColumnHandle> dynamicPredicate = dynamicFilter.getCurrentPredicate()
                .transformKeys(PaimonColumnHandle.class::cast);

        LOG.debug("Static predicate: {}", staticPredicate);
        LOG.debug("Dynamic predicate: {}", dynamicPredicate);

        // Combine with static predicate
        TupleDomain<PaimonColumnHandle> combined = staticPredicate.intersect(dynamicPredicate);

        // Simplify if too complex (prevent memory explosion)
        if (exceedsComplexityThreshold(combined, DOMAIN_COMPACTION_THRESHOLD)) {
            LOG.warn("Combined predicate exceeds complexity threshold ({}), using only static predicate",
                    DOMAIN_COMPACTION_THRESHOLD);
            return staticPredicate;
        }

        LOG.debug("Combined predicate: {}", combined);
        return combined;
    }

    private boolean exceedsComplexityThreshold(TupleDomain<PaimonColumnHandle> predicate, int threshold)
    {
        if (predicate.isAll() || predicate.isNone()) {
            return false;
        }

        if (predicate.getDomains().isEmpty()) {
            return false;
        }

        int totalValues = predicate.getDomains().get().values().stream()
                .mapToInt(domain -> domain.getValues().getRanges().getRangeCount()).sum();

        return totalValues > threshold;
    }
}
