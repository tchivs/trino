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
import io.trino.spi.predicate.TupleDomain;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DynamicFilteringTrinoSplitSource
        implements
        ConnectorSplitSource
{
    private static final Logger LOG = Logger.get(DynamicFilteringTrinoSplitSource.class);
    private static final int DOMAIN_COMPACTION_THRESHOLD = 1000;
    private static final ConnectorSplitBatch EMPTY_BATCH = new ConnectorSplitBatch(ImmutableList.of(), false);
    private static final ConnectorSplitBatch FINISHED_BATCH = new ConnectorSplitBatch(ImmutableList.of(), true);

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

    @GuardedBy("this")
    private boolean closed;

    public DynamicFilteringTrinoSplitSource(PaimonTableHandle tableHandle, ConnectorSession session,
            PaimonCatalog paimonCatalog, DynamicFilter dynamicFilter, Duration dynamicFilteringWaitTimeout)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.session = requireNonNull(session, "session is null");
        this.paimonCatalog = requireNonNull(paimonCatalog, "paimonCatalog is null");
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.dynamicFilteringWaitTimeout = requireNonNull(dynamicFilteringWaitTimeout, "dynamicFilteringWaitTimeout is null");
        this.dynamicFilteringWaitStartMillis = System.currentTimeMillis();
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        checkArgument(maxSize > 0, "Cannot fetch a batch of zero size");
        long timeLeft = computeTimeLeft();

        synchronized (this) {
            if (closed) {
                return CompletableFuture.completedFuture(FINISHED_BATCH);
            }

            // Wait for dynamic filters if not yet started planning
            if (!splitsPlanningStarted && dynamicFilter.isAwaitable() && timeLeft > 0) {
                LOG.debug("Waiting for dynamic filters, time left: %sms", timeLeft);
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
            closed = true;
            if (delegateSplitSource != null) {
                delegateSplitSource.close();
            }
        }
    }

    @Override
    public boolean isFinished()
    {
        synchronized (this) {
            if (closed) {
                return true;
            }
            if (!splitsPlanningStarted) {
                return false;
            }
            return delegateSplitSource.isFinished();
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
        TupleDomain<PaimonColumnHandle> combinedPredicate = combinePredicates(tableHandle.getFilter(), dynamicFilter);
        if (PaimonSplitManager.isEmptySplit(combinedPredicate, tableHandle)) {
            return PaimonSplitManager.emptySplitSource(tableHandle);
        }

        try {
            Catalog catalog = paimonCatalog.forSession(session);

            // Apply combined predicate to table scan
            Table table = PaimonTableHandle.schemaAwareReadTable(
                    tableHandle.tableWithDynamicOptions(catalog, session),
                    !tableHandle.usesHistoricalReadSchema(session));
            ReadBuilder readBuilder = table.newReadBuilder();
            PaimonSplitManager.pushPredicate(readBuilder, table, combinedPredicate);
            PaimonSplitManager.pushLimit(readBuilder, tableHandle);

            // Plan splits
            List<Split> splits = readBuilder.dropStats().newScan().plan().splits();

            LOG.debug("Planned %s splits after applying dynamic filters", splits.size());

            // Calculate split weights
            long maxRowCount = splits.stream().mapToLong(PaimonSplitManager::splitWeightRowCount).max().orElse(0L);
            double minimumSplitWeight = PaimonSessionProperties.getMinimumSplitWeight(session);

            return new PaimonSplitSource(splits.stream()
                    .map(split -> PaimonSplit.fromSplit(split,
                            PaimonSplitManager.calculateSplitWeight(split, maxRowCount, minimumSplitWeight)))
                    .collect(Collectors.toList()), tableHandle.getLimit());
        }
        catch (UnsupportedOperationException e) {
            throw PaimonSplitManager.unsupportedReadOperation(tableHandle, e);
        }
        catch (RuntimeException e) {
            throw PaimonSplitManager.splitPlanningException(tableHandle, e);
        }
    }

    static TupleDomain<PaimonColumnHandle> combinePredicates(TupleDomain<PaimonColumnHandle> staticPredicate,
            DynamicFilter dynamicFilter)
    {
        TupleDomain<PaimonColumnHandle> dynamicPredicate = requireNonNull(dynamicFilter, "dynamicFilter is null")
                .getCurrentPredicate()
                .transformKeys(DynamicFilteringTrinoSplitSource::getDynamicFilterColumn);

        LOG.debug("Static predicate: %s", staticPredicate);
        LOG.debug("Dynamic predicate: %s", dynamicPredicate);

        TupleDomain<PaimonColumnHandle> combined = combinePredicates(staticPredicate, dynamicPredicate,
                DOMAIN_COMPACTION_THRESHOLD);

        LOG.debug("Combined predicate: %s", combined);
        return combined;
    }

    private static PaimonColumnHandle getDynamicFilterColumn(io.trino.spi.connector.ColumnHandle column)
    {
        if (!(requireNonNull(column, "dynamicFilter predicate contains null column") instanceof PaimonColumnHandle paimonColumnHandle)) {
            throw new IllegalStateException("Paimon dynamic filter requires PaimonColumnHandle, got: "
                    + column.getClass().getName());
        }
        return paimonColumnHandle;
    }

    static TupleDomain<PaimonColumnHandle> combinePredicates(
            TupleDomain<PaimonColumnHandle> staticPredicate,
            TupleDomain<PaimonColumnHandle> dynamicPredicate,
            int domainCompactionThreshold)
    {
        requireNonNull(staticPredicate, "staticPredicate is null");
        requireNonNull(dynamicPredicate, "dynamicPredicate is null");
        checkArgument(domainCompactionThreshold > 0, "domainCompactionThreshold must be positive");
        TupleDomain<PaimonColumnHandle> combined = staticPredicate.intersect(dynamicPredicate);
        TupleDomain<PaimonColumnHandle> compacted = staticPredicate.intersect(
                combined.simplify(domainCompactionThreshold));
        if (!compacted.equals(combined)) {
            LOG.debug("Combined predicate was compacted with threshold %s", domainCompactionThreshold);
        }
        return compacted;
    }
}
