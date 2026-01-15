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
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import org.apache.paimon.annotation.VisibleForTesting;
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
        if (dynamicFilter.getCurrentPredicate().isNone()) {
            return CompletableFuture.completedFuture(FINISHED_BATCH);
        }
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
        if (dynamicFilter.getCurrentPredicate().isNone()) {
            return CompletableFuture.completedFuture(FINISHED_BATCH);
        }
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
        if (dynamicFilter.getCurrentPredicate().isNone()) {
            return true;
        }
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

        // Early termination: if combined predicate is NONE (empty result set), return empty splits
        if (combinedPredicate.isNone()) {
            LOG.info("Combined predicate is NONE (empty result set), skipping table scan");
            return new PaimonSplitSource(ImmutableList.of());
        }

        // Apply combined predicate to table scan
        Table table = tableHandle.tableWithDynamicOptions(paimonCatalog, session);
        ReadBuilder readBuilder = table.newReadBuilder();

        // Convert combined predicate to Paimon predicate
        PaimonFilterConverter filterConverter = new PaimonFilterConverter(table.rowType());
        Optional<Predicate> tuplePredicate = filterConverter.convert(combinedPredicate);
        Optional<Predicate> likePredicate = filterConverter.convertLikeFilters(tableHandle.getLikeFilters());
        Optional<Predicate> paimonPredicate = PaimonFilterConverter.combinePredicates(tuplePredicate, likePredicate);
        paimonPredicate.ifPresent(readBuilder::withFilter);

        // Apply limit if present
        SplitPlanningUtils.toPaimonLimit(tableHandle.getLimit()).ifPresent(readBuilder::withLimit);
        PaimonSplitManager.convertTopN(tableHandle.getTopN(), table.rowType()).ifPresent(readBuilder::withTopN);
        PaimonSplitManager.applyBucketFilter(table, paimonPredicate).ifPresent(readBuilder::withBucketFilter);

        // Plan splits
        List<Split> splits = readBuilder.dropStats().newScan().plan().splits();
        splits = PaimonSplitManager.applySampling(splits, tableHandle.getSampleRatio());

        LOG.info("Planned {} splits after applying dynamic filters", splits.size());

        // Calculate split weights
        long maxRowCount = splits.stream().mapToLong(Split::rowCount).max().orElse(0L);
        double minimumSplitWeight = PaimonSessionProperties.getMinimumSplitWeight(session);

        return new PaimonSplitSource(splits.stream()
                .map(split -> {
                    double weight = SplitPlanningUtils.computeSplitWeight(split.rowCount(), maxRowCount, minimumSplitWeight);
                    return PaimonSplit.fromSplit(split, weight);
                })
                .collect(Collectors.toList()));
    }

    private TupleDomain<PaimonColumnHandle> combinePredicates(TupleDomain<PaimonColumnHandle> staticPredicate,
            DynamicFilter dynamicFilter)
    {
        // Extract dynamic filter predicate
        TupleDomain<PaimonColumnHandle> dynamicPredicate = dynamicFilter.getCurrentPredicate()
                .transformKeys(PaimonColumnHandle.class::cast);

        LOG.debug("Static predicate: {}", staticPredicate);
        LOG.debug("Dynamic predicate: {}", dynamicPredicate);

        // Early termination: if dynamic predicate is NONE (empty result set from build side)
        // This is critical for performance - prevents full table scan when JOIN build side returns 0 rows
        if (dynamicPredicate.isNone()) {
            LOG.info("Dynamic filter is NONE (build side returned empty result), returning NONE to skip scan");
            return TupleDomain.none();
        }

        // Combine with static predicate
        TupleDomain<PaimonColumnHandle> combined = staticPredicate.intersect(dynamicPredicate);

        // Check if intersection resulted in NONE (contradictory predicates)
        if (combined.isNone()) {
            LOG.info("Combined predicate is NONE (contradictory predicates), returning NONE to skip scan");
            return combined;
        }

        // Simplify if too complex (prevent memory explosion)
        if (exceedsComplexityThreshold(combined, DOMAIN_COMPACTION_THRESHOLD)) {
            LOG.debug("Combined predicate exceeds complexity threshold ({}), compacting dynamic predicate",
                    DOMAIN_COMPACTION_THRESHOLD);
            // Instead of dropping dynamic filter entirely, compact it to retain most selective domains
            TupleDomain<PaimonColumnHandle> compactedDynamic = compactPredicate(dynamicPredicate, DOMAIN_COMPACTION_THRESHOLD / 2);
            combined = staticPredicate.intersect(compactedDynamic);
            LOG.debug("Compacted combined predicate: {}", combined);
        }

        LOG.debug("Combined predicate: {}", combined);
        return combined;
    }

    /**
     * Compact a predicate by retaining only the most selective domains.
     * Strategy: keep orderable range domains, drop high-cardinality discrete sets.
     */
    private TupleDomain<PaimonColumnHandle> compactPredicate(TupleDomain<PaimonColumnHandle> predicate, int targetComplexity)
    {
        if (predicate.isAll() || predicate.isNone() || predicate.getDomains().isEmpty()) {
            return predicate;
        }

        java.util.Map<PaimonColumnHandle, Domain> domains = predicate.getDomains().get();
        java.util.Map<PaimonColumnHandle, Domain> compactedDomains = new java.util.LinkedHashMap<>();
        int currentComplexity = 0;

        // First pass: add all simple domains (complexity <= 10)
        for (java.util.Map.Entry<PaimonColumnHandle, Domain> entry : domains.entrySet()) {
            int domainComplexity = estimateDomainComplexity(entry.getValue());
            if (domainComplexity <= 10) {
                compactedDomains.put(entry.getKey(), entry.getValue());
                currentComplexity += domainComplexity;
            }
        }

        // Second pass: add range-based domains up to target complexity
        for (java.util.Map.Entry<PaimonColumnHandle, Domain> entry : domains.entrySet()) {
            if (compactedDomains.containsKey(entry.getKey())) {
                continue;
            }
            Domain domain = entry.getValue();
            int domainComplexity = estimateDomainComplexity(domain);

            // Prefer range domains over discrete sets
            if (domain.getValues().getType().isOrderable() && currentComplexity + domainComplexity <= targetComplexity) {
                compactedDomains.put(entry.getKey(), domain);
                currentComplexity += domainComplexity;
            }
        }

        if (compactedDomains.isEmpty()) {
            return TupleDomain.all();
        }

        return TupleDomain.withColumnDomains(compactedDomains);
    }

    private boolean exceedsComplexityThreshold(TupleDomain<PaimonColumnHandle> predicate, int threshold)
    {
        if (predicate.isAll() || predicate.isNone()) {
            return false;
        }

        if (predicate.getDomains().isEmpty()) {
            return false;
        }

        return estimateComplexity(predicate) > threshold;
    }

    @VisibleForTesting
    static int estimateComplexity(TupleDomain<PaimonColumnHandle> predicate)
    {
        if (predicate.isAll() || predicate.isNone()) {
            return 0;
        }
        if (predicate.getDomains().isEmpty()) {
            return 0;
        }
        return predicate.getDomains().get().values().stream()
                .mapToInt(DynamicFilteringTrinoSplitSource::estimateDomainComplexity)
                .sum();
    }

    private static int estimateDomainComplexity(Domain domain)
    {
        ValueSet values = domain.getValues();
        if (values.isAll() || values.isNone()) {
            return 0;
        }

        Type type = values.getType();
        if (type.isOrderable()) {
            return values.getRanges().getRangeCount();
        }
        if (type.isComparable()) {
            return values.getDiscreteValues().getValuesCount();
        }
        return 0;
    }
}
