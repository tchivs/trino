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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitSource;
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import jakarta.annotation.PreDestroy;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.SortValue;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class PaimonSplitManager
        implements
        ConnectorSplitManager
{
    private final PaimonCatalog paimonCatalog;

    @Inject
    public PaimonSplitManager(PaimonMetadataFactory paimonMetadataFactory)
    {
        this.paimonCatalog = requireNonNull(paimonMetadataFactory, "trinoMetadataFactory is null").create().catalog();
    }

    @PreDestroy
    public void destroy()
    {
        // No resources to cleanup currently
        // Add executor shutdown here if needed in future
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session,
            ConnectorTableHandle table, DynamicFilter dynamicFilter, Constraint constraint)
    {
        return getSplits((PaimonTableHandle) table, session, dynamicFilter);
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session,
            ConnectorTableFunctionHandle function)
    {
        if (function instanceof PaimonTableHandle) {
            return getSplits((PaimonTableHandle) function, session, DynamicFilter.EMPTY);
        }
        throw new IllegalStateException("Unknown table function: " + function);
    }

    protected ConnectorSplitSource getSplits(PaimonTableHandle tableHandle, ConnectorSession session,
            DynamicFilter dynamicFilter)
    {
        // If aggregation pushdown is active, return a single empty split
        // The actual data is already computed and stored in the table handle
        if (tableHandle.getAggregationResult().isPresent()) {
            return new FixedSplitSource(ImmutableList.of(new PaimonSplit("", 1.0)));
        }

        Duration dynamicFilteringWaitTimeout = PaimonSessionProperties.getDynamicFilteringWaitTimeout(session);

        // If dynamic filtering is disabled (timeout = 0) or not awaitable, use original
        // logic
        if (dynamicFilteringWaitTimeout.toMillis() == 0 || !dynamicFilter.isAwaitable()) {
            return getSplitsWithoutDynamicFilter(tableHandle, session);
        }

        // Use dynamic filtering split source
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(tableHandle, session,
                paimonCatalog, dynamicFilter, dynamicFilteringWaitTimeout);

        return new ClassLoaderSafeConnectorSplitSource(splitSource, PaimonSplitManager.class.getClassLoader());
    }

    private ConnectorSplitSource getSplitsWithoutDynamicFilter(PaimonTableHandle tableHandle, ConnectorSession session)
    {
        Table table = tableHandle.tableWithDynamicOptions(paimonCatalog, session);
        ReadBuilder readBuilder = table.newReadBuilder();
        Optional<Predicate> paimonFilter = new PaimonFilterConverter(table.rowType()).convert(tableHandle.getFilter());
        paimonFilter.ifPresent(readBuilder::withFilter);
        SplitPlanningUtils.toPaimonLimit(tableHandle.getLimit()).ifPresent(readBuilder::withLimit);
        convertTopN(tableHandle.getTopN(), table.rowType()).ifPresent(readBuilder::withTopN);

        // Apply bucket filter if applicable
        applyBucketFilter(table, paimonFilter).ifPresent(readBuilder::withBucketFilter);

        List<Split> splits = readBuilder.dropStats().newScan().plan().splits();

        // Apply sampling if requested
        splits = applySampling(splits, tableHandle.getSampleRatio());

        long maxRowCount = splits.stream().mapToLong(Split::rowCount).max().orElse(0L);
        double minimumSplitWeight = PaimonSessionProperties.getMinimumSplitWeight(session);
        PaimonSplitSource splitSource = new PaimonSplitSource(splits.stream()
                .map(split -> {
                    double weight = SplitPlanningUtils.computeSplitWeight(split.rowCount(), maxRowCount, minimumSplitWeight);
                    return PaimonSplit.fromSplit(split, weight);
                })
                .collect(Collectors.toList()));

        // Wrap with ClassLoaderSafe wrapper for proper plugin isolation
        return new ClassLoaderSafeConnectorSplitSource(splitSource, PaimonSplitManager.class.getClassLoader());
    }

    private Optional<TopN> convertTopN(Optional<PaimonTopN> topN, RowType rowType)
    {
        if (topN.isEmpty()) {
            return Optional.empty();
        }

        PaimonTopN paimonTopN = topN.get();
        List<PaimonTopN.PaimonSortItem> sortItems = paimonTopN.getSortItems();

        if (sortItems.isEmpty()) {
            return Optional.empty();
        }

        List<DataField> fields = rowType.getFields();
        List<SortValue> sortValues = new ArrayList<>();

        for (PaimonTopN.PaimonSortItem sortItem : sortItems) {
            String columnName = sortItem.getColumnName();

            // Find field index by name (case-insensitive)
            int fieldIndex = -1;
            for (int i = 0; i < fields.size(); i++) {
                if (fields.get(i).name().equalsIgnoreCase(columnName)) {
                    fieldIndex = i;
                    break;
                }
            }

            if (fieldIndex < 0) {
                return Optional.empty();
            }

            FieldRef fieldRef = new FieldRef(fieldIndex, columnName, rowType.getTypeAt(fieldIndex));

            // Convert Trino SortOrder to Paimon SortDirection and NullOrdering
            SortValue.SortDirection direction;
            SortValue.NullOrdering nullOrdering;

            switch (sortItem.getSortOrder()) {
                case ASC_NULLS_FIRST -> {
                    direction = SortValue.SortDirection.ASCENDING;
                    nullOrdering = SortValue.NullOrdering.NULLS_FIRST;
                }
                case ASC_NULLS_LAST -> {
                    direction = SortValue.SortDirection.ASCENDING;
                    nullOrdering = SortValue.NullOrdering.NULLS_LAST;
                }
                case DESC_NULLS_FIRST -> {
                    direction = SortValue.SortDirection.DESCENDING;
                    nullOrdering = SortValue.NullOrdering.NULLS_FIRST;
                }
                case DESC_NULLS_LAST -> {
                    direction = SortValue.SortDirection.DESCENDING;
                    nullOrdering = SortValue.NullOrdering.NULLS_LAST;
                }
                default -> {
                    return Optional.empty();
                }
            }

            sortValues.add(new SortValue(fieldRef, direction, nullOrdering));
        }

        return Optional.of(new TopN(sortValues, (int) paimonTopN.getTopNCount()));
    }

    private Optional<org.apache.paimon.utils.Filter<Integer>> applyBucketFilter(
            Table table,
            Optional<Predicate> paimonFilter)
    {
        if (paimonFilter.isEmpty()) {
            return Optional.empty();
        }

        if (!(table instanceof FileStoreTable fileStoreTable)) {
            return Optional.empty();
        }

        List<String> bucketKeys = fileStoreTable.schema().bucketKeys();
        if (bucketKeys.isEmpty()) {
            return Optional.empty();
        }

        int numBuckets = fileStoreTable.schema().numBuckets();
        if (numBuckets <= 1) {
            return Optional.empty();
        }

        // Use Paimon's BucketSelectConverter to extract bucket filter
        RowType bucketKeyType = fileStoreTable.schema().logicalBucketKeyType();
        org.apache.paimon.CoreOptions coreOptions = fileStoreTable.coreOptions();

        Optional<org.apache.paimon.utils.BiFilter<Integer, Integer>> bucketSelector =
                org.apache.paimon.operation.BucketSelectConverter.create(
                        paimonFilter.get(),
                        bucketKeyType,
                        coreOptions.bucketFunctionType());

        if (bucketSelector.isEmpty()) {
            return Optional.empty();
        }

        // Convert BiFilter to Filter by binding numBuckets
        org.apache.paimon.utils.BiFilter<Integer, Integer> selector = bucketSelector.get();
        return Optional.of(bucket -> selector.test(bucket, numBuckets));
    }

    private List<Split> applySampling(List<Split> splits, Optional<Double> sampleRatio)
    {
        if (sampleRatio.isEmpty() || splits.isEmpty()) {
            return splits;
        }

        double ratio = sampleRatio.get();
        if (ratio >= 1.0) {
            return splits;
        }
        if (ratio <= 0.0) {
            return List.of();
        }

        // SYSTEM sampling: select a subset of splits based on ratio
        int targetCount = Math.max(1, (int) Math.ceil(splits.size() * ratio));
        if (targetCount >= splits.size()) {
            return splits;
        }

        // Use deterministic sampling based on split hash for reproducibility
        return splits.stream()
                .sorted((a, b) -> Integer.compare(a.hashCode(), b.hashCode()))
                .limit(targetCount)
                .collect(Collectors.toList());
    }
}
