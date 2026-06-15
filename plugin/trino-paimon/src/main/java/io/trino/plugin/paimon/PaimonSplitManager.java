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

import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitSource;
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.TupleDomain;
import jakarta.annotation.PreDestroy;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Math.toIntExact;
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
        requireNonNull(session, "session is null");
        requireNonNull(dynamicFilter, "dynamicFilter is null");
        requireNonNull(constraint, "constraint is null");
        return getSplits(getTableHandle(table), session, dynamicFilter);
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session,
            ConnectorTableFunctionHandle function)
    {
        requireNonNull(session, "session is null");
        return getSplits(getTableFunctionHandle(function), session, DynamicFilter.EMPTY);
    }

    static PaimonTableHandle getTableHandle(ConnectorTableHandle tableHandle)
    {
        if (!(requireNonNull(tableHandle, "tableHandle is null") instanceof PaimonTableHandle paimonTableHandle)) {
            throw new IllegalStateException("Paimon split planning requires PaimonTableHandle, got: "
                    + tableHandle.getClass().getName());
        }
        return paimonTableHandle;
    }

    static PaimonTableHandle getTableFunctionHandle(ConnectorTableFunctionHandle functionHandle)
    {
        if (!(requireNonNull(functionHandle, "functionHandle is null") instanceof PaimonTableHandle paimonTableHandle)) {
            throw new IllegalStateException("Paimon table function split planning requires PaimonTableHandle, got: "
                    + functionHandle.getClass().getName());
        }
        return paimonTableHandle;
    }

    protected ConnectorSplitSource getSplits(PaimonTableHandle tableHandle, ConnectorSession session,
            DynamicFilter dynamicFilter)
    {
        TupleDomain<PaimonColumnHandle> effectivePredicate = effectivePredicate(tableHandle, dynamicFilter);
        if (isEmptySplit(effectivePredicate, tableHandle)) {
            return new ClassLoaderSafeConnectorSplitSource(emptySplitSource(tableHandle),
                    PaimonSplitManager.class.getClassLoader());
        }

        Duration dynamicFilteringWaitTimeout = PaimonSessionProperties.getDynamicFilteringWaitTimeout(session);

        if (dynamicFilteringWaitTimeout.toMillis() == 0 || !dynamicFilter.isAwaitable()) {
            return planSplits(tableHandle, session, effectivePredicate);
        }

        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(tableHandle, session,
                paimonCatalog, dynamicFilter, dynamicFilteringWaitTimeout);

        return new ClassLoaderSafeConnectorSplitSource(splitSource, PaimonSplitManager.class.getClassLoader());
    }

    static TupleDomain<PaimonColumnHandle> effectivePredicate(PaimonTableHandle tableHandle, DynamicFilter dynamicFilter)
    {
        return DynamicFilteringTrinoSplitSource.combinePredicates(
                requireNonNull(tableHandle, "tableHandle is null").getFilter(),
                requireNonNull(dynamicFilter, "dynamicFilter is null"));
    }

    private ConnectorSplitSource planSplits(
            PaimonTableHandle tableHandle,
            ConnectorSession session,
            TupleDomain<PaimonColumnHandle> predicate)
    {
        if (isEmptySplit(predicate, tableHandle)) {
            return new ClassLoaderSafeConnectorSplitSource(emptySplitSource(tableHandle),
                    PaimonSplitManager.class.getClassLoader());
        }

        try {
            Catalog catalog = paimonCatalog.forSession(session);
            Table table = PaimonTableHandle.schemaAwareReadTable(
                    tableHandle.tableWithDynamicOptions(catalog, session),
                    !tableHandle.usesHistoricalReadSchema(session));
            ReadBuilder readBuilder = table.newReadBuilder();
            pushPredicate(readBuilder, table, predicate);
            pushLimit(readBuilder, tableHandle);
            List<Split> splits = readBuilder.dropStats().newScan().plan().splits();

            long maxRowCount = splits.stream().mapToLong(PaimonSplitManager::splitWeightRowCount).max().orElse(0L);
            double minimumSplitWeight = PaimonSessionProperties.getMinimumSplitWeight(session);
            PaimonSplitSource splitSource = new PaimonSplitSource(splits.stream()
                    .map(split -> PaimonSplit.fromSplit(split,
                            calculateSplitWeight(split, maxRowCount, minimumSplitWeight)))
                    .collect(Collectors.toList()), tableHandle.getLimit());

            return new ClassLoaderSafeConnectorSplitSource(splitSource, PaimonSplitManager.class.getClassLoader());
        }
        catch (UnsupportedOperationException e) {
            throw unsupportedReadOperation(tableHandle, e);
        }
        catch (RuntimeException e) {
            throw splitPlanningException(tableHandle, e);
        }
    }

    static TrinoException unsupportedReadOperation(PaimonTableHandle tableHandle, UnsupportedOperationException cause)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(cause, "cause is null");

        String message = tableHandle.hasIncrementalReadWindow()
                ? "Paimon system.table_changes uses features which are not supported by the Trino connector"
                : "Paimon table read uses features which are not supported by the Trino connector";
        return new TrinoException(NOT_SUPPORTED, message, cause);
    }

    static RuntimeException splitPlanningException(PaimonTableHandle tableHandle, Exception cause)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(cause, "cause is null");

        String message = tableHandle.hasIncrementalReadWindow()
                ? "Failed to plan Paimon table_changes splits"
                : "Failed to plan Paimon splits";
        return PaimonPageSourceProvider.wrapPaimonReadException(message, cause);
    }

    static void pushLimit(ReadBuilder readBuilder, PaimonTableHandle tableHandle)
    {
        requireNonNull(readBuilder, "readBuilder is null");
        OptionalLong limit = requireNonNull(tableHandle, "tableHandle is null").getLimit();
        if (limit.isPresent() && limit.getAsLong() <= Integer.MAX_VALUE) {
            readBuilder.withLimit(toIntExact(limit.getAsLong()));
        }
    }

    static void pushPredicate(ReadBuilder readBuilder, Table table, TupleDomain<PaimonColumnHandle> predicate)
    {
        requireNonNull(readBuilder, "readBuilder is null");
        requireNonNull(table, "table is null");
        requireNonNull(predicate, "predicate is null");

        PaimonRowRangeExtractor.extractRowIdRanges(predicate).ifPresent(readBuilder::withRowRanges);

        TupleDomain<PaimonColumnHandle> pushdownPredicate = PaimonRowRangeExtractor.removeRowIdPredicate(predicate);
        Optional<Predicate> paimonPredicate = new PaimonFilterConverter(
                PaimonTableHandle.effectiveReadRowType(table)).convert(pushdownPredicate);
        paimonPredicate.ifPresent(readBuilder::withFilter);
    }

    static double calculateSplitWeight(Split split, long maxRowCount, double minimumSplitWeight)
    {
        requireNonNull(split, "split is null");
        checkArgument(Double.isFinite(minimumSplitWeight) && minimumSplitWeight > 0 && minimumSplitWeight <= 1,
                "minimumSplitWeight must be in the range (0, 1]");
        long rowCount = splitWeightRowCount(split);
        if (maxRowCount <= 0 || rowCount <= 0) {
            return minimumSplitWeight;
        }
        return Math.min(Math.max((double) rowCount / maxRowCount, minimumSplitWeight), 1.0);
    }

    static long splitWeightRowCount(Split split)
    {
        requireNonNull(split, "split is null");
        return split.mergedRowCount().orElse(split.rowCount());
    }

    static PaimonSplitSource emptySplitSource(PaimonTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        return new PaimonSplitSource(List.of(), tableHandle.getLimit());
    }

    static boolean isEmptySplit(TupleDomain<PaimonColumnHandle> predicate, PaimonTableHandle tableHandle)
    {
        requireNonNull(predicate, "predicate is null");
        requireNonNull(tableHandle, "tableHandle is null");
        return predicate.isNone() || (tableHandle.getLimit().isPresent() && tableHandle.getLimit().getAsLong() == 0);
    }
}
