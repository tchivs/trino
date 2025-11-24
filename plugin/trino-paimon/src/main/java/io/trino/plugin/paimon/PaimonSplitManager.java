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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import jakarta.annotation.PreDestroy;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;

import java.util.List;
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
        new PaimonFilterConverter(table.rowType()).convert(tableHandle.getFilter()).ifPresent(readBuilder::withFilter);
        tableHandle.getLimit().ifPresent(limit -> readBuilder.withLimit((int) limit));
        List<Split> splits = readBuilder.dropStats().newScan().plan().splits();

        long maxRowCount = splits.stream().mapToLong(Split::rowCount).max().orElse(0L);
        double minimumSplitWeight = PaimonSessionProperties.getMinimumSplitWeight(session);
        PaimonSplitSource splitSource = new PaimonSplitSource(splits.stream()
                .map(split -> {
                    // Avoid NaN when maxRowCount is 0 (empty table or all splits have 0 rows)
                    double weight = maxRowCount == 0 ? minimumSplitWeight :
                            Math.min(Math.max((double) split.rowCount() / maxRowCount, minimumSplitWeight), 1.0);
                    return PaimonSplit.fromSplit(split, weight);
                })
                .collect(Collectors.toList()), tableHandle.getLimit());

        // Wrap with ClassLoaderSafe wrapper for proper plugin isolation
        return new ClassLoaderSafeConnectorSplitSource(splitSource, PaimonSplitManager.class.getClassLoader());
    }
}
