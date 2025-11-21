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

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Sets.immutableEnumSet;
import static io.trino.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class PaimonConnector
        implements
        Connector
{
    private final ConnectorMetadata trinoMetadata;
    private final ConnectorSplitManager trinoSplitManager;
    private final ConnectorPageSourceProvider trinoPageSourceProvider;
    private final ConnectorPageSinkProvider trinoPageSinkProvider;
    private final ConnectorNodePartitioningProvider trinoNodePartitioningProvider;
    private final List<PropertyMetadata<?>> tableProperties;
    private final List<PropertyMetadata<?>> sessionProperties;
    private final Set<ConnectorTableFunction> tableFunctions;
    private final FunctionProvider functionProvider;

    public PaimonConnector(ConnectorMetadata trinoMetadata, ConnectorSplitManager trinoSplitManager,
            ConnectorPageSourceProvider trinoPageSourceProvider, ConnectorPageSinkProvider trinoPageSinkProvider,
            ConnectorNodePartitioningProvider trinoNodePartitioningProvider, PaimonTableOptions paimonTableOptions,
            PaimonSessionProperties paimonSessionProperties, Set<ConnectorTableFunction> tableFunctions,
            FunctionProvider functionProvider)
    {
        this.trinoMetadata = requireNonNull(trinoMetadata, "trinoMetadata is null");
        this.trinoSplitManager = requireNonNull(trinoSplitManager, "trinoSplitManager is null");
        this.trinoPageSourceProvider = requireNonNull(trinoPageSourceProvider, "trinoRecordSetProvider is null");
        this.trinoPageSinkProvider = requireNonNull(trinoPageSinkProvider, "trinoPageSinkProvider is null");
        this.trinoNodePartitioningProvider = requireNonNull(trinoNodePartitioningProvider,
                "trinoNodePartitioningProvider is null");
        this.tableProperties = paimonTableOptions.getTableProperties();
        this.sessionProperties = paimonSessionProperties.getSessionProperties();
        this.tableFunctions = ImmutableSet.copyOf(requireNonNull(tableFunctions, "tableFunctions is null"));
        this.functionProvider = requireNonNull(functionProvider, "functionProvider is null");
    }

    @Override
    public Set<ConnectorCapabilities> getCapabilities()
    {
        return immutableEnumSet(NOT_NULL_COLUMN_CONSTRAINT);
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly,
            boolean autoCommit)
    {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        return PaimonTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return trinoMetadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return trinoSplitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return trinoPageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return trinoPageSinkProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return trinoNodePartitioningProvider;
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return tableFunctions;
    }

    @Override
    public Optional<FunctionProvider> getFunctionProvider()
    {
        return Optional.of(functionProvider);
    }
}
