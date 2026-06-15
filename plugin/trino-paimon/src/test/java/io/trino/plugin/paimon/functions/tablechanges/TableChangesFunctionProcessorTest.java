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
package io.trino.plugin.paimon.functions.tablechanges;

import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.paimon.PaimonColumnHandle;
import io.trino.plugin.paimon.PaimonMetadataFactory;
import io.trino.plugin.paimon.PaimonPageSourceProvider;
import io.trino.plugin.paimon.PaimonSplit;
import io.trino.plugin.paimon.PaimonTableHandle;
import io.trino.plugin.paimon.functions.PaimonFunctionProvider;
import io.trino.spi.HostAddress;
import io.trino.spi.Page;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.predicate.TupleDomain;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TableChangesFunctionProcessorTest
{
    @Test
    public void testProjectedColumnsAreRequired()
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThatThrownBy(() -> new TableChangesFunctionProcessor(
                SESSION,
                handle,
                new PaimonSplit("split", 1.0),
                pageSourceProvider()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon table_changes requires explicit projected columns");
    }

    @Test
    public void testProjectedColumnsRejectMalformedEntries()
    {
        assertThatThrownBy(() -> new TableChangesFunctionProcessor(
                SESSION,
                malformedProjectedColumnsHandle(Collections.singletonList(null)),
                new PaimonSplit("split", 1.0),
                pageSourceProvider()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("projectedColumns contains null column");

        ColumnHandle wrongColumn = new ColumnHandle() {};
        assertThatThrownBy(() -> new TableChangesFunctionProcessor(
                SESSION,
                malformedProjectedColumnsHandle(List.of(wrongColumn)),
                new PaimonSplit("split", 1.0),
                pageSourceProvider()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon table_changes requires PaimonColumnHandle, got: %s",
                        wrongColumn.getClass().getName());
    }

    @Test
    public void testConstructorArgumentsAreRequired()
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        PaimonSplit split = new PaimonSplit("split", 1.0);
        PaimonPageSourceProvider pageSourceProvider = pageSourceProvider();

        assertThatThrownBy(() -> new TableChangesFunctionProcessor(null, handle, split, pageSourceProvider))
                .hasMessage("session is null");
        assertThatThrownBy(() -> new TableChangesFunctionProcessor(SESSION, null, split, pageSourceProvider))
                .hasMessage("handle is null");
        assertThatThrownBy(() -> new TableChangesFunctionProcessor(SESSION, handle, null, pageSourceProvider))
                .hasMessage("split is null");
        assertThatThrownBy(() -> new TableChangesFunctionProcessor(SESSION, handle, split, null))
                .hasMessage("pageSourceProvider is null");
    }

    @Test
    public void testProviderArgumentsAreRequired()
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.of(List.of()),
                Optional.empty(),
                OptionalLong.empty());
        PaimonSplit split = new PaimonSplit("split", 1.0);
        TableChangesFunctionProcessorProvider provider = new TableChangesFunctionProcessorProvider(pageSourceProvider());

        assertThatThrownBy(() -> new TableChangesFunctionProcessorProvider(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("pageSourceProvider is null");
        assertThatThrownBy(() -> provider.getSplitProcessor(null, handle, split))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> provider.getSplitProcessor(SESSION, null, split))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("handle is null");
        assertThatThrownBy(() -> provider.getSplitProcessor(SESSION, handle, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("split is null");
        assertThatThrownBy(() -> provider.getSplitProcessor(SESSION, new TestingTableFunctionHandle(), split))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("handle must be PaimonTableHandle");
        assertThatThrownBy(() -> provider.getSplitProcessor(SESSION, handle, new TestingConnectorSplit()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("split must be PaimonSplit");
    }

    @Test
    public void testFunctionProviderArgumentsAreRequired()
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.of(List.of()),
                Optional.empty(),
                OptionalLong.empty());
        PaimonFunctionProvider provider = new PaimonFunctionProvider(
                new TableChangesFunctionProcessorProvider(pageSourceProvider()));

        assertThatThrownBy(() -> new PaimonFunctionProvider(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableChangesFunctionProcessorProvider is null");
        assertThatThrownBy(() -> provider.getTableFunctionProcessorProvider(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("functionHandle is null");
        assertThatThrownBy(() -> provider.getTableFunctionProcessorProvider(new TestingTableFunctionHandle()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("functionHandle must be PaimonTableHandle");

        assertThat(provider.getTableFunctionProcessorProvider(handle))
                .isNotNull();
    }

    @Test
    public void testProcessorReturnsBlockedWhenPageSourceHasNoPage()
    {
        CompletableFuture<Void> blocked = new CompletableFuture<>();
        TestingPageSource pageSource = new TestingPageSource(blocked);
        TableChangesFunctionProcessor processor = new TableChangesFunctionProcessor(
                SESSION,
                handleWithProjectedColumns(),
                new PaimonSplit("split", 1.0),
                pageSourceProvider(pageSource));

        TableFunctionProcessorState state = processor.process();

        assertThat(state).isInstanceOfSatisfying(TableFunctionProcessorState.Blocked.class, blockedState -> {
            assertThat(blockedState.getFuture()).isNotDone();
            blocked.complete(null);
            assertThat(blockedState.getFuture()).isDone();
        });
    }

    @Test
    public void testProcessorReturnsFinishedWhenPageSourceFinishesAfterNullPage()
    {
        TableChangesFunctionProcessor processor = new TableChangesFunctionProcessor(
                SESSION,
                handleWithProjectedColumns(),
                new PaimonSplit("split", 1.0),
                pageSourceProvider(new FinishingAfterNullPageSource()));

        assertThat(processor.process()).isEqualTo(TableFunctionProcessorState.Finished.FINISHED);
    }

    private static PaimonPageSourceProvider pageSourceProvider()
    {
        return pageSourceProvider(null);
    }

    private static PaimonPageSourceProvider pageSourceProvider(ConnectorPageSource pageSource)
    {
        return new PaimonPageSourceProvider(
                identity -> {
                    throw new UnsupportedOperationException("filesystem is not used by this test");
                },
                new PaimonMetadataFactory(new Options(),
                        identity -> {
                            throw new UnsupportedOperationException("filesystem is not used by this test");
                        },
                        TESTING_TYPE_MANAGER),
                new OrcReaderConfig(),
                new ParquetReaderConfig())
        {
            @Override
            public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session,
                    ConnectorSplit split, ConnectorTableHandle tableHandle, List<ColumnHandle> columns,
                    DynamicFilter dynamicFilter)
            {
                if (pageSource == null) {
                    return super.createPageSource(transaction, session, split, tableHandle, columns, dynamicFilter);
                }
                return pageSource;
            }
        };
    }

    private static PaimonTableHandle handleWithProjectedColumns()
    {
        return new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.of(List.of(PaimonColumnHandle.of("id", DataTypes.INT()))),
                Optional.empty(),
                OptionalLong.empty());
    }

    private static PaimonTableHandle malformedProjectedColumnsHandle(List<?> projectedColumns)
    {
        return new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty())
        {
            @Override
            @SuppressWarnings({"unchecked", "rawtypes"})
            public Optional<List<PaimonColumnHandle>> getProjectedColumns()
            {
                return (Optional) Optional.of(projectedColumns);
            }
        };
    }

    private record TestingTableFunctionHandle() implements ConnectorTableFunctionHandle {}

    private record TestingConnectorSplit() implements ConnectorSplit
    {
        @Override
        public boolean isRemotelyAccessible()
        {
            return true;
        }

        @Override
        public List<HostAddress> getAddresses()
        {
            return List.of();
        }

        @Override
        public Object getInfo()
        {
            return Map.of();
        }

        @Override
        public SplitWeight getSplitWeight()
        {
            return SplitWeight.standard();
        }
    }

    private record TestingPageSource(CompletableFuture<?> blocked) implements ConnectorPageSource
    {
        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public boolean isFinished()
        {
            return false;
        }

        @Override
        public Page getNextPage()
        {
            return null;
        }

        @Override
        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public void close()
                throws IOException
        {
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            return blocked;
        }
    }

    private static final class FinishingAfterNullPageSource
            implements ConnectorPageSource
    {
        private boolean finished;

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public boolean isFinished()
        {
            return finished;
        }

        @Override
        public Page getNextPage()
        {
            finished = true;
            return null;
        }

        @Override
        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public void close()
                throws IOException
        {
        }
    }
}
