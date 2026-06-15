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

import io.trino.plugin.paimon.PaimonColumnHandle;
import io.trino.plugin.paimon.PaimonPageSourceProvider;
import io.trino.plugin.paimon.PaimonSplit;
import io.trino.plugin.paimon.PaimonTableHandle;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionSplitProcessor;

import java.util.List;

import static io.trino.spi.function.table.TableFunctionProcessorState.Blocked.blocked;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static java.util.Objects.requireNonNull;

public class TableChangesFunctionProcessor
        implements
        TableFunctionSplitProcessor
{
    private final ConnectorPageSource pageSource;

    public TableChangesFunctionProcessor(ConnectorSession session, PaimonTableHandle handle, PaimonSplit split,
            PaimonPageSourceProvider pageSourceProvider)
    {
        requireNonNull(session, "session is null");
        requireNonNull(split, "split is null");
        requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        List<?> rawProjectedColumns = requireNonNull(requireNonNull(handle, "handle is null").getProjectedColumns(),
                "projectedColumns is null")
                .orElseThrow(() -> new IllegalStateException(
                        "Paimon table_changes requires explicit projected columns"));
        List<ColumnHandle> projectedColumns = rawProjectedColumns
                .stream()
                .map(column -> {
                    if (!(requireNonNull(column, "projectedColumns contains null column") instanceof PaimonColumnHandle paimonColumnHandle)) {
                        throw new IllegalStateException("Paimon table_changes requires PaimonColumnHandle, got: "
                                + column.getClass().getName());
                    }
                    return (ColumnHandle) paimonColumnHandle;
                })
                .toList();
        this.pageSource = pageSourceProvider.createPageSource(null, session, split, handle,
                projectedColumns, DynamicFilter.EMPTY);
    }

    @Override
    public TableFunctionProcessorState process()
    {
        if (pageSource.isFinished()) {
            return FINISHED;
        }
        Page dataPage = pageSource.getNextPage();
        if (dataPage == null) {
            if (pageSource.isFinished()) {
                return FINISHED;
            }
            return blocked(pageSource.isBlocked().thenRun(() -> {}));
        }
        return TableFunctionProcessorState.Processed.produced(dataPage);
    }
}
