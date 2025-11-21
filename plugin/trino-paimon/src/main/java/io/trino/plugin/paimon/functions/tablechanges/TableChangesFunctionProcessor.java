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

import io.trino.plugin.paimon.PaimonPageSourceProvider;
import io.trino.plugin.paimon.PaimonSplit;
import io.trino.plugin.paimon.PaimonTableHandle;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionSplitProcessor;

import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;

public class TableChangesFunctionProcessor
        implements
        TableFunctionSplitProcessor
{
    private static final Page EMPTY_PAGE = new Page(0);

    private final ConnectorPageSource pageSource;

    public TableChangesFunctionProcessor(ConnectorSession session, PaimonTableHandle handle, PaimonSplit split,
            PaimonPageSourceProvider pageSourceProvider)
    {
        this.pageSource = pageSourceProvider.createPageSource(null, session, split, handle,
                handle.getProjectedColumns().get(), DynamicFilter.EMPTY);
    }

    @Override
    public TableFunctionProcessorState process()
    {
        if (pageSource.isFinished()) {
            return FINISHED;
        }
        Page dataPage = pageSource.getNextPage();
        if (dataPage == null) {
            return TableFunctionProcessorState.Processed.produced(EMPTY_PAGE);
        }
        else {
            return TableFunctionProcessorState.Processed.produced(dataPage);
        }
    }
}
