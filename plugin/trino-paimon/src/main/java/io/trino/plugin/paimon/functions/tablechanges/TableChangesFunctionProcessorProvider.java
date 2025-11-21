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

import com.google.inject.Inject;
import io.trino.plugin.base.classloader.ClassLoaderSafeTableFunctionSplitProcessor;
import io.trino.plugin.paimon.PaimonPageSourceProvider;
import io.trino.plugin.paimon.PaimonSplit;
import io.trino.plugin.paimon.PaimonTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionSplitProcessor;

public class TableChangesFunctionProcessorProvider
        implements
        TableFunctionProcessorProvider
{
    private final PaimonPageSourceProvider icebergPageSourceProvider;

    @Inject
    public TableChangesFunctionProcessorProvider(PaimonPageSourceProvider icebergPageSourceProvider)
    {
        this.icebergPageSourceProvider = icebergPageSourceProvider;
    }

    @Override
    public TableFunctionSplitProcessor getSplitProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle,
            ConnectorSplit split)
    {
        return new ClassLoaderSafeTableFunctionSplitProcessor(new TableChangesFunctionProcessor(session,
                (PaimonTableHandle) handle, (PaimonSplit) split, icebergPageSourceProvider), getClass().getClassLoader());
    }
}
