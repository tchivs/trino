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

import io.trino.spi.TrinoException;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FullTextSearchTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.VectorSearchTable;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public final class PaimonTableSupport
{
    private PaimonTableSupport() {}

    public static Table requireSupportedTable(Table table)
    {
        requireNonNull(table, "table is null");
        if (table instanceof VectorSearchTable) {
            throw new TrinoException(NOT_SUPPORTED,
                    "Paimon vector search tables are not supported by the Trino connector");
        }
        if (table instanceof FullTextSearchTable) {
            throw new TrinoException(NOT_SUPPORTED,
                    "Paimon full-text search tables are not supported by the Trino connector");
        }
        return table;
    }

    public static FileStoreTable requireFileStoreTable(Table table, String operation)
    {
        Table supportedTable = requireSupportedTable(table);
        if (!(supportedTable instanceof FileStoreTable fileStoreTable)) {
            throw new TrinoException(NOT_SUPPORTED,
                    "Paimon " + operation + " requires FileStoreTable, but got: " + supportedTable.getClass().getName());
        }
        return fileStoreTable;
    }

    public static void validateInsertOverwrite(FileStoreTable table)
    {
        if (!table.partitionKeys().isEmpty() && !table.coreOptions().dynamicPartitionOverwrite()) {
            throw new TrinoException(NOT_SUPPORTED,
                    "Paimon insert overwrite requires dynamic-partition-overwrite=true for partitioned tables");
        }
    }
}
