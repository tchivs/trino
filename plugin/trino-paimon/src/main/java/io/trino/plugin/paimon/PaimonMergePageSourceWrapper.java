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

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorPageSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PaimonMergePageSourceWrapper
        implements
        ConnectorPageSource
{
    private final ConnectorPageSource pageSource;
    private final List<String> rowIdFields;
    private final HashMap<String, Integer> fieldToIndex;

    public PaimonMergePageSourceWrapper(ConnectorPageSource pageSource, List<String> rowIdFields,
            HashMap<String, Integer> fieldToIndex)
    {
        this.pageSource = pageSource;
        this.rowIdFields = List.copyOf(rowIdFields);
        this.fieldToIndex = fieldToIndex;
    }

    public static PaimonMergePageSourceWrapper wrap(ConnectorPageSource pageSource,
            List<String> rowIdFields, HashMap<String, Integer> fieldToIndex)
    {
        return new PaimonMergePageSourceWrapper(pageSource, rowIdFields, fieldToIndex);
    }

    @Override
    public long getCompletedBytes()
    {
        return pageSource.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return pageSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return pageSource.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        Page nextPage = pageSource.getNextPage();
        if (nextPage == null) {
            return null;
        }
        int rowCount = nextPage.getPositionCount();

        Block[] newBlocks = new Block[nextPage.getChannelCount() + 1];
        for (int i = 0; i < nextPage.getChannelCount(); i++) {
            newBlocks[i] = nextPage.getBlock(i);
        }

        Block[] rowIdBlocks = new Block[rowIdFields.size()];
        for (int i = 0; i < rowIdFields.size(); i++) {
            String fieldName = rowIdFields.get(i);
            rowIdBlocks[i] = nextPage.getBlock(requireNonNull(fieldToIndex.get(fieldName),
                    "Missing row id field: " + fieldName));
        }

        // The rowIsNull array size must match rowCount (number of rows), not the number
        // of fields
        // All rows are non-null in this context
        newBlocks[nextPage.getChannelCount()] = RowBlock.fromNotNullSuppressedFieldBlocks(rowCount,
                Optional.of(new boolean[rowCount]), rowIdBlocks);

        return new Page(rowCount, newBlocks);
    }

    @Override
    public long getMemoryUsage()
    {
        return pageSource.getMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        pageSource.close();
    }
}
