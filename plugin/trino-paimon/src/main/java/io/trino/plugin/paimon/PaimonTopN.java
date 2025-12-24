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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.SortOrder;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Holds TopN pushdown information for Paimon tables.
 */
public class PaimonTopN
{
    private final List<PaimonSortItem> sortItems;
    private final long topNCount;

    @JsonCreator
    public PaimonTopN(
            @JsonProperty("sortItems") List<PaimonSortItem> sortItems,
            @JsonProperty("topNCount") long topNCount)
    {
        this.sortItems = requireNonNull(sortItems, "sortItems is null");
        this.topNCount = topNCount;
    }

    @JsonProperty
    public List<PaimonSortItem> getSortItems()
    {
        return sortItems;
    }

    @JsonProperty
    public long getTopNCount()
    {
        return topNCount;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PaimonTopN that = (PaimonTopN) o;
        return topNCount == that.topNCount && Objects.equals(sortItems, that.sortItems);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sortItems, topNCount);
    }

    /**
     * Represents a sort item in TopN.
     */
    public static class PaimonSortItem
    {
        private final String columnName;
        private final SortOrder sortOrder;

        @JsonCreator
        public PaimonSortItem(
                @JsonProperty("columnName") String columnName,
                @JsonProperty("sortOrder") SortOrder sortOrder)
        {
            this.columnName = requireNonNull(columnName, "columnName is null");
            this.sortOrder = requireNonNull(sortOrder, "sortOrder is null");
        }

        @JsonProperty
        public String getColumnName()
        {
            return columnName;
        }

        @JsonProperty
        public SortOrder getSortOrder()
        {
            return sortOrder;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PaimonSortItem that = (PaimonSortItem) o;
            return Objects.equals(columnName, that.columnName) && sortOrder == that.sortOrder;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(columnName, sortOrder);
        }
    }
}
