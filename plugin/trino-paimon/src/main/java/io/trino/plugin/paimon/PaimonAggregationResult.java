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
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Holds the result of aggregation pushdown for Paimon tables.
 * When aggregation is pushed down, this class stores the pre-computed
 * aggregation values that can be returned directly without scanning data.
 */
public class PaimonAggregationResult
{
    private final List<AggregationColumn> aggregationColumns;
    private final List<Object> aggregationValues;

    @JsonCreator
    public PaimonAggregationResult(
            @JsonProperty("aggregationColumns") List<AggregationColumn> aggregationColumns,
            @JsonProperty("aggregationValues") List<Object> aggregationValues)
    {
        this.aggregationColumns = requireNonNull(aggregationColumns, "aggregationColumns is null");
        this.aggregationValues = requireNonNull(aggregationValues, "aggregationValues is null");
        if (aggregationColumns.size() != aggregationValues.size()) {
            throw new IllegalArgumentException("aggregationColumns and aggregationValues must have the same size");
        }
    }

    @JsonProperty
    public List<AggregationColumn> getAggregationColumns()
    {
        return aggregationColumns;
    }

    @JsonProperty
    public List<Object> getAggregationValues()
    {
        return aggregationValues;
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
        PaimonAggregationResult that = (PaimonAggregationResult) o;
        return Objects.equals(aggregationColumns, that.aggregationColumns)
                && Objects.equals(aggregationValues, that.aggregationValues);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(aggregationColumns, aggregationValues);
    }

    /**
     * Represents a column in the aggregation result.
     */
    public static class AggregationColumn
    {
        private final String name;
        private final Type type;

        @JsonCreator
        public AggregationColumn(
                @JsonProperty("name") String name,
                @JsonProperty("type") Type type)
        {
            this.name = requireNonNull(name, "name is null");
            this.type = requireNonNull(type, "type is null");
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public Type getType()
        {
            return type;
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
            AggregationColumn that = (AggregationColumn) o;
            return Objects.equals(name, that.name) && Objects.equals(type, that.type);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, type);
        }
    }
}
