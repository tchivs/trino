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
 * Supports both single-row results (global aggregation) and multi-row results
 * (GROUP BY partition key aggregation).
 */
public class PaimonAggregationResult
{
    private final List<AggregationColumn> aggregationColumns;
    // For single-row result (backward compatibility)
    private final List<Object> aggregationValues;
    // For multi-row results (GROUP BY partition key)
    private final List<List<Object>> aggregationRows;

    @JsonCreator
    public PaimonAggregationResult(
            @JsonProperty("aggregationColumns") List<AggregationColumn> aggregationColumns,
            @JsonProperty("aggregationValues") List<Object> aggregationValues,
            @JsonProperty("aggregationRows") List<List<Object>> aggregationRows)
    {
        this.aggregationColumns = requireNonNull(aggregationColumns, "aggregationColumns is null");
        this.aggregationValues = aggregationValues;
        this.aggregationRows = aggregationRows;
    }

    // Constructor for single-row result (backward compatibility)
    public PaimonAggregationResult(
            List<AggregationColumn> aggregationColumns,
            List<Object> aggregationValues)
    {
        this(aggregationColumns, aggregationValues, null);
    }

    // Constructor for multi-row results
    public static PaimonAggregationResult multiRow(
            List<AggregationColumn> aggregationColumns,
            List<List<Object>> aggregationRows)
    {
        return new PaimonAggregationResult(aggregationColumns, null, aggregationRows);
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

    @JsonProperty
    public List<List<Object>> getAggregationRows()
    {
        return aggregationRows;
    }

    public boolean isMultiRow()
    {
        return aggregationRows != null;
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
                && Objects.equals(aggregationValues, that.aggregationValues)
                && Objects.equals(aggregationRows, that.aggregationRows);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(aggregationColumns, aggregationValues, aggregationRows);
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
