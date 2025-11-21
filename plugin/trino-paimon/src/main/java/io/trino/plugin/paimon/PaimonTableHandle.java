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
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.TupleDomain;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

public class PaimonTableHandle
        implements
        ConnectorTableHandle,
        ConnectorInsertTableHandle,
        ConnectorOutputTableHandle,
        ConnectorTableFunctionHandle
{
    private final String schemaName;
    private final String tableName;
    private final TupleDomain<PaimonColumnHandle> filter;
    private final Optional<List<ColumnHandle>> projectedColumns;
    private final OptionalLong limit;
    private final Map<String, String> dynamicOptions;

    private transient Table table;

    public PaimonTableHandle(String schemaName, String tableName, Map<String, String> dynamicOptions)
    {
        this(schemaName, tableName, dynamicOptions, TupleDomain.all(), Optional.empty(), OptionalLong.empty());
    }

    @JsonCreator
    public PaimonTableHandle(@JsonProperty("schemaName") String schemaName, @JsonProperty("tableName") String tableName,
            @JsonProperty("dynamicOptions") Map<String, String> dynamicOptions,
            @JsonProperty("filter") TupleDomain<PaimonColumnHandle> filter,
            @JsonProperty("projectedColumns") Optional<List<ColumnHandle>> projectedColumns,
            @JsonProperty("limit") OptionalLong limit)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.dynamicOptions = dynamicOptions;
        this.filter = filter;
        this.projectedColumns = projectedColumns;
        this.limit = limit;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public Map<String, String> getDynamicOptions()
    {
        return dynamicOptions;
    }

    @JsonProperty
    public TupleDomain<PaimonColumnHandle> getFilter()
    {
        return filter;
    }

    @JsonProperty
    public Optional<List<ColumnHandle>> getProjectedColumns()
    {
        return projectedColumns;
    }

    public OptionalLong getLimit()
    {
        return limit;
    }

    public Table tableWithDynamicOptions(PaimonCatalog catalog, ConnectorSession session)
    {
        Table paimonTable = table(catalog);

        // see TrinoConnector.getSessionProperties
        Map<String, String> dynamicOptions = new HashMap<>();
        Long scanTimestampMills = PaimonSessionProperties.getScanTimestampMillis(session);
        if (scanTimestampMills != null) {
            dynamicOptions.put(CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), scanTimestampMills.toString());
        }
        Long scanSnapshotId = PaimonSessionProperties.getScanSnapshotId(session);
        if (scanSnapshotId != null) {
            dynamicOptions.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), scanSnapshotId.toString());
        }

        return dynamicOptions.size() > 0 ? paimonTable.copy(dynamicOptions) : paimonTable;
    }

    public Table table(PaimonCatalog catalog)
    {
        if (table != null) {
            return table;
        }
        try {
            table = catalog.getTable(Identifier.create(schemaName, tableName)).copy(dynamicOptions);
        }
        catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(e);
        }
        return table;
    }

    public ConnectorTableMetadata tableMetadata(PaimonCatalog catalog)
    {
        return new ConnectorTableMetadata(SchemaTableName.schemaTableName(schemaName, tableName),
                columnMetadatas(catalog), Collections.emptyMap(), Optional.empty());
    }

    public List<ColumnMetadata> columnMetadatas(PaimonCatalog catalog)
    {
        return table(catalog).rowType().getFields().stream()
                .map(column -> ColumnMetadata.builder().setName(column.name())
                        .setType(PaimonTypeUtils.fromPaimonType(column.type())).setNullable(column.type().isNullable())
                        .setComment(Optional.ofNullable(column.description())).build())
                .collect(Collectors.toList());
    }

    public PaimonColumnHandle columnHandle(PaimonCatalog catalog, String field)
    {
        Table paimonTable = table(catalog);
        List<String> lowerCaseFieldNames = FieldNameUtils.fieldNames(paimonTable.rowType());
        List<String> originFieldNames = paimonTable.rowType().getFieldNames();
        // Fix case-sensitivity: lowerCaseFieldNames contains lowercase names, so convert field to lowercase for lookup
        int index = lowerCaseFieldNames.indexOf(FieldNameUtils.toLowerCase(field));
        if (index == -1) {
            throw new RuntimeException(String.format("Cannot find field %s in schema %s", field, lowerCaseFieldNames));
        }
        return PaimonColumnHandle.of(originFieldNames.get(index), paimonTable.rowType().getTypeAt(index));
    }

    public PaimonTableHandle copy(TupleDomain<PaimonColumnHandle> filter)
    {
        return new PaimonTableHandle(schemaName, tableName, dynamicOptions, filter, projectedColumns, limit);
    }

    public PaimonTableHandle copy(Optional<List<ColumnHandle>> projectedColumns)
    {
        return new PaimonTableHandle(schemaName, tableName, dynamicOptions, filter, projectedColumns, limit);
    }

    public PaimonTableHandle copy(OptionalLong limit)
    {
        return new PaimonTableHandle(schemaName, tableName, dynamicOptions, filter, projectedColumns, limit);
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
        PaimonTableHandle that = (PaimonTableHandle) o;
        return Objects.equals(dynamicOptions, that.dynamicOptions) && Objects.equals(schemaName, that.schemaName)
                && Objects.equals(tableName, that.tableName) && Objects.equals(filter, that.filter)
                && Objects.equals(projectedColumns, that.projectedColumns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, filter, projectedColumns, dynamicOptions);
    }
}
