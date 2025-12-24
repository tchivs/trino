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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStatsEvolutions;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.plugin.paimon.PaimonColumnHandle.TRINO_ROW_ID_NAME;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.connector.RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.paimon.utils.Preconditions.checkArgument;

public record PaimonMetadata(PaimonCatalog catalog,
                             io.trino.spi.type.TypeManager typeManager) implements ConnectorMetadata
{
    private static final Logger log = Logger.get(PaimonMetadata.class);
    private static final String TAG_PREFIX = "tag-";
    private static final int GET_METADATA_BATCH_SIZE = 1000;

    private static boolean containSameElements(List<? extends ColumnHandle> first, List<? extends ColumnHandle> second)
    {
        return new HashSet<>(first).equals(new HashSet<>(second));
    }

    // todo support dynamic bucket table
    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        catalog.initSession(session);
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Table table = paimonTableHandle.table(catalog);
        if (!(table instanceof FileStoreTable storeTable)) {
            throw new IllegalArgumentException(table.getClass() + " is not supported");
        }
        BucketMode bucketMode = storeTable.bucketMode();
        switch (bucketMode) {
            case HASH_FIXED :
                try {
                    return Optional.of(new ConnectorTableLayout(
                            new PaimonPartitioningHandle(InstantiationUtil.serializeObject(storeTable.schema())),
                            storeTable.schema().bucketKeys(), false));
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            case BUCKET_UNAWARE :
                return Optional.empty();
            default :
                throw new IllegalArgumentException("Unknown table bucket mode: " + bucketMode);
        }
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata,
            Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        createTable(session, tableMetadata,
                replace ? io.trino.spi.connector.SaveMode.REPLACE : io.trino.spi.connector.SaveMode.FAIL);
        return getTableHandle(session, tableMetadata.getTable(), Collections.emptyMap());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session,
            ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        if (fragments.isEmpty()) {
            return Optional.empty();
        }
        return commit(session, (PaimonTableHandle) tableHandle, fragments);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns, RetryMode retryMode)
    {
        return (ConnectorInsertTableHandle) tableHandle;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session,
            ConnectorInsertTableHandle insertHandle, List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return commit(session, (PaimonTableHandle) insertHandle, fragments);
    }

    private Optional<ConnectorOutputMetadata> commit(ConnectorSession session, PaimonTableHandle insertHandle,
            Collection<Slice> fragments)
    {
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        List<CommitMessage> commitMessages = fragments.stream().map(slice -> {
            try {
                return serializer.deserialize(serializer.getVersion(), slice.getBytes());
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).collect(toList());

        if (commitMessages.isEmpty()) {
            return Optional.empty();
        }

        PaimonTableHandle table = insertHandle;
        BatchWriteBuilder batchWriteBuilder = table.tableWithDynamicOptions(catalog, session).newBatchWriteBuilder();
        if (PaimonSessionProperties.enableInsertOverwrite(session)) {
            batchWriteBuilder.withOverwrite();
        }
        batchWriteBuilder.newCommit().commit(commitMessages);
        return Optional.empty();
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return DELETE_ROW_AND_INSERT_ROW;
    }

    // todo support dynamic bucket table
    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        catalog.initSession(session);
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Table table = paimonTableHandle.table(catalog);
        if (!(table instanceof FileStoreTable storeTable)) {
            throw new IllegalArgumentException(table.getClass() + " is not supported");
        }
        BucketMode bucketMode = storeTable.bucketMode();
        if (bucketMode != BucketMode.HASH_FIXED) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported table bucket mode: " + bucketMode);
        }
        Set<String> pkSet = new HashSet<>(table.primaryKeys());
        DataField[] row = table.rowType().getFields().stream().filter(dataField -> pkSet.contains(dataField.name()))
                .toArray(DataField[]::new);
        return PaimonColumnHandle.of(TRINO_ROW_ID_NAME, DataTypes.ROW(row));
    }

    // todo support dynamic bucket table
    @Override
    public Optional<ConnectorPartitioningHandle> getUpdateLayout(ConnectorSession session,
            ConnectorTableHandle tableHandle)
    {
        catalog.initSession(session);
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Table table = paimonTableHandle.table(catalog);
        if (!(table instanceof FileStoreTable storeTable)) {
            throw new IllegalArgumentException(table.getClass() + " is not supported");
        }
        BucketMode bucketMode = storeTable.bucketMode();
        if (bucketMode != BucketMode.HASH_FIXED) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported table bucket mode: " + bucketMode);
        }
        try {
            return Optional.of(new PaimonPartitioningHandle(InstantiationUtil.serializeObject(storeTable.schema())));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle,
            Map<Integer, Collection<ColumnHandle>> updateCaseColumns, RetryMode retryMode)
    {
        return new PaimonMergeTableHandle((PaimonTableHandle) tableHandle);
    }

    @Override
    public void finishMerge(ConnectorSession session, ConnectorMergeTableHandle mergeTableHandle,
            List<ConnectorTableHandle> sourceTableHandles, Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        commit(session, (PaimonTableHandle) mergeTableHandle.getTableHandle(), fragments);
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        catalog.initSession(session);
        try {
            catalog.getDatabase(schemaName);
            return true;
        }
        catch (Catalog.DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        catalog.initSession(session);
        return catalog.listDatabases();
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties,
            TrinoPrincipal owner)
    {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(schemaName), "schemaName cannot be null or empty");

        try {
            catalog.initSession(session);
            // Convert properties Map<String, Object> to Map<String, String>
            Map<String, String> stringProperties = new HashMap<>();
            if (properties != null) {
                properties.forEach((key, value) -> {
                    if (value != null) {
                        stringProperties.put(key, value.toString());
                    }
                });
            }
            catalog.createDatabase(schemaName, true, stringProperties);
        }
        catch (Catalog.DatabaseAlreadyExistException e) {
            throw new RuntimeException(format("database already existed: '%s'", schemaName));
        }
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(schemaName), "schemaName cannot be null or empty");
        try {
            catalog.initSession(session);
            catalog.dropDatabase(schemaName, false, true);
        }
        catch (Catalog.DatabaseNotEmptyException e) {
            throw new RuntimeException(format("database is not empty: '%s'", schemaName));
        }
        catch (Catalog.DatabaseNotExistException e) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", schemaName));
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Read paimon table with start version is not supported");
        }

        Map<String, String> dynamicOptions = new HashMap<>();
        if (endVersion.isPresent()) {
            ConnectorTableVersion version = endVersion.get();
            Type versionType = version.getVersionType();
            switch (version.getPointerType()) {
                case TEMPORAL : {
                    if (!(versionType instanceof TimestampWithTimeZoneType timeZonedVersionType)) {
                        throw new TrinoException(NOT_SUPPORTED,
                                "Unsupported type for table version: " + versionType.getDisplayName());
                    }
                    long epochMillis = timeZonedVersionType.isShort()
                            ? unpackMillisUtc((long) version.getVersion())
                            : ((LongTimestampWithTimeZone) version.getVersion()).getEpochMillis();
                    dynamicOptions.put(CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), String.valueOf(epochMillis));
                    break;
                }
                case TARGET_ID : {
                    String tagOrVersion;
                    if (versionType instanceof VarcharType) {
                        tagOrVersion = BinaryString.fromBytes(((Slice) version.getVersion()).getBytes()).toString();
                    }
                    else {
                        tagOrVersion = version.getVersion().toString();
                    }

                    // if value is not number, set tag option
                    boolean isNumber = StringUtils.isNumeric(tagOrVersion);
                    if (!isNumber) {
                        dynamicOptions.put(CoreOptions.SCAN_TAG_NAME.key(), tagOrVersion);
                    }
                    else {
                        try {
                            catalog.initSession(session);
                            Table table = catalog
                                    .getTable(new Identifier(tableName.getSchemaName(), tableName.getTableName()));
                            String path = table.options().get("path");

                            if (table.fileIO().exists(new Path(path + "/tag/" + TAG_PREFIX + tagOrVersion))) {
                                dynamicOptions.put(CoreOptions.SCAN_TAG_NAME.key(), tagOrVersion);
                            }
                            else {
                                dynamicOptions.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), tagOrVersion);
                            }
                        }
                        catch (IOException | Catalog.TableNotExistException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    break;
                }
            }
        }
        return getTableHandle(session, tableName, dynamicOptions);
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    public PaimonTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName,
            Map<String, String> dynamicOptions)
    {
        catalog.initSession(session);
        try {
            catalog.getTable(Identifier.create(tableName.getSchemaName(), tableName.getTableName()));
            return new PaimonTableHandle(tableName.getSchemaName(), tableName.getTableName(), dynamicOptions);
        }
        catch (Catalog.TableNotExistException e) {
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        catalog.initSession(session);
        return ((PaimonTableHandle) tableHandle).tableMetadata(catalog);
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle,
            Map<String, Optional<Object>> properties)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        List<SchemaChange> changes = new ArrayList<>();

        // Handle both setting and removing options
        // When SET PROPERTIES x = DEFAULT is used, the value will be Optional.empty()
        for (Map.Entry<String, Optional<Object>> entry : properties.entrySet()) {
            String key = entry.getKey();
            Optional<Object> value = entry.getValue();

            if (value.isPresent()) {
                // Set the property to the specified value
                changes.add(SchemaChange.setOption(key, (String) value.get()));
            }
            else {
                // Remove the property (SET PROPERTIES x = DEFAULT)
                changes.add(SchemaChange.removeOption(key));
            }
        }

        try {
            catalog.initSession(session);
            catalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw new RuntimeException(format("failed to alter table: '%s'", paimonTableHandle.getTableName()), e);
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        catalog.initSession(session);
        List<SchemaTableName> tables = new ArrayList<>();
        schemaName.map(Collections::singletonList).orElseGet(catalog::listDatabases)
                .forEach(schema -> tables.addAll(listTables(schema)));
        return tables;
    }

    private List<SchemaTableName> listTables(String schema)
    {
        try {
            return catalog.listTables(schema).stream().map(table -> new SchemaTableName(schema, table))
                    .collect(toList());
        }
        catch (Catalog.DatabaseNotExistException e) {
            // Schema doesn't exist, return empty list per Trino convention
            return Collections.emptyList();
        }
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata,
            io.trino.spi.connector.SaveMode saveMode)
    {
        SchemaTableName table = tableMetadata.getTable();
        Identifier identifier = Identifier.create(table.getSchemaName(), table.getTableName());

        try {
            catalog.initSession(session);
            if (saveMode == io.trino.spi.connector.SaveMode.REPLACE) {
                // For REPLACE mode, drop the table if it exists first
                try {
                    catalog.dropTable(identifier, false);
                }
                catch (Catalog.TableNotExistException e) {
                    // Table doesn't exist, continue with creation
                }
            }
            catalog.createTable(identifier, prepareSchema(tableMetadata),
                    saveMode == io.trino.spi.connector.SaveMode.IGNORE);
        }
        catch (Catalog.DatabaseNotExistException e) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", table.getSchemaName()));
        }
        catch (Catalog.TableAlreadyExistException e) {
            if (saveMode == io.trino.spi.connector.SaveMode.FAIL) {
                throw new RuntimeException(format("table already existed: '%s'", table.getTableName()));
            }
            // For IGNORE mode, silently ignore the error
        }
    }

    private Schema prepareSchema(ConnectorTableMetadata tableMetadata)
    {
        Map<String, Object> properties = new HashMap<>(tableMetadata.getProperties());
        Schema.Builder builder = Schema.newBuilder().primaryKey(PaimonTableOptions.getPrimaryKeys(properties))
                .partitionKeys(PaimonTableOptions.getPartitionedKeys(properties));

        for (ColumnMetadata column : tableMetadata.getColumns()) {
            builder.column(column.getName(), PaimonTypeUtils.toPaimonType(column.getType()), column.getComment());
        }

        PaimonTableOptionUtils.buildOptions(builder, properties);

        return builder.build();
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        PaimonTableHandle oldTableHandle = (PaimonTableHandle) tableHandle;
        try {
            catalog.initSession(session);
            catalog.renameTable(new Identifier(oldTableHandle.getSchemaName(), oldTableHandle.getTableName()),
                    new Identifier(newTableName.getSchemaName(), newTableName.getTableName()), false);
        }
        catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(format("table not exists: '%s'", oldTableHandle.getTableName()));
        }
        catch (Catalog.TableAlreadyExistException e) {
            throw new RuntimeException(format("table already existed: '%s'", newTableName.getTableName()));
        }
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        try {
            catalog.initSession(session);
            catalog.dropTable(new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName()), false);
        }
        catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(format("table not exists: '%s'", paimonTableHandle.getTableName()));
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PaimonTableHandle table = (PaimonTableHandle) tableHandle;
        Map<String, ColumnHandle> handleMap = new HashMap<>();
        for (ColumnMetadata column : table.columnMetadatas(catalog)) {
            handleMap.put(column.getName(), table.columnHandle(catalog, column.getName()));
        }
        return handleMap;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        return ((PaimonColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        Map<SchemaTableName, List<ColumnMetadata>> result = new LinkedHashMap<>();
        streamTableColumns(session, prefix).forEachRemaining(tableColumnsMetadata ->
                tableColumnsMetadata.getColumns().ifPresent(columns -> result.put(tableColumnsMetadata.getTable(), columns)));
        return result;
    }

    @Override
    public Iterator<io.trino.spi.connector.TableColumnsMetadata> streamTableColumns(ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        List<SchemaTableName> tableNames;
        if (prefix.getTable().isPresent()) {
            tableNames = Collections.singletonList(prefix.toSchemaTableName());
        }
        else {
            tableNames = listTables(session, prefix.getSchema());
        }

        // Process tables in batches to improve performance
        return Lists.partition(tableNames, GET_METADATA_BATCH_SIZE).stream()
                .map(tableBatch -> {
                    ImmutableList.Builder<io.trino.spi.connector.TableColumnsMetadata> tableMetadatas =
                            ImmutableList.builderWithExpectedSize(tableBatch.size());

                    for (SchemaTableName tableName : tableBatch) {
                        try {
                            PaimonTableHandle tableHandle = (PaimonTableHandle) getTableHandle(
                                    session, tableName, Optional.empty(), Optional.empty());
                            if (tableHandle != null) {
                                List<ColumnMetadata> columns = tableHandle.columnMetadatas(catalog);
                                tableMetadatas.add(io.trino.spi.connector.TableColumnsMetadata.forTable(tableName, columns));
                            }
                        }
                        catch (RuntimeException e) {
                            // Table can be being removed and this may cause all sorts of exceptions
                            // Log and skip this table
                            log.warn(e, "Failed to access metadata of table %s during streaming table columns for %s",
                                    tableName, prefix);
                        }
                    }

                    return tableMetadatas.build();
                })
                .flatMap(List::stream)
                .iterator();
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column,
            ColumnPosition position)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.addColumn(column.getName(), PaimonTypeUtils.toPaimonType(column.getType())));
        try {
            catalog.initSession(session);
            catalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw new RuntimeException(format("failed to alter table: '%s'", paimonTableHandle.getTableName()), e);
        }
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source,
            String target)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        PaimonColumnHandle paimonColumnHandle = (PaimonColumnHandle) source;
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.renameColumn(paimonColumnHandle.getColumnName(), target));
        try {
            catalog.initSession(session);
            catalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw new RuntimeException(format("failed to alter table: '%s'", paimonTableHandle.getTableName()), e);
        }
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        PaimonColumnHandle paimonColumnHandle = (PaimonColumnHandle) column;
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.dropColumn(paimonColumnHandle.getColumnName()));
        try {
            catalog.initSession(session);
            catalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw new RuntimeException(format("failed to alter table: '%s'", paimonTableHandle.getTableName()), e);
        }
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.updateComment(comment.orElse(null)));
        try {
            catalog.initSession(session);
            catalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw new RuntimeException(format("failed to set table comment: '%s'", paimonTableHandle.getTableName()), e);
        }
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column,
            Optional<String> comment)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        PaimonColumnHandle paimonColumnHandle = (PaimonColumnHandle) column;
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(new SchemaChange.UpdateColumnComment(new String[] {paimonColumnHandle.getColumnName()},
                comment.orElse(null)));
        try {
            catalog.initSession(session);
            catalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw new RuntimeException(format("failed to set column comment for '%s'.'%s'",
                    paimonTableHandle.getTableName(), paimonColumnHandle.getColumnName()), e);
        }
    }

    @Override
    public void setColumnType(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column,
            Type type)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        PaimonColumnHandle paimonColumnHandle = (PaimonColumnHandle) column;

        // Convert Trino Type to Paimon DataType
        DataType paimonType = PaimonTypeUtils.toPaimonType(type);

        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.updateColumnType(paimonColumnHandle.getColumnName(), paimonType));

        try {
            catalog.initSession(session);
            catalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw new RuntimeException(format("failed to set column type for '%s'.'%s' to '%s'",
                    paimonTableHandle.getTableName(), paimonColumnHandle.getColumnName(), type), e);
        }
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        PaimonColumnHandle paimonColumnHandle = (PaimonColumnHandle) column;

        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.updateColumnNullability(paimonColumnHandle.getColumnName(), true));

        try {
            catalog.initSession(session);
            catalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw new RuntimeException(format("failed to drop NOT NULL constraint on column '%s'.'%s'",
                    paimonTableHandle.getTableName(), paimonColumnHandle.getColumnName()), e);
        }
    }

    @Override
    public void addField(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> parentPath,
            String fieldName, Type type, boolean ignoreExisting)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());

        // Build field path: parentPath + fieldName
        String[] fieldNames = buildFieldNamesArray(parentPath, fieldName);

        // Convert Trino Type to Paimon DataType
        DataType paimonType = PaimonTypeUtils.toPaimonType(type);

        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.addColumn(fieldNames, paimonType, null, null));

        try {
            catalog.initSession(session);
            catalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            if (!ignoreExisting) {
                throw new RuntimeException(format("failed to add field '%s' to '%s'", String.join(".", fieldNames),
                        paimonTableHandle.getTableName()), e);
            }
        }
    }

    @Override
    public void dropField(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column,
            List<String> fieldPath)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        PaimonColumnHandle paimonColumnHandle = (PaimonColumnHandle) column;

        // Build full field path: columnName + fieldPath
        String[] fieldNames = buildFieldNamesArray(List.of(paimonColumnHandle.getColumnName()), fieldPath);

        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.dropColumn(fieldNames));

        try {
            catalog.initSession(session);
            catalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw new RuntimeException(format("failed to drop field '%s' from '%s'", String.join(".", fieldNames),
                    paimonTableHandle.getTableName()), e);
        }
    }

    @Override
    public void renameField(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> fieldPath,
            String target)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());

        // fieldPath includes column name and nested path
        String[] fieldNames = fieldPath.toArray(new String[0]);

        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.renameColumn(fieldNames, target));

        try {
            catalog.initSession(session);
            catalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw new RuntimeException(format("failed to rename field '%s' to '%s' in table '%s'",
                    String.join(".", fieldNames), target, paimonTableHandle.getTableName()), e);
        }
    }

    @Override
    public void setFieldType(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> fieldPath,
            Type type)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());

        // fieldPath includes column name and nested path
        String[] fieldNames = fieldPath.toArray(new String[0]);

        // Convert Trino Type to Paimon DataType
        DataType paimonType = PaimonTypeUtils.toPaimonType(type);

        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.updateColumnType(fieldNames, paimonType, false));

        try {
            catalog.initSession(session);
            catalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw new RuntimeException(format("failed to set field type '%s' to '%s' in table '%s'",
                    String.join(".", fieldNames), type, paimonTableHandle.getTableName()), e);
        }
    }

    /**
     * Helper method to build field names array from parent path and field name.
     * Used for nested field operations.
     */
    private String[] buildFieldNamesArray(List<String> parentPath, String fieldName)
    {
        List<String> fullPath = new ArrayList<>(parentPath);
        fullPath.add(fieldName);
        return fullPath.toArray(new String[0]);
    }

    /**
     * Helper method to build field names array from column name and field path.
     * Used for nested field operations where we have a column handle and a nested
     * path.
     */
    private String[] buildFieldNamesArray(List<String> columnList, List<String> fieldPath)
    {
        List<String> fullPath = new ArrayList<>(columnList);
        fullPath.addAll(fieldPath);
        return fullPath.toArray(new String[0]);
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());

        try {
            catalog.initSession(session);
            Table table = catalog.getTable(identifier);
            if (!(table instanceof FileStoreTable fileStoreTable)) {
                throw new IllegalArgumentException("Table is not a FileStoreTable: " + table.getClass());
            }

            // Use BatchTableCommit to truncate the table
            try (BatchTableCommit commit = fileStoreTable.newBatchWriteBuilder().newCommit()) {
                commit.truncateTable();
            }
        }
        catch (Exception e) {
            throw new RuntimeException(format("failed to truncate table '%s'", paimonTableHandle.getTableName()), e);
        }
    }

    // TODO: Enhancement - SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN
    // Current implementation only supports column-based predicates via TupleDomain.
    // To support expression predicates (e.g., WHERE lower(name) = 'alice'):
    // 1. Implement applyFilter() to accept ConnectorExpression parameters (Trino
    // SPI enhancement)
    // 2. Convert Trino expressions to Paimon expressions (if Paimon supports)
    // 3. Identify which functions are pushdown-safe (deterministic, supported by
    // Paimon)
    // Reference:
    // tmp-docs/PUSHDOWN_OPTIMIZATION_GUIDE.md#3-supports_predicate_expression_pushdown
    // Note: Iceberg and Hudi connectors also don't support this - indicates high
    // complexity
    // Estimated effort: 12-16 hours
    // Priority: P2 (Medium value, high cost)
    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session,
            ConnectorTableHandle handle, Constraint constraint)
    {
        catalog.initSession(session);
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) handle;
        Optional<PaimonFilterExtractor.TrinoFilter> extract = PaimonFilterExtractor.extract(catalog, paimonTableHandle,
                constraint);
        if (extract.isPresent()) {
            PaimonFilterExtractor.TrinoFilter trinoFilter = extract.get();
            return Optional.of(new ConstraintApplicationResult<>(paimonTableHandle.copy(trinoFilter.filter()),
                    trinoFilter.remainFilter(), trinoFilter.remainingExpression(), false));
        }
        else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session,
            ConnectorTableHandle handle, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) handle;
        List<ColumnHandle> newColumns = new ArrayList<>(assignments.values());

        if (paimonTableHandle.getProjectedColumns().isPresent()
                && containSameElements(newColumns, paimonTableHandle.getProjectedColumns().get())) {
            return Optional.empty();
        }

        List<Assignment> assignmentList = new ArrayList<>();
        assignments.forEach((name, column) -> assignmentList
                .add(new Assignment(name, column, ((PaimonColumnHandle) column).getTrinoType())));

        return Optional.of(new ProjectionApplicationResult<>(paimonTableHandle.copy(Optional.of(newColumns)),
                projections, assignmentList, false));
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session,
            ConnectorTableHandle handle, long limit)
    {
        catalog.initSession(session);
        // TODO: Enhancement - SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR
        // Current implementation supports basic TOP-N pushdown but not with VARCHAR
        // sorting keys.
        // To implement VARCHAR support:
        // 1. Add logic to check if sorting columns contain VARCHAR types
        // 2. Verify Paimon supports VARCHAR-based sorting at storage level
        // 3. May need to implement sorting logic in SplitManager
        // Reference:
        // tmp-docs/PUSHDOWN_OPTIMIZATION_GUIDE.md#1-supports_topn_pushdown_with_varchar
        // Estimated effort: 4-6 hours
        // Priority: P1 (High value, moderate cost)

        PaimonTableHandle table = (PaimonTableHandle) handle;

        if (table.getLimit().isPresent() && table.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }

        if (!table.getFilter().isAll()) {
            Table paimonTable = table.table(catalog);
            HashMap<PaimonColumnHandle, Domain> acceptedDomains = new LinkedHashMap<>();
            HashMap<PaimonColumnHandle, Domain> unsupportedDomains = new LinkedHashMap<>();
            new PaimonFilterConverter(paimonTable.rowType()).convert(table.getFilter(), acceptedDomains,
                    unsupportedDomains);
            Set<String> acceptedFields = acceptedDomains.keySet().stream().map(PaimonColumnHandle::getColumnName)
                    .collect(Collectors.toSet());
            if (!unsupportedDomains.isEmpty()
                    || !new HashSet<>(paimonTable.partitionKeys()).containsAll(acceptedFields)) {
                return Optional.empty();
            }
        }

        table = table.copy(OptionalLong.of(limit));

        return Optional.of(new LimitApplicationResult<>(table, false, false));
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        catalog.initSession(session);
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) handle;

        // Only support global aggregation (no GROUP BY) for now
        // Global aggregation is represented by [[]]
        if (groupingSets.size() != 1 || !groupingSets.get(0).isEmpty()) {
            return Optional.empty();
        }

        // Check if all aggregates are supported
        if (!canPushdownAggregation(aggregates, assignments)) {
            return Optional.empty();
        }

        try {
            Table table = paimonTableHandle.tableWithDynamicOptions(catalog, session);

            // Aggregation pushdown computes results from split-level metadata, so it is only correct without row-level
            // filtering. Partition-only filters are safe because they only restrict which splits are selected.
            if (!isPartitionOnlyFilter(table, paimonTableHandle.getFilter())) {
                return Optional.empty();
            }

            // Only support FileStoreTable for MIN/MAX pushdown
            if (!(table instanceof FileStoreTable)) {
                log.debug("Aggregation pushdown not supported: table is not FileStoreTable");
                return Optional.empty();
            }
            FileStoreTable fileStoreTable = (FileStoreTable) table;

            // Check table properties for COUNT(*) pushdown eligibility
            boolean hasPrimaryKeys = !fileStoreTable.schema().primaryKeys().isEmpty();
            boolean deletionVectorsEnabled = fileStoreTable.coreOptions().deletionVectorsEnabled();

            boolean hasMinMax = aggregates.stream()
                    .map(AggregateFunction::getFunctionName)
                    .map(String::toLowerCase)
                    .anyMatch(functionName -> "min".equals(functionName) || "max".equals(functionName));

            // MIN/MAX based on file-level statistics may include rows removed by deletion vectors
            // and may also be incorrect for primary key tables due to updates/deduplication.
            if (hasMinMax && (hasPrimaryKeys || deletionVectorsEnabled)) {
                log.debug("MIN/MAX pushdown not supported for table %s.%s: table has primary keys or deletion vectors enabled",
                        paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
                return Optional.empty();
            }

            // For primary key tables, COUNT(*) pushdown is only supported when deletion-vectors is enabled
            // Without deletion-vectors, mergedRowCount would be incorrect as it doesn't account for key deduplication
            boolean countPushdownSupported = !hasPrimaryKeys || deletionVectorsEnabled;

            // Get all splits to compute aggregation
            org.apache.paimon.table.source.ReadBuilder readBuilder = table.newReadBuilder();
            new PaimonFilterConverter(table.rowType()).convert(paimonTableHandle.getFilter()).ifPresent(readBuilder::withFilter);
            List<Split> splits = readBuilder.newScan().plan().splits();

            // Check if all splits support mergedRowCount for COUNT(*)
            List<DataSplit> dataSplits = splits.stream()
                    .filter(split -> split instanceof DataSplit)
                    .map(split -> (DataSplit) split)
                    .collect(toList());

            // For COUNT(*), check if mergedRowCount is available
            // This is only true for append-only tables (no primary keys)
            boolean allMergedRowCountAvailable = dataSplits.stream()
                    .allMatch(DataSplit::mergedRowCountAvailable);

            if (!allMergedRowCountAvailable) {
                log.debug("Aggregation pushdown not supported for table %s.%s: mergedRowCount not available (table may have primary keys or deletion vectors)",
                        paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
            }

            // Prepare for MIN/MAX computation
            TableSchema tableSchema = fileStoreTable.schema();
            SchemaManager schemaManager = fileStoreTable.schemaManager();
            Map<Long, TableSchema> schemaCache = new HashMap<>();
            SimpleStatsEvolutions evolutions = new SimpleStatsEvolutions(
                    id -> schemaCache.computeIfAbsent(id, key ->
                            key == tableSchema.id() ? tableSchema : schemaManager.schema(key)).fields(),
                    tableSchema.id());

            // Compute aggregation results
            List<Object> aggregationValues = new ArrayList<>();
            List<PaimonAggregationResult.AggregationColumn> aggregationColumns = new ArrayList<>();
            ImmutableList.Builder<ConnectorExpression> projections = ImmutableList.builder();
            ImmutableList.Builder<Assignment> resultAssignments = ImmutableList.builder();

            int columnIndex = 0;
            for (AggregateFunction aggregate : aggregates) {
                String functionName = aggregate.getFunctionName().toLowerCase();
                String syntheticColumnName = "paimon_agg_" + columnIndex;

                if ("count".equals(functionName) && (aggregate.getArguments().isEmpty() || isCountNonNull(aggregate, assignments))) {
                    // COUNT(*) - sum up mergedRowCount from all splits
                    // For PK tables without deletion-vectors, count would be incorrect
                    if (!countPushdownSupported) {
                        log.debug("COUNT(*) pushdown not supported for table %s.%s: primary key table without deletion-vectors",
                                paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
                        return Optional.empty();
                    }
                    if (!allMergedRowCountAvailable) {
                        return Optional.empty();
                    }

                    long count = dataSplits.stream()
                            .mapToLong(DataSplit::mergedRowCount)
                            .sum();

                    aggregationValues.add(count);
                    aggregationColumns.add(new PaimonAggregationResult.AggregationColumn(
                            syntheticColumnName, BigintType.BIGINT));

                    PaimonColumnHandle columnHandle = PaimonColumnHandle.of(
                            syntheticColumnName,
                            org.apache.paimon.types.DataTypes.BIGINT());

                    projections.add(new Variable(syntheticColumnName, BigintType.BIGINT));
                    resultAssignments.add(new Assignment(syntheticColumnName, columnHandle, BigintType.BIGINT));
                }
                else if ("min".equals(functionName) || "max".equals(functionName)) {
                    // MIN/MAX aggregation
                    if (aggregate.getArguments().size() != 1) {
                        return Optional.empty();
                    }

                    ConnectorExpression argument = aggregate.getArguments().get(0);
                    if (!(argument instanceof Variable)) {
                        return Optional.empty();
                    }

                    String columnName = ((Variable) argument).getName();
                    ColumnHandle sourceColumnHandle = assignments.get(columnName);
                    if (!(sourceColumnHandle instanceof PaimonColumnHandle)) {
                        return Optional.empty();
                    }

                    PaimonColumnHandle paimonColumn = (PaimonColumnHandle) sourceColumnHandle;
                    DataType paimonType = paimonColumn.logicalType();

                    // Check if the type supports MIN/MAX statistics
                    if (!isMinMaxSupported(paimonType)) {
                        return Optional.empty();
                    }

                    // Check if all splits have statistics for this column
                    String paimonColumnName = paimonColumn.getColumnName();
                    Set<String> columnSet = Set.of(paimonColumnName);
                    boolean statsAvailable = dataSplits.stream()
                            .allMatch(split -> org.apache.paimon.table.source.PushDownUtils.minmaxAvailable(split, columnSet));

                    if (!statsAvailable) {
                        return Optional.empty();
                    }

                    // Find the field index
                    int fieldIndex = -1;
                    DataField targetField = null;
                    for (int i = 0; i < tableSchema.fields().size(); i++) {
                        DataField field = tableSchema.fields().get(i);
                        if (field.name().equals(paimonColumnName)) {
                            fieldIndex = i;
                            targetField = field;
                            break;
                        }
                    }

                    if (fieldIndex < 0 || targetField == null) {
                        return Optional.empty();
                    }

                    // Compute MIN or MAX across all splits
                    Object result = null;
                    boolean isMin = "min".equals(functionName);

                    for (DataSplit split : dataSplits) {
                        Object value = isMin
                                ? split.minValue(fieldIndex, targetField, evolutions)
                                : split.maxValue(fieldIndex, targetField, evolutions);

                        if (value != null) {
                            if (result == null) {
                                result = value;
                            }
                            else {
                                int cmp = org.apache.paimon.predicate.CompareUtils.compareLiteral(targetField.type(), result, value);
                                if (isMin ? cmp > 0 : cmp < 0) {
                                    result = value;
                                }
                            }
                        }
                    }

                    // Convert Paimon value to Trino value
                    Type trinoType = paimonColumn.getTrinoType();
                    Object trinoValue = convertPaimonValueToTrino(result, paimonType, trinoType);

                    aggregationValues.add(trinoValue);
                    aggregationColumns.add(new PaimonAggregationResult.AggregationColumn(
                            syntheticColumnName, trinoType));

                    PaimonColumnHandle resultColumnHandle = PaimonColumnHandle.of(
                            syntheticColumnName, paimonType);

                    projections.add(new Variable(syntheticColumnName, trinoType));
                    resultAssignments.add(new Assignment(syntheticColumnName, resultColumnHandle, trinoType));
                }
                else {
                    // Unsupported aggregation
                    return Optional.empty();
                }

                columnIndex++;
            }

            PaimonAggregationResult aggregationResult = new PaimonAggregationResult(
                    aggregationColumns, aggregationValues);

            PaimonTableHandle newHandle = paimonTableHandle.copyWithAggregationResult(aggregationResult);

            return Optional.of(new AggregationApplicationResult<>(
                    newHandle,
                    projections.build(),
                    resultAssignments.build(),
                    ImmutableMap.of(),
                    false));
        }
        catch (Exception e) {
            log.debug(e, "Failed to push down aggregation for table %s.%s",
                    paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
            return Optional.empty();
        }
    }

    private boolean isMinMaxSupported(DataType paimonType)
    {
        // Based on PushDownUtils.minmaxAvailable()
        return paimonType instanceof org.apache.paimon.types.BooleanType
                || paimonType instanceof org.apache.paimon.types.TinyIntType
                || paimonType instanceof org.apache.paimon.types.SmallIntType
                || paimonType instanceof org.apache.paimon.types.IntType
                || paimonType instanceof org.apache.paimon.types.BigIntType
                || paimonType instanceof org.apache.paimon.types.FloatType
                || paimonType instanceof org.apache.paimon.types.DoubleType
                || paimonType instanceof org.apache.paimon.types.DateType;
    }

    private Object convertPaimonValueToTrino(Object paimonValue, DataType paimonType, Type trinoType)
    {
        if (paimonValue == null) {
            return null;
        }

        // Handle numeric types
        if (paimonType instanceof org.apache.paimon.types.TinyIntType
                || paimonType instanceof org.apache.paimon.types.SmallIntType
                || paimonType instanceof org.apache.paimon.types.IntType) {
            return ((Number) paimonValue).longValue();
        }
        if (paimonType instanceof org.apache.paimon.types.BigIntType) {
            return ((Number) paimonValue).longValue();
        }
        if (paimonType instanceof org.apache.paimon.types.FloatType) {
            // Trino represents REAL as long bits
            return (long) Float.floatToIntBits(((Number) paimonValue).floatValue());
        }
        if (paimonType instanceof org.apache.paimon.types.DoubleType) {
            return ((Number) paimonValue).doubleValue();
        }
        if (paimonType instanceof org.apache.paimon.types.BooleanType) {
            return paimonValue;
        }
        if (paimonType instanceof org.apache.paimon.types.DateType) {
            // Paimon stores date as days since epoch (int)
            return ((Number) paimonValue).longValue();
        }

        return paimonValue;
    }

    private boolean canPushdownAggregation(List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments)
    {
        for (AggregateFunction aggregate : aggregates) {
            String functionName = aggregate.getFunctionName().toLowerCase();

            // Don't support DISTINCT
            if (aggregate.isDistinct()) {
                return false;
            }
            // Don't support filters
            if (aggregate.getFilter().isPresent()) {
                return false;
            }

            if ("count".equals(functionName)) {
                // COUNT(*) and COUNT(non-null column) are supported
                if (!aggregate.getArguments().isEmpty() && !isCountNonNull(aggregate, assignments)) {
                    return false;
                }
            }
            else if ("min".equals(functionName) || "max".equals(functionName)) {
                // MIN/MAX requires exactly one argument
                if (aggregate.getArguments().size() != 1) {
                    return false;
                }
                // Argument must be a column reference
                ConnectorExpression argument = aggregate.getArguments().get(0);
                if (!(argument instanceof Variable)) {
                    return false;
                }
                // Column must exist in assignments
                String columnName = ((Variable) argument).getName();
                if (!assignments.containsKey(columnName)) {
                    return false;
                }
            }
            else {
                // Other aggregations not supported
                return false;
            }
        }
        return !aggregates.isEmpty();
    }

    private boolean isCountNonNull(AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        if (aggregate.getArguments().size() != 1) {
            return false;
        }
        ConnectorExpression argument = aggregate.getArguments().get(0);
        if (!(argument instanceof Variable)) {
            return false;
        }
        String columnName = ((Variable) argument).getName();
        ColumnHandle sourceColumnHandle = assignments.get(columnName);
        if (!(sourceColumnHandle instanceof PaimonColumnHandle)) {
            return false;
        }
        return !((PaimonColumnHandle) sourceColumnHandle).logicalType().isNullable();
    }

    private boolean isPartitionOnlyFilter(Table table, io.trino.spi.predicate.TupleDomain<PaimonColumnHandle> filter)
    {
        if (filter.isAll() || filter.isNone()) {
            return true;
        }

        HashMap<PaimonColumnHandle, Domain> acceptedDomains = new LinkedHashMap<>();
        HashMap<PaimonColumnHandle, Domain> unsupportedDomains = new LinkedHashMap<>();
        new PaimonFilterConverter(table.rowType()).convert(filter, acceptedDomains, unsupportedDomains);

        if (!unsupportedDomains.isEmpty()) {
            return false;
        }

        Set<String> acceptedFields = acceptedDomains.keySet().stream()
                .map(PaimonColumnHandle::getColumnName)
                .collect(Collectors.toSet());

        return new HashSet<>(table.partitionKeys()).containsAll(acceptedFields);
    }

    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
            ConnectorSession session,
            ConnectorTableHandle handle,
            long topNCount,
            List<SortItem> sortItems,
            Map<String, ColumnHandle> assignments)
    {
        // Split-level TopN pruning based on file statistics is not guaranteed to be conservative,
        // and may drop splits containing qualifying rows. Until we can guarantee correctness,
        // do not apply TopN pushdown.
        return Optional.empty();
    }

    // ========== View Support ==========

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition,
            Map<String, Object> viewProperties, boolean replace)
    {
        catalog.initSession(session);
        Identifier identifier = new Identifier(viewName.getSchemaName(), viewName.getTableName());

        try {
            // Build Paimon View from Trino ViewDefinition
            List<ConnectorViewDefinition.ViewColumn> columns = definition.getColumns();
            List<DataField> fields = IntStream.range(0, columns.size())
                    .mapToObj(index -> {
                        ConnectorViewDefinition.ViewColumn column = columns.get(index);
                        return new DataField(index, column.getName(), PaimonTypeUtils.toPaimonType(typeManager.getType(column.getType())));
                    })
                    .collect(toList());

            // Store Trino dialect SQL
            Map<String, String> dialects = new HashMap<>();
            dialects.put("trino", definition.getOriginalSql());

            // Build options from view metadata
            Map<String, String> options = new HashMap<>();
            definition.getComment().ifPresent(c -> options.put("comment", c));
            definition.getCatalog().ifPresent(catalog -> options.put("trino.catalog", catalog));
            definition.getSchema().ifPresent(schema -> options.put("trino.schema", schema));

            // Create ViewImpl
            org.apache.paimon.view.View paimonView = new org.apache.paimon.view.ViewImpl(identifier, fields,
                    definition.getOriginalSql(), dialects, definition.getComment().orElse(null), options);

            // Create the view in catalog
            catalog.createView(identifier, paimonView, replace);
        }
        catch (Catalog.ViewAlreadyExistException e) {
            if (!replace) {
                throw new TrinoException(io.trino.spi.StandardErrorCode.ALREADY_EXISTS,
                        format("View '%s' already exists", viewName));
            }
        }
        catch (Catalog.DatabaseNotExistException e) {
            throw new TrinoException(io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND,
                    format("Schema '%s' does not exist", viewName.getSchemaName()));
        }
        catch (Exception e) {
            throw new RuntimeException(format("Failed to create view '%s'", viewName), e);
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        catalog.initSession(session);
        Identifier identifier = new Identifier(viewName.getSchemaName(), viewName.getTableName());

        try {
            catalog.dropView(identifier, false);
        }
        catch (Catalog.ViewNotExistException e) {
            throw new TrinoException(io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND,
                    format("View '%s' does not exist", viewName));
        }
        catch (Exception e) {
            throw new RuntimeException(format("Failed to drop view '%s'", viewName), e);
        }
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        catalog.initSession(session);
        Identifier identifier = new Identifier(viewName.getSchemaName(), viewName.getTableName());

        try {
            org.apache.paimon.view.View paimonView = catalog.getView(identifier);

            // Convert Paimon View to Trino ConnectorViewDefinition
            List<ConnectorViewDefinition.ViewColumn> columns = paimonView.rowType().getFields().stream()
                    .map(field -> new ConnectorViewDefinition.ViewColumn(field.name(),
                            PaimonTypeUtils.fromPaimonType(field.type()).getTypeId(), Optional.empty()))
                    .collect(toList());

            // Get Trino-specific SQL from dialects, fallback to default query
            String originalSql = paimonView.dialects().getOrDefault("trino", paimonView.query());

            Optional<String> catalogName = Optional.ofNullable(paimonView.options().get("trino.catalog"));
            Optional<String> schemaName = Optional.ofNullable(paimonView.options().get("trino.schema"));
            if (catalogName.isEmpty()) {
                schemaName = Optional.empty();
            }

            return Optional.of(new ConnectorViewDefinition(originalSql, catalogName,
                    schemaName,
                    columns, paimonView.comment(), // comment
                    Optional.empty(), // owner
                    false, // runAsInvoker
                    List.of())); // path
        }
        catch (Catalog.ViewNotExistException e) {
            return Optional.empty();
        }
        catch (Exception e) {
            throw new RuntimeException(format("Failed to get view '%s'", viewName), e);
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        catalog.initSession(session);
        List<SchemaTableName> views = new ArrayList<>();
        schemaName.map(Collections::singletonList).orElseGet(catalog::listDatabases)
                .forEach(schema -> {
                    try {
                        catalog.listViews(schema).stream()
                                .map(view -> new SchemaTableName(schema, view))
                                .forEach(views::add);
                    }
                    catch (Catalog.DatabaseNotExistException e) {
                        // Schema doesn't exist, skip
                    }
                });
        return views;
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        catalog.initSession(session);

        if (schemaName.isEmpty()) {
            // If no schema specified, return empty map
            return Map.of();
        }

        try {
            List<String> viewNames = catalog.listViews(schemaName.get());
            Map<SchemaTableName, ConnectorViewDefinition> views = new HashMap<>();

            for (String viewName : viewNames) {
                SchemaTableName tableName = new SchemaTableName(schemaName.get(), viewName);
                getView(session, tableName).ifPresent(def -> views.put(tableName, def));
            }

            return views;
        }
        catch (Catalog.DatabaseNotExistException e) {
            // Schema doesn't exist, return empty map per Trino convention
            return Map.of();
        }
        catch (Exception e) {
            throw new RuntimeException(format("Failed to list views in schema '%s'", schemaName.orElse("ALL")), e);
        }
    }

    @Override
    public void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        catalog.initSession(session);
        Identifier identifier = new Identifier(viewName.getSchemaName(), viewName.getTableName());

        try {
            List<org.apache.paimon.view.ViewChange> changes = List
                    .of(org.apache.paimon.view.ViewChange.updateComment(comment.orElse(null)));
            catalog.alterView(identifier, changes, false);
        }
        catch (Catalog.ViewNotExistException e) {
            throw new TrinoException(io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND,
                    format("View '%s' does not exist", viewName));
        }
        catch (Exception e) {
            throw new RuntimeException(format("Failed to set comment on view '%s'", viewName), e);
        }
    }

    @Override
    public io.trino.spi.statistics.TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        catalog.initSession(session);
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;

        try {
            Table table = paimonTableHandle.tableWithDynamicOptions(catalog, session);
            org.apache.paimon.table.source.ReadBuilder readBuilder = table.newReadBuilder();

            // Apply filters if present
            new PaimonFilterConverter(table.rowType()).convert(paimonTableHandle.getFilter()).ifPresent(readBuilder::withFilter);

            // Get splits with statistics (do NOT drop stats)
            List<org.apache.paimon.table.source.Split> splits = readBuilder.newScan().plan().splits();

            // Calculate total row count from splits
            long totalRowCount = splits.stream()
                    .mapToLong(org.apache.paimon.table.source.Split::rowCount)
                    .sum();

            // Build table statistics with column-level statistics
            io.trino.spi.statistics.TableStatistics.Builder statisticsBuilder = io.trino.spi.statistics.TableStatistics.builder()
                    .setRowCount(io.trino.spi.statistics.Estimate.of(totalRowCount));

            // Add column-level statistics if we have projected columns
            if (paimonTableHandle.getProjectedColumns().isPresent() && totalRowCount > 0) {
                Map<String, PaimonColumnHandle> columnsByName = paimonTableHandle.getProjectedColumns().get().stream()
                        .map(PaimonColumnHandle.class::cast)
                        .collect(Collectors.toMap(PaimonColumnHandle::getColumnName, col -> col));

                Map<PaimonColumnHandle, io.trino.spi.statistics.ColumnStatistics> columnStats =
                        buildColumnStatistics(splits, table.rowType(), columnsByName, totalRowCount);

                columnStats.forEach(statisticsBuilder::setColumnStatistics);
            }

            return statisticsBuilder.build();
        }
        catch (Exception e) {
            // If we fail to get statistics, return empty statistics
            // This allows queries to continue with default cost-based optimization
            return io.trino.spi.statistics.TableStatistics.empty();
        }
    }

    private Map<PaimonColumnHandle, io.trino.spi.statistics.ColumnStatistics> buildColumnStatistics(
            List<org.apache.paimon.table.source.Split> splits,
            org.apache.paimon.types.RowType rowType,
            Map<String, PaimonColumnHandle> columnsByName,
            long totalRowCount)
    {
        Map<PaimonColumnHandle, io.trino.spi.statistics.ColumnStatistics> result = new HashMap<>();

        // Only process DataSplit which contains DataFileMeta with statistics
        List<org.apache.paimon.table.source.DataSplit> dataSplits = splits.stream()
                .filter(split -> split instanceof org.apache.paimon.table.source.DataSplit)
                .map(split -> (org.apache.paimon.table.source.DataSplit) split)
                .collect(toList());

        if (dataSplits.isEmpty()) {
            return result;
        }

        // Build field name to index mapping
        List<String> fieldNames = rowType.getFieldNames();
        Map<String, Integer> fieldIndexMap = new HashMap<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            fieldIndexMap.put(fieldNames.get(i).toLowerCase(), i);
        }

        // Aggregate statistics for each column
        for (Map.Entry<String, PaimonColumnHandle> entry : columnsByName.entrySet()) {
            String columnName = entry.getKey();
            PaimonColumnHandle columnHandle = entry.getValue();

            Integer fieldIndex = fieldIndexMap.get(columnName.toLowerCase());
            if (fieldIndex == null) {
                continue;
            }

            try {
                io.trino.spi.statistics.ColumnStatistics columnStats =
                        aggregateColumnStatistics(dataSplits, fieldIndex, columnHandle.getTrinoType(), totalRowCount);
                result.put(columnHandle, columnStats);
            }
            catch (Exception e) {
                // Skip this column if we fail to get statistics
                log.debug(e, "Failed to get statistics for column: %s", columnName);
            }
        }

        return result;
    }

    private io.trino.spi.statistics.ColumnStatistics aggregateColumnStatistics(
            List<org.apache.paimon.table.source.DataSplit> dataSplits,
            int fieldIndex,
            Type trinoType,
            long totalRowCount)
    {
        io.trino.spi.statistics.ColumnStatistics.Builder builder = new io.trino.spi.statistics.ColumnStatistics.Builder();

        long totalNullCount = 0;
        boolean hasNullCount = false;

        // Aggregate statistics from all data files
        for (org.apache.paimon.table.source.DataSplit split : dataSplits) {
            for (org.apache.paimon.io.DataFileMeta fileMeta : split.dataFiles()) {
                org.apache.paimon.stats.SimpleStats valueStats = fileMeta.valueStats();

                // Aggregate null counts
                org.apache.paimon.data.BinaryArray nullCounts = valueStats.nullCounts();
                if (nullCounts != null && fieldIndex < nullCounts.size()) {
                    Long nullCount = nullCounts.getLong(fieldIndex);
                    if (nullCount != null) {
                        totalNullCount += nullCount;
                        hasNullCount = true;
                    }
                }
            }
        }

        // Set null fraction
        if (hasNullCount && totalRowCount > 0) {
            builder.setNullsFraction(io.trino.spi.statistics.Estimate.of((double) totalNullCount / totalRowCount));
        }

        return builder.build();
    }

    // TODO: Long-term Enhancement - SUPPORTS_JOIN_PUSHDOWN
    // Join pushdown to storage layer is extremely rare and not recommended.
    // Status: Not implemented (almost no Trino connectors support this)
    // Recommendation: Do NOT implement - storage layers are not designed for JOIN
    // operations
    // Trino's distributed JOIN is already highly optimized
    // Reference: tmp-docs/PUSHDOWN_OPTIMIZATION_GUIDE.md#6-supports_join_pushdown
}
