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

import io.airlift.slice.Slice;
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
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
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.Domain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
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
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
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
import java.util.function.Function;
import java.util.stream.Collectors;

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
    private static final String TAG_PREFIX = "tag-";

    private static boolean containSameElements(List<? extends ColumnHandle> first, List<? extends ColumnHandle> second)
    {
        return new HashSet<>(first).equals(new HashSet<>(second));
    }

    // todo support dynamic bucket table
    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
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
            Optional<ConnectorTableLayout> layout, RetryMode retryMode)
    {
        // Deprecated method - delegate to the new version with replace parameter
        return beginCreateTable(session, tableMetadata, layout, retryMode, false);
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
            ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        // Deprecated method - delegate to new version with sourceTableHandles
        return finishInsert(session, insertHandle, Collections.emptyList(), fragments, computedStatistics);
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
            RetryMode retryMode)
    {
        return new PaimonMergeTableHandle((PaimonTableHandle) tableHandle);
    }

    @Override
    public void finishMerge(ConnectorSession session, ConnectorMergeTableHandle mergeTableHandle,
            Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
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
            catalog.createDatabase(schemaName, true);
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

    @Deprecated
    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        // Deprecated method - delegate to the version with table versions
        return getTableHandle(session, tableName, Optional.empty(), Optional.empty());
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
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        // Deprecated method - delegate to the new SaveMode version
        createTable(session, tableMetadata,
                ignoreExisting ? io.trino.spi.connector.SaveMode.IGNORE : io.trino.spi.connector.SaveMode.FAIL);
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
        List<SchemaTableName> tableNames;
        if (prefix.getTable().isPresent()) {
            tableNames = Collections.singletonList(prefix.toSchemaTableName());
        }
        else {
            tableNames = listTables(session, prefix.getSchema());
        }

        return tableNames.stream().collect(Collectors.toMap(Function.identity(),
                table -> ((PaimonTableHandle) requireNonNull(getTableHandle(session, table))).columnMetadatas(catalog)));
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

        return tableNames.stream().map(tableName -> io.trino.spi.connector.TableColumnsMetadata.forTable(tableName,
                ((PaimonTableHandle) requireNonNull(getTableHandle(session, tableName))).columnMetadatas(catalog)))
                .iterator();
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
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
        changes.add(new SchemaChange.UpdateColumnComment(new String[]{paimonColumnHandle.getColumnName()},
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

    // TODO: Long-term Enhancement - SUPPORTS_AGGREGATION_PUSHDOWN
    // Aggregation pushdown (COUNT, SUM, AVG, etc.) to storage layer is a complex
    // feature.
    // Current status: Not implemented (Iceberg and Hudi also don't support this)
    //
    // Implementation path:
    // 1. Research if Paimon supports aggregation at storage level
    // - Check:
    // /opt/source/paimon/paimon-core/src/main/java/org/apache/paimon/table/
    // - Look for: Statistics, Aggregation-related interfaces
    // 2. If supported, implement applyAggregation() method:
    // @Override
    // public Optional<AggregationApplicationResult<ConnectorTableHandle>>
    // applyAggregation(
    // ConnectorSession session,
    // ConnectorTableHandle handle,
    // List<AggregateFunction> aggregates,
    // Map<String, ColumnHandle> assignments,
    // List<List<ColumnHandle>> groupingSets)
    // {
    // // Convert Trino aggregates to Paimon API
    // // Return partial aggregation results
    // }
    // 3. Handle edge cases: NULL values, precision, type conversions
    // 4. Performance benchmarking - pushdown not always faster
    //
    // Reference:
    // tmp-docs/PUSHDOWN_OPTIMIZATION_GUIDE.md#5-supports_aggregation_pushdown
    // Estimated effort: 40-60 hours (requires dedicated research)
    // Priority: P3 (High uncertainty, major implementation effort)
    //
    // ========== View Support ==========

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition,
            boolean replace)
    {
        catalog.initSession(session);
        Identifier identifier = new Identifier(viewName.getSchemaName(), viewName.getTableName());

        try {
            // Build Paimon View from Trino ViewDefinition
            List<DataField> fields = definition.getColumns().stream().map(column -> new DataField(0, // id will be
                                                                                                     // auto-assigned
                    column.getName(), PaimonTypeUtils.toPaimonType(typeManager.getType(column.getType()))))
                    .collect(toList());

            // Store Trino dialect SQL
            Map<String, String> dialects = new HashMap<>();
            dialects.put("trino", definition.getOriginalSql());

            // Build options from view metadata
            Map<String, String> options = new HashMap<>();
            definition.getComment().ifPresent(c -> options.put("comment", c));

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

            return Optional.of(new ConnectorViewDefinition(originalSql, Optional.empty(), // catalog
                    Optional.of(viewName.getSchemaName()), // schema
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
            throw new TrinoException(io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND,
                    format("Schema '%s' does not exist", schemaName.get()));
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

    // TODO: Long-term Enhancement - SUPPORTS_JOIN_PUSHDOWN
    // Join pushdown to storage layer is extremely rare and not recommended.
    // Status: Not implemented (almost no Trino connectors support this)
    // Recommendation: Do NOT implement - storage layers are not designed for JOIN
    // operations
    // Trino's distributed JOIN is already highly optimized
    // Reference: tmp-docs/PUSHDOWN_OPTIMIZATION_GUIDE.md#6-supports_join_pushdown
}
