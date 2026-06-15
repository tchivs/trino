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
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.manifest.PartitionEntry;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.plugin.paimon.PaimonColumnHandle.TRINO_ROW_ID_NAME;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_COMMIT_ERROR;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_METADATA_ERROR;
import static io.trino.spi.StandardErrorCode.COLUMN_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.READ_ONLY_VIOLATION;
import static io.trino.spi.StandardErrorCode.SCHEMA_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.connector.RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
import static io.trino.spi.expression.Constant.TRUE;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.paimon.catalog.Catalog.SYSTEM_DATABASE_NAME;
import static org.apache.paimon.utils.Preconditions.checkArgument;

public record PaimonMetadata(PaimonCatalog catalog,
                             io.trino.spi.type.TypeManager typeManager) implements ConnectorMetadata
{
    public PaimonMetadata
    {
        catalog = requireNonNull(catalog, "catalog is null");
        typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("insert layout", tableHandle);
        Catalog sessionCatalog = catalog.forSession(session);
        Table table = paimonTableHandle.table(sessionCatalog);
        FileStoreTable storeTable = latestFileStoreTable(table, "insert layout");
        BucketMode bucketMode = storeTable.bucketMode();
        switch (bucketMode) {
            case HASH_FIXED :
                try {
                    return Optional.of(new ConnectorTableLayout(
                            new PaimonPartitioningHandle(InstantiationUtil.serializeObject(storeTable.schema())),
                            storeTable.schema().bucketKeys(), false));
                }
                catch (IOException e) {
                    throw new TrinoException(PAIMON_METADATA_ERROR,
                            format("Failed to prepare Paimon insert layout for table '%s'",
                                    schemaTableName(paimonTableHandle)),
                            e);
                }
            case BUCKET_UNAWARE :
                return Optional.empty();
            default :
                throw new TrinoException(NOT_SUPPORTED, "Unsupported table bucket mode: " + bucketMode);
        }
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata,
            Optional<ConnectorTableLayout> layout, RetryMode retryMode)
    {
        return beginCreateTable(session, tableMetadata, layout, retryMode, false);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata,
            Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableMetadata, "tableMetadata is null");
        requireNonNull(layout, "layout is null");
        requireNonNull(retryMode, "retryMode is null");
        validateNoQueryRetries(retryMode);
        createTable(session, tableMetadata,
                replace ? io.trino.spi.connector.SaveMode.REPLACE : io.trino.spi.connector.SaveMode.FAIL);
        PaimonTableHandle tableHandle = requireNonNull(getTableHandle(session, tableMetadata.getTable(),
                Collections.emptyMap()));
        Catalog sessionCatalog = catalog.forSession(session);
        Table table = tableHandle.table(sessionCatalog);
        validateNoCaseInsensitiveDuplicateCreatedFieldNames(table.rowType().getFields(), tableMetadata.getTable());
        return tableHandle.withWriteColumns(tableMetadata.getColumns().stream()
                .map(column -> {
                    DataField field = createdTableField(table.rowType().getFields(), column.getName(),
                            tableMetadata.getTable());
                    return PaimonColumnHandle.of(field.name(), field.type(), typeManager);
                })
                .collect(toList()));
    }

    private static void validateNoCaseInsensitiveDuplicateCreatedFieldNames(List<DataField> fields,
            SchemaTableName tableName)
    {
        Set<String> fieldNames = new HashSet<>();
        for (DataField field : fields) {
            String lowerFieldName = FieldNameUtils.toLowerCase(field.name());
            if (!fieldNames.add(lowerFieldName)) {
                throw new IllegalStateException(
                        "Created Paimon table '%s' schema contains case-insensitive duplicate field name '%s'"
                                .formatted(tableName, lowerFieldName));
            }
        }
    }

    private static DataField createdTableField(List<DataField> fields, String columnName, SchemaTableName tableName)
    {
        String lowerColumnName = FieldNameUtils.toLowerCase(columnName);
        for (DataField field : fields) {
            if (FieldNameUtils.toLowerCase(field.name()).equals(lowerColumnName)) {
                return field;
            }
        }
        throw new IllegalStateException(format(
                "Created Paimon table '%s' is missing write column '%s'",
                tableName, columnName));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session,
            ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return commit(session, getOutputTableHandle(tableHandle), fragments,
                PaimonSessionProperties.InsertExistingPartitionsBehavior.APPEND);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns, RetryMode retryMode)
    {
        requireNonNull(session, "session is null");
        requireNonNull(retryMode, "retryMode is null");
        validateNoQueryRetries(retryMode);
        return getTableHandle("begin insert", tableHandle).withWriteColumns(columns);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session,
            ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return finishInsert(session, insertHandle, Collections.emptyList(), fragments, computedStatistics);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session,
            ConnectorInsertTableHandle insertHandle, List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        requireNonNull(session, "session is null");
        return commit(session, getInsertTableHandle(insertHandle), fragments,
                PaimonSessionProperties.getInsertExistingPartitionsBehavior(session));
    }

    private Optional<ConnectorOutputMetadata> commit(
            ConnectorSession session,
            PaimonTableHandle tableHandle,
            Collection<Slice> fragments,
            PaimonSessionProperties.InsertExistingPartitionsBehavior insertBehavior)
    {
        requireNonNull(session, "session is null");
        requireNonNull(insertBehavior, "insertBehavior is null");
        List<Slice> fragmentsList = copyFragments(fragments);
        if (fragmentsList.isEmpty()) {
            return Optional.empty();
        }

        List<CommitMessage> commitMessages = deserializeCommitMessages(fragmentsList);
        Catalog sessionCatalog = catalog.forSession(session);
        FileStoreTable fileStoreTable = latestFileStoreTable(tableHandle.tableWithWriteDynamicOptions(sessionCatalog),
                "commit writes");

        try {
            if (insertBehavior == PaimonSessionProperties.InsertExistingPartitionsBehavior.ERROR) {
                validateInsertTargetIsNew(fileStoreTable, schemaTableName(tableHandle), commitMessages);
            }

            BatchWriteBuilder batchWriteBuilder = fileStoreTable.newBatchWriteBuilder();
            if (insertBehavior == PaimonSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE) {
                PaimonTableSupport.validateInsertOverwrite(fileStoreTable);
                batchWriteBuilder.withOverwrite();
            }

            try (BatchTableCommit commit = batchWriteBuilder.newCommit()) {
                commit.commit(commitMessages);
            }
        }
        catch (Exception e) {
            if (e instanceof TrinoException trinoException) {
                throw trinoException;
            }
            if (e instanceof IllegalArgumentException || e instanceof IllegalStateException) {
                throw (RuntimeException) e;
            }
            if (e instanceof RuntimeException runtimeException) {
                throw new TrinoException(PAIMON_COMMIT_ERROR, "Failed to commit Paimon write fragments", runtimeException);
            }
            throw new TrinoException(PAIMON_COMMIT_ERROR, "Failed to commit Paimon write fragments", e);
        }
        return Optional.empty();
    }

    private static List<CommitMessage> deserializeCommitMessages(List<Slice> fragments)
    {
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        return fragments.stream().map(slice -> {
            try {
                return serializer.deserialize(serializer.getVersion(), slice.getBytes());
            }
            catch (IOException e) {
                throw new TrinoException(PAIMON_COMMIT_ERROR, "Failed to deserialize Paimon commit fragment", e);
            }
        }).collect(toList());
    }

    private static void validateInsertTargetIsNew(
            FileStoreTable fileStoreTable,
            SchemaTableName tableName,
            List<CommitMessage> commitMessages)
    {
        Set<BinaryRow> existingPartitions = fileStoreTable.newSnapshotReader().partitionEntries().stream()
                .map(PartitionEntry::partition)
                .collect(Collectors.toSet());
        if (existingPartitions.isEmpty()) {
            return;
        }

        if (fileStoreTable.partitionKeys().isEmpty()) {
            throw new TrinoException(READ_ONLY_VIOLATION,
                    format("Cannot insert into an existing non-partitioned Paimon table: %s", tableName));
        }

        boolean writesExistingPartition = commitMessages.stream()
                .map(CommitMessage::partition)
                .anyMatch(existingPartitions::contains);
        if (writesExistingPartition) {
            throw new TrinoException(READ_ONLY_VIOLATION,
                    format("Cannot insert into an existing partition of Paimon table: %s", tableName));
        }
    }

    private static List<Slice> copyFragments(Collection<Slice> fragments)
    {
        requireNonNull(fragments, "fragments is null");
        fragments.forEach(fragment -> requireNonNull(fragment, "fragments contains null fragment"));
        return List.copyOf(fragments);
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(session, "session is null");
        getTableHandle("row change paradigm", tableHandle);
        return DELETE_ROW_AND_INSERT_ROW;
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("merge row id", tableHandle);
        Catalog sessionCatalog = catalog.forSession(session);
        Table table = paimonTableHandle.table(sessionCatalog);
        FileStoreTable storeTable = requireFileStoreTable(table, "merge row id").copyWithLatestSchema();
        BucketMode bucketMode = storeTable.bucketMode();
        if (bucketMode != BucketMode.HASH_FIXED) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported table bucket mode: " + bucketMode);
        }
        if (storeTable.primaryKeys().isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Paimon merge row id requires primary keys");
        }
        DataField[] row = storeTable.primaryKeys().stream()
                .map(primaryKey -> {
                    if (!storeTable.rowType().containsField(primaryKey)) {
                        throw new IllegalStateException("Paimon primary key '%s' is not present in table schema %s"
                                .formatted(primaryKey, storeTable.rowType().getFieldNames()));
                    }
                    return storeTable.rowType().getField(primaryKey);
                })
                .toArray(DataField[]::new);
        return PaimonColumnHandle.of(TRINO_ROW_ID_NAME, DataTypes.ROW(row), typeManager);
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getUpdateLayout(ConnectorSession session,
            ConnectorTableHandle tableHandle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("update layout", tableHandle);
        Catalog sessionCatalog = catalog.forSession(session);
        Table table = paimonTableHandle.table(sessionCatalog);
        FileStoreTable storeTable = requireFileStoreTable(table, "update layout").copyWithLatestSchema();
        BucketMode bucketMode = storeTable.bucketMode();
        if (bucketMode != BucketMode.HASH_FIXED) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported table bucket mode: " + bucketMode);
        }
        try {
            return Optional.of(new PaimonPartitioningHandle(InstantiationUtil.serializeObject(storeTable.schema())));
        }
        catch (IOException e) {
            throw new TrinoException(PAIMON_METADATA_ERROR,
                    format("Failed to prepare Paimon update layout for table '%s'",
                            schemaTableName(paimonTableHandle)),
                    e);
        }
    }

    private static FileStoreTable requireFileStoreTable(Table table, String operation)
    {
        return PaimonTableSupport.requireFileStoreTable(table, operation);
    }

    private static FileStoreTable latestFileStoreTable(Table table, String operation)
    {
        return requireFileStoreTable(table, operation).copyWithLatestSchema();
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle,
            RetryMode retryMode)
    {
        requireNonNull(session, "session is null");
        requireNonNull(retryMode, "retryMode is null");
        validateNoQueryRetries(retryMode);
        PaimonTableHandle paimonTableHandle = getTableHandle("begin merge", tableHandle);
        Catalog sessionCatalog = catalog.forSession(session);
        Table table = paimonTableHandle.table(sessionCatalog);
        FileStoreTable storeTable = latestFileStoreTable(table, "merge");
        BucketMode bucketMode = storeTable.bucketMode();
        if (bucketMode != BucketMode.HASH_FIXED) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported table bucket mode: " + bucketMode);
        }
        List<ColumnHandle> writeColumns = storeTable.rowType().getFields().stream()
                .map(column -> PaimonColumnHandle.of(column.name(), column.type(), typeManager))
                .collect(toList());
        return new PaimonMergeTableHandle(paimonTableHandle.withWriteColumns(writeColumns));
    }

    private static void validateNoQueryRetries(RetryMode retryMode)
    {
        if (retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }
    }

    @Override
    public void finishMerge(ConnectorSession session, ConnectorMergeTableHandle mergeTableHandle,
            Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        commit(session, getMergeTableHandle(mergeTableHandle), fragments,
                PaimonSessionProperties.InsertExistingPartitionsBehavior.APPEND);
    }

    static PaimonTableHandle getOutputTableHandle(ConnectorOutputTableHandle tableHandle)
    {
        if (!(requireNonNull(tableHandle, "tableHandle is null") instanceof PaimonTableHandle paimonTableHandle)) {
            throw new IllegalStateException("Paimon finish create table requires PaimonTableHandle, got: "
                    + tableHandle.getClass().getName());
        }
        return paimonTableHandle;
    }

    static PaimonTableHandle getInsertTableHandle(ConnectorInsertTableHandle insertHandle)
    {
        if (!(requireNonNull(insertHandle, "insertHandle is null") instanceof PaimonTableHandle paimonTableHandle)) {
            throw new IllegalStateException("Paimon finish insert requires PaimonTableHandle, got: "
                    + insertHandle.getClass().getName());
        }
        return paimonTableHandle;
    }

    static PaimonTableHandle getMergeTableHandle(ConnectorMergeTableHandle mergeTableHandle)
    {
        ConnectorTableHandle tableHandle = requireNonNull(mergeTableHandle, "mergeTableHandle is null").getTableHandle();
        if (!(requireNonNull(tableHandle, "mergeTableHandle tableHandle is null") instanceof PaimonTableHandle paimonTableHandle)) {
            throw new IllegalStateException("Paimon finish merge requires PaimonTableHandle, got: "
                    + tableHandle.getClass().getName());
        }
        return paimonTableHandle;
    }

    static PaimonTableHandle getTableHandle(String operation, ConnectorTableHandle tableHandle)
    {
        if (!(requireNonNull(tableHandle, "tableHandle is null") instanceof PaimonTableHandle paimonTableHandle)) {
            throw new IllegalStateException("Paimon " + operation + " requires PaimonTableHandle, got: "
                    + tableHandle.getClass().getName());
        }
        return paimonTableHandle;
    }

    static PaimonColumnHandle getColumnHandle(String operation, ColumnHandle columnHandle)
    {
        if (!(requireNonNull(columnHandle, "columnHandle is null") instanceof PaimonColumnHandle paimonColumnHandle)) {
            throw new IllegalStateException("Paimon " + operation + " requires PaimonColumnHandle, got: "
                    + columnHandle.getClass().getName());
        }
        return paimonColumnHandle;
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        requireNonNull(session, "session is null");
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(schemaName), "schemaName cannot be null or empty");
        if (SYSTEM_DATABASE_NAME.equals(schemaName)) {
            return true;
        }
        Catalog sessionCatalog = catalog.forSession(session);
        try {
            sessionCatalog.getDatabase(schemaName);
            return true;
        }
        catch (Catalog.DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        requireNonNull(session, "session is null");
        Catalog sessionCatalog = catalog.forSession(session);
        List<String> schemaNames = new ArrayList<>(sessionCatalog.listDatabases());
        if (!schemaNames.contains(SYSTEM_DATABASE_NAME)) {
            schemaNames.add(SYSTEM_DATABASE_NAME);
        }
        return schemaNames;
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties,
            TrinoPrincipal owner)
    {
        requireNonNull(session, "session is null");
        requireNonNull(properties, "properties is null");
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(schemaName), "schemaName cannot be null or empty");
        rejectSystemSchemaWrite(schemaName, "create schema");

        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.createDatabase(schemaName, false);
        }
        catch (Catalog.DatabaseAlreadyExistException e) {
            throw new TrinoException(SCHEMA_ALREADY_EXISTS, format("Schema '%s' already exists", schemaName), e);
        }
        catch (Exception e) {
            throw paimonMetadataException(format("Failed to create Paimon schema '%s'", schemaName), e);
        }
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        requireNonNull(session, "session is null");
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(schemaName), "schemaName cannot be null or empty");
        rejectSystemSchemaWrite(schemaName, "drop schema");
        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.dropDatabase(schemaName, false, cascade);
        }
        catch (Catalog.DatabaseNotEmptyException e) {
            throw new TrinoException(SCHEMA_NOT_EMPTY, format("Schema '%s' is not empty", schemaName), e);
        }
        catch (Catalog.DatabaseNotExistException e) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", schemaName));
        }
        catch (Exception e) {
            throw paimonMetadataException(format("Failed to drop Paimon schema '%s'", schemaName), e);
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(startVersion, "startVersion is null");
        requireNonNull(endVersion, "endVersion is null");
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
                    String versionValue;
                    if (versionType instanceof VarcharType) {
                        versionValue = BinaryString.fromBytes(((Slice) version.getVersion()).getBytes()).toString();
                    }
                    else {
                        versionValue = version.getVersion().toString();
                    }
                    if (versionValue.isBlank()) {
                        throw new TrinoException(INVALID_ARGUMENTS, "Paimon table version may not be blank");
                    }
                    dynamicOptions.put(CoreOptions.SCAN_VERSION.key(), versionValue);
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
        return getTableHandle(session, tableName, Optional.empty(), Optional.empty());
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        requireNonNull(session, "session is null");
        getTableHandle("table properties", table);
        return new ConnectorTableProperties();
    }

    public PaimonTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName,
            Map<String, String> dynamicOptions)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(dynamicOptions, "dynamicOptions is null");
        PaimonTableHandle tableHandle = new PaimonTableHandle(tableName.getSchemaName(), tableName.getTableName(),
                dynamicOptions);
        Catalog sessionCatalog = catalog.forSession(session);
        try {
            sessionCatalog.getTable(Identifier.create(tableName.getSchemaName(), tableName.getTableName()));
            return tableHandle;
        }
        catch (Catalog.TableNotExistException e) {
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("table metadata", tableHandle);
        Catalog sessionCatalog = catalog.forSession(session);
        return paimonTableHandle.tableMetadata(sessionCatalog, typeManager, session);
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle,
            Map<String, Optional<Object>> properties)
    {
        requireNonNull(session, "session is null");
        requireNonNull(properties, "properties is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("set table properties", tableHandle);
        rejectSystemSchemaWrite(paimonTableHandle.getSchemaName(), "set table properties");
        if (properties.isEmpty()) {
            return;
        }
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        rejectUnsupportedTablePropertyUpdates(properties);
        List<SchemaChange> changes = new ArrayList<>();

        // Handle both setting and removing options
        // When SET PROPERTIES x = DEFAULT is used, the value will be Optional.empty()
        for (Map.Entry<String, Optional<Object>> entry : properties.entrySet()) {
            String propertyName = requireNonNull(entry.getKey(), "properties contains null property name");
            checkArgument(!StringUtils.isNullOrWhitespaceOnly(propertyName), "properties contains blank property name");
            String key = PaimonTableOptionUtils.toPaimonOptionKey(propertyName);
            Optional<Object> value = requireNonNull(entry.getValue(),
                    "properties contains null value for property '%s'".formatted(propertyName));

            if (value.isPresent()) {
                // Set the property to the specified value
                changes.add(SchemaChange.setOption(key,
                        PaimonTableOptionUtils.requireNonBlankStringOptionValue(propertyName, value.get())));
            }
            else {
                // Remove the property (SET PROPERTIES x = DEFAULT)
                changes.add(SchemaChange.removeOption(key));
            }
        }

        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw paimonAlterTableException(schemaTableName(paimonTableHandle), e);
        }
    }

    private static void rejectUnsupportedTablePropertyUpdates(Map<String, Optional<Object>> properties)
    {
        List<String> unsupportedProperties = properties.keySet().stream()
                .filter(property -> PaimonTableOptions.PRIMARY_KEY_IDENTIFIER.equals(property)
                        || PaimonTableOptions.PARTITIONED_BY_PROPERTY.equals(property))
                .sorted()
                .toList();
        if (!unsupportedProperties.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "The following properties cannot be updated: " + String.join(", ", unsupportedProperties));
        }
    }

    private static void rejectSystemSchemaWrite(String schemaName, String operation)
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(operation, "operation is null");
        if (SYSTEM_DATABASE_NAME.equals(schemaName)) {
            throw new TrinoException(NOT_SUPPORTED,
                    "Paimon " + operation + " is not supported for the system schema '" + SYSTEM_DATABASE_NAME + "'");
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(schemaName, "schemaName is null");
        schemaName.ifPresent(schema -> checkArgument(!StringUtils.isNullOrWhitespaceOnly(schema),
                "schemaName cannot be null or empty"));
        Catalog sessionCatalog = catalog.forSession(session);
        List<SchemaTableName> tables = new ArrayList<>();
        schemaName.map(Collections::singletonList)
                .orElseGet(() -> listSchemaNames(session))
                .forEach(schema -> tables.addAll(listTables(sessionCatalog, schema)));
        return tables;
    }

    private List<SchemaTableName> listTables(Catalog sessionCatalog, String schema)
    {
        try {
            return sessionCatalog.listTables(schema).stream().map(table -> new SchemaTableName(schema, table))
                    .collect(toList());
        }
        catch (Catalog.DatabaseNotExistException e) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", schema), e);
        }
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        createTable(session, tableMetadata,
                ignoreExisting ? io.trino.spi.connector.SaveMode.IGNORE : io.trino.spi.connector.SaveMode.FAIL);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata,
            io.trino.spi.connector.SaveMode saveMode)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableMetadata, "tableMetadata is null");
        requireNonNull(saveMode, "saveMode is null");
        SchemaTableName table = tableMetadata.getTable();
        rejectSystemSchemaWrite(table.getSchemaName(), "create table");
        Identifier identifier = Identifier.create(table.getSchemaName(), table.getTableName());
        Schema schema = prepareSchema(tableMetadata);

        try {
            Catalog sessionCatalog = catalog.forSession(session);
            if (saveMode == io.trino.spi.connector.SaveMode.REPLACE) {
                // For REPLACE mode, drop the table if it exists first
                try {
                    sessionCatalog.dropTable(identifier, false);
                }
                catch (Catalog.TableNotExistException e) {
                    // Table doesn't exist, continue with creation
                }
            }
            sessionCatalog.createTable(identifier, schema, saveMode == io.trino.spi.connector.SaveMode.IGNORE);
        }
        catch (Catalog.DatabaseNotExistException e) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", table.getSchemaName()));
        }
        catch (Catalog.TableAlreadyExistException e) {
            if (saveMode == io.trino.spi.connector.SaveMode.IGNORE) {
                return;
            }
            throw new TrinoException(TABLE_ALREADY_EXISTS, format("Table '%s' already exists", table), e);
        }
        catch (Exception e) {
            throw paimonMetadataException(format("Failed to create Paimon table '%s'", table), e);
        }
    }

    private Schema prepareSchema(ConnectorTableMetadata tableMetadata)
    {
        Map<String, Object> properties = new HashMap<>(tableMetadata.getProperties());
        Schema.Builder builder = Schema.newBuilder().primaryKey(PaimonTableOptions.getPrimaryKeys(properties))
                .partitionKeys(PaimonTableOptions.getPartitionedKeys(properties))
                .comment(tableMetadata.getComment().orElse(null));

        for (ColumnMetadata column : tableMetadata.getColumns()) {
            builder.column(column.getName(), toPaimonType(column), column.getComment());
        }

        PaimonTableOptionUtils.buildOptions(builder, properties);

        return builder.build();
    }

    private static DataType toPaimonType(ColumnMetadata column)
    {
        return PaimonTypeUtils.toPaimonType(column.getType()).copy(column.isNullable());
    }

    private static RuntimeException paimonAlterTableException(SchemaTableName tableName, Exception exception)
    {
        if (exception instanceof TrinoException trinoException) {
            return trinoException;
        }
        if (exception instanceof Catalog.TableNotExistException) {
            return new TrinoException(TABLE_NOT_FOUND, format("Table '%s' does not exist", tableName), exception);
        }
        if (exception instanceof Catalog.ColumnAlreadyExistException columnAlreadyExistException) {
            return new TrinoException(COLUMN_ALREADY_EXISTS,
                    format("Column '%s' already exists in table '%s'", columnAlreadyExistException.column(), tableName),
                    exception);
        }
        if (exception instanceof Catalog.ColumnNotExistException columnNotExistException) {
            return new TrinoException(COLUMN_NOT_FOUND,
                    format("Column '%s' does not exist in table '%s'", columnNotExistException.column(), tableName),
                    exception);
        }
        if (exception instanceof Catalog.DatabaseNotExistException) {
            return new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", tableName.getSchemaName()),
                    exception);
        }
        return paimonMetadataException(format("Failed to alter Paimon table '%s'", tableName), exception);
    }

    private static SchemaTableName schemaTableName(PaimonTableHandle tableHandle)
    {
        return new SchemaTableName(tableHandle.getSchemaName(), tableHandle.getTableName());
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle oldTableHandle = getTableHandle("rename table", tableHandle);
        requireNonNull(newTableName, "newTableName is null");
        rejectSystemSchemaWrite(oldTableHandle.getSchemaName(), "rename table");
        rejectSystemSchemaWrite(newTableName.getSchemaName(), "rename table");
        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.renameTable(new Identifier(oldTableHandle.getSchemaName(), oldTableHandle.getTableName()),
                    new Identifier(newTableName.getSchemaName(), newTableName.getTableName()), false);
        }
        catch (Catalog.TableNotExistException e) {
            throw new TrinoException(TABLE_NOT_FOUND, format("Table '%s.%s' does not exist",
                    oldTableHandle.getSchemaName(), oldTableHandle.getTableName()), e);
        }
        catch (Catalog.TableAlreadyExistException e) {
            throw new TrinoException(TABLE_ALREADY_EXISTS, format("Table '%s' already exists", newTableName), e);
        }
        catch (Exception e) {
            throw paimonMetadataException(
                    format("Failed to rename Paimon table '%s' to '%s'",
                            schemaTableName(oldTableHandle),
                            newTableName),
                    e);
        }
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("drop table", tableHandle);
        rejectSystemSchemaWrite(paimonTableHandle.getSchemaName(), "drop table");
        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.dropTable(new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName()), false);
        }
        catch (Catalog.TableNotExistException e) {
            throw new TrinoException(TABLE_NOT_FOUND, format("Table '%s.%s' does not exist",
                    paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName()), e);
        }
        catch (Exception e) {
            throw paimonMetadataException(
                    format("Failed to drop Paimon table '%s'", schemaTableName(paimonTableHandle)),
                    e);
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle table = getTableHandle("column handles", tableHandle);
        Catalog sessionCatalog = catalog.forSession(session);
        Map<String, ColumnHandle> handleMap = new HashMap<>();
        for (ColumnMetadata column : table.columnMetadatas(sessionCatalog, typeManager, session)) {
            handleMap.put(column.getName(), table.columnHandle(sessionCatalog, typeManager, session, column.getName()));
        }
        return handleMap;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("column metadata", tableHandle);
        PaimonColumnHandle paimonColumnHandle = getColumnHandle("column metadata", columnHandle);
        if (paimonColumnHandle.isRowId()) {
            return paimonColumnHandle.getColumnMetadata();
        }
        Catalog sessionCatalog = catalog.forSession(session);
        Table table = PaimonTableHandle.schemaAwareReadTable(
                paimonTableHandle.tableWithDynamicOptions(sessionCatalog, session),
                !paimonTableHandle.usesHistoricalReadSchema(session));
        try {
            return PaimonTableHandle.columnMetadata(
                    table,
                    paimonColumnHandle.getColumnName(),
                    typeManager);
        }
        catch (TrinoException e) {
            if (e.getErrorCode().equals(COLUMN_NOT_FOUND.toErrorCode())
                    && !PaimonColumnHandle.isHiddenColumnName(paimonColumnHandle.getColumnName())) {
                // Trino may ask for metadata using a stale ordinary column handle immediately after
                // rename/drop DDL has already changed the table schema.
                return paimonColumnHandle.getColumnMetadata();
            }
            throw e;
        }
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        requireNonNull(session, "session is null");
        requireNonNull(prefix, "prefix is null");
        List<SchemaTableName> tableNames = prefix.getTable()
                .map(ignored -> Collections.singletonList(prefix.toSchemaTableName()))
                .orElseGet(() -> listTables(session, prefix.getSchema()));

        return tableNames.stream()
                .map(tableName -> getTableColumnsMetadata(session, tableName)
                        .map(columns -> Map.entry(tableName, columns)))
                .flatMap(Optional::stream)
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> List.copyOf(entry.getValue())));
    }

    @Override
    public Iterator<io.trino.spi.connector.TableColumnsMetadata> streamTableColumns(ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        requireNonNull(session, "session is null");
        requireNonNull(prefix, "prefix is null");
        List<SchemaTableName> tableNames = prefix.getTable()
                .map(ignored -> Collections.singletonList(prefix.toSchemaTableName()))
                .orElseGet(() -> listTables(session, prefix.getSchema()));

        return tableNames.stream()
                .map(tableName -> getTableColumnsMetadata(session, tableName)
                        .map(columns -> io.trino.spi.connector.TableColumnsMetadata.forTable(tableName, columns)))
                .flatMap(Optional::stream)
                .iterator();
    }

    private Optional<List<ColumnMetadata>> getTableColumnsMetadata(ConnectorSession session, SchemaTableName tableName)
    {
        PaimonTableHandle tableHandle = getTableHandle(session, tableName, Collections.emptyMap());
        if (tableHandle == null) {
            return Optional.empty();
        }
        Catalog sessionCatalog = catalog.forSession(session);
        return Optional.of(tableHandle.columnMetadatas(sessionCatalog, typeManager, session));
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("add column", tableHandle);
        rejectSystemSchemaWrite(paimonTableHandle.getSchemaName(), "add column");
        requireNonNull(column, "column is null");
        if (!column.isNullable()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding not null columns");
        }

        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.addColumn(column.getName(), toPaimonType(column), column.getComment(), null));
        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw paimonAlterTableException(schemaTableName(paimonTableHandle), e);
        }
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source,
            String target)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("rename column", tableHandle);
        rejectSystemSchemaWrite(paimonTableHandle.getSchemaName(), "rename column");
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        PaimonColumnHandle paimonColumnHandle = getColumnHandle("rename column", source);
        validateFieldName("target", target);
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.renameColumn(paimonColumnHandle.getColumnName(), target));
        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw paimonAlterTableException(schemaTableName(paimonTableHandle), e);
        }
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("drop column", tableHandle);
        rejectSystemSchemaWrite(paimonTableHandle.getSchemaName(), "drop column");
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        PaimonColumnHandle paimonColumnHandle = getColumnHandle("drop column", column);
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.dropColumn(paimonColumnHandle.getColumnName()));
        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw paimonAlterTableException(schemaTableName(paimonTableHandle), e);
        }
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("set table comment", tableHandle);
        rejectSystemSchemaWrite(paimonTableHandle.getSchemaName(), "set table comment");
        requireNonNull(comment, "comment is null");
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.updateComment(comment.orElse(null)));
        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw paimonAlterTableException(schemaTableName(paimonTableHandle), e);
        }
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column,
            Optional<String> comment)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("set column comment", tableHandle);
        rejectSystemSchemaWrite(paimonTableHandle.getSchemaName(), "set column comment");
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        PaimonColumnHandle paimonColumnHandle = getColumnHandle("set column comment", column);
        requireNonNull(comment, "comment is null");
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.updateColumnComment(paimonColumnHandle.getColumnName(), comment.orElse(null)));
        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw paimonAlterTableException(schemaTableName(paimonTableHandle), e);
        }
    }

    @Override
    public void setColumnType(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column,
            Type type)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("set column type", tableHandle);
        rejectSystemSchemaWrite(paimonTableHandle.getSchemaName(), "set column type");
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        PaimonColumnHandle paimonColumnHandle = getColumnHandle("set column type", column);

        DataType paimonType = PaimonTypeUtils.toPaimonType(requireNonNull(type, "type is null"))
                .copy(paimonColumnHandle.logicalType().isNullable());

        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.updateColumnType(paimonColumnHandle.getColumnName(), paimonType, true));

        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw paimonAlterTableException(schemaTableName(paimonTableHandle), e);
        }
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("drop not null constraint", tableHandle);
        rejectSystemSchemaWrite(paimonTableHandle.getSchemaName(), "drop not null constraint");
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        PaimonColumnHandle paimonColumnHandle = getColumnHandle("drop not null constraint", column);

        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.updateColumnNullability(paimonColumnHandle.getColumnName(), true));

        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw paimonAlterTableException(schemaTableName(paimonTableHandle), e);
        }
    }

    @Override
    public void addField(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> parentPath,
            String fieldName, Type type, boolean ignoreExisting)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("add field", tableHandle);
        rejectSystemSchemaWrite(paimonTableHandle.getSchemaName(), "add field");
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());

        // Build field path: parentPath + fieldName
        String[] fieldNames = buildFieldNamesArray(parentPath, fieldName);

        // Convert Trino Type to Paimon DataType
        DataType paimonType = PaimonTypeUtils.toPaimonType(requireNonNull(type, "type is null"));

        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.addColumn(fieldNames, paimonType, null, null));

        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            if (ignoreExisting && e instanceof Catalog.ColumnAlreadyExistException) {
                return;
            }
            throw paimonAlterTableException(schemaTableName(paimonTableHandle), e);
        }
    }

    @Override
    public void dropField(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column,
            List<String> fieldPath)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("drop field", tableHandle);
        rejectSystemSchemaWrite(paimonTableHandle.getSchemaName(), "drop field");
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        PaimonColumnHandle paimonColumnHandle = getColumnHandle("drop field", column);
        validateRelativeFieldPath("drop field", fieldPath);

        // Build full field path: columnName + fieldPath
        String[] fieldNames = buildFieldNamesArray(List.of(paimonColumnHandle.getColumnName()), fieldPath);

        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.dropColumn(fieldNames));

        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw paimonAlterTableException(schemaTableName(paimonTableHandle), e);
        }
    }

    @Override
    public void renameField(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> fieldPath,
            String target)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("rename field", tableHandle);
        rejectSystemSchemaWrite(paimonTableHandle.getSchemaName(), "rename field");
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        validateAbsoluteFieldPath("rename field", fieldPath);
        validateFieldName("target", target);

        // fieldPath includes column name and nested path
        String[] fieldNames = fieldPath.toArray(new String[0]);

        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.renameColumn(fieldNames, target));

        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw paimonAlterTableException(schemaTableName(paimonTableHandle), e);
        }
    }

    @Override
    public void setFieldType(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> fieldPath,
            Type type)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("set field type", tableHandle);
        rejectSystemSchemaWrite(paimonTableHandle.getSchemaName(), "set field type");
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        validateAbsoluteFieldPath("set field type", fieldPath);

        // fieldPath includes column name and nested path
        String[] fieldNames = fieldPath.toArray(new String[0]);

        // Convert Trino Type to Paimon DataType
        DataType paimonType = PaimonTypeUtils.toPaimonType(requireNonNull(type, "type is null"));

        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.updateColumnType(fieldNames, paimonType, true));

        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw paimonAlterTableException(schemaTableName(paimonTableHandle), e);
        }
    }

    /**
     * Helper method to build field names array from parent path and field name.
     * Used for nested field operations.
     */
    private String[] buildFieldNamesArray(List<String> parentPath, String fieldName)
    {
        requireNonNull(parentPath, "parentPath is null");
        parentPath.forEach(field -> validateFieldName("parentPath", field));
        validateFieldName("fieldName", fieldName);
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
        requireNonNull(columnList, "columnList is null");
        requireNonNull(fieldPath, "fieldPath is null");
        columnList.forEach(field -> validateFieldName("columnList", field));
        List<String> fullPath = new ArrayList<>(columnList);
        fullPath.addAll(fieldPath);
        return fullPath.toArray(new String[0]);
    }

    private static void validateRelativeFieldPath(String operation, List<String> fieldPath)
    {
        requireNonNull(fieldPath, operation + " fieldPath is null");
        checkArgument(!fieldPath.isEmpty(), operation + " fieldPath is empty");
        fieldPath.forEach(field -> validateFieldName(operation + " fieldPath", field));
    }

    private static void validateAbsoluteFieldPath(String operation, List<String> fieldPath)
    {
        requireNonNull(fieldPath, operation + " fieldPath is null");
        checkArgument(fieldPath.size() >= 2, operation + " fieldPath must include a column name and nested field");
        fieldPath.forEach(field -> validateFieldName(operation + " fieldPath", field));
    }

    private static void validateFieldName(String label, String fieldName)
    {
        requireNonNull(fieldName, label + " contains null field");
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(fieldName), label + " contains blank field");
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("truncate table", tableHandle);
        rejectSystemSchemaWrite(paimonTableHandle.getSchemaName(), "truncate table");
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());

        try {
            Catalog sessionCatalog = catalog.forSession(session);
            Table table = sessionCatalog.getTable(identifier);
            FileStoreTable fileStoreTable = latestFileStoreTable(table, "truncate table");

            // Use BatchTableCommit to truncate the table
            try (BatchTableCommit commit = fileStoreTable.newBatchWriteBuilder().newCommit()) {
                commit.truncateTable();
            }
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Catalog.TableNotExistException e) {
            throw new TrinoException(TABLE_NOT_FOUND,
                    format("Table '%s' does not exist", schemaTableName(paimonTableHandle)), e);
        }
        catch (Exception e) {
            throw paimonMetadataException(
                    format("Failed to truncate Paimon table '%s'", paimonTableHandle.getTableName()),
                    e);
        }
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session,
            ConnectorTableHandle handle, Constraint constraint)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("filter pushdown", handle);
        requireNonNull(constraint, "constraint is null");
        validateFilterColumns(constraint);
        if (paimonTableHandle.getFilter().isNone()) {
            return Optional.empty();
        }
        if (constraint.getSummary().isNone()) {
            return Optional.of(new ConstraintApplicationResult<>(paimonTableHandle.copy(TupleDomain.none()),
                    TupleDomain.all(), TRUE, false));
        }
        if (paimonTableHandle.getLimit().isPresent()) {
            return Optional.empty();
        }
        if (constraint.getSummary().isAll() && constraint.getExpression().equals(TRUE)) {
            return Optional.empty();
        }
        Catalog sessionCatalog = catalog.forSession(session);
        Optional<PaimonFilterExtractor.TrinoFilter> extract = PaimonFilterExtractor.extract(sessionCatalog,
                paimonTableHandle, session, constraint);
        if (extract.isPresent()) {
            PaimonFilterExtractor.TrinoFilter trinoFilter = extract.get();
            return Optional.of(new ConstraintApplicationResult<>(paimonTableHandle.copy(trinoFilter.filter()),
                    trinoFilter.remainFilter(), trinoFilter.remainingExpression(), false));
        }
        else {
            return Optional.empty();
        }
    }

    private static void validateFilterColumns(Constraint constraint)
    {
        constraint.getSummary().transformKeys(column -> getColumnHandle("filter pushdown", column));
        constraint.getPredicateColumns().ifPresent(columns -> columns.forEach(column ->
                getColumnHandle("filter pushdown", column)));
        constraint.getAssignments().values().forEach(column -> getColumnHandle("filter pushdown", column));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session,
            ConnectorTableHandle handle, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("projection pushdown", handle);
        requireNonNull(projections, "projections is null");
        requireNonNull(assignments, "assignments is null");
        assignments.forEach((name, column) -> {
            requireNonNull(name, "assignments contains null variable");
            getColumnHandle("projection pushdown", column);
        });
        LinkedHashMap<String, PaimonColumnHandle> projectedAssignments = projectedAssignments(projections,
                assignments);
        if (projectedAssignments.isEmpty()) {
            return Optional.empty();
        }

        List<ColumnHandle> newColumns = new ArrayList<>(projectedAssignments.values());

        if (paimonTableHandle.getProjectedColumns().isPresent()
                && newColumns.equals(paimonTableHandle.getProjectedColumns().get())) {
            return Optional.empty();
        }

        List<Assignment> assignmentList = new ArrayList<>();
        projectedAssignments.forEach((name, column) -> assignmentList
                .add(new Assignment(name, column, column.getTrinoType())));

        return Optional.of(new ProjectionApplicationResult<>(paimonTableHandle.copy(Optional.of(newColumns)),
                projections, assignmentList, false));
    }

    private static LinkedHashMap<String, PaimonColumnHandle> projectedAssignments(
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        LinkedHashMap<String, PaimonColumnHandle> projectedAssignments = new LinkedHashMap<>();
        projections.forEach(projection -> collectProjectionVariables(projection, assignments, projectedAssignments));
        return projectedAssignments;
    }

    private static void collectProjectionVariables(
            ConnectorExpression projection,
            Map<String, ColumnHandle> assignments,
            LinkedHashMap<String, PaimonColumnHandle> projectedAssignments)
    {
        requireNonNull(projection, "projections contains null expression");
        if (projection instanceof Variable variable) {
            if (!assignments.containsKey(variable.getName())) {
                throw new IllegalStateException("Paimon projection pushdown assignments missing variable: "
                        + variable.getName());
            }
            projectedAssignments.putIfAbsent(variable.getName(),
                    getColumnHandle("projection pushdown", assignments.get(variable.getName())));
            return;
        }
        projection.getChildren().forEach(child -> collectProjectionVariables(child, assignments, projectedAssignments));
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session,
            ConnectorTableHandle handle, long limit)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle table = getTableHandle("limit pushdown", handle);
        checkArgument(limit >= 0, "limit must be non-negative");

        if (table.getLimit().isPresent() && table.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }

        if (table.getFilter().isNone()) {
            return Optional.of(new LimitApplicationResult<>(table.copy(OptionalLong.of(limit)), false, false));
        }

        if (!table.getFilter().isAll()) {
            Catalog sessionCatalog = catalog.forSession(session);
            Table paimonTable = PaimonTableHandle.schemaAwareReadTable(
                    table.tableWithDynamicOptions(sessionCatalog, session),
                    !table.usesHistoricalReadSchema(session));
            HashMap<PaimonColumnHandle, Domain> acceptedDomains = new LinkedHashMap<>();
            HashMap<PaimonColumnHandle, Domain> unsupportedDomains = new LinkedHashMap<>();
            new PaimonFilterConverter(PaimonTableHandle.effectiveReadRowType(paimonTable)).convert(
                    table.getFilter(), acceptedDomains, unsupportedDomains);
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
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition,
            boolean replace)
    {
        requireNonNull(session, "session is null");
        requireNonNull(viewName, "viewName is null");
        requireNonNull(definition, "definition is null");
        rejectSystemSchemaWrite(viewName.getSchemaName(), "create view");
        Identifier identifier = new Identifier(viewName.getSchemaName(), viewName.getTableName());
        org.apache.paimon.view.View paimonView = toPaimonView(identifier, definition);

        try {
            Catalog sessionCatalog = catalog.forSession(session);
            if (replace) {
                sessionCatalog.dropView(identifier, true);
            }
            sessionCatalog.createView(identifier, paimonView, false);
        }
        catch (Catalog.ViewAlreadyExistException e) {
            throw new TrinoException(io.trino.spi.StandardErrorCode.ALREADY_EXISTS,
                    format("View '%s' already exists", viewName));
        }
        catch (Catalog.DatabaseNotExistException e) {
            throw new TrinoException(io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND,
                    format("Schema '%s' does not exist", viewName.getSchemaName()));
        }
        catch (UnsupportedOperationException e) {
            throw unsupportedViewOperation("create", e);
        }
        catch (Exception e) {
            throw paimonViewException(format("Failed to create view '%s'", viewName), e);
        }
    }

    private org.apache.paimon.view.View toPaimonView(Identifier identifier, ConnectorViewDefinition definition)
    {
        List<DataField> fields = IntStream.range(0, definition.getColumns().size())
                .mapToObj(index -> {
                    ConnectorViewDefinition.ViewColumn column = definition.getColumns().get(index);
                    return new DataField(index, column.getName(),
                            PaimonTypeUtils.toPaimonType(typeManager.getType(column.getType())),
                            column.getComment().orElse(null));
                })
                .collect(toList());

        Map<String, String> dialects = new HashMap<>();
        dialects.put("trino", definition.getOriginalSql());

        Map<String, String> options = new HashMap<>();
        definition.getComment().ifPresent(c -> options.put("comment", c));

        return new org.apache.paimon.view.ViewImpl(identifier, fields, definition.getOriginalSql(), dialects,
                definition.getComment().orElse(null), options);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(viewName, "viewName is null");
        rejectSystemSchemaWrite(viewName.getSchemaName(), "drop view");
        Catalog sessionCatalog = catalog.forSession(session);
        Identifier identifier = new Identifier(viewName.getSchemaName(), viewName.getTableName());

        try {
            sessionCatalog.dropView(identifier, false);
        }
        catch (Catalog.ViewNotExistException e) {
            throw new TrinoException(io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND,
                    format("View '%s' does not exist", viewName));
        }
        catch (UnsupportedOperationException e) {
            throw unsupportedViewOperation("drop", e);
        }
        catch (Exception e) {
            throw paimonViewException(format("Failed to drop view '%s'", viewName), e);
        }
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        requireNonNull(session, "session is null");
        requireNonNull(source, "source is null");
        requireNonNull(target, "target is null");
        rejectSystemSchemaWrite(source.getSchemaName(), "rename view");
        rejectSystemSchemaWrite(target.getSchemaName(), "rename view");
        Catalog sessionCatalog = catalog.forSession(session);
        Identifier sourceIdentifier = new Identifier(source.getSchemaName(), source.getTableName());
        Identifier targetIdentifier = new Identifier(target.getSchemaName(), target.getTableName());

        try {
            sessionCatalog.renameView(sourceIdentifier, targetIdentifier, false);
        }
        catch (Catalog.ViewNotExistException e) {
            throw new TrinoException(io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND,
                    format("View '%s' does not exist", source));
        }
        catch (Catalog.ViewAlreadyExistException e) {
            throw new TrinoException(io.trino.spi.StandardErrorCode.ALREADY_EXISTS,
                    format("View '%s' already exists", target));
        }
        catch (UnsupportedOperationException e) {
            throw unsupportedViewOperation("rename", e);
        }
        catch (Exception e) {
            throw paimonViewException(format("Failed to rename view '%s' to '%s'", source, target), e);
        }
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(viewName, "viewName is null");
        Catalog sessionCatalog = catalog.forSession(session);
        Identifier identifier = new Identifier(viewName.getSchemaName(), viewName.getTableName());

        org.apache.paimon.view.View paimonView;
        try {
            paimonView = sessionCatalog.getView(identifier);
        }
        catch (Catalog.ViewNotExistException e) {
            return Optional.empty();
        }
        catch (UnsupportedOperationException e) {
            throw unsupportedViewOperation("read", e);
        }
        catch (Exception e) {
            throw paimonViewException(format("Failed to get view '%s'", viewName), e);
        }

        // Convert Paimon View to Trino ConnectorViewDefinition
        List<ConnectorViewDefinition.ViewColumn> columns = paimonView.rowType().getFields().stream()
                .map(field -> new ConnectorViewDefinition.ViewColumn(field.name(),
                        PaimonTypeUtils.fromPaimonType(field.type(), typeManager).getTypeId(),
                        Optional.ofNullable(field.description()).filter(comment -> !comment.isEmpty())))
                .collect(toList());

        String originalSql = paimonView.dialects().get("trino");
        if (originalSql == null) {
            throw new TrinoException(NOT_SUPPORTED,
                    format("Paimon view '%s' does not contain a Trino SQL dialect", viewName));
        }

        return Optional.of(new ConnectorViewDefinition(originalSql, Optional.empty(), // catalog
                Optional.empty(), // schema
                columns, paimonView.comment(), // comment
                Optional.empty(), // owner
                false, // runAsInvoker
                List.of())); // path
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(schemaName, "schemaName is null");
        schemaName.ifPresent(schema -> checkArgument(!StringUtils.isNullOrWhitespaceOnly(schema),
                "schemaName cannot be null or empty"));
        Catalog sessionCatalog = catalog.forSession(session);

        List<String> schemas = schemaName.map(Collections::singletonList).orElseGet(sessionCatalog::listDatabases);
        Map<SchemaTableName, ConnectorViewDefinition> views = new HashMap<>();
        for (String schema : schemas) {
            views.putAll(getViews(sessionCatalog, session, schema));
        }
        return views;
    }

    private Map<SchemaTableName, ConnectorViewDefinition> getViews(Catalog sessionCatalog, ConnectorSession session, String schemaName)
    {
        List<String> viewNames;
        try {
            viewNames = sessionCatalog.listViews(schemaName);
        }
        catch (Catalog.DatabaseNotExistException e) {
            throw new TrinoException(io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND,
                    format("Schema '%s' does not exist", schemaName));
        }
        catch (UnsupportedOperationException e) {
            throw unsupportedViewOperation("list", e);
        }
        catch (Exception e) {
            throw paimonViewException(format("Failed to list views in schema '%s'", schemaName), e);
        }

        Map<SchemaTableName, ConnectorViewDefinition> views = new HashMap<>();
        for (String viewName : viewNames) {
            SchemaTableName tableName = new SchemaTableName(schemaName, viewName);
            getView(session, tableName).ifPresent(def -> views.put(tableName, def));
        }
        return views;
    }

    @Override
    public void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        requireNonNull(session, "session is null");
        requireNonNull(viewName, "viewName is null");
        requireNonNull(comment, "comment is null");
        rejectSystemSchemaWrite(viewName.getSchemaName(), "set view comment");
        Catalog sessionCatalog = catalog.forSession(session);
        Identifier identifier = new Identifier(viewName.getSchemaName(), viewName.getTableName());

        try {
            List<org.apache.paimon.view.ViewChange> changes = List
                    .of(org.apache.paimon.view.ViewChange.updateComment(comment.orElse(null)));
            sessionCatalog.alterView(identifier, changes, false);
        }
        catch (Catalog.ViewNotExistException e) {
            throw new TrinoException(io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND,
                    format("View '%s' does not exist", viewName));
        }
        catch (UnsupportedOperationException e) {
            throw unsupportedViewOperation("alter", e);
        }
        catch (Exception e) {
            throw paimonViewException(format("Failed to set comment on view '%s'", viewName), e);
        }
    }

    private static RuntimeException paimonViewException(String message, Exception exception)
    {
        return paimonMetadataException(message, exception);
    }

    private static RuntimeException paimonMetadataException(String message, Exception exception)
    {
        if (exception instanceof TrinoException trinoException) {
            return trinoException;
        }
        if (exception instanceof IllegalArgumentException
                || exception instanceof IllegalStateException
                || exception instanceof NullPointerException) {
            return (RuntimeException) exception;
        }
        if (exception instanceof RuntimeException runtimeException) {
            Throwable cause = runtimeException.getCause();
            if (cause instanceof Exception nestedException) {
                return new TrinoException(PAIMON_METADATA_ERROR, message, nestedException);
            }
            return new TrinoException(PAIMON_METADATA_ERROR, message, runtimeException);
        }
        return new TrinoException(PAIMON_METADATA_ERROR, message, exception);
    }

    private static TrinoException unsupportedViewOperation(String operation, UnsupportedOperationException cause)
    {
        String message = "Paimon catalog does not support view " + operation + " operations";
        if (operation.equals("create")) {
            message = "This connector does not support creating views: " + message;
        }
        return new TrinoException(NOT_SUPPORTED, message, cause);
    }
}
