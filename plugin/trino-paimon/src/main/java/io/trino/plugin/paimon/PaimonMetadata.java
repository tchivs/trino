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
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.ColStats;
import org.apache.paimon.stats.Statistics;
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
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.StringUtils;
import org.apache.paimon.view.ViewChange;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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
import static io.trino.plugin.paimon.PaimonSchemaProperties.COMMENT_PROPERTY;
import static io.trino.plugin.paimon.PaimonSchemaProperties.LOCATION_PROPERTY;
import static io.trino.plugin.paimon.PaimonSchemaProperties.OWNER_PROPERTY;
import static io.trino.plugin.paimon.PaimonTrinoTypeConversions.paimonTimestampToTrino;
import static io.trino.plugin.paimon.PaimonTrinoTypeConversions.paimonTimestampToTrinoTimestampWithTimeZone;
import static io.trino.spi.StandardErrorCode.COLUMN_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
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
    private static final int MAX_LIST_PARTITIONS_BY_NAMES_BATCH_SIZE = 1000;
    private static final int MAX_PARTITION_DELETE_SPECS = 1024;

    public PaimonMetadata
    {
        catalog = requireNonNull(catalog, "catalog is null");
        typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public Optional<ConnectorTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableMetadata, "tableMetadata is null");
        rejectSystemSchemaWrite(tableMetadata.getTable().getSchemaName(), "create table");
        TableSchema tableSchema = TableSchema.create(0, prepareSchema(tableMetadata));
        return writeLayout(tableSchema, "new table layout", tableMetadata.getTable().toString());
    }

    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("insert layout", tableHandle);
        Catalog sessionCatalog = catalog.forSession(session);
        FileStoreTable storeTable = latestWriteFileStoreTable(paimonTableHandle, sessionCatalog, "insert layout");
        return writeLayout(storeTable.schema(), storeTable.bucketMode(), "insert layout",
                schemaTableName(paimonTableHandle).toString());
    }

    private static Optional<ConnectorTableLayout> writeLayout(TableSchema tableSchema, String operation, String tableName)
    {
        return writeLayout(tableSchema, bucketMode(tableSchema), operation, tableName);
    }

    private static Optional<ConnectorTableLayout> writeLayout(
            TableSchema tableSchema,
            BucketMode bucketMode,
            String operation,
            String tableName)
    {
        requireNonNull(tableSchema, "tableSchema is null");
        requireNonNull(bucketMode, "bucketMode is null");
        requireNonNull(operation, "operation is null");
        requireNonNull(tableName, "tableName is null");
        switch (bucketMode) {
            case HASH_FIXED :
                try {
                    return Optional.of(new ConnectorTableLayout(
                            new PaimonPartitioningHandle(InstantiationUtil.serializeObject(tableSchema)),
                            fixedBucketWritePartitionColumns(tableSchema), false));
                }
                catch (IOException e) {
                    throw new TrinoException(PAIMON_METADATA_ERROR,
                            format("Failed to prepare Paimon %s for table '%s'", operation, tableName),
                            e);
                }
            case HASH_DYNAMIC :
                try {
                    // TODO: Replace this single-writer HASH_DYNAMIC INSERT layout with a Flink-style
                    // two-stage assigner/writer topology that coordinates dynamic bucket index state.
                    return Optional.of(new ConnectorTableLayout(
                            new PaimonPartitioningHandle(InstantiationUtil.serializeObject(tableSchema), true),
                            List.of(), false));
                }
                catch (IOException e) {
                    throw new TrinoException(PAIMON_METADATA_ERROR,
                            format("Failed to prepare Paimon %s for table '%s'", operation, tableName),
                            e);
                }
            case BUCKET_UNAWARE :
                return Optional.empty();
            default :
                throw PaimonTableSupport.unsupportedBucketMode(operation, bucketMode);
        }
    }

    private static List<String> fixedBucketWritePartitionColumns(TableSchema schema)
    {
        List<String> partitionColumns = new ArrayList<>(schema.partitionKeys());
        partitionColumns.addAll(schema.bucketKeys());
        return List.copyOf(partitionColumns);
    }

    private static BucketMode bucketMode(TableSchema schema)
    {
        requireNonNull(schema, "schema is null");
        int bucket = CoreOptions.fromMap(schema.options()).bucket();
        if (bucket == BucketMode.POSTPONE_BUCKET) {
            return BucketMode.POSTPONE_MODE;
        }
        if (bucket != -1) {
            return BucketMode.HASH_FIXED;
        }
        if (schema.primaryKeys().isEmpty()) {
            return BucketMode.BUCKET_UNAWARE;
        }
        return schema.crossPartitionUpdate() ? BucketMode.KEY_DYNAMIC : BucketMode.HASH_DYNAMIC;
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
        Table table = tableHandle.tableWithWriteDynamicOptions(sessionCatalog);
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
        if (fragmentsList.isEmpty()
                && insertBehavior != PaimonSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE) {
            return Optional.empty();
        }

        List<CommitMessage> commitMessages = deserializeCommitMessages(fragmentsList);
        Catalog sessionCatalog = catalog.forSession(session);
        FileStoreTable fileStoreTable = latestWriteFileStoreTable(tableHandle, sessionCatalog, "commit writes");

        try {
            if (insertBehavior == PaimonSessionProperties.InsertExistingPartitionsBehavior.ERROR) {
                validateInsertTargetIsNew(sessionCatalog, fileStoreTable, tableHandle, commitMessages);
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
            Catalog catalog,
            FileStoreTable fileStoreTable,
            PaimonTableHandle tableHandle,
            List<CommitMessage> commitMessages)
            throws Catalog.TableNotExistException
    {
        SchemaTableName tableName = schemaTableName(tableHandle);
        if (fileStoreTable.partitionKeys().isEmpty()) {
            if (!fileStoreTable.newSnapshotReader().partitionEntries().isEmpty()) {
                throw new TrinoException(READ_ONLY_VIOLATION,
                        format("Cannot insert into an existing non-partitioned Paimon table: %s", tableName));
            }
            return;
        }

        List<Map<String, String>> writtenPartitions = writtenPartitionSpecs(fileStoreTable, commitMessages);
        for (int start = 0; start < writtenPartitions.size(); start += MAX_LIST_PARTITIONS_BY_NAMES_BATCH_SIZE) {
            int end = Math.min(start + MAX_LIST_PARTITIONS_BY_NAMES_BATCH_SIZE, writtenPartitions.size());
            List<Partition> existingPartitions = catalog.listPartitionsByNames(
                    new Identifier(
                            tableHandle.getSchemaName(),
                            tableHandle.getTableName(),
                            fileStoreTable.coreOptions().branch()),
                    writtenPartitions.subList(start, end));
            if (!existingPartitions.isEmpty()) {
                throw new TrinoException(READ_ONLY_VIOLATION,
                        format("Cannot insert into an existing partition of Paimon table: %s", tableName));
            }
        }
    }

    private static List<Map<String, String>> writtenPartitionSpecs(
            FileStoreTable fileStoreTable,
            List<CommitMessage> commitMessages)
    {
        RowType partitionType = new RowType(fileStoreTable.partitionKeys().stream()
                .map(partitionKey -> fileStoreTable.rowType().getField(partitionKey))
                .collect(toList()));
        InternalRowPartitionComputer partitionComputer = new InternalRowPartitionComputer(
                fileStoreTable.coreOptions().partitionDefaultName(),
                partitionType,
                fileStoreTable.partitionKeys().toArray(new String[0]),
                fileStoreTable.coreOptions().legacyPartitionName());
        Set<Map<String, String>> writtenPartitions = new LinkedHashSet<>();
        for (CommitMessage commitMessage : commitMessages) {
            writtenPartitions.add(partitionComputer.generatePartValues(commitMessage.partition()));
        }
        return List.copyOf(writtenPartitions);
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
        PaimonTableHandle paimonTableHandle = getTableHandle("row change paradigm", tableHandle);
        Catalog sessionCatalog = catalog.forSession(session);
        rowLevelChangeFileStoreTable(paimonTableHandle, sessionCatalog, "row-level change");
        return DELETE_ROW_AND_INSERT_ROW;
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("merge row id", tableHandle);
        Catalog sessionCatalog = catalog.forSession(session);
        FileStoreTable storeTable = rowLevelChangeFileStoreTable(paimonTableHandle, sessionCatalog, "merge row id");
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
        FileStoreTable storeTable = rowLevelChangeFileStoreTable(paimonTableHandle, sessionCatalog, "update layout");
        try {
            return Optional.of(new PaimonPartitioningHandle(
                    InstantiationUtil.serializeObject(storeTable.schema()),
                    storeTable.bucketMode() == BucketMode.HASH_DYNAMIC));
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

    private static FileStoreTable latestWriteFileStoreTable(
            PaimonTableHandle tableHandle,
            Catalog sessionCatalog,
            String operation)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(sessionCatalog, "sessionCatalog is null");
        try {
            return latestFileStoreTable(tableHandle.tableWithWriteDynamicOptions(sessionCatalog), operation);
        }
        catch (TrinoException e) {
            if (e.getErrorCode().equals(TABLE_NOT_FOUND.toErrorCode())) {
                throw new TrinoException(TABLE_NOT_FOUND,
                        format("Table '%s' does not exist", schemaTableName(tableHandle)),
                        e.getCause() != null ? e.getCause() : e);
            }
            throw e;
        }
    }

    private static FileStoreTable rowLevelChangeFileStoreTable(
            PaimonTableHandle tableHandle,
            Catalog sessionCatalog,
            String operation)
    {
        FileStoreTable storeTable = latestWriteFileStoreTable(tableHandle, sessionCatalog, operation);
        BucketMode bucketMode = storeTable.bucketMode();
        if (bucketMode != BucketMode.HASH_FIXED) {
            throw PaimonTableSupport.unsupportedBucketMode(operation, bucketMode);
        }
        PaimonTableSupport.validateRowLevelDelete(storeTable, operation);
        return storeTable;
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
        FileStoreTable storeTable = rowLevelChangeFileStoreTable(paimonTableHandle, sessionCatalog, "merge");
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
        Map<String, String> paimonProperties = schemaProperties(properties, owner);

        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.createDatabase(schemaName, false, paimonProperties);
        }
        catch (Catalog.DatabaseAlreadyExistException e) {
            throw new TrinoException(SCHEMA_ALREADY_EXISTS, format("Schema '%s' already exists", schemaName), e);
        }
        catch (Exception e) {
            throw paimonMetadataException(format("Failed to create Paimon schema '%s'", schemaName), e);
        }
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, String schemaName)
    {
        requireNonNull(session, "session is null");
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(schemaName), "schemaName cannot be null or empty");
        if (SYSTEM_DATABASE_NAME.equals(schemaName)) {
            throw new TrinoException(NOT_SUPPORTED,
                    "Paimon schema properties are not supported for the system schema '" + SYSTEM_DATABASE_NAME + "'");
        }
        try {
            Catalog sessionCatalog = catalog.forSession(session);
            return supportedSchemaProperties(sessionCatalog.getDatabase(schemaName).options());
        }
        catch (Catalog.DatabaseNotExistException e) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", schemaName), e);
        }
        catch (Exception e) {
            throw paimonMetadataException(format("Failed to get Paimon schema properties for '%s'", schemaName), e);
        }
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(ConnectorSession session, String schemaName)
    {
        return Optional.empty();
    }

    @Override
    public void setSchemaAuthorization(ConnectorSession session, String schemaName, TrinoPrincipal principal)
    {
        requireNonNull(session, "session is null");
        requireNonNull(principal, "principal is null");
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(schemaName), "schemaName cannot be null or empty");
        rejectSystemSchemaWrite(schemaName, "set schema authorization");

        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.alterDatabase(schemaName, List.of(PropertyChange.setProperty(OWNER_PROPERTY,
                    principal.getName())), false);
        }
        catch (Catalog.DatabaseNotExistException e) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", schemaName), e);
        }
        catch (Exception e) {
            throw paimonMetadataException(format("Failed to set authorization on Paimon schema '%s'", schemaName), e);
        }
    }

    private static Map<String, Object> supportedSchemaProperties(Map<String, String> properties)
    {
        Map<String, Object> result = new HashMap<>();
        copySchemaProperty(properties, result, LOCATION_PROPERTY);
        copySchemaProperty(properties, result, COMMENT_PROPERTY);
        copySchemaProperty(properties, result, OWNER_PROPERTY);
        return Map.copyOf(result);
    }

    private static void copySchemaProperty(Map<String, String> properties, Map<String, Object> result, String property)
    {
        String value = properties.get(property);
        if (value != null && !value.isBlank()) {
            result.put(property, value);
        }
    }

    private static Map<String, String> schemaProperties(Map<String, Object> properties, TrinoPrincipal owner)
    {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            String propertyName = requireNonNull(entry.getKey(), "properties contains null property name");
            checkArgument(!StringUtils.isNullOrWhitespaceOnly(propertyName), "properties contains blank property name");
            Object value = entry.getValue();
            if (value == null) {
                continue;
            }
            if (!(value instanceof String stringValue)) {
                throw new IllegalArgumentException("properties value for property '%s' must be a string".formatted(propertyName));
            }
            if (stringValue.isBlank()) {
                throw new IllegalArgumentException("properties value for property '%s' is blank".formatted(propertyName));
            }
            result.put(propertyName, stringValue);
        }
        if (owner != null) {
            result.putIfAbsent(OWNER_PROPERTY, owner.getName());
        }
        return Map.copyOf(result);
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
        if (endVersion.isPresent() && !PaimonTableHandle.supportsHistoricalRead(
                Identifier.create(tableName.getSchemaName(), tableName.getTableName()))) {
            throw new TrinoException(NOT_SUPPORTED, PaimonTableHandle.UNSUPPORTED_HISTORICAL_READ_MESSAGE);
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

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("table statistics", tableHandle);
        if (paimonTableHandle.getFilter().isNone()
                || (paimonTableHandle.getLimit().isPresent() && paimonTableHandle.getLimit().getAsLong() == 0)) {
            return TableStatistics.builder().setRowCount(Estimate.zero()).build();
        }
        if (!paimonTableHandle.getFilter().isAll() || paimonTableHandle.getLimit().isPresent()) {
            return TableStatistics.empty();
        }
        if (paimonTableHandle.hasIncrementalReadMode()) {
            return TableStatistics.empty();
        }

        Catalog sessionCatalog = catalog.forSession(session);
        Table table = PaimonTableHandle.schemaAwareReadTable(
                paimonTableHandle.tableWithDynamicOptions(sessionCatalog, session),
                !paimonTableHandle.usesHistoricalReadSchema(session));

        Optional<Statistics> statistics;
        try {
            statistics = table.statistics();
        }
        catch (RuntimeException e) {
            return TableStatistics.empty();
        }
        return statistics.map(value -> toTableStatistics(table, value)).orElse(TableStatistics.empty());
    }

    private TableStatistics toTableStatistics(Table table, Statistics statistics)
    {
        TableStatistics.Builder builder = TableStatistics.builder();

        OptionalLong mergedRecordCount = statistics.mergedRecordCount();
        mergedRecordCount.ifPresent(rowCount -> {
            if (rowCount >= 0) {
                builder.setRowCount(Estimate.of(rowCount));
            }
        });

        Map<String, ColStats<?>> colStats = statistics.colStats();
        if (colStats == null || colStats.isEmpty()) {
            return builder.build();
        }

        for (DataField field : PaimonTableHandle.effectiveReadRowType(table).getFields()) {
            ColStats<?> columnStats = colStats.get(field.name());
            if (columnStats != null) {
                builder.setColumnStatistics(
                        PaimonColumnHandle.of(field.name(), field.type(), typeManager),
                        toColumnStatistics(field.type(), columnStats, mergedRecordCount, typeManager));
            }
        }
        return builder.build();
    }

    private static ColumnStatistics toColumnStatistics(
            DataType logicalType,
            ColStats<?> stats,
            OptionalLong rowCount,
            TypeManager typeManager)
    {
        ColumnStatistics.Builder builder = ColumnStatistics.builder();

        stats.distinctCount().ifPresent(distinctCount -> {
            if (distinctCount >= 0) {
                builder.setDistinctValuesCount(Estimate.of(distinctCount));
            }
        });
        if (rowCount.isPresent()) {
            long records = rowCount.getAsLong();
            stats.nullCount().ifPresent(nullCount -> {
                if (records == 0) {
                    builder.setNullsFraction(Estimate.zero());
                }
                else if (records > 0 && nullCount >= 0 && nullCount <= records) {
                    builder.setNullsFraction(Estimate.of((double) nullCount / records));
                }
            });
            stats.avgLen().ifPresent(avgLen -> {
                if (records >= 0 && avgLen >= 0) {
                    long nullCount = stats.nullCount().orElse(0);
                    long nonNullRecords = Math.max(0, records - nullCount);
                    builder.setDataSize(Estimate.of((double) nonNullRecords * avgLen));
                }
            });
        }
        toRange(logicalType, stats, typeManager).ifPresent(builder::setRange);

        return builder.build();
    }

    private static Optional<DoubleRange> toRange(DataType logicalType, ColStats<?> stats, TypeManager typeManager)
    {
        Optional<?> min = stats.min();
        Optional<?> max = stats.max();
        if (min.isEmpty() || max.isEmpty()) {
            return Optional.empty();
        }

        try {
            Type trinoType = PaimonTypeUtils.fromPaimonType(logicalType, typeManager);
            Object minValue = toTrinoNativeStatsValue(trinoType, logicalType, min.get());
            Object maxValue = toTrinoNativeStatsValue(trinoType, logicalType, max.get());
            return DoubleRange.from(trinoType, minValue, maxValue);
        }
        catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private static Object toTrinoNativeStatsValue(Type trinoType, DataType logicalType, Object value)
    {
        return switch (logicalType.getTypeRoot()) {
            case BOOLEAN -> value;
            case TINYINT, SMALLINT, INTEGER, BIGINT, DATE -> ((Number) value).longValue();
            case FLOAT -> (long) Float.floatToIntBits(((Number) value).floatValue());
            case DOUBLE -> ((Number) value).doubleValue();
            case DECIMAL -> toTrinoNativeDecimalValue((DecimalType) trinoType, (Decimal) value);
            case TIMESTAMP_WITHOUT_TIME_ZONE -> paimonTimestampToTrino(trinoType, (Timestamp) value);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE -> paimonTimestampToTrinoTimestampWithTimeZone(trinoType, value);
            default -> throw new IllegalArgumentException("Unsupported Paimon statistics range type: " + logicalType);
        };
    }

    private static Object toTrinoNativeDecimalValue(DecimalType trinoType, Decimal value)
    {
        if (trinoType.isShort()) {
            return Decimals.encodeShortScaledValue(value.toBigDecimal(), trinoType.getScale());
        }
        return Decimals.encodeScaledValue(value.toBigDecimal(), trinoType.getScale());
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
                .peek(property -> requireNonNull(property, "properties contains null property name"))
                .peek(property -> checkArgument(!StringUtils.isNullOrWhitespaceOnly(property),
                        "properties contains blank property name"))
                .filter(property -> PaimonTableOptions.PRIMARY_KEY_IDENTIFIER.equals(property)
                        || PaimonTableOptions.PARTITIONED_BY_PROPERTY.equals(property)
                        || PaimonTableOptionUtils.isRuntimeOnlyTableProperty(property))
                .sorted()
                .toList();
        if (!unsupportedProperties.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "The following properties cannot be updated: " + String.join(", ", unsupportedProperties));
        }
    }

    @Override
    public void setTableAuthorization(ConnectorSession session, SchemaTableName tableName, TrinoPrincipal principal)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(principal, "principal is null");
        rejectSystemSchemaWrite(tableName.getSchemaName(), "set table authorization");

        Identifier identifier = new Identifier(tableName.getSchemaName(), tableName.getTableName());
        List<SchemaChange> changes = List.of(SchemaChange.setOption(OWNER_PROPERTY, principal.getName()));
        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw paimonAlterTableException(tableName, e);
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
                replaceOrCreateTable(sessionCatalog, identifier, schema);
                return;
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
        catch (UnsupportedOperationException e) {
            throw new TrinoException(NOT_SUPPORTED,
                    format("Paimon create or replace table '%s' is not supported: %s", table, e.getMessage()), e);
        }
        catch (Exception e) {
            throw paimonMetadataException(format("Failed to create Paimon table '%s'", table), e);
        }
    }

    private static void replaceOrCreateTable(Catalog sessionCatalog, Identifier identifier, Schema schema)
            throws Catalog.TableAlreadyExistException, Catalog.DatabaseNotExistException, Catalog.TableNotExistException
    {
        try {
            sessionCatalog.replaceTable(identifier, schema, false);
        }
        catch (Catalog.TableNotExistException e) {
            sessionCatalog.createTable(identifier, schema, false);
        }
    }

    private Schema prepareSchema(ConnectorTableMetadata tableMetadata)
    {
        Map<String, Object> properties = new HashMap<>(tableMetadata.getProperties());
        List<String> primaryKeys = PaimonTableOptions.getPrimaryKeys(properties);
        List<String> partitionKeys = PaimonTableOptions.getPartitionedKeys(properties);
        primaryKeys.forEach(column -> rejectPaimonSystemColumnName("create table primary key", column));
        partitionKeys.forEach(column -> rejectPaimonSystemColumnName("create table partition key", column));
        List<String> columnNames = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(toList());
        validateKeyColumns(PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, primaryKeys, columnNames);
        validateKeyColumns(PaimonTableOptions.PARTITIONED_BY_PROPERTY, partitionKeys, columnNames);
        Schema.Builder builder = Schema.newBuilder().primaryKey(primaryKeys)
                .partitionKeys(partitionKeys)
                .comment(tableMetadata.getComment().orElse(null));

        for (ColumnMetadata column : tableMetadata.getColumns()) {
            rejectPaimonSystemColumnName("create table", column.getName());
            builder.column(column.getName(), toPaimonType(column), column.getComment());
        }

        PaimonTableOptionUtils.buildOptions(builder, properties);

        return builder.build();
    }

    private static void validateKeyColumns(String propertyName, List<String> keyColumns, List<String> columnNames)
    {
        requireNonNull(propertyName, "propertyName is null");
        requireNonNull(keyColumns, "keyColumns is null");
        requireNonNull(columnNames, "columnNames is null");
        if (keyColumns.isEmpty()) {
            return;
        }

        Set<String> duplicateColumns = duplicates(keyColumns);
        if (!duplicateColumns.isEmpty()) {
            throw new TrinoException(INVALID_TABLE_PROPERTY,
                    "Paimon " + propertyName + " must not contain duplicate columns: " + duplicateColumns);
        }

        Set<String> tableColumns = new LinkedHashSet<>(columnNames);
        List<String> missingColumns = keyColumns.stream()
                .filter(column -> !tableColumns.contains(column))
                .toList();
        if (!missingColumns.isEmpty()) {
            throw new TrinoException(INVALID_TABLE_PROPERTY,
                    "Paimon " + propertyName + " columns not present in schema: " + missingColumns);
        }
    }

    private static Set<String> duplicates(List<String> values)
    {
        Set<String> seen = new HashSet<>();
        Set<String> duplicates = new LinkedHashSet<>();
        for (String value : values) {
            if (!seen.add(value)) {
                duplicates.add(value);
            }
        }
        return duplicates;
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
        rejectPaimonSystemColumnName("add column", column.getName());
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
        rejectPaimonSystemColumn(paimonColumnHandle, "rename column");
        validateFieldName("target", target);
        rejectPaimonSystemColumnName("rename column", target);
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
        rejectPaimonSystemColumn(paimonColumnHandle, "drop column");
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
        rejectPaimonSystemColumn(paimonColumnHandle, "set column comment");
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
        rejectPaimonSystemColumn(paimonColumnHandle, "set column type");

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
        rejectPaimonSystemColumn(paimonColumnHandle, "drop not null constraint");

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
        rejectPaimonSystemRootField("add field", fieldNames[0]);

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
        rejectPaimonSystemColumn(paimonColumnHandle, "drop field");
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
        rejectPaimonSystemRootField("rename field", fieldPath.get(0));
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
        rejectPaimonSystemRootField("set field type", fieldPath.get(0));

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

    private static void rejectPaimonSystemColumn(PaimonColumnHandle columnHandle, String operation)
    {
        requireNonNull(columnHandle, "columnHandle is null");
        if (PaimonColumnHandle.isPaimonSystemColumnName(columnHandle.getColumnName())) {
            throw new TrinoException(NOT_SUPPORTED,
                    "Paimon " + operation + " is not supported for system column '"
                            + columnHandle.getColumnName() + "'");
        }
    }

    private static void rejectPaimonSystemColumnName(String operation, String columnName)
    {
        requireNonNull(columnName, "columnName is null");
        if (PaimonColumnHandle.isPaimonSystemColumnName(columnName)) {
            throw new TrinoException(NOT_SUPPORTED,
                    "Paimon " + operation + " is not supported for system column '" + columnName + "'");
        }
    }

    private static void rejectPaimonSystemRootField(String operation, String rootField)
    {
        requireNonNull(rootField, "rootField is null");
        if (PaimonColumnHandle.isPaimonSystemColumnName(rootField)) {
            throw new TrinoException(NOT_SUPPORTED,
                    "Paimon " + operation + " is not supported for system column '" + rootField + "'");
        }
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("truncate table", tableHandle);
        truncatePaimonTable(session, paimonTableHandle, "truncate table", "truncate");
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("delete", handle);
        rejectSystemSchemaWrite(paimonTableHandle.getSchemaName(), "delete");

        Catalog sessionCatalog = catalog.forSession(session);
        FileStoreTable fileStoreTable = latestWriteFileStoreTable(paimonTableHandle, sessionCatalog, "delete");
        if (paimonTableHandle.getFilter().isAll()) {
            return Optional.of(paimonTableHandle);
        }
        return partitionDeleteSpecs(paimonTableHandle, fileStoreTable)
                .map(paimonTableHandle::withDeletePartitionSpecs)
                .map(ConnectorTableHandle.class::cast);
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("delete", handle);
        if (!paimonTableHandle.getFilter().isAll() && paimonTableHandle.getDeletePartitionSpecs().isEmpty()) {
            throw new IllegalStateException(
                    "Paimon delete requires an unfiltered table handle or a validated partition delete handle");
        }
        truncatePaimonTable(session, paimonTableHandle, "delete", "delete rows from",
                paimonTableHandle.getDeletePartitionSpecs());
        return OptionalLong.empty();
    }

    private static Optional<List<Map<String, String>>> partitionDeleteSpecs(
            PaimonTableHandle tableHandle,
            FileStoreTable fileStoreTable)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(fileStoreTable, "fileStoreTable is null");
        if (tableHandle.getLimit().isPresent() || tableHandle.getProjectedColumns().isPresent()
                || tableHandle.getFilter().isNone() || fileStoreTable.partitionKeys().isEmpty()) {
            return Optional.empty();
        }

        Optional<Map<PaimonColumnHandle, Domain>> domains = tableHandle.getFilter().getDomains();
        if (domains.isEmpty() || domains.get().size() != fileStoreTable.partitionKeys().size()) {
            return Optional.empty();
        }

        Map<String, Domain> domainsByName = new HashMap<>();
        Map<String, PaimonColumnHandle> columnsByName = new HashMap<>();
        for (Map.Entry<PaimonColumnHandle, Domain> entry : domains.get().entrySet()) {
            String columnName = entry.getKey().getColumnName();
            domainsByName.put(columnName, entry.getValue());
            columnsByName.put(columnName, entry.getKey());
        }

        RowType partitionType = new RowType(fileStoreTable.partitionKeys().stream()
                .map(partitionKey -> fileStoreTable.rowType().getField(partitionKey))
                .collect(toList()));
        InternalRowPartitionComputer partitionComputer = new InternalRowPartitionComputer(
                fileStoreTable.coreOptions().partitionDefaultName(),
                partitionType,
                fileStoreTable.partitionKeys().toArray(new String[0]),
                fileStoreTable.coreOptions().legacyPartitionName());
        List<List<Object>> partitionValueRows = List.of(List.of());
        for (String partitionKey : fileStoreTable.partitionKeys()) {
            Domain domain = domainsByName.get(partitionKey);
            PaimonColumnHandle columnHandle = columnsByName.get(partitionKey);
            if (domain == null || columnHandle == null || !domain.isNullableDiscreteSet()) {
                return Optional.empty();
            }
            Optional<List<Object>> partitionValues = partitionValues(columnHandle, domain);
            if (partitionValues.isEmpty()) {
                return Optional.empty();
            }
            if (partitionValues.get().isEmpty()
                    || partitionValueRows.size() > MAX_PARTITION_DELETE_SPECS / partitionValues.get().size()) {
                return Optional.empty();
            }
            partitionValueRows = appendPartitionValues(partitionValueRows, partitionValues.get());
        }

        return Optional.of(partitionValueRows.stream()
                .map(values -> partitionComputer.generatePartValues(GenericRow.of(values.toArray())))
                .collect(toList()));
    }

    private static Optional<List<Object>> partitionValues(PaimonColumnHandle columnHandle, Domain domain)
    {
        requireNonNull(columnHandle, "columnHandle is null");
        requireNonNull(domain, "domain is null");
        List<Object> values = new ArrayList<>();
        Domain.DiscreteSet discreteSet = domain.getNullableDiscreteSet();
        for (Object value : discreteSet.getNonNullValues()) {
            Optional<Object> partitionValue = partitionValue(columnHandle, value);
            if (partitionValue.isEmpty()) {
                return Optional.empty();
            }
            values.add(partitionValue.get());
        }
        if (discreteSet.containsNull()) {
            values.add(null);
        }
        return Optional.of(Collections.unmodifiableList(new ArrayList<>(values)));
    }

    private static List<List<Object>> appendPartitionValues(
            List<List<Object>> partitionValueRows,
            List<Object> partitionValues)
    {
        requireNonNull(partitionValueRows, "partitionValueRows is null");
        requireNonNull(partitionValues, "partitionValues is null");
        List<List<Object>> result = new ArrayList<>(partitionValueRows.size() * partitionValues.size());
        for (List<Object> partitionValueRow : partitionValueRows) {
            for (Object partitionValue : partitionValues) {
                List<Object> newPartitionValueRow = new ArrayList<>(partitionValueRow);
                newPartitionValueRow.add(partitionValue);
                result.add(Collections.unmodifiableList(newPartitionValueRow));
            }
        }
        return List.copyOf(result);
    }

    private static Optional<Object> partitionValue(PaimonColumnHandle columnHandle, Object value)
    {
        requireNonNull(columnHandle, "columnHandle is null");
        requireNonNull(value, "value is null");
        try {
            return Optional.of(PaimonFilterConverter.getLiteralValue(columnHandle.getTrinoType(), value));
        }
        catch (UnsupportedOperationException | ClassCastException | ArithmeticException e) {
            return Optional.empty();
        }
    }

    private void truncatePaimonTable(ConnectorSession session, PaimonTableHandle paimonTableHandle, String operation,
            String failureOperation)
    {
        truncatePaimonTable(session, paimonTableHandle, operation, failureOperation, Optional.empty());
    }

    private void truncatePaimonTable(ConnectorSession session, PaimonTableHandle paimonTableHandle, String operation,
            String failureOperation, Optional<List<Map<String, String>>> deletePartitionSpecs)
    {
        rejectSystemSchemaWrite(paimonTableHandle.getSchemaName(), operation);

        try {
            Catalog sessionCatalog = catalog.forSession(session);
            FileStoreTable fileStoreTable = latestWriteFileStoreTable(paimonTableHandle, sessionCatalog, operation);

            // Use BatchTableCommit to truncate the table
            try (BatchTableCommit commit = fileStoreTable.newBatchWriteBuilder().newCommit()) {
                if (deletePartitionSpecs.isPresent()) {
                    commit.truncatePartitions(deletePartitionSpecs.get());
                }
                else {
                    commit.truncateTable();
                }
            }
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Exception e) {
            throw paimonMetadataException(
                    format("Failed to %s Paimon table '%s'", failureOperation, paimonTableHandle.getTableName()),
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
        definition.getOwner().ifPresent(owner -> options.put(OWNER_PROPERTY, owner));

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
    public void setViewAuthorization(ConnectorSession session, SchemaTableName viewName, TrinoPrincipal principal)
    {
        requireNonNull(session, "session is null");
        requireNonNull(viewName, "viewName is null");
        requireNonNull(principal, "principal is null");
        rejectSystemSchemaWrite(viewName.getSchemaName(), "set view authorization");

        Catalog sessionCatalog = catalog.forSession(session);
        Identifier identifier = new Identifier(viewName.getSchemaName(), viewName.getTableName());

        try {
            sessionCatalog.alterView(identifier, List.of(ViewChange.setOption(OWNER_PROPERTY, principal.getName())),
                    false);
        }
        catch (Catalog.ViewNotExistException e) {
            throw new TrinoException(io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND,
                    format("View '%s' does not exist", viewName));
        }
        catch (UnsupportedOperationException e) {
            throw unsupportedViewOperation("alter", e);
        }
        catch (Exception e) {
            throw paimonViewException(format("Failed to set authorization on view '%s'", viewName), e);
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
                Optional.ofNullable(paimonView.options().get(OWNER_PROPERTY)), // owner
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
        if (exception instanceof UnsupportedOperationException unsupportedOperationException) {
            return new TrinoException(NOT_SUPPORTED,
                    unsupportedOperationException.getMessage() == null || unsupportedOperationException.getMessage().isBlank()
                            ? message
                            : unsupportedOperationException.getMessage(),
                    unsupportedOperationException);
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
