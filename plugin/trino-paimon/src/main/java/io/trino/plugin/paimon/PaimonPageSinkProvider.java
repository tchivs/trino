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

import com.google.inject.Inject;
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.plugin.paimon.ClassLoaderUtils.runWithContextClassLoader;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class PaimonPageSinkProvider
        implements
        ConnectorPageSinkProvider
{
    private final PaimonCatalog paimonCatalog;

    @Inject
    public PaimonPageSinkProvider(PaimonMetadataFactory paimonMetadataFactory)
    {
        this.paimonCatalog = requireNonNull(paimonMetadataFactory, "trinoMetadataFactory is null").create().catalog();
    }

    static void validateWriteBucketMode(Table table)
    {
        BucketMode mode = requireFileStoreTable(table, "writes").bucketMode();
        switch (mode) {
            case HASH_FIXED :
            case BUCKET_UNAWARE :
                break;
            default :
                throw new TrinoException(NOT_SUPPORTED, "Unsupported table bucket mode: " + mode);
        }
    }

    static void validateMergeBucketMode(Table table)
    {
        BucketMode mode = requireFileStoreTable(table, "merge writes").bucketMode();
        if (mode != BucketMode.HASH_FIXED) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported table bucket mode: " + mode);
        }
    }

    private static FileStoreTable requireFileStoreTable(Table table, String operation)
    {
        return PaimonTableSupport.requireFileStoreTable(table, operation);
    }

    static FileStoreTable latestFileStoreTable(Table table, String operation)
    {
        return requireFileStoreTable(table, operation).copyWithLatestSchema();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle, ConnectorPageSinkId pageSinkId)
    {
        requireNonNull(session, "session is null");
        return createPageSink(getOutputTableHandle(outputTableHandle), session);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
            ConnectorInsertTableHandle insertTableHandle, ConnectorPageSinkId pageSinkId)
    {
        requireNonNull(session, "session is null");
        return createPageSink(getInsertTableHandle(insertTableHandle), session);
    }

    private ConnectorPageSink createPageSink(PaimonTableHandle tableHandle, ConnectorSession session)
    {
        requireNonNull(session, "session is null");
        List<PaimonColumnHandle> writeColumns = getWriteColumns(tableHandle);
        Catalog catalog = paimonCatalog.forSession(session);
        FileStoreTable table = latestFileStoreTable(tableHandle.tableWithWriteDynamicOptions(catalog),
                "writes");
        validateWriteBucketMode(table);

        validateWriteColumns(table, writeColumns);
        return runWithContextClassLoader(() -> createPageSink(table, session, getWriteColumnTypes(writeColumns),
                getWriteLogicalTypes(writeColumns)),
                PaimonPageSinkProvider.class.getClassLoader());
    }

    static List<PaimonColumnHandle> getWriteColumns(PaimonTableHandle tableHandle)
    {
        return requireNonNull(tableHandle, "tableHandle is null").getWriteColumns()
                .orElseThrow(() -> new IllegalStateException("Paimon page sink requires explicit write columns"))
                .stream()
                .map(PaimonPageSinkProvider::getWriteColumn)
                .collect(Collectors.toList());
    }

    static PaimonTableHandle getOutputTableHandle(ConnectorOutputTableHandle outputTableHandle)
    {
        if (!(requireNonNull(outputTableHandle, "outputTableHandle is null") instanceof PaimonTableHandle paimonTableHandle)) {
            throw new IllegalStateException("Paimon create table page sink requires PaimonTableHandle, got: "
                    + outputTableHandle.getClass().getName());
        }
        return paimonTableHandle;
    }

    static PaimonTableHandle getInsertTableHandle(ConnectorInsertTableHandle insertTableHandle)
    {
        if (!(requireNonNull(insertTableHandle, "insertTableHandle is null") instanceof PaimonTableHandle paimonTableHandle)) {
            throw new IllegalStateException("Paimon insert page sink requires PaimonTableHandle, got: "
                    + insertTableHandle.getClass().getName());
        }
        return paimonTableHandle;
    }

    private static PaimonColumnHandle getWriteColumn(ColumnHandle column)
    {
        if (!(requireNonNull(column, "writeColumns contains null column") instanceof PaimonColumnHandle paimonColumnHandle)) {
            throw new IllegalStateException("Paimon page sink requires PaimonColumnHandle, got: "
                    + column.getClass().getName());
        }
        return paimonColumnHandle;
    }

    private static List<Type> getWriteColumnTypes(List<PaimonColumnHandle> writeColumns)
    {
        return writeColumns.stream()
                .map(PaimonColumnHandle::getTrinoType)
                .collect(Collectors.toList());
    }

    private static List<DataType> getWriteLogicalTypes(List<PaimonColumnHandle> writeColumns)
    {
        return writeColumns.stream()
                .map(PaimonColumnHandle::logicalType)
                .collect(Collectors.toList());
    }

    static void validateWriteColumns(FileStoreTable table, List<PaimonColumnHandle> writeColumns)
    {
        requireNonNull(table, "table is null");
        requireNonNull(writeColumns, "writeColumns is null");
        if (writeColumns.isEmpty()) {
            throw new IllegalStateException("Paimon page sink requires non-empty write columns");
        }
        validateNoCaseInsensitiveDuplicateFieldNames(table.rowType().getFields());
        Set<String> seenColumnNames = new HashSet<>();
        for (PaimonColumnHandle column : writeColumns) {
            requireNonNull(column, "writeColumns contains null column");
            String columnName = column.getColumnName();
            String lowerColumnName = FieldNameUtils.toLowerCase(columnName);
            if (!seenColumnNames.add(lowerColumnName)) {
                throw new IllegalStateException("Write column '%s' appears more than once".formatted(columnName));
            }
            DataField latestField = latestField(table.rowType().getFields(), columnName);
            if (!latestField.type().equals(column.logicalType())) {
                throw new IllegalStateException("Write column '%s' type %s does not match latest Paimon table schema type %s"
                        .formatted(columnName, column.logicalType().asSQLString(), latestField.type().asSQLString()));
            }
        }
    }

    private static DataField latestField(List<DataField> fields, String columnName)
    {
        String lowerColumnName = FieldNameUtils.toLowerCase(columnName);
        DataField match = null;
        for (DataField field : fields) {
            if (FieldNameUtils.toLowerCase(field.name()).equals(lowerColumnName)) {
                if (match != null) {
                    throw new IllegalStateException(
                            "Latest Paimon table schema contains case-insensitive duplicate field name '%s'"
                                    .formatted(lowerColumnName));
                }
                match = field;
            }
        }
        if (match == null) {
            throw new IllegalStateException("Write column '%s' is not present in latest Paimon table schema %s"
                    .formatted(columnName, fields.stream().map(DataField::name).collect(Collectors.toList())));
        }
        return match;
    }

    static void validateNoCaseInsensitiveDuplicateFieldNames(List<DataField> fields)
    {
        requireNonNull(fields, "fields is null");
        Set<String> fieldNames = new HashSet<>();
        for (DataField field : fields) {
            requireNonNull(field, "fields contains null field");
            String lowerFieldName = FieldNameUtils.toLowerCase(field.name());
            if (!fieldNames.add(lowerFieldName)) {
                throw new IllegalStateException(
                        "Latest Paimon table schema contains case-insensitive duplicate field name '%s'"
                                .formatted(lowerFieldName));
            }
        }
    }

    static void validateMergeWriteColumns(FileStoreTable table, List<PaimonColumnHandle> writeColumns)
    {
        validateWriteColumns(table, writeColumns);
        List<String> latestFieldNames = table.rowType().getFieldNames();
        List<String> writeColumnNames = writeColumns.stream()
                .map(PaimonColumnHandle::getColumnName)
                .collect(Collectors.toList());
        if (!writeColumnNames.equals(latestFieldNames)) {
            throw new IllegalStateException("Merge write columns %s must match latest Paimon table schema columns %s"
                    .formatted(writeColumnNames, latestFieldNames));
        }
    }

    private PaimonPageSink createPageSink(FileStoreTable table, ConnectorSession session, List<Type> columnTypes,
            List<DataType> logicalTypes)
    {
        BatchWriteBuilder batchWriteBuilder = table.newBatchWriteBuilder();
        if (PaimonSessionProperties.enableInsertOverwrite(session)) {
            batchWriteBuilder.withOverwrite();
        }
        BatchTableWrite write = batchWriteBuilder.newWrite();
        return new PaimonPageSink(write, columnTypes, logicalTypes);
    }

    @Override
    public ConnectorMergeSink createMergeSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
            ConnectorMergeTableHandle mergeHandle, ConnectorPageSinkId pageSinkId)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getMergeTableHandle(mergeHandle);
        List<PaimonColumnHandle> writeColumns = getWriteColumns(paimonTableHandle);
        Catalog catalog = paimonCatalog.forSession(session);
        FileStoreTable table = latestFileStoreTable(paimonTableHandle.tableWithWriteDynamicOptions(catalog),
                "merge writes");
        validateMergeBucketMode(table);
        validateMergeWriteColumns(table, writeColumns);

        return runWithContextClassLoader(() -> new PaimonMergeSink(
                createPageSink(table, session, getWriteColumnTypes(writeColumns), getWriteLogicalTypes(writeColumns)),
                table.rowType().getFields().size()), PaimonPageSinkProvider.class.getClassLoader());
    }

    static PaimonTableHandle getMergeTableHandle(ConnectorMergeTableHandle mergeHandle)
    {
        ConnectorTableHandle tableHandle = requireNonNull(mergeHandle, "mergeHandle is null").getTableHandle();
        if (!(requireNonNull(tableHandle, "mergeHandle tableHandle is null") instanceof PaimonTableHandle paimonTableHandle)) {
            throw new IllegalStateException("Paimon merge sink requires PaimonTableHandle, got: "
                    + tableHandle.getClass().getName());
        }
        return paimonTableHandle;
    }
}
