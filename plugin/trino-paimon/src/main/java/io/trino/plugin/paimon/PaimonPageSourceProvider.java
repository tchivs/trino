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
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.orc.OrcColumn;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcReader;
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.OrcRecordReader;
import io.trino.orc.TupleDomainOrcPredicate;
import io.trino.parquet.Column;
import io.trino.parquet.Field;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.orc.OrcPageSource;
import io.trino.plugin.hive.parquet.ParquetPageSource;
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fileindex.FileIndexPredicate;
import org.apache.paimon.fs.Path;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.IndexFile;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.RowType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createDataSource;
import static io.trino.plugin.paimon.ClassLoaderUtils.runWithContextClassLoader;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class PaimonPageSourceProvider
        implements
        ConnectorPageSourceProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final PaimonCatalog paimonCatalog;
    private final OrcReaderOptions orcReaderOptions;
    private final ParquetReaderOptions parquetReaderOptions;

    @Inject
    public PaimonPageSourceProvider(TrinoFileSystemFactory fileSystemFactory, PaimonMetadataFactory paimonMetadataFactory,
            io.trino.plugin.hive.orc.OrcReaderConfig orcReaderConfig,
            io.trino.plugin.hive.parquet.ParquetReaderConfig parquetReaderConfig)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.paimonCatalog = requireNonNull(paimonMetadataFactory, "trinoMetadataFactory is null").create().catalog();
        this.orcReaderOptions = requireNonNull(orcReaderConfig, "orcReaderConfig is null").toOrcReaderOptions()
                // Default tiny stripe size 8 M is too big for paimon.
                // Cache stripe will cause more read (I want to read one column,
                // but not the whole stripe)
                .withTinyStripeThreshold(DataSize.of(4, DataSize.Unit.KILOBYTE));
        this.parquetReaderOptions = requireNonNull(parquetReaderConfig, "parquetReaderConfig is null").toParquetReaderOptions();
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session,
            ConnectorSplit split, ConnectorTableHandle tableHandle, List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        requireNonNull(session, "session is null");
        requireNonNull(dynamicFilter, "dynamicFilter is null");
        PaimonTableHandle paimonTableHandle = getTableHandle(tableHandle);
        PaimonSplit paimonSplit = getSplit(split);
        List<PaimonColumnHandle> paimonColumns = getColumnHandles(columns);
        if (paimonTableHandle.getFilter().isNone()) {
            return emptyPageSource();
        }
        Catalog catalog = paimonCatalog.forSession(session);
        Table table = paimonTableHandle.tableWithDynamicOptions(catalog, session);
        boolean refreshToLatestSchema = !paimonTableHandle.usesHistoricalReadSchema(session);
        return runWithContextClassLoader(() -> {
            Optional<PaimonColumnHandle> rowId = rowIdColumn(paimonColumns);
            if (rowId.isPresent()) {
                List<PaimonColumnHandle> dataColumns = paimonColumns.stream()
                        .filter(column -> !column.isRowId()).collect(Collectors.toList());
                List<String> rowIdFields = rowIdFieldNames(rowId.get().getTrinoType());
                Set<String> rowIdFieldSet = Set.copyOf(rowIdFields);

                HashMap<String, Integer> fieldToIndex = new HashMap<>();
                for (int i = 0; i < dataColumns.size(); i++) {
                    PaimonColumnHandle paimonColumnHandle = dataColumns.get(i);
                    if (rowIdFieldSet.contains(paimonColumnHandle.getColumnName())) {
                        fieldToIndex.put(paimonColumnHandle.getColumnName(), i);
                    }
                }
                return PaimonMergePageSourceWrapper.wrap(createPageSource(session, paimonTableHandle, table,
                        paimonTableHandle.getFilter(), paimonSplit, dataColumns, paimonTableHandle.getLimit(),
                        refreshToLatestSchema), rowIdFields, fieldToIndex);
            }
            else {
                return createPageSource(session, paimonTableHandle, table, paimonTableHandle.getFilter(), paimonSplit,
                        paimonColumns, paimonTableHandle.getLimit(), refreshToLatestSchema);
            }
        }, PaimonPageSourceProvider.class.getClassLoader());
    }

    static Optional<PaimonColumnHandle> rowIdColumn(List<PaimonColumnHandle> columns)
    {
        requireNonNull(columns, "columns is null");
        List<PaimonColumnHandle> rowIdColumns = columns.stream()
                .map(column -> requireNonNull(column, "columns contains null column"))
                .filter(PaimonColumnHandle::isRowId)
                .toList();
        if (rowIdColumns.size() > 1) {
            throw new IllegalStateException("Paimon page source expected at most one row id column, got: "
                    + rowIdColumns.size());
        }
        return rowIdColumns.stream().findFirst();
    }

    static PaimonTableHandle getTableHandle(ConnectorTableHandle tableHandle)
    {
        if (!(requireNonNull(tableHandle, "tableHandle is null") instanceof PaimonTableHandle paimonTableHandle)) {
            throw new IllegalStateException("Paimon page source requires PaimonTableHandle, got: "
                    + tableHandle.getClass().getName());
        }
        return paimonTableHandle;
    }

    static PaimonSplit getSplit(ConnectorSplit split)
    {
        if (!(requireNonNull(split, "split is null") instanceof PaimonSplit paimonSplit)) {
            throw new IllegalStateException("Paimon page source requires PaimonSplit, got: "
                    + split.getClass().getName());
        }
        return paimonSplit;
    }

    static List<PaimonColumnHandle> getColumnHandles(List<? extends ColumnHandle> columns)
    {
        requireNonNull(columns, "columns is null");
        return columns.stream()
                .map(column -> {
                    if (!(requireNonNull(column, "columns contains null column") instanceof PaimonColumnHandle paimonColumnHandle)) {
                        throw new IllegalStateException("Paimon page source requires PaimonColumnHandle, got: "
                                + column.getClass().getName());
                    }
                    return paimonColumnHandle;
                })
                .toList();
    }

    static List<String> rowIdFieldNames(Type rowIdType)
    {
        requireNonNull(rowIdType, "rowIdType is null");
        if (!(rowIdType instanceof io.trino.spi.type.RowType trinoRowIdType)) {
            throw new IllegalArgumentException("Paimon row id column must be ROW, got: "
                    + rowIdType.getDisplayName());
        }
        List<String> rowIdFields = new ArrayList<>();
        Set<String> seenFields = new HashSet<>();
        for (int index = 0; index < trinoRowIdType.getFields().size(); index++) {
            int fieldIndex = index;
            io.trino.spi.type.RowType.Field field = trinoRowIdType.getFields().get(index);
            String fieldName = field.getName()
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Paimon row id field at index %s must be named".formatted(fieldIndex)));
            if (fieldName.isBlank()) {
                throw new IllegalArgumentException("Paimon row id field at index %s is blank".formatted(fieldIndex));
            }
            if (!seenFields.add(fieldName)) {
                throw new IllegalArgumentException("Paimon row id field '%s' appears more than once".formatted(fieldName));
            }
            rowIdFields.add(fieldName);
        }
        return List.copyOf(rowIdFields);
    }

    private ConnectorPageSource createPageSource(ConnectorSession session, PaimonTableHandle tableHandle, Table table,
            TupleDomain<PaimonColumnHandle> filter, PaimonSplit split, List<PaimonColumnHandle> columns,
            OptionalLong limit, boolean refreshToLatestSchema)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        if (filter.isNone()) {
            return emptyPageSource();
        }

        List<String> projectedFields = columns.stream().map(PaimonColumnHandle::getColumnName).toList();
        TupleDomain<PaimonColumnHandle> readerFilter = readerFilter(filter);

        try {
            Split paimonSplit = split.decodeSplit();
            Optional<List<RawFile>> optionalRawFiles = paimonSplit.convertToRawFiles();
            if (checkRawFile(tableHandle, optionalRawFiles, columns, filter) && directReaderSupportsFilter(projectedFields, filter)) {
                DirectReadTableContext directReadTableContext = directReadTableContext(table, filter,
                        refreshToLatestSchema);
                FileStoreTable fileStoreTable = directReadTableContext.table();
                RowType rowType = directReadTableContext.rowType();
                boolean readIndex = fileStoreTable.coreOptions().fileIndexReadEnabled();
                List<Domain> filterDomains = orderDomains(projectedFields, filter);

                Optional<List<DeletionFile>> deletionFiles = paimonSplit.deletionFiles();
                Optional<List<IndexFile>> indexFiles = readIndex ? paimonSplit.indexFiles() : Optional.empty();
                Optional<Predicate> fileIndexFilter = directReadTableContext.fileIndexFilter();
                SchemaManager schemaManager = new SchemaManager(fileStoreTable.fileIO(), fileStoreTable.location());
                List<Type> type = columns.stream().map(PaimonColumnHandle::getTrinoType)
                        .collect(Collectors.toList());
                TrinoFileSystem fileSystem = fileSystemFactory.create(session);

                try {
                    List<RawFile> files = optionalRawFiles.orElseThrow();
                    validateAlignedMetadataFiles("indexFiles", indexFiles, files.size());
                    validateAlignedMetadataFiles("deletionFiles", deletionFiles, files.size());
                    LinkedList<ConnectorPageSource> sources = new LinkedList<>();

                    // if file index exists, do the filter.
                    for (int i = 0; i < files.size(); i++) {
                        RawFile rawFile = files.get(i);
                        if (indexFiles.isPresent()) {
                            IndexFile indexFile = indexFiles.get().get(i);
                            if (indexFile != null && fileIndexFilter.isPresent()) {
                                try (FileIndexPredicate fileIndexPredicate = new FileIndexPredicate(
                                        new Path(indexFile.path()), fileStoreTable.fileIO(), rowType)) {
                                    if (!fileIndexPredicate.evaluate(fileIndexFilter.get()).remain()) {
                                        continue;
                                    }
                                }
                            }
                        }

                        // Schema evolution: map table column names to data file column names
                        // Paimon stores column names in lowercase in ORC/Parquet files,
                        // so we need to convert to lowercase for file reading
                        List<String> dataFileColumns;
                        List<DataField> dataSchemaFields;
                        long tableSchemaId = fileStoreTable.schema().id();
                        long fileSchemaId = rawFile.schemaId();

                        if (tableSchemaId == fileSchemaId) {
                            dataSchemaFields = rowType.getFields();
                            dataFileColumns = currentSchemaFieldNames(projectedFields, dataSchemaFields);
                        }
                        else {
                            dataSchemaFields = schemaManager.schema(fileSchemaId).fields();
                            // Schema evolution: map table fields to data file fields by ID
                            dataFileColumns = schemaEvolutionFieldNames(projectedFields, rowType.getFields(),
                                    dataSchemaFields);
                        }

                        if (canSkipDirectReadFile(dataFileColumns, filterDomains, dataSchemaFields)) {
                            continue;
                        }

                        ConnectorPageSource source = createDataPageSource(rawFile.format(),
                                fileSystem.newInputFile(Location.of(rawFile.path())),
                                dataFileColumns, type, directReaderDomains(projectedFields, filter,
                                        deletionFileAt(deletionFiles, i).isPresent()));

                        Optional<DeletionFile> deletionFile = deletionFileAt(deletionFiles, i);
                        if (deletionFile.isPresent()) {
                            source = PaimonPageSourceWrapper.wrap(source, deletionFile.map(file -> {
                                try {
                                    return DeletionVector.read(fileStoreTable.fileIO(), file);
                                }
                                catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }));
                        }
                        sources.add(source);
                    }

                    return new DirectTrinoPageSource(sources, limit);
                }
                catch (Exception e) {
                    throw wrapPaimonReadException(e);
                }
            }
            else {
                Table readTable = PaimonTableHandle.schemaAwareReadTable(table, refreshToLatestSchema);
                RowType rowType = PaimonTableHandle.effectiveReadRowType(readTable);
                List<String> fieldNames = rowType.getFieldNames();
                Optional<Predicate> paimonFilter = new PaimonFilterConverter(rowType).convert(readerFilter);
                int[] columnIndex = projectionIndexes(fieldNames, projectedFields);
                RowType projectedReadType = isIdentityProjection(columnIndex, fieldNames.size())
                        ? rowType
                        : rowType.project(columnIndex);

                ReadBuilder read = readTable.newReadBuilder();
                paimonFilter.ifPresent(read::withFilter);
                if (!readTable.rowType().equals(projectedReadType)) {
                    read.withReadType(projectedReadType);
                }

                return new PaimonPageSource(read.newRead().executeFilter().createReader(paimonSplit), columns, limit);
            }
        }
        catch (Exception e) {
            throw wrapPaimonReadException(e);
        }
    }

    static TupleDomain<PaimonColumnHandle> readerFilter(TupleDomain<PaimonColumnHandle> filter)
    {
        requireNonNull(filter, "filter is null");
        return PaimonRowRangeExtractor.removeRowIdPredicate(filter);
    }

    static boolean directReaderSupportsFilter(List<String> projectedFields, TupleDomain<PaimonColumnHandle> filter)
    {
        requireNonNull(projectedFields, "projectedFields is null");
        requireNonNull(filter, "filter is null");
        if (filter.isAll()) {
            return true;
        }
        if (filter.isNone()) {
            return false;
        }

        Set<String> projectedFieldNames = projectedFields.stream()
                .map(field -> requireNonNull(field, "projectedFields contains null field"))
                .map(FieldNameUtils::toLowerCase)
                .collect(Collectors.toSet());

        return filter.getDomains()
                .orElseThrow(() -> new IllegalStateException("Expected filter domains for non-trivial TupleDomain"))
                .keySet().stream()
                .map(PaimonColumnHandle::getColumnName)
                .map(FieldNameUtils::toLowerCase)
                .allMatch(projectedFieldNames::contains);
    }

    static boolean canSkipDirectReadFile(List<String> dataFileColumns, List<Domain> filterDomains, List<DataField> dataSchemaFields)
    {
        requireNonNull(dataFileColumns, "dataFileColumns is null");
        requireNonNull(filterDomains, "filterDomains is null");
        requireNonNull(dataSchemaFields, "dataSchemaFields is null");
        if (dataFileColumns.size() != filterDomains.size()) {
            throw new IllegalArgumentException("filterDomains count (%s) must match dataFileColumns count (%s)"
                    .formatted(filterDomains.size(), dataFileColumns.size()));
        }

        Set<String> dataFieldNames = new HashSet<>();
        for (DataField field : dataSchemaFields) {
            requireNonNull(field, "dataSchemaFields contains null field");
            String lowerFieldName = FieldNameUtils.toLowerCase(field.name());
            if (!dataFieldNames.add(lowerFieldName)) {
                throw new IllegalStateException("Paimon data file schema contains case-insensitive duplicate field name '%s'"
                        .formatted(lowerFieldName));
            }
        }

        for (int index = 0; index < dataFileColumns.size(); index++) {
            Domain domain = filterDomains.get(index);
            if (domain == null) {
                continue;
            }
            String dataFileColumn = dataFileColumns.get(index);
            if ((dataFileColumn == null || !dataFieldNames.contains(FieldNameUtils.toLowerCase(dataFileColumn)))
                    && !domain.includesNullableValue(null)) {
                return true;
            }
        }
        return false;
    }

    static DirectReadTableContext directReadTableContext(
            Table table,
            TupleDomain<PaimonColumnHandle> filter,
            boolean refreshToLatestSchema)
    {
        requireNonNull(filter, "filter is null");
        FileStoreTable fileStoreTable = fileStoreTableForDirectRead(table, refreshToLatestSchema);
        RowType rowType = fileStoreTable.rowType();
        return new DirectReadTableContext(fileStoreTable, rowType,
                new PaimonFilterConverter(rowType).convertForFileIndex(filter));
    }

    record DirectReadTableContext(FileStoreTable table, RowType rowType, Optional<Predicate> fileIndexFilter)
    {
        DirectReadTableContext
        {
            requireNonNull(table, "table is null");
            requireNonNull(rowType, "rowType is null");
            requireNonNull(fileIndexFilter, "fileIndexFilter is null");
        }
    }

    static RuntimeException wrapPaimonReadException(Exception exception)
    {
        if (exception instanceof TrinoException trinoException) {
            return trinoException;
        }
        if (exception instanceof UnsupportedOperationException unsupportedOperationException) {
            return unsupportedReadException("Paimon page read uses features which are not supported by the Trino connector",
                    unsupportedOperationException);
        }
        if (exception instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        return new RuntimeException(exception);
    }

    static RuntimeException wrapPaimonReadException(String message, Exception exception)
    {
        if (exception instanceof TrinoException trinoException) {
            return trinoException;
        }
        if (exception instanceof UnsupportedOperationException unsupportedOperationException) {
            return unsupportedReadException(message, unsupportedOperationException);
        }
        if (exception instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        return new RuntimeException(message, exception);
    }

    static TrinoException unsupportedReadException(String message, UnsupportedOperationException exception)
    {
        requireNonNull(message, "message is null");
        return new TrinoException(NOT_SUPPORTED, message, requireNonNull(exception, "exception is null"));
    }

    static ConnectorPageSource emptyPageSource()
    {
        return new FixedPageSource(List.of());
    }

    static FileStoreTable fileStoreTableForDirectRead(Table table, boolean refreshToLatestSchema)
    {
        FileStoreTable fileStoreTable = requireFileStoreTableForDirectRead(table);
        if (refreshToLatestSchema) {
            return fileStoreTable.copyWithLatestSchema();
        }
        return fileStoreTable;
    }

    static FileStoreTable requireFileStoreTableForDirectRead(Table table)
    {
        requireNonNull(table, "table is null");
        if (!(table instanceof FileStoreTable fileStoreTable)) {
            throw new TrinoException(NOT_SUPPORTED,
                    "Direct raw-file reads require FileStoreTable, but got: " + table.getClass().getName());
        }
        return fileStoreTable;
    }

    static void validateAlignedMetadataFiles(String name, Optional<? extends List<?>> files, int rawFileCount)
    {
        requireNonNull(name, "name is null");
        requireNonNull(files, "files is null");
        if (rawFileCount < 0) {
            throw new IllegalArgumentException("rawFileCount is negative: " + rawFileCount);
        }
        if (files.isPresent() && files.get().size() != rawFileCount) {
            throw new IllegalStateException("%s count (%s) must match raw file count (%s)"
                    .formatted(name, files.get().size(), rawFileCount));
        }
    }

    // make domains(filters) to be ordered by projected fields' order.
    static List<Domain> orderDomains(List<String> projectedFields, TupleDomain<PaimonColumnHandle> filter)
    {
        requireNonNull(projectedFields, "projectedFields is null");
        requireNonNull(filter, "filter is null");
        Optional<Map<PaimonColumnHandle, Domain>> optionalFilter = filter.getDomains();
        Map<String, Domain> domainMap = new HashMap<>();
        optionalFilter.ifPresent(trinoColumnHandleDomainMap -> trinoColumnHandleDomainMap
                .forEach((k, v) -> {
                    String fieldName = FieldNameUtils.toLowerCase(k.getColumnName());
                    Domain previous = domainMap.putIfAbsent(fieldName, v);
                    if (previous != null) {
                        throw new IllegalStateException("Filter contains conflicting domains for field '%s'"
                                .formatted(fieldName));
                    }
                }));

        return projectedFields.stream()
                .map(FieldNameUtils::toLowerCase)
                .map(name -> domainMap.getOrDefault(name, null))
                .collect(Collectors.toList());
    }

    static List<Domain> directReaderDomains(List<String> projectedFields, TupleDomain<PaimonColumnHandle> filter,
            boolean hasDeletionVectors)
    {
        requireNonNull(projectedFields, "projectedFields is null");
        requireNonNull(filter, "filter is null");
        if (filter.isNone()) {
            throw new IllegalStateException("Direct raw-file reads must not receive TupleDomain.none()");
        }
        if (hasDeletionVectors) {
            return projectedFields.stream().map(field -> (Domain) null).collect(Collectors.toList());
        }
        return orderDomains(projectedFields, filter);
    }

    static Optional<DeletionFile> deletionFileAt(Optional<List<DeletionFile>> deletionFiles, int fileIndex)
    {
        requireNonNull(deletionFiles, "deletionFiles is null");
        if (fileIndex < 0) {
            throw new IllegalArgumentException("fileIndex is negative: " + fileIndex);
        }
        return deletionFiles.flatMap(files -> {
            if (fileIndex >= files.size()) {
                throw new IllegalArgumentException("fileIndex %s is out of range for deletionFiles count %s"
                        .formatted(fileIndex, files.size()));
            }
            return Optional.ofNullable(files.get(fileIndex));
        });
    }

    static int[] projectionIndexes(List<String> fieldNames, List<String> projectedFields)
    {
        requireNonNull(fieldNames, "fieldNames is null");
        requireNonNull(projectedFields, "projectedFields is null");
        int[] indexes = new int[projectedFields.size()];
        for (int projectedIndex = 0; projectedIndex < projectedFields.size(); projectedIndex++) {
            String projectedField = requireNonNull(projectedFields.get(projectedIndex), "projectedFields contains null field");
            int fieldIndex = -1;
            for (int index = 0; index < fieldNames.size(); index++) {
                String fieldName = requireNonNull(fieldNames.get(index), "fieldNames contains null field");
                if (fieldName.equalsIgnoreCase(projectedField)) {
                    if (fieldIndex >= 0) {
                        throw new IllegalStateException("Table fields contain case-insensitive duplicate field name '%s': %s"
                                .formatted(projectedField, fieldNames));
                    }
                    fieldIndex = index;
                }
            }
            if (fieldIndex < 0) {
                throw new IllegalStateException("Projected field '%s' does not exist in table fields %s"
                        .formatted(projectedField, fieldNames));
            }
            indexes[projectedIndex] = fieldIndex;
        }
        return indexes;
    }

    static boolean isIdentityProjection(int[] projectionIndexes, int fieldCount)
    {
        requireNonNull(projectionIndexes, "projectionIndexes is null");
        if (projectionIndexes.length != fieldCount) {
            return false;
        }
        for (int index = 0; index < projectionIndexes.length; index++) {
            if (projectionIndexes[index] != index) {
                return false;
            }
        }
        return true;
    }

    private boolean checkRawFile(PaimonTableHandle tableHandle, Optional<List<RawFile>> optionalRawFiles,
            List<? extends ColumnHandle> columns, TupleDomain<PaimonColumnHandle> filter)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(filter, "filter is null");
        return optionalRawFiles.isPresent() && canUseTrinoPageSource(tableHandle, optionalRawFiles.get(), columns)
                && PaimonRowRangeExtractor.extractRowIdRanges(filter).isEmpty();
    }

    // Support ORC and Parquet direct reads. Other formats, including Avro, fall back to Paimon's reader.
    static boolean canUseTrinoPageSource(
            PaimonTableHandle tableHandle,
            List<RawFile> rawFiles,
            List<? extends ColumnHandle> columns)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        // Incremental window reads back the system.table_changes contract. Keep them on Paimon's
        // reader path until the raw-file fast path is explicitly validated for those semantics.
        return !tableHandle.hasIncrementalReadWindow() && canUseTrinoPageSource(rawFiles, columns);
    }

    // Support ORC and Parquet direct reads. Other formats, including Avro, fall back to Paimon's reader.
    static boolean canUseTrinoPageSource(List<RawFile> rawFiles, List<? extends ColumnHandle> columns)
    {
        requireNonNull(rawFiles, "rawFiles is null");
        if (rawFiles.isEmpty()) {
            return false;
        }
        boolean hasOrcRawFiles = false;
        for (RawFile rawFile : rawFiles) {
            requireNonNull(rawFile, "rawFiles contains null file");
            String format = requireNonNull(rawFile.format(), "rawFiles contains file with null format");
            if (format.isBlank()) {
                throw new IllegalArgumentException("rawFiles contains file with blank format");
            }
            if (!"orc".equalsIgnoreCase(format) && !"parquet".equalsIgnoreCase(format)) {
                return false;
            }
            hasOrcRawFiles = hasOrcRawFiles || "orc".equalsIgnoreCase(format);
        }
        for (PaimonColumnHandle paimonColumn : getColumnHandles(columns)) {
            if (SpecialFields.isSystemField(paimonColumn.getColumnName())
                    || containsUnsupportedDirectReadType(paimonColumn.logicalType(), hasOrcRawFiles)) {
                return false;
            }
        }
        for (RawFile rawFile : rawFiles) {
            String path = requireNonNull(rawFile.path(), "rawFiles contains file with null path");
            if (path.isBlank()) {
                throw new IllegalArgumentException("rawFiles contains file with blank path");
            }
        }
        return true;
    }

    private static boolean containsUnsupportedDirectReadType(DataType type, boolean hasOrcRawFiles)
    {
        return switch (type.getTypeRoot()) {
            case BLOB, VARIANT, VECTOR, MULTISET -> true;
            case ARRAY, MAP, ROW -> DataTypeChecks.getNestedTypes(type).stream()
                    .anyMatch(nestedType -> containsUnsupportedDirectReadType(nestedType, hasOrcRawFiles));
            // Paimon ORC stores TIME as int millis. Trino's ORC TimeType reader only accepts Iceberg-style
            // long time columns, while the Parquet TIME(MILLIS) path performs the millis-to-picos conversion.
            case TIME_WITHOUT_TIME_ZONE -> hasOrcRawFiles;
            case CHAR, VARCHAR, BOOLEAN, BINARY, VARBINARY, DECIMAL, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE,
                    DATE, TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> false;
        };
    }

    // map the table schema column names to data schema column names
    // Paimon stores column names in lowercase, so we return lowercase names
    static List<String> currentSchemaFieldNames(List<String> fieldNames, List<DataField> tableFields)
    {
        requireNonNull(fieldNames, "fieldNames is null");
        requireNonNull(tableFields, "tableFields is null");
        return schemaEvolutionFieldNames(fieldNames, tableFields, tableFields);
    }

    static List<String> schemaEvolutionFieldNames(List<String> fieldNames, List<DataField> tableFields,
            List<DataField> dataFields)
    {
        requireNonNull(fieldNames, "fieldNames is null");
        requireNonNull(tableFields, "tableFields is null");
        requireNonNull(dataFields, "dataFields is null");
        Map<String, Integer> fieldNameToId = new HashMap<>();
        Map<String, Integer> dataFieldNameToId = new HashMap<>();
        Map<Integer, String> idToFieldName = new HashMap<>();
        List<String> result = new ArrayList<>();

        // Build maps: lowercase name -> field ID (from table), field ID -> lowercase field name (from data file)
        tableFields.forEach(field -> {
            requireNonNull(field, "tableFields contains null field");
            String lowerName = FieldNameUtils.toLowerCase(field.name());
            Integer previous = fieldNameToId.putIfAbsent(lowerName, field.id());
            if (previous != null) {
                throw new IllegalStateException("Current Paimon table schema contains case-insensitive duplicate field name '%s'"
                        .formatted(lowerName));
            }
        });
        dataFields.forEach(field -> {
            requireNonNull(field, "dataFields contains null field");
            // Store lowercase field name because Paimon writes files with lowercase column names
            String lowerName = FieldNameUtils.toLowerCase(field.name());
            Integer previousId = dataFieldNameToId.putIfAbsent(lowerName, field.id());
            if (previousId != null) {
                throw new IllegalStateException("Paimon data file schema contains case-insensitive duplicate field name '%s'"
                        .formatted(lowerName));
            }
            String previous = idToFieldName.putIfAbsent(field.id(), lowerName);
            if (previous != null) {
                throw new IllegalStateException("Paimon data file schema contains duplicate field id %s"
                        .formatted(field.id()));
            }
        });

        for (String fieldName : fieldNames) {
            // Convert to lowercase for case-insensitive lookup
            String lowerFieldName = FieldNameUtils.toLowerCase(fieldName);
            Integer id = fieldNameToId.get(lowerFieldName);
            if (id == null) {
                throw new IllegalStateException("Projected field '%s' does not exist in current Paimon table fields %s"
                        .formatted(fieldName, tableFields.stream().map(DataField::name).toList()));
            }
            if (idToFieldName.containsKey(id)) {
                // Return the lowercase field name for file reading
                result.add(idToFieldName.get(id));
            }
            else if (dataFieldNameToId.containsKey(lowerFieldName)) {
                // A same-name field with a different ID belongs to an old dropped column.
                result.add(null);
            }
            else {
                result.add(lowerFieldName);
            }
        }
        return result;
    }

    private ConnectorPageSource createDataPageSource(String format, TrinoInputFile inputFile,
            List<String> columns, List<Type> types, List<Domain> domains)
    {
        validateDirectPageSourceInputs(format, inputFile, columns, types, domains);
        switch (format.toLowerCase(Locale.ENGLISH)) {
            case "orc" : {
                return createOrcDataPageSource(inputFile, orcReaderOptions, columns, types, domains);
            }
            case "parquet" : {
                try {
                    return createParquetDataPageSource(inputFile, parquetReaderOptions, columns, types, domains,
                            inputFile.length());
                }
                catch (IOException e) {
                    throw new RuntimeException("Failed to get file length for Parquet file", e);
                }
            }
            default : {
                throw new RuntimeException("Unsupport file format: " + format);
            }
        }
    }

    static void validateDirectPageSourceInputs(String format, TrinoInputFile inputFile,
            List<String> columns, List<Type> types, List<Domain> domains)
    {
        requireNonNull(format, "format is null");
        if (format.isBlank()) {
            throw new IllegalArgumentException("format is blank");
        }
        requireNonNull(inputFile, "inputFile is null");
        requireNonNull(columns, "columns is null");
        requireNonNull(types, "types is null");
        requireNonNull(domains, "domains is null");
        if (types.size() != columns.size()) {
            throw new IllegalArgumentException("types count (%s) must match columns count (%s)"
                    .formatted(types.size(), columns.size()));
        }
        if (domains.size() != columns.size()) {
            throw new IllegalArgumentException("domains count (%s) must match columns count (%s)"
                    .formatted(domains.size(), columns.size()));
        }
        for (String column : columns) {
            if (column != null && column.isBlank()) {
                throw new IllegalArgumentException("columns contains blank column");
            }
        }
        for (Type type : types) {
            requireNonNull(type, "types contains null type");
        }
    }

    private ConnectorPageSource createOrcDataPageSource(TrinoInputFile inputFile, OrcReaderOptions options,
            List<String> columns, List<Type> types, List<Domain> domains)
    {
        try {
            OrcDataSource orcDataSource = new PaimonOrcDataSource(inputFile, options);
            OrcReader reader = OrcReader.createOrcReader(orcDataSource, options)
                    .orElseThrow(() -> new RuntimeException("ORC file is zero length"));

            List<OrcColumn> fileColumns = reader.getRootColumn().getNestedColumns();
            // Use case-insensitive map for column name lookup
            Map<String, OrcColumn> fieldsMap = orcFieldsByLowercaseName(fileColumns);
            TupleDomainOrcPredicate.TupleDomainOrcPredicateBuilder predicateBuilder = TupleDomainOrcPredicate.builder();
            List<OrcPageSource.ColumnAdaptation> columnAdaptations = new ArrayList<>();
            List<OrcColumn> fileReadColumns = new ArrayList<>(columns.size());
            List<Type> fileReadTypes = new ArrayList<>(columns.size());

            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i) != null) {
                    OrcColumn orcColumn = fieldsMap.get(FieldNameUtils.toLowerCase(columns.get(i)));
                    if (orcColumn == null) {
                        columnAdaptations.add(OrcPageSource.ColumnAdaptation.nullColumn(types.get(i)));
                        continue;
                    }
                    columnAdaptations.add(OrcPageSource.ColumnAdaptation.sourceColumn(fileReadColumns.size()));
                    fileReadColumns.add(orcColumn);
                    fileReadTypes.add(types.get(i));
                    if (domains.get(i) != null) {
                        predicateBuilder.addColumn(orcColumn.getColumnId(), domains.get(i));
                    }
                }
                else {
                    columnAdaptations.add(OrcPageSource.ColumnAdaptation.nullColumn(types.get(i)));
                }
            }

            AggregatedMemoryContext memoryUsage = newSimpleAggregatedMemoryContext();
            OrcRecordReader recordReader = reader.createRecordReader(fileReadColumns, fileReadTypes,
                    predicateBuilder.build(), DateTimeZone.UTC, memoryUsage, INITIAL_BATCH_SIZE, RuntimeException::new);

            return new OrcPageSource(recordReader, columnAdaptations, orcDataSource, Optional.empty(), Optional.empty(),
                    memoryUsage, new FileFormatDataSourceStats(), reader.getCompressionKind());
        }
        catch (Exception e) {
            throw wrapPaimonReadException(e);
        }
    }

    private ConnectorPageSource createParquetDataPageSource(TrinoInputFile inputFile, ParquetReaderOptions options,
            List<String> columns, List<Type> types, List<Domain> domains, long fileSize)
    {
        try {
            AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
            ParquetDataSource dataSource = createDataSource(inputFile, OptionalLong.of(fileSize), options,
                    memoryContext, new FileFormatDataSourceStats());

            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            // Build column name to Parquet field mapping (case-insensitive)
            Map<String, org.apache.parquet.schema.Type> fieldsByName = parquetFieldsByLowercaseName(
                    fileSchema.getFields());

            // Build requested schema from requested columns
            List<org.apache.parquet.schema.Type> requestedFields = new ArrayList<>();
            for (String columnName : columns) {
                // Use lowercase for case-insensitive lookup
                if (columnName != null && fieldsByName.containsKey(FieldNameUtils.toLowerCase(columnName))) {
                    requestedFields.add(fieldsByName.get(FieldNameUtils.toLowerCase(columnName)));
                }
            }

            MessageType requestedSchema = new MessageType(fileSchema.getName(), requestedFields);
            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, requestedSchema);
            Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);

            // Build predicate for row group filtering
            TupleDomain<ColumnDescriptor> parquetTupleDomain = buildParquetTupleDomain(descriptorsByPath, columns,
                    domains, fieldsByName);
            TupleDomainParquetPredicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain,
                    descriptorsByPath, DateTimeZone.UTC);

            // Filter row groups based on predicate
            List<RowGroupInfo> rowGroups = getFilteredRowGroups(0, fileSize, dataSource,
                    parquetMetadata.getBlocks(), com.google.common.collect.ImmutableList.of(parquetTupleDomain),
                    com.google.common.collect.ImmutableList.of(parquetPredicate), descriptorsByPath, DateTimeZone.UTC,
                    100, options);

            // Build ParquetPageSource
            ParquetPageSource.Builder pageSourceBuilder = ParquetPageSource.builder();
            com.google.common.collect.ImmutableList.Builder<Column> parquetColumnsBuilder = com.google.common.collect.ImmutableList
                    .builder();
            int parquetSourceChannel = 0;

            for (int i = 0; i < columns.size(); i++) {
                String columnName = columns.get(i);
                Type type = types.get(i);
                String lowerColumnName = columnName == null ? null : FieldNameUtils.toLowerCase(columnName);

                if (lowerColumnName == null || !fieldsByName.containsKey(lowerColumnName)) {
                    parquetSourceChannel = addParquetColumn(columnName, type, Optional.empty(), Optional.empty(),
                            pageSourceBuilder, parquetColumnsBuilder, parquetSourceChannel);
                }
                else {
                    org.apache.parquet.schema.Type parquetField = fieldsByName.get(lowerColumnName);
                    org.apache.parquet.io.ColumnIO columnIO = messageColumnIO.getChild(parquetField.getName());

                    // Convert Parquet field to Trino Field
                    Optional<Field> field = constructField(type, columnIO);
                    parquetSourceChannel = addParquetColumn(columnName, type, Optional.of(parquetField.getName()),
                            field, pageSourceBuilder, parquetColumnsBuilder, parquetSourceChannel);
                }
            }

            ParquetDataSourceId dataSourceId = dataSource.getId();
            ParquetReader parquetReader = new ParquetReader(Optional.ofNullable(fileMetaData.getCreatedBy()),
                    parquetColumnsBuilder.build(), rowGroups, dataSource, DateTimeZone.UTC, memoryContext, options,
                    exception -> handleParquetException(dataSourceId, exception), Optional.of(parquetPredicate),
                    Optional.empty());

            return pageSourceBuilder.build(parquetReader);
        }
        catch (Exception e) {
            throw wrapPaimonReadException("Failed to create Parquet page source", e);
        }
    }

    static int addParquetColumn(String columnName, Type type, Optional<String> parquetFieldName, Optional<Field> field,
            ParquetPageSource.Builder pageSourceBuilder,
            com.google.common.collect.ImmutableList.Builder<Column> parquetColumnsBuilder, int parquetSourceChannel)
    {
        requireNonNull(type, "type is null");
        requireNonNull(parquetFieldName, "parquetFieldName is null");
        requireNonNull(field, "field is null");
        requireNonNull(pageSourceBuilder, "pageSourceBuilder is null");
        requireNonNull(parquetColumnsBuilder, "parquetColumnsBuilder is null");
        if (parquetSourceChannel < 0) {
            throw new IllegalArgumentException("parquetSourceChannel is negative: " + parquetSourceChannel);
        }
        if (parquetFieldName.isEmpty()) {
            pageSourceBuilder.addNullColumn(type);
            return parquetSourceChannel;
        }
        if (field.isEmpty()) {
            throw new IllegalStateException("Parquet file column '%s' exists but cannot be read as %s"
                    .formatted(columnName, type.getDisplayName()));
        }
        parquetColumnsBuilder.add(new Column(parquetFieldName.get(), field.get()));
        pageSourceBuilder.addSourceColumn(parquetSourceChannel);
        return parquetSourceChannel + 1;
    }

    private TupleDomain<ColumnDescriptor> buildParquetTupleDomain(Map<List<String>, ColumnDescriptor> descriptorsByPath,
            List<String> columns, List<Domain> domains, Map<String, org.apache.parquet.schema.Type> fieldsByName)
    {
        com.google.common.collect.ImmutableMap.Builder<ColumnDescriptor, Domain> predicateBuilder = com.google.common.collect.ImmutableMap
                .builder();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i) != null && domains.get(i) != null) {
                String columnName = FieldNameUtils.toLowerCase(columns.get(i));
                if (fieldsByName.containsKey(columnName)) {
                    org.apache.parquet.schema.Type parquetType = fieldsByName.get(columnName);
                    if (parquetType.isPrimitive()) {
                        ColumnDescriptor descriptor = descriptorsByPath
                                .get(com.google.common.collect.ImmutableList.of(parquetType.getName()));
                        if (descriptor != null) {
                            predicateBuilder.put(descriptor, domains.get(i));
                        }
                    }
                }
            }
        }
        return TupleDomain.withColumnDomains(predicateBuilder.buildOrThrow());
    }

    static Map<String, OrcColumn> orcFieldsByLowercaseName(List<OrcColumn> columns)
    {
        requireNonNull(columns, "columns is null");
        Map<String, OrcColumn> fieldsByName = new HashMap<>();
        for (OrcColumn column : columns) {
            requireNonNull(column, "columns contains null column");
            String lowerColumnName = FieldNameUtils.toLowerCase(column.getColumnName());
            OrcColumn previous = fieldsByName.putIfAbsent(lowerColumnName, column);
            if (previous != null) {
                throw new IllegalStateException("ORC file schema contains case-insensitive duplicate field name '%s'"
                        .formatted(lowerColumnName));
            }
        }
        return fieldsByName;
    }

    static Map<String, org.apache.parquet.schema.Type> parquetFieldsByLowercaseName(
            List<org.apache.parquet.schema.Type> fields)
    {
        requireNonNull(fields, "fields is null");
        Map<String, org.apache.parquet.schema.Type> fieldsByName = new HashMap<>();
        for (org.apache.parquet.schema.Type field : fields) {
            requireNonNull(field, "fields contains null field");
            String lowerFieldName = FieldNameUtils.toLowerCase(field.getName());
            org.apache.parquet.schema.Type previous = fieldsByName.putIfAbsent(lowerFieldName, field);
            if (previous != null) {
                throw new IllegalStateException("Parquet file schema contains case-insensitive duplicate field name '%s'"
                        .formatted(lowerFieldName));
            }
        }
        return fieldsByName;
    }

    private Optional<Field> constructField(Type type, org.apache.parquet.io.ColumnIO columnIO)
    {
        if (columnIO == null) {
            return Optional.empty();
        }
        return io.trino.parquet.ParquetTypeUtils.constructField(type, columnIO);
    }

    private static RuntimeException handleParquetException(ParquetDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof io.trino.parquet.ParquetCorruptionException) {
            return new RuntimeException("Parquet file is corrupted: " + dataSourceId, exception);
        }
        if (exception instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        return new RuntimeException("Error reading Parquet file: " + dataSourceId, exception);
    }
}
