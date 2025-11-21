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
import io.trino.hive.formats.avro.AvroTypeException;
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
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.avro.AvroPageSource;
import io.trino.plugin.hive.orc.OrcPageSource;
import io.trino.plugin.hive.parquet.ParquetPageSource;
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fileindex.FileIndexPredicate;
import org.apache.paimon.fs.Path;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.IndexFile;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
        this.orcReaderOptions = orcReaderConfig.toOrcReaderOptions()
                // Default tiny stripe size 8 M is too big for paimon.
                // Cache stripe will cause more read (I want to read one column,
                // but not the whole stripe)
                .withTinyStripeThreshold(DataSize.of(4, DataSize.Unit.KILOBYTE));
        this.parquetReaderOptions = parquetReaderConfig.toParquetReaderOptions();
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session,
            ConnectorSplit split, ConnectorTableHandle tableHandle, List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        paimonCatalog.initSession(session);
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Table table = paimonTableHandle.tableWithDynamicOptions(paimonCatalog, session);
        return runWithContextClassLoader(() -> {
            Optional<PaimonColumnHandle> rowId = columns.stream().map(PaimonColumnHandle.class::cast)
                    .filter(column -> column.isRowId()).findFirst();
            if (rowId.isPresent()) {
                List<ColumnHandle> dataColumns = columns.stream().map(PaimonColumnHandle.class::cast)
                        .filter(column -> !column.isRowId()).collect(Collectors.toList());
                Set<String> rowIdFileds = ((io.trino.spi.type.RowType) rowId.get().getTrinoType()).getFields().stream()
                        .map(io.trino.spi.type.RowType.Field::getName).map(Optional::get).collect(Collectors.toSet());

                HashMap<String, Integer> fieldToIndex = new HashMap<>();
                for (int i = 0; i < dataColumns.size(); i++) {
                    PaimonColumnHandle paimonColumnHandle = (PaimonColumnHandle) dataColumns.get(i);
                    if (rowIdFileds.contains(paimonColumnHandle.getColumnName())) {
                        fieldToIndex.put(paimonColumnHandle.getColumnName(), i);
                    }
                }
                return PaimonMergePageSourceWrapper.wrap(createPageSource(session, table, paimonTableHandle.getFilter(),
                        (PaimonSplit) split, dataColumns, paimonTableHandle.getLimit()), fieldToIndex);
            }
            else {
                return createPageSource(session, table, paimonTableHandle.getFilter(), (PaimonSplit) split, columns,
                        paimonTableHandle.getLimit());
            }
        }, PaimonPageSourceProvider.class.getClassLoader());
    }

    private ConnectorPageSource createPageSource(ConnectorSession session, Table table,
            TupleDomain<PaimonColumnHandle> filter, PaimonSplit split, List<ColumnHandle> columns, OptionalLong limit)
    {
        RowType rowType = table.rowType();
        List<String> fieldNames = rowType.getFieldNames();
        List<String> projectedFields = columns.stream().map(PaimonColumnHandle.class::cast)
                .map(PaimonColumnHandle::getColumnName).toList();
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        Optional<Predicate> paimonFilter = new PaimonFilterConverter(rowType).convert(filter);

        try {
            Split paimonSplit = split.decodeSplit();
            Optional<List<RawFile>> optionalRawFiles = paimonSplit.convertToRawFiles();
            if (checkRawFile(optionalRawFiles)) {
                FileStoreTable fileStoreTable = (FileStoreTable) table;
                boolean readIndex = fileStoreTable.coreOptions().fileIndexReadEnabled();

                Optional<List<DeletionFile>> deletionFiles = paimonSplit.deletionFiles();
                Optional<List<IndexFile>> indexFiles = readIndex ? paimonSplit.indexFiles() : Optional.empty();
                SchemaManager schemaManager = new SchemaManager(fileStoreTable.fileIO(), fileStoreTable.location());
                List<Type> type = columns.stream().map(s -> ((PaimonColumnHandle) s).getTrinoType())
                        .collect(Collectors.toList());

                try {
                    List<RawFile> files = optionalRawFiles.orElseThrow();
                    LinkedList<ConnectorPageSource> sources = new LinkedList<>();

                    // if file index exists, do the filter.
                    for (int i = 0; i < files.size(); i++) {
                        RawFile rawFile = files.get(i);
                        if (indexFiles.isPresent()) {
                            IndexFile indexFile = indexFiles.get().get(i);
                            if (indexFile != null && paimonFilter.isPresent()) {
                                try (FileIndexPredicate fileIndexPredicate = new FileIndexPredicate(
                                        new Path(indexFile.path()), table.fileIO(), rowType)) {
                                    if (!fileIndexPredicate.evaluate(paimonFilter.get()).remain()) {
                                        continue;
                                    }
                                }
                            }
                        }

                        // Schema evolution: map table column names to data file column names
                        // Paimon stores column names in lowercase in ORC/Parquet files,
                        // so we need to convert to lowercase for file reading
                        List<String> dataFileColumns;
                        long tableSchemaId = fileStoreTable.schema().id();
                        long fileSchemaId = rawFile.schemaId();

                        if (tableSchemaId == fileSchemaId) {
                            // No schema evolution - convert projected fields to lowercase
                            // because Paimon writes column names in lowercase to files
                            dataFileColumns = FieldNameUtils.toLowerCase(projectedFields);
                        }
                        else {
                            // Schema evolution: map table fields to data file fields by ID
                            dataFileColumns = schemaEvolutionFieldNames(projectedFields, rowType.getFields(),
                                    schemaManager.schema(fileSchemaId).fields());
                        }

                        ConnectorPageSource source = createDataPageSource(rawFile.format(),
                                fileSystem.newInputFile(Location.of(rawFile.path())), fileStoreTable.coreOptions(),
                                dataFileColumns, type, orderDomains(projectedFields, filter));

                        if (deletionFiles.isPresent()) {
                            source = PaimonPageSourceWrapper.wrap(source,
                                    Optional.ofNullable(deletionFiles.get().get(i)).map(deletionFile -> {
                                        try {
                                            return DeletionVector.read(fileStoreTable.fileIO(), deletionFile);
                                        }
                                        catch (IOException e) {
                                            throw new RuntimeException(e);
                                        }
                                    }));
                        }
                        sources.add(source);
                    }

                    return new DirectTrinoPageSource(sources);
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            else {
                // Use case-insensitive lookup for field names
                int[] columnIndex = projectedFields.stream().mapToInt(field -> {
                    for (int i = 0; i < fieldNames.size(); i++) {
                        if (fieldNames.get(i).equalsIgnoreCase(field)) {
                            return i;
                        }
                    }
                    return -1;
                }).toArray();

                // old read way
                ReadBuilder read = table.newReadBuilder();
                paimonFilter.ifPresent(read::withFilter);

                if (!fieldNames.equals(projectedFields)) {
                    read.withProjection(columnIndex);
                }

                return new PaimonPageSource(read.newRead().executeFilter().createReader(paimonSplit), columns, limit);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // make domains(filters) to be ordered by projected fields' order.
    private List<Domain> orderDomains(List<String> projectedFields, TupleDomain<PaimonColumnHandle> filter)
    {
        Optional<Map<PaimonColumnHandle, Domain>> optionalFilter = filter.getDomains();
        Map<String, Domain> domainMap = new HashMap<>();
        optionalFilter.ifPresent(trinoColumnHandleDomainMap -> trinoColumnHandleDomainMap
                .forEach((k, v) -> domainMap.put(k.getColumnName(), v)));

        return projectedFields.stream().map(name -> domainMap.getOrDefault(name, null)).collect(Collectors.toList());
    }

    private boolean checkRawFile(Optional<List<RawFile>> optionalRawFiles)
    {
        return optionalRawFiles.isPresent() && canUseTrinoPageSource(optionalRawFiles.get());
    }

    // Support orc, parquet and avro formats
    private boolean canUseTrinoPageSource(List<RawFile> rawFiles)
    {
        for (RawFile rawFile : rawFiles) {
            String format = rawFile.format();
            if (!format.equals("orc") && !format.equals("parquet") && !format.equals("avro")) {
                return false;
            }
        }
        return true;
    }

    // map the table schema column names to data schema column names
    // Paimon stores column names in lowercase, so we return lowercase names
    private List<String> schemaEvolutionFieldNames(List<String> fieldNames, List<DataField> tableFields,
            List<DataField> dataFields)
    {
        Map<String, Integer> fieldNameToId = new HashMap<>();
        Map<Integer, String> idToFieldName = new HashMap<>();
        List<String> result = new ArrayList<>();

        // Build maps: lowercase name -> field ID (from table), field ID -> lowercase field name (from data file)
        tableFields.forEach(field -> {
            String lowerName = FieldNameUtils.toLowerCase(field.name());
            fieldNameToId.put(lowerName, field.id());
        });
        dataFields.forEach(field -> {
            // Store lowercase field name because Paimon writes files with lowercase column names
            idToFieldName.put(field.id(), FieldNameUtils.toLowerCase(field.name()));
        });

        for (String fieldName : fieldNames) {
            // Convert to lowercase for case-insensitive lookup
            String lowerFieldName = FieldNameUtils.toLowerCase(fieldName);
            Integer id = fieldNameToId.get(lowerFieldName);
            if (id != null && idToFieldName.containsKey(id)) {
                // Return the lowercase field name for file reading
                result.add(idToFieldName.get(id));
            }
            else {
                result.add(null);
            }
        }
        return result;
    }

    /**
     * Create a page source for reading data files directly using Trino's native
     * readers. Currently uses globally configured reader options (from
     * OrcReaderConfig/ParquetReaderConfig). Future enhancement: Could override
     * reader options based on table-level CoreOptions (e.g., compression settings,
     * bloom filter options).
     *
     * @param format
     *            the file format (orc, parquet, or avro)
     * @param inputFile
     *            the input file to read
     * @param coreOptions
     *            table-level core options (currently unused, reserved for future
     *            enhancements)
     * @param columns
     *            the columns to read
     * @param types
     *            the column types
     * @param domains
     *            the filter domains
     * @return a ConnectorPageSource for reading the file
     */
    private ConnectorPageSource createDataPageSource(String format, TrinoInputFile inputFile, CoreOptions coreOptions,
            List<String> columns, List<Type> types, List<Domain> domains)
    {
        switch (format) {
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
            case "avro" : {
                try {
                    return createAvroDataPageSource(inputFile, columns, types, inputFile.length());
                }
                catch (IOException e) {
                    throw new RuntimeException("Failed to create Avro page source", e);
                }
                catch (AvroTypeException e) {
                    throw new RuntimeException("Avro type resolution error", e);
                }
            }
            default : {
                throw new RuntimeException("Unsupport file format: " + format);
            }
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
            Map<String, OrcColumn> fieldsMap = new HashMap<>();
            for (OrcColumn column : fileColumns) {
                fieldsMap.put(FieldNameUtils.toLowerCase(column.getColumnName()), column);
            }
            TupleDomainOrcPredicate.TupleDomainOrcPredicateBuilder predicateBuilder = TupleDomainOrcPredicate.builder();
            List<OrcColumn> fileReadColumns = new ArrayList<>(columns.size());
            List<Type> fileReadTypes = new ArrayList<>(columns.size());

            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i) != null) {
                    // Use lowercase for case-insensitive lookup
                    OrcColumn orcColumn = fieldsMap.get(FieldNameUtils.toLowerCase(columns.get(i)));
                    if (orcColumn == null) {
                        throw new RuntimeException("Column " + columns.get(i) + " does not exist in orc file.");
                    }
                    fileReadColumns.add(orcColumn);
                    fileReadTypes.add(types.get(i));
                    if (domains.get(i) != null) {
                        predicateBuilder.addColumn(orcColumn.getColumnId(), domains.get(i));
                    }
                }
            }

            AggregatedMemoryContext memoryUsage = newSimpleAggregatedMemoryContext();
            OrcRecordReader recordReader = reader.createRecordReader(fileReadColumns, fileReadTypes, false,
                    predicateBuilder.build(), DateTimeZone.UTC, memoryUsage, INITIAL_BATCH_SIZE, RuntimeException::new);

            return new OrcPageSource(recordReader, orcDataSource, Optional.empty(), Optional.empty(),
                    memoryUsage, new FileFormatDataSourceStats(), reader.getCompressionKind());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ConnectorPageSource createParquetDataPageSource(TrinoInputFile inputFile, ParquetReaderOptions options,
            List<String> columns, List<Type> types, List<Domain> domains, long fileSize)
    {
        try {
            AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
            ParquetDataSource dataSource = createDataSource(inputFile, OptionalLong.of(fileSize), options,
                    memoryContext, new FileFormatDataSourceStats());

            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, options.getMaxFooterReadSize(), Optional.empty());
            io.trino.parquet.metadata.FileMetadata fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            // Build column name to Parquet field mapping (case-insensitive)
            Map<String, org.apache.parquet.schema.Type> fieldsByName = new HashMap<>();
            for (org.apache.parquet.schema.Type field : fileSchema.getFields()) {
                fieldsByName.put(FieldNameUtils.toLowerCase(field.getName()), field);
            }

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
            List<RowGroupInfo> rowGroups = getFilteredRowGroups(0, inputFile.length(), dataSource,
                    parquetMetadata, com.google.common.collect.ImmutableList.of(parquetTupleDomain),
                    com.google.common.collect.ImmutableList.of(parquetPredicate), descriptorsByPath, DateTimeZone.UTC,
                    100, options);

            // Build ParquetPageSource with schema evolution support
            com.google.common.collect.ImmutableList.Builder<Column> parquetColumnsBuilder = com.google.common.collect.ImmutableList
                    .builder();

            // Track column mapping: output index -> delegate index (-1 if missing)
            int[] columnMapping = new int[columns.size()];
            int delegateIndex = 0;
            boolean hasMissingColumns = false;

            for (int i = 0; i < columns.size(); i++) {
                String columnName = columns.get(i);
                Type type = types.get(i);

                if (columnName != null && fieldsByName.containsKey(FieldNameUtils.toLowerCase(columnName))) {
                    org.apache.parquet.schema.Type parquetField = fieldsByName.get(FieldNameUtils.toLowerCase(columnName));
                    org.apache.parquet.io.ColumnIO columnIO = messageColumnIO.getChild(parquetField.getName());

                    // Convert Parquet field to Trino Field
                    Optional<Field> field = constructField(type, columnIO);
                    if (field.isPresent()) {
                        parquetColumnsBuilder.add(new Column(parquetField.getName(), field.get()));
                        columnMapping[i] = delegateIndex++;
                    }
                    else {
                        columnMapping[i] = -1;
                        hasMissingColumns = true;
                    }
                }
                else {
                    columnMapping[i] = -1;
                    hasMissingColumns = true;
                }
            }

            ParquetDataSourceId dataSourceId = dataSource.getId();
            ParquetReader parquetReader = new ParquetReader(
                    Optional.ofNullable(fileMetaData.getCreatedBy()),
                    parquetColumnsBuilder.build(),
                    false,  // appendRowNumberColumn
                    rowGroups,
                    dataSource,
                    DateTimeZone.UTC,
                    memoryContext,
                    options,
                    exception -> handleParquetException(dataSourceId, exception),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());

            ConnectorPageSource pageSource = new ParquetPageSource(parquetReader);

            // Wrap with SchemaEvolutionPageSource if there are missing columns
            if (hasMissingColumns) {
                pageSource = new SchemaEvolutionPageSource(pageSource, columns.size(), columnMapping, types);
            }

            return pageSource;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to create Parquet page source", e);
        }
    }

    private TupleDomain<ColumnDescriptor> buildParquetTupleDomain(Map<List<String>, ColumnDescriptor> descriptorsByPath,
            List<String> columns, List<Domain> domains, Map<String, org.apache.parquet.schema.Type> fieldsByName)
    {
        com.google.common.collect.ImmutableMap.Builder<ColumnDescriptor, Domain> predicateBuilder = com.google.common.collect.ImmutableMap
                .builder();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i) != null && domains.get(i) != null) {
                String columnName = columns.get(i);
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

    private ConnectorPageSource createAvroDataPageSource(TrinoInputFile inputFile, List<String> columns,
            List<Type> types, long length)
            throws IOException,
            AvroTypeException
    {
        // Build Avro schema for requested columns
        Schema avroSchema = buildAvroSchema(columns, types);

        // Create HiveAvroTypeBlockHandler with default timestamp precision
        io.trino.hive.formats.avro.HiveAvroTypeBlockHandler avroTypeHandler = new io.trino.hive.formats.avro.HiveAvroTypeBlockHandler(
                io.trino.spi.type.TimestampType.createTimestampType(3));

        // Create AvroPageSource
        return new AvroPageSource(inputFile, avroSchema, avroTypeHandler, 0, length);
    }

    private Schema buildAvroSchema(List<String> columns, List<Type> types)
    {
        SchemaBuilder.FieldAssembler<Schema> schemaBuilder = SchemaBuilder.record("paimon_record").fields();

        for (int i = 0; i < columns.size(); i++) {
            String columnName = columns.get(i);
            Type trinoType = types.get(i);

            if (columnName == null) {
                // Skip null column names
                continue;
            }

            Schema.Type avroType = mapTrinoTypeToAvroType(trinoType);
            // Create a nullable union type: [null, actualType]
            schemaBuilder = schemaBuilder.name(columnName).type().unionOf().nullType().and()
                    .type(Schema.create(avroType)).endUnion().noDefault();
        }

        return schemaBuilder.endRecord();
    }

    private Schema.Type mapTrinoTypeToAvroType(Type trinoType)
    {
        String typeName = trinoType.getDisplayName().toLowerCase(Locale.ENGLISH);

        if (typeName.equals("boolean")) {
            return Schema.Type.BOOLEAN;
        }
        else if (typeName.equals("tinyint") || typeName.equals("smallint") || typeName.equals("integer")) {
            return Schema.Type.INT;
        }
        else if (typeName.equals("bigint")) {
            return Schema.Type.LONG;
        }
        else if (typeName.equals("real")) {
            return Schema.Type.FLOAT;
        }
        else if (typeName.equals("double")) {
            return Schema.Type.DOUBLE;
        }
        else if (typeName.startsWith("varchar") || typeName.startsWith("char") || typeName.equals("varbinary")) {
            return Schema.Type.STRING;
        }
        else if (typeName.startsWith("array")) {
            return Schema.Type.ARRAY;
        }
        else if (typeName.startsWith("map")) {
            return Schema.Type.MAP;
        }
        else if (typeName.startsWith("row")) {
            return Schema.Type.RECORD;
        }
        else {
            // Default to string for unknown types
            return Schema.Type.STRING;
        }
    }
}
