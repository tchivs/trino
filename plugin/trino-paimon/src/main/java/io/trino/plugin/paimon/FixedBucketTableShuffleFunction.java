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

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.bucket.BucketFunction;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class FixedBucketTableShuffleFunction
        implements
        io.trino.spi.connector.BucketFunction
{
    private final int workerCount;
    private final int bucketCount;
    private final boolean isRowId;
    private final ThreadLocal<Projection> partitionProjectionContext;
    private final ThreadLocal<Projection> bucketKeyProjectionContext;
    private final BucketFunction bucketFunction;
    private final List<Type> paimonRowTypes;
    private final List<DataType> paimonLogicalTypes;

    public FixedBucketTableShuffleFunction(List<Type> partitionChannelTypes, PaimonPartitioningHandle partitioningHandle,
            int workerCount)
    {
        requireNonNull(partitionChannelTypes, "partitionChannelTypes is null");
        partitionChannelTypes.forEach(type -> requireNonNull(type, "partitionChannelTypes contains null type"));
        requireNonNull(partitioningHandle, "partitioningHandle is null");
        checkArgument(workerCount > 0, "workerCount must be positive: %s", workerCount);
        TableSchema schema = partitioningHandle.getOriginalSchema();
        this.isRowId = partitionChannelTypes.size() == 1 && partitionChannelTypes.get(0) instanceof RowType;
        if (isRowId) {
            validateRowIdType((RowType) partitionChannelTypes.get(0), schema);
        }
        List<String> inputFields = isRowId ? schema.primaryKeys() : fixedBucketWritePartitionColumns(schema);
        this.paimonRowTypes = isRowId ? partitionChannelTypes.get(0).getTypeParameters()
                : List.copyOf(partitionChannelTypes);
        this.paimonLogicalTypes = projectedTypes(schema, inputFields);
        verify(paimonLogicalTypes.size() == paimonRowTypes.size(), "Paimon row type metadata size mismatch");
        org.apache.paimon.types.RowType inputType = schema.projectedLogicalRowType(inputFields);
        this.partitionProjectionContext = ThreadLocal.withInitial(() ->
                CodeGenUtils.newProjection(inputType, projection(inputFields, schema.partitionKeys(), "partition key")));
        this.bucketKeyProjectionContext = ThreadLocal.withInitial(() ->
                CodeGenUtils.newProjection(inputType, projection(inputFields, schema.bucketKeys(), "bucket key")));
        this.bucketFunction = BucketFunction.create(new CoreOptions(schema.options()), schema.logicalBucketKeyType());
        this.bucketCount = new CoreOptions(schema.options()).bucket();
        this.workerCount = workerCount;
    }

    @Override
    public int getBucket(Page page, int position)
    {
        if (isRowId) {
            RowBlock rowBlock = (RowBlock) page.getBlock(0);
            page = new Page(rowBlock.getPositionCount(), rowBlock.getFieldBlocks().toArray(Block[]::new));
        }

        PaimonRow paimonRow = new PaimonRow(page, position, RowKind.INSERT, paimonRowTypes,
                paimonLogicalTypes);
        BinaryRow partition = partitionProjectionContext.get().apply(paimonRow);
        BinaryRow bucketKey = bucketKeyProjectionContext.get().apply(paimonRow);
        int bucket = bucketFunction.bucket(bucketKey, bucketCount);
        return ChannelComputer.select(partition, bucket, workerCount);
    }

    private static void validateRowIdType(RowType rowIdType, TableSchema schema)
    {
        List<DataField> primaryKeyFields = primaryKeyFields(schema);
        verify(rowIdType.getFields().size() == schema.primaryKeys().size(),
                "Paimon row id field count (%s) must match primary key count (%s)",
                rowIdType.getFields().size(), schema.primaryKeys().size());
        for (int index = 0; index < schema.primaryKeys().size(); index++) {
            String primaryKey = schema.primaryKeys().get(index);
            RowType.Field field = rowIdType.getFields().get(index);
            verify(field.getName().isPresent(),
                    "Paimon row id field at index %s must be named", index);
            verify(field.getName().get().equals(primaryKey),
                    "Paimon row id field at index %s must be primary key '%s', got '%s'",
                    index, primaryKey, field.getName().get());
            DataType expectedType = primaryKeyFields.get(index).type();
            verify(PaimonColumnHandle.matchesTrinoType(expectedType, field.getType()),
                    "Paimon row id field '%s' type must match Paimon primary key type %s, got %s",
                    primaryKey, expectedType.asSQLString(), field.getType());
        }
    }

    private static List<DataField> primaryKeyFields(TableSchema schema)
    {
        return schema.primaryKeys().stream()
                .map(primaryKey -> {
                    verify(schema.logicalRowType().containsField(primaryKey),
                            "Paimon primary key '%s' is not present in table schema", primaryKey);
                    return schema.logicalRowType().getField(primaryKey);
                })
                .toList();
    }

    private static List<String> fixedBucketWritePartitionColumns(TableSchema schema)
    {
        List<String> partitionColumns = new ArrayList<>(schema.partitionKeys());
        partitionColumns.addAll(schema.bucketKeys());
        return List.copyOf(partitionColumns);
    }

    private static List<DataType> projectedTypes(TableSchema schema, List<String> fieldNames)
    {
        return fieldNames.stream()
                .map(fieldName -> {
                    verify(schema.logicalRowType().containsField(fieldName),
                            "Paimon field '%s' is not present in table schema", fieldName);
                    return schema.logicalRowType().getField(fieldName).type();
                })
                .toList();
    }

    private static int[] projection(List<String> inputFields, List<String> projectedFields, String fieldDescription)
    {
        return projectedFields.stream()
                .mapToInt(projectedField -> {
                    int index = inputFields.indexOf(projectedField);
                    verify(index >= 0, "Paimon %s '%s' is not present in shuffle input fields %s",
                            fieldDescription, projectedField, inputFields);
                    return index;
                })
                .toArray();
    }
}
