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
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.type.Type;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.BucketAssigner;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.paimon.PaimonDynamicBucketUtils.dynamicBucketNumAssigners;
import static io.trino.plugin.paimon.PaimonDynamicBucketUtils.dynamicBucketWritePartitionColumns;
import static java.util.Objects.requireNonNull;

public class DynamicBucketTableShuffleFunction
        implements BucketFunction
{
    private final int assignerChannelCount;
    private final int numAssigners;
    private final ThreadLocal<Projection> partitionProjectionContext;
    private final ThreadLocal<Projection> trimmedPrimaryKeyProjectionContext;
    private final List<Type> paimonRowTypes;
    private final List<DataType> paimonLogicalTypes;

    public DynamicBucketTableShuffleFunction(List<Type> partitionChannelTypes, PaimonPartitioningHandle partitioningHandle,
            int assignerCount)
    {
        requireNonNull(partitionChannelTypes, "partitionChannelTypes is null");
        partitionChannelTypes.forEach(type -> requireNonNull(type, "partitionChannelTypes contains null type"));
        requireNonNull(partitioningHandle, "partitioningHandle is null");
        checkArgument(assignerCount > 0, "assignerCount must be positive: %s", assignerCount);
        TableSchema schema = partitioningHandle.getOriginalSchema();
        List<String> inputFields = dynamicBucketWritePartitionColumns(schema);
        checkArgument(partitionChannelTypes.size() == inputFields.size(),
                "partitionChannelTypes size (%s) must match dynamic bucket input field count (%s)",
                partitionChannelTypes.size(), inputFields.size());
        this.paimonRowTypes = List.copyOf(partitionChannelTypes);
        this.paimonLogicalTypes = projectedTypes(schema, inputFields);
        verify(paimonLogicalTypes.size() == paimonRowTypes.size(), "Paimon row type metadata size mismatch");
        org.apache.paimon.types.RowType inputType = schema.projectedLogicalRowType(inputFields);
        this.partitionProjectionContext = ThreadLocal.withInitial(() ->
                CodeGenUtils.newProjection(inputType, projection(inputFields, schema.partitionKeys(), "partition key")));
        this.trimmedPrimaryKeyProjectionContext = ThreadLocal.withInitial(() ->
                CodeGenUtils.newProjection(inputType,
                        projection(inputFields, schema.trimmedPrimaryKeys(), "trimmed primary key")));
        this.assignerChannelCount = assignerCount;
        this.numAssigners = dynamicBucketNumAssigners(new CoreOptions(schema.options()), assignerCount);
    }

    @Override
    public int getBucket(Page page, int position)
    {
        PaimonRow paimonRow = new PaimonRow(page, position, RowKind.INSERT, paimonRowTypes, paimonLogicalTypes);
        BinaryRow partition = partitionProjectionContext.get().apply(paimonRow);
        BinaryRow trimmedPrimaryKey = trimmedPrimaryKeyProjectionContext.get().apply(paimonRow);
        return BucketAssigner.computeAssigner(
                partition.hashCode(),
                trimmedPrimaryKey.hashCode(),
                assignerChannelCount,
                numAssigners);
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
        Map<String, Integer> inputFieldIndexes = new HashMap<>();
        for (int index = 0; index < inputFields.size(); index++) {
            inputFieldIndexes.putIfAbsent(inputFields.get(index), index);
        }
        return projectedFields.stream()
                .mapToInt(projectedField -> {
                    Integer index = inputFieldIndexes.get(projectedField);
                    verify(index != null, "Paimon %s '%s' is not present in shuffle input fields %s",
                            fieldDescription, projectedField, inputFields);
                    return index;
                })
                .toArray();
    }
}
