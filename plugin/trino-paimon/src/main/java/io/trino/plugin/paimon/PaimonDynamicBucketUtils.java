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

import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.TableSchema;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

final class PaimonDynamicBucketUtils
{
    private PaimonDynamicBucketUtils() {}

    static List<String> dynamicBucketWritePartitionColumns(TableSchema schema)
    {
        requireNonNull(schema, "schema is null");
        List<String> partitionColumns = new ArrayList<>(schema.partitionKeys());
        partitionColumns.addAll(schema.trimmedPrimaryKeys());
        return List.copyOf(partitionColumns);
    }

    static int dynamicBucketAssignerParallelism(CoreOptions coreOptions, int workerCount)
    {
        requireNonNull(coreOptions, "coreOptions is null");
        checkArgument(workerCount > 0, "workerCount must be positive: %s", workerCount);
        Integer configuredParallelism = coreOptions.dynamicBucketAssignerParallelism();
        if (configuredParallelism == null) {
            return workerCount;
        }
        checkArgument(configuredParallelism > 0,
                "dynamic-bucket.assigner-parallelism must be positive: %s", configuredParallelism);
        return Math.min(configuredParallelism, workerCount);
    }

    static List<Node> dynamicBucketAssignerNodes(NodeManager nodeManager, CoreOptions coreOptions)
    {
        requireNonNull(nodeManager, "nodeManager is null");
        List<Node> workers = nodeManager.getRequiredWorkerNodes().stream()
                .sorted(Comparator.comparing((Node node) -> node.getHostAndPort().toString())
                        .thenComparing(Node::getNodeIdentifier))
                .toList();
        int assignerParallelism = dynamicBucketAssignerParallelism(coreOptions, workers.size());
        return List.copyOf(workers.subList(0, assignerParallelism));
    }
}
