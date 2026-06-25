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
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PaimonNodePartitioningProvider
        implements
        ConnectorNodePartitioningProvider
{
    @Inject
    public PaimonNodePartitioningProvider()
    {
    }

    @Override
    public Optional<ConnectorBucketNodeMap> getBucketNodeMapping(ConnectorTransactionHandle transactionHandle,
            ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        if (getPartitioningHandle(partitioningHandle).isSingleNode()) {
            return Optional.of(ConnectorBucketNodeMap.createBucketNodeMap(1));
        }
        return Optional.empty();
    }

    @Override
    public BucketFunction getBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle, List<Type> partitionChannelTypes, int workerCount)
    {
        PaimonPartitioningHandle paimonPartitioningHandle = getPartitioningHandle(partitioningHandle);
        requireNonNull(partitionChannelTypes, "partitionChannelTypes is null");
        partitionChannelTypes.forEach(type -> requireNonNull(type, "partitionChannelTypes contains null type"));
        checkArgument(workerCount > 0, "workerCount must be positive: %s", workerCount);
        if (paimonPartitioningHandle.isSingleNode()) {
            return (page, position) -> 0;
        }
        return new FixedBucketTableShuffleFunction(partitionChannelTypes, paimonPartitioningHandle, workerCount);
    }

    static PaimonPartitioningHandle getPartitioningHandle(ConnectorPartitioningHandle partitioningHandle)
    {
        if (!(requireNonNull(partitioningHandle, "partitioningHandle is null") instanceof PaimonPartitioningHandle paimonPartitioningHandle)) {
            throw new IllegalStateException("Paimon node partitioning requires PaimonPartitioningHandle, got: "
                    + partitioningHandle.getClass().getName());
        }
        return paimonPartitioningHandle;
    }
}
