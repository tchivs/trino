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
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.index.BucketAssigner;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;

import java.util.ArrayList;
import java.util.Collection;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_WRITER_CLOSE_ERROR;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_WRITER_DATA_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class PaimonPageSink
        implements
        ConnectorPageSink
{
    private final BatchTableWrite writer;
    private final List<Type> columnTypes;
    private final List<DataType> logicalTypes;
    @Nullable
    private final DynamicBucketWriter dynamicBucketWriter;

    public PaimonPageSink(BatchTableWrite writer, List<Type> columnTypes, List<DataType> logicalTypes)
    {
        this(writer, columnTypes, logicalTypes, null);
    }

    public PaimonPageSink(BatchTableWrite writer, List<Type> columnTypes, List<DataType> logicalTypes,
            @Nullable DynamicBucketWriter dynamicBucketWriter)
    {
        this.writer = requireNonNull(writer, "writer is null");
        this.columnTypes = copyColumnTypes(columnTypes);
        this.logicalTypes = copyLogicalTypes(logicalTypes);
        this.dynamicBucketWriter = dynamicBucketWriter;
        checkArgument(this.columnTypes.size() == this.logicalTypes.size(),
                "columnTypes and logicalTypes size mismatch: %s != %s",
                this.columnTypes.size(), this.logicalTypes.size());
    }

    private static List<Type> copyColumnTypes(List<Type> columnTypes)
    {
        requireNonNull(columnTypes, "columnTypes is null").forEach(columnType ->
                requireNonNull(columnType, "columnTypes contains null type"));
        return List.copyOf(columnTypes);
    }

    private static List<DataType> copyLogicalTypes(List<DataType> logicalTypes)
    {
        requireNonNull(logicalTypes, "logicalTypes is null").forEach(logicalType ->
                requireNonNull(logicalType, "logicalTypes contains null type"));
        return List.copyOf(logicalTypes);
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        try {
            writePage(page, RowKind.INSERT);
        }
        catch (Exception e) {
            throw wrapWriteException(e);
        }
        return NOT_BLOCKED;
    }

    public void writePage(Page page, RowKind rowKind)
    {
        requireNonNull(page, "page is null");
        requireNonNull(rowKind, "rowKind is null");
        checkArgument(page.getChannelCount() == columnTypes.size(),
                "page channel count (%s) must match write column count (%s)",
                page.getChannelCount(), columnTypes.size());
        try {
            for (int i = 0; i < page.getPositionCount(); i++) {
                PaimonRow row = new PaimonRow(page, i, rowKind, columnTypes, logicalTypes);
                if (dynamicBucketWriter == null) {
                    writer.write(row);
                }
                else {
                    dynamicBucketWriter.write(writer, row);
                }
            }
        }
        catch (Exception e) {
            throw wrapWriteException(e);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        Collection<Slice> commitTasks = new ArrayList<>();
        RuntimeException failure = null;
        try {
            if (dynamicBucketWriter != null) {
                dynamicBucketWriter.prepareCommit();
            }
            List<CommitMessage> commitMessages = requireNonNull(writer.prepareCommit(), "Paimon writer returned null commit messages");
            CommitMessageSerializer serializer = new CommitMessageSerializer();
            for (CommitMessage commitMessage : commitMessages) {
                commitTasks.add(wrappedBuffer(serializer.serialize(
                        requireNonNull(commitMessage, "Paimon writer returned null commit message"))));
            }
        }
        catch (Exception e) {
            failure = wrapWriteException(e);
        }
        failure = closeWriter(failure);
        if (failure != null) {
            throw failure;
        }
        return completedFuture(commitTasks);
    }

    @Override
    public void abort()
    {
        try {
            writer.close();
        }
        catch (Exception e) {
            throw wrapWriterCloseException(e);
        }
    }

    @Nullable
    static RuntimeException closeWriter(BatchTableWrite writer, @Nullable RuntimeException failure)
    {
        try {
            writer.close();
        }
        catch (Exception e) {
            RuntimeException closeFailure = wrapWriterCloseException(e);
            if (failure != null) {
                failure.addSuppressed(closeFailure);
            }
            else {
                failure = closeFailure;
            }
        }
        return failure;
    }

    @Nullable
    private RuntimeException closeWriter(@Nullable RuntimeException failure)
    {
        return closeWriter(writer, failure);
    }

    static RuntimeException wrapWriteException(Exception exception)
    {
        if (exception instanceof TrinoException trinoException) {
            return trinoException;
        }
        if (exception instanceof UnsupportedOperationException unsupportedOperationException) {
            String detail = unsupportedOperationException.getMessage();
            return new TrinoException(NOT_SUPPORTED,
                    detail == null || detail.isBlank()
                            ? "Paimon write uses features which are not supported by the Trino connector"
                            : "Paimon write uses features which are not supported by the Trino connector: " + detail,
                    unsupportedOperationException);
        }
        if (exception instanceof IllegalArgumentException
                || exception instanceof IllegalStateException
                || exception instanceof NullPointerException
                || exception instanceof IllegalFormatException) {
            return (RuntimeException) exception;
        }
        if (exception instanceof RuntimeException runtimeException) {
            return new TrinoException(PAIMON_WRITER_DATA_ERROR, "Failed to write data to Paimon", runtimeException);
        }
        return new TrinoException(PAIMON_WRITER_DATA_ERROR, "Failed to write data to Paimon", exception);
    }

    static RuntimeException wrapWriterCloseException(Exception exception)
    {
        if (exception instanceof TrinoException trinoException) {
            return trinoException;
        }
        if (exception instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        return new TrinoException(PAIMON_WRITER_CLOSE_ERROR, "Failed to close Paimon writer", exception);
    }

    static class DynamicBucketWriter
    {
        private final RowPartitionKeyExtractor keyExtractor;
        private final BucketAssigner bucketAssigner;

        DynamicBucketWriter(RowPartitionKeyExtractor keyExtractor, BucketAssigner bucketAssigner)
        {
            this.keyExtractor = requireNonNull(keyExtractor, "keyExtractor is null");
            this.bucketAssigner = requireNonNull(bucketAssigner, "bucketAssigner is null");
        }

        void write(BatchTableWrite writer, InternalRow row)
                throws Exception
        {
            BinaryRow partition = keyExtractor.partition(row);
            int bucket = bucketAssigner.assign(partition, keyExtractor.trimmedPrimaryKey(row).hashCode());
            // TODO: Split HASH_DYNAMIC writes into a Flink-style bucket assigner stage and
            // partition+bucket writer stage once the connector can coordinate bucket index state.
            writer.write(row, bucket);
        }

        void prepareCommit()
        {
            bucketAssigner.prepareCommit(BatchWriteBuilder.COMMIT_IDENTIFIER);
        }
    }
}
