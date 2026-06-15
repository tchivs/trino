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
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class PaimonPageSink
        implements
        ConnectorPageSink
{
    private final BatchTableWrite writer;
    private final List<Type> columnTypes;
    private final List<DataType> logicalTypes;

    public PaimonPageSink(BatchTableWrite writer, List<Type> columnTypes, List<DataType> logicalTypes)
    {
        this.writer = requireNonNull(writer, "writer is null");
        this.columnTypes = copyColumnTypes(columnTypes);
        this.logicalTypes = copyLogicalTypes(logicalTypes);
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
                writer.write(new PaimonRow(page.getSingleValuePage(i), rowKind, columnTypes, logicalTypes));
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
            throw wrapWriteException(e);
        }
    }

    @Nullable
    static RuntimeException closeWriter(BatchTableWrite writer, @Nullable RuntimeException failure)
    {
        try {
            writer.close();
        }
        catch (Exception e) {
            RuntimeException closeFailure = wrapWriteException(e);
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
        if (exception instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        return new RuntimeException(exception);
    }
}
