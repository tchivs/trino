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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.Type;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.paimon.PaimonLongUtils.saturatedAdd;
import static java.util.Objects.requireNonNull;

public class PaimonPageSource
        implements
        ConnectorPageSource
{
    private static final int ROWS_PER_REQUEST = 4096;

    private final CloseableIterator<InternalRow> iterator;
    private final OptionalLong limit;
    private final PaimonPageBuilder pageBuilder;

    private boolean isFinished;
    private boolean closed;
    private long numReturn;
    private long readTimeNanos;

    public PaimonPageSource(RecordReader<InternalRow> reader, List<? extends ColumnHandle> projectedColumns,
            OptionalLong limit)
    {
        this.limit = requireNonNull(limit, "limit is null");
        checkArgument(this.limit.isEmpty() || this.limit.getAsLong() >= 0, "limit must be non-negative");
        RecordReader<InternalRow> recordReader = requireNonNull(reader, "reader is null");
        List<Type> columnTypes = new ArrayList<>();
        List<DataType> logicalTypes = new ArrayList<>();
        requireNonNull(projectedColumns, "projectedColumns is null");
        for (ColumnHandle handle : projectedColumns) {
            if (!(requireNonNull(handle, "projectedColumns contains null column") instanceof PaimonColumnHandle paimonColumnHandle)) {
                throw new IllegalArgumentException("Paimon page source requires PaimonColumnHandle, got: "
                        + handle.getClass().getName());
            }
            columnTypes.add(paimonColumnHandle.getTrinoType());
            logicalTypes.add(paimonColumnHandle.logicalType());
        }

        this.pageBuilder = new PaimonPageBuilder(columnTypes, logicalTypes);
        this.iterator = recordReader.toCloseableIterator();
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(numReturn);
    }

    @Override
    public boolean isFinished()
    {
        return isFinished;
    }

    @Override
    public Page getNextPage()
    {
        return ClassLoaderUtils.runWithContextClassLoader(() -> {
            long start = System.nanoTime();
            try {
                return nextPage();
            }
            catch (TrinoException e) {
                closeAllSuppress(e, this);
                throw e;
            }
            catch (IOException e) {
                closeAllSuppress(e, this);
                throw PaimonPageSourceProvider.wrapPaimonReadException(e);
            }
            catch (UnsupportedOperationException e) {
                closeAllSuppress(e, this);
                throw PaimonPageSourceProvider.wrapPaimonReadException(e);
            }
            catch (RuntimeException e) {
                closeAllSuppress(e, this);
                throw PaimonPageSourceProvider.wrapPaimonReadException(e);
            }
            finally {
                readTimeNanos += System.nanoTime() - start;
            }
        }, PaimonPageSource.class.getClassLoader());
    }

    @Override
    public long getMemoryUsage()
    {
        return pageBuilder.getSizeInBytes();
    }

    @Nullable
    private Page nextPage()
            throws IOException
    {
        if (isFinished) {
            return null;
        }
        int count = 0;
        while (count < ROWS_PER_REQUEST && !pageBuilder.isFull()) {
            if (limit.isPresent() && count >= limit.getAsLong() - numReturn) {
                return finishPage(count);
            }

            if (!iterator.hasNext()) {
                return finishPage(count);
            }

            InternalRow row = iterator.next();
            pageBuilder.appendRow(row);
            count++;
        }

        return returnPage(count);
    }

    @Nullable
    private Page finishPage(int count)
            throws IOException
    {
        isFinished = true;
        Page page = returnPage(count);
        close();
        return page;
    }

    private Page returnPage(int count)
    {
        if (count == 0) {
            return null;
        }
        numReturn = saturatedAdd(numReturn, count, "page position count");
        return pageBuilder.build();
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;
        isFinished = true;
        try {
            this.iterator.close();
        }
        catch (IOException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }
}
