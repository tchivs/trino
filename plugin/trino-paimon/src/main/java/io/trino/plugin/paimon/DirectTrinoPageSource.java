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
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedList;
import java.util.OptionalLong;

import static java.lang.Math.toIntExact;

public class DirectTrinoPageSource
        implements
        ConnectorPageSource
{
    private final LinkedList<ConnectorPageSource> pageSourceQueue;
    private final OptionalLong limit;
    private ConnectorPageSource current;
    private long completedBytes;
    private long completedPositions;
    private boolean closed;

    public DirectTrinoPageSource(LinkedList<ConnectorPageSource> pageSourceQueue)
    {
        this(pageSourceQueue, OptionalLong.empty());
    }

    public DirectTrinoPageSource(LinkedList<ConnectorPageSource> pageSourceQueue, OptionalLong limit)
    {
        this.pageSourceQueue = pageSourceQueue;
        this.limit = limit;
        this.current = pageSourceQueue.poll();
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes + (current == null ? 0 : current.getCompletedBytes());
    }

    @Override
    public long getReadTimeNanos()
    {
        return current == null ? 0 : current.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed || limitReached() || current == null || (current.isFinished() && pageSourceQueue.isEmpty());
    }

    @Override
    public Page getNextPage()
    {
        if (closed || current == null || limitReached()) {
            close();
            return null;
        }

        while (current != null) {
            Page dataPage = current.getNextPage();
            if (dataPage == null) {
                if (!current.isFinished()) {
                    return null;
                }
                advance();
                continue;
            }

            if (limit.isPresent() && completedPositions + dataPage.getPositionCount() > limit.getAsLong()) {
                int remainingPositions = toIntExact(limit.getAsLong() - completedPositions);
                Page limitedPage = dataPage.getRegion(0, remainingPositions);
                completedPositions += limitedPage.getPositionCount();
                close();
                return limitedPage;
            }

            completedPositions += dataPage.getPositionCount();
            return dataPage;
        }
        return null;
    }

    private boolean limitReached()
    {
        return limit.isPresent() && completedPositions >= limit.getAsLong();
    }

    private void advance()
    {
        if (current == null) {
            throw new RuntimeException("Current is null, should not invoke advance");
        }
        try {
            completedBytes += current.getCompletedBytes();
            current.close();
            current = null;
        }
        catch (IOException e) {
            current = null;
            close();
            throw new UncheckedIOException("error happens while advance and close old page source.", e);
        }
        current = pageSourceQueue.poll();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        IOException exception = null;
        try {
            if (current != null) {
                completedBytes += current.getCompletedBytes();
                current.close();
                current = null;
            }
        }
        catch (IOException e) {
            exception = e;
        }
        try {
            for (ConnectorPageSource source : pageSourceQueue) {
                try {
                    completedBytes += source.getCompletedBytes();
                    source.close();
                }
                catch (IOException e) {
                    if (exception == null) {
                        exception = e;
                    }
                    else {
                        exception.addSuppressed(e);
                    }
                }
            }
            pageSourceQueue.clear();
        }
        finally {
            if (exception != null) {
                throw new UncheckedIOException(exception);
            }
        }
    }

    @Override
    public String toString()
    {
        return current == null ? null : current.toString();
    }

    @Override
    public long getMemoryUsage()
    {
        return current == null ? 0 : current.getMemoryUsage();
    }

    @Override
    public Metrics getMetrics()
    {
        return current == null ? Metrics.EMPTY : current.getMetrics();
    }
}
