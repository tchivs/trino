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
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.paimon.PaimonLongUtils.saturatedAdd;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class DirectTrinoPageSource
        implements
        ConnectorPageSource
{
    private final Deque<PageSourceHandle> pageSourceQueue;
    private final OptionalLong limit;
    private PageSourceHandle current;
    private long completedBytes;
    private long completedReadTimeNanos;
    private long completedPositions;
    private Metrics completedMetrics = Metrics.EMPTY;
    private boolean closed;

    public DirectTrinoPageSource(List<ConnectorPageSource> pageSourceQueue)
    {
        this(OptionalLong.empty(), wrapPageSources(pageSourceQueue));
    }

    public DirectTrinoPageSource(List<ConnectorPageSource> pageSourceQueue, OptionalLong limit)
    {
        this(limit, wrapPageSources(pageSourceQueue));
    }

    static DirectTrinoPageSource lazyPageSources(List<Supplier<ConnectorPageSource>> pageSourceSuppliers, OptionalLong limit)
    {
        requireNonNull(pageSourceSuppliers, "pageSourceSuppliers is null");
        Deque<PageSourceHandle> pageSourceQueue = new ArrayDeque<>(pageSourceSuppliers.size());
        pageSourceSuppliers.forEach(supplier -> pageSourceQueue.add(PageSourceHandle.lazy(supplier)));
        return new DirectTrinoPageSource(limit, pageSourceQueue);
    }

    private DirectTrinoPageSource(OptionalLong limit, Deque<PageSourceHandle> pageSourceQueue)
    {
        this.pageSourceQueue = requireNonNull(pageSourceQueue, "pageSourceQueue is null");
        this.pageSourceQueue.forEach(source -> requireNonNull(source, "pageSourceQueue contains null source"));
        this.limit = requireNonNull(limit, "limit is null");
        if (this.limit.isPresent() && this.limit.getAsLong() < 0) {
            throw new IllegalArgumentException("limit must be non-negative");
        }
        this.current = this.pageSourceQueue.poll();
    }

    @Override
    public long getCompletedBytes()
    {
        return saturatedAdd(completedBytes, currentSource()
                .map(ConnectorPageSource::getCompletedBytes)
                .orElse(0L), "completed bytes");
    }

    @Override
    public long getReadTimeNanos()
    {
        return saturatedAdd(completedReadTimeNanos, currentSource()
                .map(ConnectorPageSource::getReadTimeNanos)
                .orElse(0L), "read time nanos");
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public boolean isFinished()
    {
        if (closed || limitReached() || current == null) {
            return true;
        }
        return currentSource()
                .map(source -> source.isFinished() && pageSourceQueue.isEmpty())
                .orElse(false);
    }

    @Override
    public Page getNextPage()
    {
        try {
            if (closed || current == null || limitReached()) {
                close();
                return null;
            }

            while (current != null) {
                ConnectorPageSource currentSource = current.pageSource();
                Page dataPage = currentSource.getNextPage();
                if (dataPage == null) {
                    if (!currentSource.isFinished()) {
                        return null;
                    }
                    advance();
                    continue;
                }

                if (limit.isPresent() && dataPage.getPositionCount() > limit.getAsLong() - completedPositions) {
                    int remainingPositions = toIntExact(limit.getAsLong() - completedPositions);
                    Page limitedPage = dataPage.getRegion(0, remainingPositions).getLoadedPage();
                    completedPositions = saturatedAdd(completedPositions, limitedPage.getPositionCount(),
                            "page position count");
                    close();
                    return limitedPage;
                }

                completedPositions = saturatedAdd(completedPositions, dataPage.getPositionCount(),
                        "page position count");
                return dataPage;
            }
            return null;
        }
        catch (Exception e) {
            closeAllSuppress(e, this);
            throw PaimonPageSourceProvider.wrapPaimonReadException(e);
        }
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
        PageSourceHandle exhausted = current;
        current = null;
        try {
            closePageSource(exhausted);
        }
        catch (IOException e) {
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
        if (current != null) {
            try {
                closePageSource(current);
            }
            catch (IOException e) {
                exception = e;
            }
            current = null;
        }
        try {
            for (PageSourceHandle source : pageSourceQueue) {
                try {
                    closePageSource(source);
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
        long memoryUsage = memoryUsage(current);
        for (PageSourceHandle source : pageSourceQueue) {
            memoryUsage = saturatedAdd(memoryUsage, memoryUsage(source), "memory usage");
        }
        return memoryUsage;
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        Optional<ConnectorPageSource> currentSource = currentSource();
        if (closed || current == null || limitReached()) {
            return NOT_BLOCKED;
        }
        if (currentSource.isEmpty()) {
            return NOT_BLOCKED;
        }
        ConnectorPageSource source = currentSource.orElseThrow();
        if (source.isFinished()) {
            return NOT_BLOCKED;
        }
        return source.isBlocked();
    }

    @Override
    public Metrics getMetrics()
    {
        return currentSource()
                .map(source -> completedMetrics.mergeWith(source.getMetrics()))
                .orElse(completedMetrics);
    }

    private Optional<ConnectorPageSource> currentSource()
    {
        if (current == null || !current.isOpened()) {
            return Optional.empty();
        }
        return Optional.of(current.pageSource());
    }

    private static long memoryUsage(PageSourceHandle source)
    {
        if (source == null || !source.isOpened()) {
            return 0;
        }
        return source.pageSource().getMemoryUsage();
    }

    private void accumulateCompletedState(PageSourceHandle source)
    {
        if (!source.isOpened()) {
            return;
        }
        ConnectorPageSource pageSource = source.pageSource();
        completedBytes = saturatedAdd(completedBytes, pageSource.getCompletedBytes(), "completed bytes");
        completedReadTimeNanos = saturatedAdd(completedReadTimeNanos, pageSource.getReadTimeNanos(), "read time nanos");
        completedMetrics = completedMetrics.mergeWith(pageSource.getMetrics());
    }

    private void closePageSource(PageSourceHandle source)
            throws IOException
    {
        IOException failure = null;
        try {
            accumulateCompletedState(source);
        }
        catch (RuntimeException e) {
            failure = new IOException("Failed to accumulate completed state before closing Paimon direct page source", e);
        }
        try {
            source.close();
        }
        catch (IOException e) {
            if (failure == null) {
                failure = e;
            }
            else {
                failure.addSuppressed(e);
            }
        }
        catch (RuntimeException e) {
            IOException closeFailure = new IOException("Failed to close Paimon direct page source", e);
            if (failure == null) {
                failure = closeFailure;
            }
            else {
                failure.addSuppressed(closeFailure);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static Deque<PageSourceHandle> wrapPageSources(List<ConnectorPageSource> pageSourceQueue)
    {
        requireNonNull(pageSourceQueue, "pageSourceQueue is null");
        Deque<PageSourceHandle> sources = new ArrayDeque<>(pageSourceQueue.size());
        pageSourceQueue.forEach(source -> {
            requireNonNull(source, "pageSourceQueue contains null source");
            sources.add(PageSourceHandle.eager(source));
        });
        return sources;
    }

    private static final class PageSourceHandle
    {
        private final Supplier<ConnectorPageSource> supplier;
        private ConnectorPageSource pageSource;

        private PageSourceHandle(Supplier<ConnectorPageSource> supplier, ConnectorPageSource pageSource)
        {
            this.supplier = supplier;
            this.pageSource = pageSource;
        }

        static PageSourceHandle eager(ConnectorPageSource pageSource)
        {
            return new PageSourceHandle(null, requireNonNull(pageSource, "pageSource is null"));
        }

        static PageSourceHandle lazy(Supplier<ConnectorPageSource> supplier)
        {
            return new PageSourceHandle(requireNonNull(supplier, "supplier is null"), null);
        }

        boolean isOpened()
        {
            return pageSource != null;
        }

        ConnectorPageSource pageSource()
        {
            if (pageSource == null) {
                pageSource = requireNonNull(supplier.get(), "supplier returned null page source");
            }
            return pageSource;
        }

        void close()
                throws IOException
        {
            if (pageSource != null) {
                pageSource.close();
            }
        }
    }
}
