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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.deletionvectors.DeletionVector;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.paimon.PaimonLongUtils.saturatedAdd;
import static java.util.Objects.requireNonNull;

public class PaimonPageSourceWrapper
        implements
        ConnectorPageSource
{
    private final ConnectorPageSource source;

    private final Optional<DeletionVector> deletionVector;
    private final long retainedDeletionVectorBytes;
    private long completedPositions;

    public PaimonPageSourceWrapper(ConnectorPageSource source, Optional<DeletionVector> deletionVector,
            long retainedDeletionVectorBytes)
    {
        this.source = requireNonNull(source, "source is null");
        this.deletionVector = requireNonNull(deletionVector, "deletionVector is null");
        checkArgument(retainedDeletionVectorBytes >= 0,
                "retainedDeletionVectorBytes must be non-negative: %s", retainedDeletionVectorBytes);
        this.retainedDeletionVectorBytes = retainedDeletionVectorBytes;
    }

    public static ConnectorPageSource wrap(ConnectorPageSource connectorPageSource,
            Optional<DeletionVector> deletionVector, long retainedDeletionVectorBytes)
    {
        return new PaimonPageSourceWrapper(connectorPageSource, deletionVector, retainedDeletionVectorBytes);
    }

    @Override
    public long getCompletedBytes()
    {
        return source.getCompletedBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        if (deletionVector.isPresent()) {
            return OptionalLong.of(completedPositions);
        }
        return source.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return source.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return source.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        try {
            OptionalLong startPosition = deletionVector.isPresent() ? OptionalLong.of(startPosition()) : OptionalLong.empty();
            Page next = source.getNextPage();
            if (next == null) {
                return next;
            }
            if (deletionVector.isEmpty()) {
                return next;
            }

            int pageCount = next.getPositionCount();

            Page retained = convertToRetained(next, deletionVector.get(), startPosition.orElseThrow(), pageCount);
            completedPositions = saturatedAdd(completedPositions, retained.getPositionCount(),
                    "retained page position count");
            return retained;
        }
        catch (RuntimeException e) {
            closeAllSuppress(e, this);
            throw PaimonPageSourceProvider.wrapPaimonReadException(e);
        }
    }

    private long startPosition()
    {
        return source.getCompletedPositions()
                .orElseThrow(() -> new IllegalStateException(
                        "Deletion-vector page source requires completed positions"));
    }

    @VisibleForTesting
    Page convertToRetained(Page page, DeletionVector deletionVector, long startPosition, int pageCount)
    {
        int[] retained = new int[pageCount];
        int retainedLength = 0;
        for (int pagePosition = 0; pagePosition < pageCount; pagePosition++) {
            if (!deletionVector.isDeleted(deletionVectorRowPosition(startPosition, pagePosition))) {
                retained[retainedLength++] = pagePosition;
            }
        }
        if (retainedLength == pageCount) {
            return page;
        }

        return page.getPositions(retained, 0, retainedLength);
    }

    @VisibleForTesting
    static long deletionVectorRowPosition(long startPosition, int pagePosition)
    {
        try {
            return Math.addExact(startPosition, pagePosition);
        }
        catch (ArithmeticException e) {
            throw new IllegalStateException("Deletion-vector row position overflow for start position %s and page position %s"
                    .formatted(startPosition, pagePosition), e);
        }
    }

    @Override
    public long getMemoryUsage()
    {
        // The deletion vector is read eagerly and stays retained for the split lifetime, so Trino must
        // see its bytes in addition to the wrapped source.
        return saturatedAdd(source.getMemoryUsage(), retainedDeletionVectorBytes, "Paimon deletion vector bytes");
    }

    @Override
    public void close()
            throws IOException
    {
        source.close();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return source.isBlocked();
    }

    @Override
    public Metrics getMetrics()
    {
        return source.getMetrics();
    }
}
