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

import io.trino.spi.connector.ConnectorSplitSource.ConnectorSplitBatch;
import org.apache.paimon.table.source.Split;
import org.junit.jupiter.api.Test;

import java.io.Serial;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestPaimonSplitSource
{
    @Test
    public void testDoesNotStopEarlyBasedOnSplitRowCount()
    {
        PaimonSplit split1 = PaimonSplit.fromSplit(new TestingSplit(10), 1.0);
        PaimonSplit split2 = PaimonSplit.fromSplit(new TestingSplit(10), 1.0);
        PaimonSplit split3 = PaimonSplit.fromSplit(new TestingSplit(10), 1.0);

        PaimonSplitSource splitSource = new PaimonSplitSource(List.of(split1, split2, split3));

        ConnectorSplitBatch batch1 = splitSource.getNextBatch(2).join();
        assertThat(batch1.getSplits()).hasSize(2);
        assertThat(batch1.isNoMoreSplits()).isFalse();

        ConnectorSplitBatch batch2 = splitSource.getNextBatch(2).join();
        assertThat(batch2.getSplits()).hasSize(1);
        assertThat(batch2.isNoMoreSplits()).isTrue();

        assertThat(splitSource.isFinished()).isTrue();
    }

    private static final class TestingSplit
            implements Split
    {
        @Serial
        private static final long serialVersionUID = 1L;

        private final long rowCount;

        private TestingSplit(long rowCount)
        {
            this.rowCount = rowCount;
        }

        @Override
        public long rowCount()
        {
            return rowCount;
        }

        @Override
        public Optional<List<org.apache.paimon.table.source.RawFile>> convertToRawFiles()
        {
            return Optional.empty();
        }

        @Override
        public Optional<List<org.apache.paimon.table.source.DeletionFile>> deletionFiles()
        {
            return Optional.empty();
        }

        @Override
        public Optional<List<org.apache.paimon.table.source.IndexFile>> indexFiles()
        {
            return Optional.empty();
        }
    }
}
