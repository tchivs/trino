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

import io.airlift.json.JsonCodec;
import io.trino.spi.SplitWeight;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.Split;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

public class TrinoSplitTest
{
    private final JsonCodec<PaimonSplit> codec = JsonCodec.jsonCodec(PaimonSplit.class);

    @Test
    public void testJsonRoundTrip()
            throws Exception
    {
        byte[] serializedTable = PaimonTestUtils.getSerializedTable();
        PaimonSplit expected = new PaimonSplit(Arrays.toString(serializedTable), 0.1);
        String json = codec.toJson(expected);
        PaimonSplit actual = codec.fromJson(json);
        assertThat(actual.splitSerialized()).isEqualTo(expected.splitSerialized());
    }

    @Test
    public void testLegacyJsonMissingWeightDefaultsToStandardWeight()
    {
        PaimonSplit actual = codec.fromJson("""
                {
                  "splitSerialized": "legacy"
                }
                """);

        assertThat(actual.weight()).isEqualTo(1.0);
        assertThat(actual.getSplitWeight()).isEqualTo(SplitWeight.standard());
    }

    @Test
    public void testZeroRowSplitUsesMinimumWeight()
    {
        double minimumSplitWeight = 0.05;

        double weight = PaimonSplitManager.calculateSplitWeight(new TestingSplit(0), 0, minimumSplitWeight);

        assertThat(weight).isEqualTo(minimumSplitWeight);
        assertThat(new PaimonSplit("ignored", weight).getSplitWeight())
                .isEqualTo(SplitWeight.fromProportion(minimumSplitWeight));
    }

    @Test
    public void testSplitWeightIsBoundedByMinimumAndStandardWeight()
    {
        assertThat(PaimonSplitManager.calculateSplitWeight(new TestingSplit(1), 100, 0.05)).isEqualTo(0.05);
        assertThat(PaimonSplitManager.calculateSplitWeight(new TestingSplit(200), 100, 0.05)).isEqualTo(1.0);
    }

    private record TestingSplit(long rowCount) implements Split
    {
        @Override
        public OptionalLong mergedRowCount()
        {
            return OptionalLong.empty();
        }

        @Override
        public Optional<List<RawFile>> convertToRawFiles()
        {
            return Optional.empty();
        }
    }
}
