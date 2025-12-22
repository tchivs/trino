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

import io.airlift.slice.Slices;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.type.JsonType.JSON;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDynamicFilteringSplitSourceComplexity
{
    @Test
    public void testEstimateComplexityForComparableNonOrderableType()
    {
        PaimonColumnHandle column = PaimonColumnHandle.of("c", new org.apache.paimon.types.VarCharType());
        Domain domain = Domain.multipleValues(JSON, List.of(
                Slices.utf8Slice("{\"a\":1}"),
                Slices.utf8Slice("{\"a\":2}"),
                Slices.utf8Slice("{\"a\":3}")));

        TupleDomain<PaimonColumnHandle> predicate = TupleDomain.withColumnDomains(Map.of(column, domain));
        assertThat(DynamicFilteringTrinoSplitSource.estimateComplexity(predicate)).isEqualTo(3);
    }

    @Test
    public void testEstimateComplexityForOrderableTypeUsesRangesCount()
    {
        PaimonColumnHandle column = PaimonColumnHandle.of("c", new org.apache.paimon.types.BigIntType());
        Domain domain = Domain.create(ValueSet.ofRanges(
                Range.lessThan(BIGINT, 10L),
                Range.greaterThanOrEqual(BIGINT, 100L)), false);

        TupleDomain<PaimonColumnHandle> predicate = TupleDomain.withColumnDomains(Map.of(column, domain));
        assertThat(DynamicFilteringTrinoSplitSource.estimateComplexity(predicate)).isEqualTo(2);
    }
}
