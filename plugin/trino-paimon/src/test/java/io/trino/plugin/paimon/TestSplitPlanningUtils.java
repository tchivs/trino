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

import org.junit.jupiter.api.Test;

import java.util.OptionalInt;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSplitPlanningUtils
{
    @Test
    public void testComputeSplitWeightDoesNotProduceNaN()
    {
        assertThat(SplitPlanningUtils.computeSplitWeight(0, 0, 0.01)).isEqualTo(0.01);
        assertThat(SplitPlanningUtils.computeSplitWeight(10, 0, 0.01)).isEqualTo(0.01);
        assertThat(Double.isNaN(SplitPlanningUtils.computeSplitWeight(10, 0, 0.01))).isFalse();
    }

    @Test
    public void testComputeSplitWeightBounds()
    {
        assertThat(SplitPlanningUtils.computeSplitWeight(1, 100, 0.25)).isEqualTo(0.25);
        assertThat(SplitPlanningUtils.computeSplitWeight(50, 100, 0.01)).isEqualTo(0.5);
        assertThat(SplitPlanningUtils.computeSplitWeight(200, 100, 0.01)).isEqualTo(1.0);
    }

    @Test
    public void testToPaimonLimit()
    {
        assertThat(SplitPlanningUtils.toPaimonLimit(OptionalLong.empty())).isEqualTo(OptionalInt.empty());
        assertThat(SplitPlanningUtils.toPaimonLimit(OptionalLong.of(0))).isEqualTo(OptionalInt.empty());
        assertThat(SplitPlanningUtils.toPaimonLimit(OptionalLong.of(-1))).isEqualTo(OptionalInt.empty());
        assertThat(SplitPlanningUtils.toPaimonLimit(OptionalLong.of((long) Integer.MAX_VALUE + 1))).isEqualTo(OptionalInt.empty());
        assertThat(SplitPlanningUtils.toPaimonLimit(OptionalLong.of(1))).isEqualTo(OptionalInt.of(1));
        assertThat(SplitPlanningUtils.toPaimonLimit(OptionalLong.of(Integer.MAX_VALUE))).isEqualTo(OptionalInt.of(Integer.MAX_VALUE));
    }
}
