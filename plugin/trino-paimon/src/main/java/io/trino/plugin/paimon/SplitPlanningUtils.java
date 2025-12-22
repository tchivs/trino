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

import java.util.OptionalInt;
import java.util.OptionalLong;

import static java.lang.Math.max;
import static java.lang.Math.min;

final class SplitPlanningUtils
{
    private SplitPlanningUtils() {}

    static double computeSplitWeight(long splitRowCount, long maxRowCount, double minimumSplitWeight)
    {
        if (maxRowCount <= 0) {
            return minimumSplitWeight;
        }
        return min(max(((double) splitRowCount) / maxRowCount, minimumSplitWeight), 1.0);
    }

    static OptionalInt toPaimonLimit(OptionalLong limit)
    {
        if (limit.isEmpty()) {
            return OptionalInt.empty();
        }
        long value = limit.getAsLong();
        if (value <= 0 || value > Integer.MAX_VALUE) {
            return OptionalInt.empty();
        }
        return OptionalInt.of((int) value);
    }
}
