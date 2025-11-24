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

import io.trino.operator.RetryPolicy;
import io.trino.testing.BaseFailureRecoveryTest;

/**
 * Base class for fault tolerance and failure recovery tests.
 *
 * Tests various failure scenarios including: - Worker node failures - Network
 * failures - Task failures - Query retries
 *
 * Subclasses should specify the retry policy (QUERY or TASK level).
 */
public abstract class BasePaimonFailureRecoveryTest
        extends
        BaseFailureRecoveryTest
{
    protected BasePaimonFailureRecoveryTest(RetryPolicy retryPolicy)
    {
        super(retryPolicy);
    }
}
