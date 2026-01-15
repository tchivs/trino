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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPaimonAggregationPushdownCorrectness
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TrinoQueryRunner.createPrestoQueryRunner(ImmutableMap.of(), ImmutableMap.of(), false);
    }

    @Test
    public void testCountPushdownOnAppendOnlyTable()
    {
        String tableName = "test_count_append_" + randomNameSuffix();

        // Create append-only table (no primary key)
        assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, v BIGINT)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 10), (2, 20), (3, 30)", 3);

        assertThat(query("SELECT count(*) FROM " + tableName)).matches("VALUES BIGINT '3'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testMinMaxPushdownOnAppendOnlyTable()
    {
        String tableName = "test_minmax_append_" + randomNameSuffix();

        // Create append-only table (no primary key)
        assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, v BIGINT)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 10), (2, 5), (3, 30)", 3);

        assertThat(query("SELECT min(v) FROM " + tableName)).matches("VALUES BIGINT '5'");
        assertThat(query("SELECT max(v) FROM " + tableName)).matches("VALUES BIGINT '30'");

        assertUpdate("DROP TABLE " + tableName);
    }
}
