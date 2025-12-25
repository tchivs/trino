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

public class TestPaimonTopNPushdownCorrectness
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TrinoQueryRunner.createPrestoQueryRunner(
                ImmutableMap.of(), ImmutableMap.of(), false);
    }

    @Test
    public void testTopNAscending()
    {
        String tableName = "test_topn_asc_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (name VARCHAR, v BIGINT)");
        assertUpdate("INSERT INTO " + tableName + " VALUES "
                + "('a', 10), ('b', 5), ('c', 20), ('d', 15), ('e', 1)", 5);

        // Test ORDER BY ASC LIMIT
        assertThat(query("SELECT v FROM " + tableName + " ORDER BY v LIMIT 3"))
                .matches("VALUES BIGINT '1', BIGINT '5', BIGINT '10'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testTopNDescending()
    {
        String tableName = "test_topn_desc_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (name VARCHAR, v BIGINT)");
        assertUpdate("INSERT INTO " + tableName + " VALUES "
                + "('a', 10), ('b', 5), ('c', 20), ('d', 15), ('e', 1)", 5);

        // Test ORDER BY DESC LIMIT
        assertThat(query("SELECT v FROM " + tableName + " ORDER BY v DESC LIMIT 3"))
                .matches("VALUES BIGINT '20', BIGINT '15', BIGINT '10'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testTopNWithFilter()
    {
        String tableName = "test_topn_filter_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (name VARCHAR, v BIGINT)");
        assertUpdate("INSERT INTO " + tableName + " VALUES "
                + "('a', 10), ('b', 5), ('c', 20), ('d', 15), ('e', 1)", 5);

        // Test TopN with WHERE clause
        assertThat(query("SELECT v FROM " + tableName + " WHERE v > 5 ORDER BY v LIMIT 2"))
                .matches("VALUES BIGINT '10', BIGINT '15'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testTopNWithNulls()
    {
        String tableName = "test_topn_nulls_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (name VARCHAR, v BIGINT)");
        assertUpdate("INSERT INTO " + tableName + " VALUES "
                + "('a', 10), ('b', NULL), ('c', 20), ('d', NULL), ('e', 5)", 5);

        // Test TopN with NULL values - NULLS FIRST is default for ASC
        assertThat(query("SELECT v FROM " + tableName + " ORDER BY v ASC NULLS FIRST LIMIT 3"))
                .matches("VALUES CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), BIGINT '5'");

        // Test TopN with NULL values - NULLS LAST
        assertThat(query("SELECT v FROM " + tableName + " ORDER BY v ASC NULLS LAST LIMIT 3"))
                .matches("VALUES BIGINT '5', BIGINT '10', BIGINT '20'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testTopNOnVarchar()
    {
        String tableName = "test_topn_varchar_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (name VARCHAR, v BIGINT)");
        assertUpdate("INSERT INTO " + tableName + " VALUES "
                + "('charlie', 1), ('alice', 2), ('bob', 3), ('david', 4)", 4);

        // Test TopN on VARCHAR column
        assertQuery(
                "SELECT name FROM " + tableName + " ORDER BY name LIMIT 2",
                "VALUES 'alice', 'bob'");

        assertUpdate("DROP TABLE " + tableName);
    }
}
