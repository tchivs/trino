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
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@Execution(ExecutionMode.SAME_THREAD)
@TestInstance(PER_CLASS)
public class TestPaimonDeletePushdown
        extends AbstractTestQueryFramework
{
    public static final String SQL = "INSERT INTO test_delete_part VALUES " +
            "(1, 'alice', 'us'), (2, 'bob', 'us'), " +
            "(3, 'charlie', 'eu'), (4, 'david', 'eu')";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TrinoQueryRunner.createPrestoQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of(),
                false);
    }

    @Test
    public void testDeleteFromNonPartitionedTable()
    {
        // Create non-partitioned table with primary key
        assertUpdate("CREATE TABLE test_delete_non_part (" +
                "id BIGINT, " +
                "name VARCHAR) " +
                "WITH (primary_key = ARRAY['id'], bucket = '1')");

        // Insert data
        assertUpdate("INSERT INTO test_delete_non_part VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')", 3);

        // Verify initial data
        assertThat(query("SELECT COUNT(*) FROM test_delete_non_part"))
                .matches("VALUES BIGINT '3'");

        // Delete all rows (truncate) - this should use applyDelete pushdown
        // Note: Paimon delete pushdown doesn't return exact row count
        assertQuerySucceeds("DELETE FROM test_delete_non_part");

        // Verify table is empty
        assertThat(query("SELECT COUNT(*) FROM test_delete_non_part"))
                .matches("VALUES BIGINT '0'");

        assertUpdate("DROP TABLE test_delete_non_part");
    }

    @Test
    public void testDeleteFromPartitionedTableByPartitionKey()
    {
        // Create partitioned table
        assertUpdate("CREATE TABLE test_delete_part (" +
                "id BIGINT, " +
                "name VARCHAR, " +
                "region VARCHAR) " +
                "WITH (primary_key = ARRAY['id'], partitioned_by = ARRAY['region'], bucket = '1')");

        // Insert data into different partitions
        assertUpdate(SQL, 4);

        // Verify initial data
        assertThat(query("SELECT COUNT(*) FROM test_delete_part"))
                .matches("VALUES BIGINT '4'");

        // Delete by partition key - this should use partition drop
        // Note: Paimon delete pushdown doesn't return exact row count
        assertQuerySucceeds("DELETE FROM test_delete_part WHERE region = 'us'");

        // Verify remaining data
        assertThat(query("SELECT COUNT(*) FROM test_delete_part"))
                .matches("VALUES BIGINT '2'");

        assertThat(query("SELECT DISTINCT region FROM test_delete_part"))
                .skippingTypesCheck()
                .matches("VALUES 'eu'");

        assertUpdate("DROP TABLE test_delete_part");
    }

    @Test
    public void testDeleteFromPartitionedTableAllPartitions()
    {
        // Create partitioned table
        assertUpdate("CREATE TABLE test_delete_all_part (" +
                "id BIGINT, " +
                "name VARCHAR, " +
                "region VARCHAR) " +
                "WITH (primary_key = ARRAY['id'], partitioned_by = ARRAY['region'], bucket = '1')");

        // Insert data
        assertUpdate("INSERT INTO test_delete_all_part VALUES " +
                "(1, 'alice', 'us'), (2, 'bob', 'eu')", 2);

        // Verify initial data
        assertThat(query("SELECT COUNT(*) FROM test_delete_all_part"))
                .matches("VALUES BIGINT '2'");

        // Delete all (no WHERE clause)
        // Note: Paimon delete pushdown doesn't return exact row count
        assertQuerySucceeds("DELETE FROM test_delete_all_part");

        // Verify table is empty
        assertThat(query("SELECT COUNT(*) FROM test_delete_all_part"))
                .matches("VALUES BIGINT '0'");

        assertUpdate("DROP TABLE test_delete_all_part");
    }
}
