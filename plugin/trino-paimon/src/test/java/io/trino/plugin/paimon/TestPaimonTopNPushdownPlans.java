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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.assertions.BasePushdownPlanTest;
import io.trino.testing.PlanTester;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.InnerTableWrite;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@Execution(ExecutionMode.SAME_THREAD)
@TestInstance(PER_CLASS)
public class TestPaimonTopNPushdownPlans
        extends BasePushdownPlanTest
{
    private static final String CATALOG = "paimon";
    private static final String SCHEMA = "default";

    private Path warehouseDirectory;

    @Override
    protected PlanTester createPlanTester()
    {
        Session session = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(SCHEMA)
                .setSystemProperty("task_concurrency", "1")
                .build();

        PlanTester planTester = PlanTester.create(session);

        try {
            warehouseDirectory = Files.createTempDirectory("paimon-topn-test-warehouse");
            planTester.installPlugin(new TestingPaimonPlugin(Path.of("/")));
            planTester.createCatalog(
                    CATALOG,
                    CATALOG,
                    ImmutableMap.of("warehouse", warehouseDirectory.toUri().toString()));
        }
        catch (Exception e) {
            try {
                planTester.close();
            }
            catch (Exception ignored) {
            }
            throw new RuntimeException(e);
        }

        return planTester;
    }

    @AfterAll
    public void cleanup()
    {
        if (warehouseDirectory == null) {
            return;
        }
        try {
            deleteRecursively(warehouseDirectory, ALLOW_INSECURE);
        }
        catch (IOException ignored) {
        }
        finally {
            warehouseDirectory = null;
        }
    }

    @Test
    public void testTopNPushdown()
            throws Exception
    {
        String tableName = "test_topn_pushdown_" + randomNameSuffix();
        createTestTable(tableName);

        // TopN with single sort column should be pushed down
        assertPlan(
                "SELECT * FROM " + tableName + " ORDER BY v LIMIT 10",
                anyTree(tableScan(
                        handle -> hasTopNForTable(handle, tableName),
                        TupleDomain.all(),
                        ImmutableMap.of())));

        // TopN with DESC should also be pushed down
        assertPlan(
                "SELECT * FROM " + tableName + " ORDER BY v DESC LIMIT 5",
                anyTree(tableScan(
                        handle -> hasTopNForTable(handle, tableName),
                        TupleDomain.all(),
                        ImmutableMap.of())));
    }

    @Test
    public void testTopNNotPushedDownWithMultipleSortColumns()
            throws Exception
    {
        String tableName = "test_topn_multi_sort_" + randomNameSuffix();
        createTestTable(tableName);

        // TopN with multiple sort columns should NOT be pushed down
        assertPlan(
                "SELECT * FROM " + tableName + " ORDER BY name, v LIMIT 10",
                anyTree(tableScan(
                        handle -> !hasTopNForTable(handle, tableName),
                        TupleDomain.all(),
                        ImmutableMap.of())));
    }

    private void createTestTable(String tableName)
            throws Exception
    {
        String warehouse = warehouseDirectory.toUri().toString();
        org.apache.paimon.fs.Path tablePath = new org.apache.paimon.fs.Path(warehouse, SCHEMA + ".db/" + tableName);

        RowType rowType = new RowType(java.util.List.of(
                new DataField(0, "name", DataTypes.STRING()),
                new DataField(1, "v", DataTypes.BIGINT())));

        new SchemaManager(LocalFileIO.create(), tablePath).createTable(new Schema(
                rowType.getFields(),
                Collections.emptyList(),
                Collections.emptyList(),
                new HashMap<>(),
                ""));

        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
        InnerTableWrite writer = table.newWrite("user");
        InnerTableCommit commit = table.newCommit("user");

        writer.write(GenericRow.of(BinaryString.fromString("alice"), 10L));
        writer.write(GenericRow.of(BinaryString.fromString("bob"), 20L));
        writer.write(GenericRow.of(BinaryString.fromString("charlie"), 5L));
        writer.write(GenericRow.of(BinaryString.fromString("david"), 15L));

        commit.commit(0, writer.prepareCommit(true, 0));
    }

    private static boolean hasTopNForTable(ConnectorTableHandle handle, String tableName)
    {
        if (!(handle instanceof PaimonTableHandle paimonTableHandle)) {
            return false;
        }
        return paimonTableHandle.getTableName().equals(tableName)
                && paimonTableHandle.getTopN().isPresent();
    }
}
