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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Minio;
import io.trino.testing.minio.MinioClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static org.assertj.core.api.Assertions.assertThat;

@Execution(ExecutionMode.SAME_THREAD)
public class TestPaimonMinioSmokeTest
        extends
        AbstractTestQueryFramework
{
    private static final String CATALOG = "paimon";
    private static final String SCHEMA = "minio_smoke";
    private static final String WAREHOUSE_PREFIX = "warehouse";

    private final String bucketName = "test-paimon-minio-" + randomNameSuffix();
    private Minio minio;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        minio = closeAfterClass(Minio.builder().build());
        minio.start();
        minio.createBucket(bucketName);

        Session session = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(SCHEMA)
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .build();

        queryRunner.installPlugin(new PaimonPlugin());
        queryRunner.createCatalog(
                CATALOG,
                CATALOG,
                ImmutableMap.<String, String>builder()
                        .put("warehouse", "s3://%s/%s".formatted(bucketName, WAREHOUSE_PREFIX))
                        .put("fs.hadoop.enabled", "false")
                        .put("fs.native-s3.enabled", "true")
                        .put("s3.access-key", MINIO_ACCESS_KEY)
                        .put("s3.secret-key", MINIO_SECRET_KEY)
                        .put("s3.region", MINIO_REGION)
                        .put("s3.endpoint", minio.getMinioAddress())
                        .put("s3.path-style-access", "true")
                        .buildOrThrow());

        return queryRunner;
    }

    @Test
    public void testMinioWarehouseCrudDdlSmoke()
    {
        String tableName = "orders_" + randomNameSuffix();
        String qualifiedSchemaName = CATALOG + "." + SCHEMA;
        String qualifiedTableName = qualifiedSchemaName + "." + tableName;

        assertUpdate("CREATE SCHEMA " + qualifiedSchemaName);
        try {
            assertThat(computeActual("SHOW SCHEMAS FROM " + CATALOG).getOnlyColumnAsSet()).contains(SCHEMA);

            assertUpdate("CREATE TABLE " + qualifiedTableName + " ("
                    + "orderkey bigint, "
                    + "status varchar COMMENT 'order status') "
                    + "COMMENT 'orders smoke table'");
            assertUpdate("INSERT INTO " + qualifiedTableName + " VALUES (1, 'ok'), (2, 'ready')", 2);

            assertQuery(
                    "SELECT * FROM " + qualifiedTableName + " ORDER BY orderkey",
                    "VALUES (CAST(1 AS BIGINT), CAST('ok' AS VARCHAR)), (CAST(2 AS BIGINT), CAST('ready' AS VARCHAR))");
            assertThat((String) computeScalar("SHOW CREATE TABLE " + qualifiedTableName))
                    .contains("COMMENT 'orders smoke table'")
                    .contains("COMMENT 'order status'");

            try (MinioClient minioClient = minio.createMinioClient()) {
                List<String> warehouseObjects = minioClient.listObjects(bucketName, WAREHOUSE_PREFIX + "/" + SCHEMA + ".db/" + tableName);
                assertThat(warehouseObjects)
                        .isNotEmpty()
                        .anyMatch(path -> !path.endsWith("_trino_paimon_directory_marker"));
                assertThat(warehouseObjects).allMatch(path -> path.startsWith(WAREHOUSE_PREFIX + "/" + SCHEMA + ".db/" + tableName));
            }

            assertUpdate("ALTER TABLE " + qualifiedTableName + " RENAME COLUMN status TO state");
            assertUpdate("COMMENT ON COLUMN " + qualifiedTableName + ".state IS 'current state'");
            assertThat((String) computeScalar("SHOW CREATE TABLE " + qualifiedTableName))
                    .contains("state varchar COMMENT 'current state'");
            assertQuery(
                    "SELECT orderkey, state FROM " + qualifiedTableName + " ORDER BY orderkey",
                    "VALUES (CAST(1 AS BIGINT), CAST('ok' AS VARCHAR)), (CAST(2 AS BIGINT), CAST('ready' AS VARCHAR))");

            assertUpdate("DELETE FROM " + qualifiedTableName);
            assertQuery("SELECT count(*) FROM " + qualifiedTableName, "VALUES CAST(0 AS BIGINT)");

            assertUpdate("INSERT INTO " + qualifiedTableName + " VALUES (3, 'shipped'), (4, 'closed')", 2);
            assertUpdate("TRUNCATE TABLE " + qualifiedTableName);
            assertQuery("SELECT count(*) FROM " + qualifiedTableName, "VALUES CAST(0 AS BIGINT)");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + qualifiedTableName);
            assertUpdate("DROP SCHEMA IF EXISTS " + qualifiedSchemaName);
        }
    }
}
