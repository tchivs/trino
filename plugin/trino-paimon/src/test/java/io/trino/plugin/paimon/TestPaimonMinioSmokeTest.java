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
import java.util.Locale;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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
        assumeLocalMinioProxyBypassed();
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
                        .put("s3.aws-access-key", MINIO_ACCESS_KEY)
                        .put("s3.aws-secret-key", MINIO_SECRET_KEY)
                        .put("s3.region", MINIO_REGION)
                        .put("s3.endpoint", minio.getMinioAddress())
                        .put("s3.path-style-access", "true")
                        .buildOrThrow());

        return queryRunner;
    }

    private static void assumeLocalMinioProxyBypassed()
    {
        boolean proxyConfigured = isNonBlank(System.getenv("HTTP_PROXY"))
                || isNonBlank(System.getenv("HTTPS_PROXY"))
                || isNonBlank(System.getenv("http_proxy"))
                || isNonBlank(System.getenv("https_proxy"));
        if (!proxyConfigured) {
            return;
        }

        assumeTrue(noProxyCovers("localhost", System.getenv("NO_PROXY"), System.getenv("no_proxy"))
                        && noProxyCovers("127.0.0.1", System.getenv("NO_PROXY"), System.getenv("no_proxy")),
                "Local MinIO smoke requires NO_PROXY/no_proxy to include localhost,127.0.0.1 when HTTP proxy variables are set");
    }

    private static boolean noProxyCovers(String host, String... noProxyValues)
    {
        for (String noProxy : noProxyValues) {
            if (noProxyValueCovers(noProxy, host)) {
                return true;
            }
        }
        return false;
    }

    private static boolean noProxyValueCovers(String noProxy, String host)
    {
        if (!isNonBlank(noProxy)) {
            return false;
        }

        String normalizedHost = host.toLowerCase(Locale.ROOT);
        for (String entry : noProxy.split(",")) {
            String normalizedEntry = entry.trim().toLowerCase(Locale.ROOT);
            if (normalizedEntry.equals("*") || normalizedEntry.equals(normalizedHost)) {
                return true;
            }
            if (normalizedHost.equals("localhost") && normalizedEntry.equals(".localhost")) {
                return true;
            }
            if (normalizedHost.equals("127.0.0.1")
                    && (normalizedEntry.equals("127.0.0.0/8") || normalizedEntry.equals("127.*"))) {
                return true;
            }
        }
        return false;
    }

    private static boolean isNonBlank(String value)
    {
        return value != null && !value.isBlank();
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

    @Test
    public void testMinioPartitionPredicateDeleteSmoke()
    {
        String tableName = "partition_delete_" + randomNameSuffix();
        String qualifiedSchemaName = CATALOG + "." + SCHEMA;
        String qualifiedTableName = qualifiedSchemaName + "." + tableName;

        assertUpdate("CREATE SCHEMA " + qualifiedSchemaName);
        try {
            assertUpdate("CREATE TABLE " + qualifiedTableName + " ("
                    + "orderkey bigint, "
                    + "status varchar, "
                    + "ds varchar) "
                    + "WITH (partitioned_by = ARRAY['ds'])");
            assertUpdate("INSERT INTO " + qualifiedTableName + " VALUES "
                    + "(1, 'queued', '2026-07-01'), "
                    + "(2, 'ready', '2026-07-02'), "
                    + "(3, 'done', '2026-07-02'), "
                    + "(4, 'held', '2026-07-03')", 4);

            assertUpdate("DELETE FROM " + qualifiedTableName + " WHERE ds = '2026-07-01'");
            assertQuery(
                    "SELECT orderkey, status, ds FROM " + qualifiedTableName + " ORDER BY orderkey",
                    "VALUES "
                            + "(CAST(2 AS BIGINT), CAST('ready' AS VARCHAR), CAST('2026-07-02' AS VARCHAR)), "
                            + "(CAST(3 AS BIGINT), CAST('done' AS VARCHAR), CAST('2026-07-02' AS VARCHAR)), "
                            + "(CAST(4 AS BIGINT), CAST('held' AS VARCHAR), CAST('2026-07-03' AS VARCHAR))");

            assertUpdate("DELETE FROM " + qualifiedTableName + " WHERE ds IN ('2026-07-02', '2026-07-03')");
            assertQuery("SELECT count(*) FROM " + qualifiedTableName, "VALUES CAST(0 AS BIGINT)");

            assertUpdate("INSERT INTO " + qualifiedTableName + " VALUES "
                    + "(5, 'partial', '2026-07-04'), "
                    + "(6, 'keep', '2026-07-04')", 2);
            assertQueryFails(
                    "DELETE FROM " + qualifiedTableName + " WHERE ds = '2026-07-04' AND orderkey = 5",
                    ".*Paimon.*delete.*");
            assertQuery(
                    "SELECT orderkey, status, ds FROM " + qualifiedTableName + " ORDER BY orderkey",
                    "VALUES "
                            + "(CAST(5 AS BIGINT), CAST('partial' AS VARCHAR), CAST('2026-07-04' AS VARCHAR)), "
                            + "(CAST(6 AS BIGINT), CAST('keep' AS VARCHAR), CAST('2026-07-04' AS VARCHAR))");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + qualifiedTableName);
            assertUpdate("DROP SCHEMA IF EXISTS " + qualifiedSchemaName);
        }
    }

    @Test
    public void testMinioSpillableWriteBufferSmoke()
    {
        String tableName = "spillable_write_" + randomNameSuffix();
        String qualifiedSchemaName = CATALOG + "." + SCHEMA;
        String qualifiedTableName = qualifiedSchemaName + "." + tableName;

        assertUpdate("CREATE SCHEMA " + qualifiedSchemaName);
        try {
            assertUpdate("CREATE TABLE " + qualifiedTableName + " ("
                    + "orderkey bigint, "
                    + "status varchar, "
                    + "ds varchar) "
                    + "WITH ("
                    + "partitioned_by = ARRAY['ds'], "
                    + "write_buffer_for_append = 'true', "
                    + "write_buffer_spillable = 'true', "
                    + "write_max_writers_to_spill = '1')");

            assertUpdate("INSERT INTO " + qualifiedTableName + " VALUES "
                    + "(1, 'queued', '2026-07-01'), "
                    + "(2, 'ready', '2026-07-02')", 2);

            assertQuery(
                    "SELECT orderkey, status, ds FROM " + qualifiedTableName + " ORDER BY orderkey",
                    "VALUES "
                            + "(CAST(1 AS BIGINT), CAST('queued' AS VARCHAR), CAST('2026-07-01' AS VARCHAR)), "
                            + "(CAST(2 AS BIGINT), CAST('ready' AS VARCHAR), CAST('2026-07-02' AS VARCHAR))");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + qualifiedTableName);
            assertUpdate("DROP SCHEMA IF EXISTS " + qualifiedSchemaName);
        }
    }
}
