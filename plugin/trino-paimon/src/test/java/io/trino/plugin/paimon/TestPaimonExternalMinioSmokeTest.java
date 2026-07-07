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
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Execution(ExecutionMode.SAME_THREAD)
public class TestPaimonExternalMinioSmokeTest
{
    private static final String CATALOG = "paimon";
    private static final String PROPERTY_PREFIX = "paimon.external-minio.";
    private static final String ENV_PREFIX = "PAIMON_EXTERNAL_MINIO_";

    @Test
    public void testExternalMinioReadOnlySmoke()
            throws Exception
    {
        assumeTrue(configured("enabled").map(Boolean::parseBoolean).orElse(false),
                "Set paimon.external-minio.enabled=true to run the external MinIO smoke test");

        ExternalMinioConfig config = ExternalMinioConfig.load();
        Session session = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(config.schema().orElse("default"))
                .build();

        try (DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build()) {
            queryRunner.installPlugin(new PaimonPlugin());
            queryRunner.createCatalog(CATALOG, CATALOG, config.catalogProperties());

            ReadTarget readTarget = discoverReadTarget(queryRunner, config);

            assertThat(queryRunner.execute("SHOW SCHEMAS FROM " + quote(CATALOG)).getOnlyColumnAsSet())
                    .contains(readTarget.schema());
            assertThat(queryRunner.execute("SHOW TABLES FROM " + qualifiedName(CATALOG, readTarget.schema())).getOnlyColumnAsSet())
                    .contains(readTarget.table());

            String tableName = qualifiedName(CATALOG, readTarget.schema(), readTarget.table());
            assertThat(queryRunner.execute("SELECT table_name FROM " + qualifiedName(CATALOG, "information_schema", "tables")
                    + " WHERE table_schema = " + stringLiteral(readTarget.schema())
                    + " AND table_name = " + stringLiteral(readTarget.table())).getOnlyColumnAsSet())
                    .contains(readTarget.table());

            String createTable = (String) queryRunner.execute("SHOW CREATE TABLE " + tableName)
                    .getOnlyValue();
            assertThat(createTable)
                    .contains("CREATE TABLE")
                    .contains(readTarget.table());

            MaterializedResult columns = queryRunner.execute("SHOW COLUMNS FROM " + tableName);
            assertThat(columns.getRowCount()).isGreaterThan(0);

            MaterializedResult rows = queryRunner.execute("SELECT * FROM " + tableName + " LIMIT " + config.limit());
            assertThat(rows.getMaterializedRows()).hasSizeLessThanOrEqualTo(config.limit());
        }
    }

    @Test
    public void testExternalMinioCrudDdlSmoke()
            throws Exception
    {
        assumeTrue(configured("write-enabled").map(Boolean::parseBoolean).orElse(false),
                "Set paimon.external-minio.write-enabled=true to run the external MinIO CRUD/DDL smoke test");

        ExternalMinioConfig config = ExternalMinioConfig.loadForWrite();
        String schema = "trino_paimon_smoke_" + randomNameSuffix();
        String table = "orders_" + randomNameSuffix();
        String view = "orders_view_" + randomNameSuffix();

        Session session = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(schema)
                .build();

        try (DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build()) {
            queryRunner.installPlugin(new PaimonPlugin());
            queryRunner.createCatalog(CATALOG, CATALOG, config.catalogProperties());

            String schemaName = qualifiedName(CATALOG, schema);
            String tableName = qualifiedName(CATALOG, schema, table);
            String viewName = qualifiedName(CATALOG, schema, view);
            try {
                queryRunner.execute("CREATE SCHEMA " + schemaName);
                assertThat(queryRunner.execute("SHOW SCHEMAS FROM " + quote(CATALOG)).getOnlyColumnAsSet())
                        .contains(schema);

                queryRunner.execute("CREATE TABLE " + tableName + " ("
                        + "orderkey bigint, "
                        + "status varchar COMMENT 'order status') "
                        + "COMMENT 'external orders smoke table'");
                queryRunner.execute("INSERT INTO " + tableName + " VALUES (1, 'ok'), (2, 'ready')");
                assertThat(queryRunner.execute("SELECT count(*) FROM " + tableName).getOnlyValue())
                        .isEqualTo(2L);

                String createTable = (String) queryRunner.execute("SHOW CREATE TABLE " + tableName)
                        .getOnlyValue();
                assertThat(createTable)
                        .contains("COMMENT 'external orders smoke table'")
                        .contains("COMMENT 'order status'");

                queryRunner.execute("ALTER TABLE " + tableName + " RENAME COLUMN status TO state");
                queryRunner.execute("COMMENT ON COLUMN " + tableName + ".state IS 'current state'");
                assertThat((String) queryRunner.execute("SHOW CREATE TABLE " + tableName).getOnlyValue())
                        .contains("state varchar COMMENT 'current state'");
                assertThat(queryRunner.execute("SELECT state FROM " + tableName + " WHERE orderkey = 1").getOnlyValue())
                        .isEqualTo("ok");

                queryRunner.execute("CREATE VIEW " + viewName + " AS SELECT orderkey, state FROM " + tableName);
                assertThat(queryRunner.execute("SELECT count(*) FROM " + viewName).getOnlyValue())
                        .isEqualTo(2L);

                queryRunner.execute("DELETE FROM " + tableName);
                assertThat(queryRunner.execute("SELECT count(*) FROM " + tableName).getOnlyValue())
                        .isEqualTo(0L);

                queryRunner.execute("INSERT INTO " + tableName + " VALUES (3, 'shipped'), (4, 'closed')");
                queryRunner.execute("TRUNCATE TABLE " + tableName);
                assertThat(queryRunner.execute("SELECT count(*) FROM " + tableName).getOnlyValue())
                        .isEqualTo(0L);
            }
            finally {
                queryRunner.execute("DROP VIEW IF EXISTS " + viewName);
                queryRunner.execute("DROP TABLE IF EXISTS " + tableName);
                queryRunner.execute("DROP SCHEMA IF EXISTS " + schemaName);
            }
        }
    }

    private static ReadTarget discoverReadTarget(DistributedQueryRunner queryRunner, ExternalMinioConfig config)
    {
        if (config.table().isPresent()) {
            checkArgument(config.schema().isPresent(),
                    "paimon.external-minio.schema is required when paimon.external-minio.table is set");
            return new ReadTarget(config.schema().orElseThrow(), config.table().orElseThrow());
        }

        if (config.schema().isPresent()) {
            Optional<String> table = firstTable(queryRunner, config.schema().orElseThrow());
            assumeTrue(table.isPresent(), "External MinIO schema has no visible Paimon tables: " + config.schema().orElseThrow());
            return new ReadTarget(config.schema().orElseThrow(), table.orElseThrow());
        }

        for (Object schemaObject : queryRunner.execute("SHOW SCHEMAS FROM " + quote(CATALOG)).getOnlyColumnAsSet()) {
            String schema = (String) schemaObject;
            if (schema.equalsIgnoreCase("information_schema")) {
                continue;
            }
            Optional<String> table = firstTable(queryRunner, schema);
            if (table.isPresent()) {
                return new ReadTarget(schema, table.orElseThrow());
            }
        }
        assumeTrue(false, "External MinIO warehouse has no visible Paimon tables");
        throw new AssertionError("unreachable");
    }

    private static Optional<String> firstTable(DistributedQueryRunner queryRunner, String schema)
    {
        return queryRunner.execute("SHOW TABLES FROM " + qualifiedName(CATALOG, schema)).getOnlyColumnAsSet().stream()
                .map(String.class::cast)
                .findFirst();
    }

    private static Optional<String> configured(String name)
    {
        String property = System.getProperty(PROPERTY_PREFIX + name);
        if (property != null && !property.isBlank()) {
            return Optional.of(property.trim());
        }
        String env = System.getenv(ENV_PREFIX + name.toUpperCase(Locale.ROOT).replace('-', '_'));
        if (env != null && !env.isBlank()) {
            return Optional.of(env.trim());
        }
        return Optional.empty();
    }

    private static String required(String name)
    {
        return configured(name)
                .orElseThrow(() -> new IllegalArgumentException(
                        "External MinIO smoke test requires %s%s or %s%s"
                                .formatted(PROPERTY_PREFIX, name, ENV_PREFIX, name.toUpperCase(Locale.ROOT).replace('-', '_'))));
    }

    private static String qualifiedName(String... parts)
    {
        return String.join(".", java.util.Arrays.stream(parts)
                .map(TestPaimonExternalMinioSmokeTest::quote)
                .toList());
    }

    private static String quote(String identifier)
    {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static String stringLiteral(String value)
    {
        return "'" + value.replace("'", "''") + "'";
    }

    private record ReadTarget(String schema, String table) {}

    private record ExternalMinioConfig(
            String warehouse,
            String endpoint,
            String accessKey,
            String secretKey,
            String region,
            boolean pathStyleAccess,
            Optional<String> schema,
            Optional<String> table,
            int limit)
    {
        static ExternalMinioConfig load()
        {
            return load(true);
        }

        static ExternalMinioConfig loadForWrite()
        {
            return load(false);
        }

        private static ExternalMinioConfig load(boolean requireReadTarget)
        {
            int limit = configured("limit")
                    .map(Integer::parseInt)
                    .orElse(1);
            checkArgument(limit >= 0, "paimon.external-minio.limit must be non-negative: %s", limit);
            return new ExternalMinioConfig(
                    required("warehouse"),
                    required("endpoint"),
                    required("access-key"),
                    required("secret-key"),
                    configured("region").orElse("us-east-1"),
                    configured("path-style-access").map(Boolean::parseBoolean).orElse(true),
                    configured("schema"),
                    configured("table"),
                    limit);
        }

        Map<String, String> catalogProperties()
        {
            return ImmutableMap.<String, String>builder()
                    .put("warehouse", warehouse)
                    .put("fs.hadoop.enabled", "false")
                    .put("fs.native-s3.enabled", "true")
                    .put("s3.endpoint", endpoint)
                    .put("s3.aws-access-key", accessKey)
                    .put("s3.aws-secret-key", secretKey)
                    .put("s3.region", region)
                    .put("s3.path-style-access", Boolean.toString(pathStyleAccess))
                    .buildOrThrow();
        }
    }
}
