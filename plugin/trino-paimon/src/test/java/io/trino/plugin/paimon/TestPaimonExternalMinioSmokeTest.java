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
                .setSchema(config.schema())
                .build();

        try (DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build()) {
            queryRunner.installPlugin(new PaimonPlugin());
            queryRunner.createCatalog(CATALOG, CATALOG, config.catalogProperties());

            assertThat(queryRunner.execute("SHOW SCHEMAS FROM " + quote(CATALOG)).getOnlyColumnAsSet())
                    .contains(config.schema());
            assertThat(queryRunner.execute("SHOW TABLES FROM " + qualifiedName(CATALOG, config.schema())).getOnlyColumnAsSet())
                    .contains(config.table());

            String tableName = qualifiedName(CATALOG, config.schema(), config.table());
            assertThat(queryRunner.execute("SELECT table_name FROM " + qualifiedName(CATALOG, "information_schema", "tables")
                    + " WHERE table_schema = " + stringLiteral(config.schema())
                    + " AND table_name = " + stringLiteral(config.table())).getOnlyColumnAsSet())
                    .contains(config.table());

            String createTable = (String) queryRunner.execute("SHOW CREATE TABLE " + tableName)
                    .getOnlyValue();
            assertThat(createTable)
                    .contains("CREATE TABLE")
                    .contains(config.table());

            MaterializedResult columns = queryRunner.execute("SHOW COLUMNS FROM " + tableName);
            assertThat(columns.getRowCount()).isGreaterThan(0);

            MaterializedResult rows = queryRunner.execute("SELECT * FROM " + tableName + " LIMIT " + config.limit());
            assertThat(rows.getMaterializedRows()).hasSizeLessThanOrEqualTo(config.limit());
        }
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

    private record ExternalMinioConfig(
            String warehouse,
            String endpoint,
            String accessKey,
            String secretKey,
            String region,
            boolean pathStyleAccess,
            String schema,
            String table,
            int limit)
    {
        static ExternalMinioConfig load()
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
                    required("schema"),
                    required("table"),
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
