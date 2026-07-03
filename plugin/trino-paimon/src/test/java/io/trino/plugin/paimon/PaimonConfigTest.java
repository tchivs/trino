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

import org.apache.paimon.options.Options;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.plugin.paimon.catalog.PaimonCatalog.DEFAULT_SESSION_CATALOG_CACHE_MAXIMUM_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

public class PaimonConfigTest
{
    @Test
    public void testDefaultConfigToOptionsIsEmpty()
    {
        PaimonConfig config = new PaimonConfig();

        Options options = config.toOptions();

        assertThat(options.toMap()).isEmpty();
    }

    @Test
    public void testWarehouseIsMapped()
    {
        PaimonConfig config = new PaimonConfig().setWarehouse("/tmp/warehouse");

        Options options = config.toOptions();

        assertThat(options.toMap()).containsEntry("warehouse", "/tmp/warehouse");
    }

    @Test
    public void testS3CredentialsAreMapped()
    {
        PaimonConfig config = new PaimonConfig()
                .setS3AccessKey("access")
                .setS3SecretKey("secret");

        Options options = config.toOptions();

        assertThat(options.toMap())
                .containsEntry("s3.access-key", "access")
                .containsEntry("s3.secret-key", "secret");
    }

    @Test
    public void testS3EndpointAndRegionAreMapped()
    {
        PaimonConfig config = new PaimonConfig()
                .setS3Endpoint("http://localhost:9000")
                .setS3Region("us-east-1");

        Options options = config.toOptions();

        assertThat(options.toMap())
                .containsEntry("s3.endpoint", "http://localhost:9000")
                .containsEntry("s3.region", "us-east-1");
    }

    @Test
    public void testS3PathStyleAccessIsMapped()
    {
        PaimonConfig config = new PaimonConfig().setS3PathStyleAccess(true);

        Options options = config.toOptions();

        assertThat(options.toMap()).containsEntry("s3.path-style-access", "true");
    }

    @Test
    public void testFileSystemFlagsAreMapped()
    {
        PaimonConfig config = new PaimonConfig()
                .setFsNativeS3Enabled(true)
                .setFsHadoopEnabled(false);

        Options options = config.toOptions();

        assertThat(options.toMap())
                .containsEntry("fs.native-s3.enabled", "true")
                .containsEntry("fs.hadoop.enabled", "false");
    }

    @Test
    public void testUnsetPropertiesAreNotIncluded()
    {
        PaimonConfig config = new PaimonConfig().setWarehouse("/tmp/warehouse");

        Options options = config.toOptions();

        assertThat(options.toMap())
                .containsEntry("warehouse", "/tmp/warehouse")
                .doesNotContainKeys("s3.access-key", "s3.secret-key", "fs.native-s3.enabled");
    }

    @Test
    public void testGettersReturnSetValues()
    {
        PaimonConfig config = new PaimonConfig()
                .setWarehouse("/tmp/warehouse")
                .setS3AccessKey("access")
                .setS3SecretKey("secret")
                .setS3Endpoint("http://localhost:9000")
                .setS3Region("us-east-1")
                .setS3PathStyleAccess(true)
                .setFsNativeS3Enabled(true)
                .setFsHadoopEnabled(false);

        assertThat(config.getWarehouse()).isEqualTo("/tmp/warehouse");
        assertThat(config.getS3AccessKey()).isEqualTo("access");
        assertThat(config.getS3SecretKey()).isEqualTo("secret");
        assertThat(config.getS3Endpoint()).isEqualTo("http://localhost:9000");
        assertThat(config.getS3Region()).isEqualTo("us-east-1");
        assertThat(config.getS3PathStyleAccess()).isTrue();
        assertThat(config.getFsNativeS3Enabled()).isTrue();
        assertThat(config.getFsHadoopEnabled()).isFalse();
        assertThat(config.getCatalogSessionCacheMaximumSize()).isEqualTo(DEFAULT_SESSION_CATALOG_CACHE_MAXIMUM_SIZE);
    }

    @Test
    public void testCatalogSessionCacheMaximumSizeIsConnectorOnly()
    {
        PaimonConfig config = new PaimonConfig()
                .setWarehouse("/tmp/warehouse")
                .setCatalogSessionCacheMaximumSize(10);

        Options options = config.toOptions();

        assertThat(config.getCatalogSessionCacheMaximumSize()).isEqualTo(10);
        assertThat(options.toMap())
                .containsEntry("warehouse", "/tmp/warehouse")
                .doesNotContainKey("catalog.session-cache.maximum-size");
    }

    @Test
    public void testAllPropertiesTogether()
    {
        PaimonConfig config = new PaimonConfig()
                .setWarehouse("/tmp/warehouse")
                .setS3AccessKey("access")
                .setS3SecretKey("secret")
                .setS3Endpoint("http://localhost:9000")
                .setS3Region("us-east-1")
                .setS3PathStyleAccess(true)
                .setFsNativeS3Enabled(true)
                .setFsHadoopEnabled(false);

        Options options = config.toOptions();

        assertThat(options.toMap()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "warehouse", "/tmp/warehouse",
                "s3.access-key", "access",
                "s3.secret-key", "secret",
                "s3.endpoint", "http://localhost:9000",
                "s3.region", "us-east-1",
                "s3.path-style-access", "true",
                "fs.native-s3.enabled", "true",
                "fs.hadoop.enabled", "false"));
    }
}
