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

import io.trino.spi.Plugin;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.UUID;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;

public class TrinoPluginTest
{
    @Test
    public void testCreatePrestoConnector()
            throws IOException
    {
        String warehouse = Files.createTempDirectory(UUID.randomUUID().toString()).toUri().toString();
        Plugin plugin = new PaimonPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        Connector connector = factory.create("paimon", Map.of("warehouse", warehouse),
                new TestingConnectorContext());
        assertThat(connector).isNotNull();
    }

    @Test
    void testTrinoFormatFactoriesAreRegistered()
    {
        // The connector must register TrinoPaimonParquetFileFormatFactory and
        // TrinoPaimonOrcFileFormatFactory as FileFormatFactory service providers.
        // This is a regression guard for the ServiceLoader conflict where
        // paimon-bundle's native factories (requiring Hadoop) shadow the Trino
        // no-Hadoop factories.
        java.util.List<org.apache.paimon.format.FileFormatFactory> factories = java.util.ServiceLoader.load(
                org.apache.paimon.format.FileFormatFactory.class,
                PaimonPlugin.class.getClassLoader())
                .stream()
                .map(java.util.ServiceLoader.Provider::get)
                .toList();
        assertThat(factories).isNotEmpty();
        assertThat(factories.stream().map(f -> f.getClass().getName()))
                .as("Trino no-Hadoop parquet factory must be discoverable")
                .contains("io.trino.plugin.paimon.format.TrinoPaimonParquetFileFormatFactory");
        assertThat(factories.stream().map(f -> f.getClass().getName()))
                .as("Trino no-Hadoop orc factory must be discoverable")
                .contains("io.trino.plugin.paimon.format.TrinoPaimonOrcFileFormatFactory");
    }
}
