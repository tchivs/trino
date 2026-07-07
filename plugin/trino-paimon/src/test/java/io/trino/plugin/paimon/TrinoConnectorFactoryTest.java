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

import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.type.Type;
import io.trino.testing.TestingConnectorContext;
import org.apache.paimon.options.Options;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TrinoConnectorFactoryTest
{
    @TempDir
    java.nio.file.Path tempFile;

    @Test
    public void testCreateConnector()
    {
        Map<String, String> config = Map.of("warehouse", tempFile.toString());
        ConnectorFactory factory = new PaimonConnectorFactory();
        Connector connector = factory.create("paimon", config, new TestingConnectorContext());
        assertThat(connector).isNotNull();
    }

    @Test
    public void testConnectorShutdownDoesNotPropagateCatalogCloseFailure()
    {
        AtomicBoolean closeCalled = new AtomicBoolean();
        PaimonConnector connector = new PaimonConnector(
                new ConnectorMetadata() {},
                new ConnectorSplitManager() {},
                (transaction, session, split, table, columns, dynamicFilter) -> {
                    throw new UnsupportedOperationException("not used");
                },
                new ConnectorPageSinkProvider()
                {
                    @Override
                    public io.trino.spi.connector.ConnectorPageSink createPageSink(
                            ConnectorTransactionHandle transactionHandle,
                            ConnectorSession session,
                            ConnectorOutputTableHandle outputTableHandle,
                            ConnectorPageSinkId pageSinkId)
                    {
                        throw new UnsupportedOperationException("not used");
                    }

                    @Override
                    public io.trino.spi.connector.ConnectorPageSink createPageSink(
                            ConnectorTransactionHandle transactionHandle,
                            ConnectorSession session,
                            ConnectorInsertTableHandle insertTableHandle,
                            ConnectorPageSinkId pageSinkId)
                    {
                        throw new UnsupportedOperationException("not used");
                    }
                },
                new ConnectorNodePartitioningProvider()
                {
                    @Override
                    public BucketFunction getBucketFunction(
                            ConnectorTransactionHandle transactionHandle,
                            ConnectorSession session,
                            ConnectorPartitioningHandle partitioningHandle,
                            List<Type> partitionChannelTypes,
                            int bucketCount)
                    {
                        throw new UnsupportedOperationException("not used");
                    }
                },
                new FailingClosePaimonCatalog(closeCalled),
                new PaimonSchemaProperties(),
                new PaimonTableOptions(),
                new PaimonSessionProperties(),
                Set.of(),
                new FunctionProvider() {});

        assertThatCode(connector::shutdown).doesNotThrowAnyException();
        assertThat(closeCalled).isTrue();
    }

    @Test
    public void testMetadataFactoryRejectsNullDependencies()
    {
        assertThatThrownBy(() -> new PaimonMetadataFactory(null, session -> {
            throw new UnsupportedOperationException("not used");
        }, TESTING_TYPE_MANAGER))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("options is null");
        assertThatThrownBy(() -> new PaimonMetadataFactory(new Options(), null, TESTING_TYPE_MANAGER))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fileSystemFactory is null");
        assertThatThrownBy(() -> new PaimonMetadataFactory(new Options(), session -> {
            throw new UnsupportedOperationException("not used");
        }, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("typeManager is null");
    }

    @Test
    public void testPaimonObjectStoreCredentialsAreMappedToTrinoNativeCredentials()
    {
        Map<String, String> config = new HashMap<>();
        config.put("s3.access-key", "paimon-access");
        config.put("s3.secret-key", "paimon-secret");

        PaimonConnectorFactory.addS3CredentialProperties(config);

        assertThat(config)
                .containsEntry("s3.aws-access-key", "paimon-access")
                .containsEntry("s3.aws-secret-key", "paimon-secret");
    }

    @Test
    public void testTrinoNativeObjectStoreCredentialsAreMappedToPaimonCredentials()
    {
        Map<String, String> config = new HashMap<>();
        config.put("s3.aws-access-key", "trino-access");
        config.put("s3.aws-secret-key", "trino-secret");

        PaimonConnectorFactory.addS3CredentialProperties(config);

        assertThat(config)
                .containsEntry("s3.access-key", "trino-access")
                .containsEntry("s3.secret-key", "trino-secret");
    }

    @Test
    public void testExplicitTrinoObjectStoreCredentialsArePreserved()
    {
        Map<String, String> config = new HashMap<>();
        config.put("s3.access-key", "paimon-access");
        config.put("s3.secret-key", "paimon-secret");
        config.put("s3.aws-access-key", "trino-access");
        config.put("s3.aws-secret-key", "trino-secret");

        PaimonConnectorFactory.addS3CredentialProperties(config);

        assertThat(config)
                .containsEntry("s3.aws-access-key", "trino-access")
                .containsEntry("s3.aws-secret-key", "trino-secret");
    }

    @Test
    public void testExplicitPaimonObjectStoreCredentialsArePreserved()
    {
        Map<String, String> config = new HashMap<>();
        config.put("s3.access-key", "paimon-access");
        config.put("s3.secret-key", "paimon-secret");
        config.put("s3.aws-access-key", "trino-access");
        config.put("s3.aws-secret-key", "trino-secret");

        PaimonConnectorFactory.addS3CredentialProperties(config);

        assertThat(config)
                .containsEntry("s3.access-key", "paimon-access")
                .containsEntry("s3.secret-key", "paimon-secret");
    }

    @Test
    public void testBlankTrinoNativeObjectStoreCredentialsAreReplaced()
    {
        Map<String, String> config = new HashMap<>();
        config.put("s3.access-key", "paimon-access");
        config.put("s3.secret-key", "paimon-secret");
        config.put("s3.aws-access-key", " ");
        config.put("s3.aws-secret-key", "\t");

        PaimonConnectorFactory.addS3CredentialProperties(config);

        assertThat(config)
                .containsEntry("s3.aws-access-key", "paimon-access")
                .containsEntry("s3.aws-secret-key", "paimon-secret");
    }

    @Test
    public void testBlankPaimonObjectStoreCredentialsAreReplaced()
    {
        Map<String, String> config = new HashMap<>();
        config.put("s3.access-key", " ");
        config.put("s3.secret-key", "\t");
        config.put("s3.aws-access-key", "trino-access");
        config.put("s3.aws-secret-key", "trino-secret");

        PaimonConnectorFactory.addS3CredentialProperties(config);

        assertThat(config)
                .containsEntry("s3.access-key", "trino-access")
                .containsEntry("s3.secret-key", "trino-secret");
    }

    private static class FailingClosePaimonCatalog
            extends PaimonCatalog
    {
        private final AtomicBoolean closeCalled;

        private FailingClosePaimonCatalog(AtomicBoolean closeCalled)
        {
            super(new Options(), identity -> {
                throw new UnsupportedOperationException("not used");
            });
            this.closeCalled = closeCalled;
        }

        @Override
        public void close()
                throws Exception
        {
            closeCalled.set(true);
            throw new IOException("close failed");
        }
    }
}
