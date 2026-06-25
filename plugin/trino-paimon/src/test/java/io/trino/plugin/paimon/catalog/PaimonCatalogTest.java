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
package io.trino.plugin.paimon.catalog;

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingConnectorSession;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.view.View;
import org.apache.paimon.view.ViewImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PaimonCatalogTest
{
    @TempDir
    Path root;

    @Test
    public void testCatalogLoaderRequiresSessionInitialization()
    {
        PaimonCatalog catalog = catalog();

        assertThatThrownBy(catalog::catalogLoader)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon catalog has not been initialized for a Trino session");
    }

    @Test
    public void testCatalogRejectsNullDependencies()
    {
        assertThatThrownBy(() -> new PaimonCatalog(null, new LocalFileSystemFactory(root)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("options is null");
        assertThatThrownBy(() -> new PaimonCatalog(new Options(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("paimonFileSystemFactory is null");
        assertThatThrownBy(() -> catalog().initSession(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("connectorSession is null");
    }

    @Test
    public void testCatalogLoaderDelegatesToInitializedCatalog()
            throws Exception
    {
        PaimonCatalog catalog = catalog();
        catalog.initSession(TestingConnectorSession.SESSION);
        catalog.createDatabase("test_schema", false, Map.of());

        CatalogLoader catalogLoader = catalog.catalogLoader();

        assertThat(catalogLoader).isNotNull();
        try (Catalog reloaded = catalogLoader.load()) {
            assertThat(reloaded.listDatabases()).contains("test_schema");
        }
    }

    @Test
    public void testCatalogDoesNotInjectNoHadoopFormatProviders()
    {
        PaimonCatalog catalog = catalog();
        catalog.initSession(TestingConnectorSession.SESSION);

        assertThat(catalog.options().keySet())
                .noneMatch(key -> key.startsWith("table.runtime." + "file.format."));
    }

    @Test
    public void testLocalCatalogViewOperationsRemainUnsupported()
            throws Exception
    {
        PaimonCatalog catalog = catalog();
        catalog.initSession(TestingConnectorSession.SESSION);
        Identifier viewName = new Identifier("view_db", "source_view");
        View view = view(viewName, "SELECT id FROM source_table", "initial comment");

        catalog.createDatabase(viewName.getDatabaseName(), false, Map.of());

        assertThatThrownBy(() -> catalog.createView(viewName, view, false))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testCatalogReusesDelegateForSameIdentity()
            throws Exception
    {
        RecordingFileSystemFactory fileSystemFactory = new RecordingFileSystemFactory(root);
        PaimonCatalog catalog = catalog(fileSystemFactory);
        TestingConnectorSession alice = session("alice");

        catalog.initSession(alice);
        catalog.createDatabase("alice_db", false, Map.of());

        catalog.initSession(alice);

        assertThat(catalog.listDatabases()).contains("alice_db");
        assertThat(fileSystemFactory.createCalls()).hasValue(1);
    }

    @Test
    public void testCatalogSeparatesDelegatesForDifferentIdentities()
            throws Exception
    {
        RecordingFileSystemFactory fileSystemFactory = new RecordingFileSystemFactory(root);
        PaimonCatalog catalog = catalog(fileSystemFactory);
        TestingConnectorSession alice = session("alice");
        TestingConnectorSession bob = session("bob");

        catalog.initSession(alice);
        catalog.createDatabase("alice_db", false, Map.of());

        catalog.initSession(bob);
        assertThat(catalog.listDatabases()).doesNotContain("alice_db");

        catalog.createDatabase("bob_db", false, Map.of());
        assertThat(catalog.listDatabases()).contains("bob_db").doesNotContain("alice_db");

        catalog.initSession(alice);
        assertThat(catalog.listDatabases()).contains("alice_db").doesNotContain("bob_db");
        assertThat(fileSystemFactory.createCalls()).hasValue(2);
    }

    @Test
    public void testCatalogSeparatesDelegatesForDifferentExtraCredentials()
            throws Exception
    {
        RecordingFileSystemFactory fileSystemFactory = new RecordingFileSystemFactory(root);
        PaimonCatalog catalog = catalog(fileSystemFactory);
        TestingConnectorSession aliceOne = session("alice", Map.of("token", "one"));
        TestingConnectorSession aliceTwo = session("alice", Map.of("token", "two"));

        catalog.initSession(aliceOne);
        catalog.createDatabase("first_db", false, Map.of());

        catalog.initSession(aliceTwo);
        assertThat(catalog.listDatabases()).doesNotContain("first_db");

        catalog.createDatabase("second_db", false, Map.of());
        assertThat(catalog.listDatabases()).contains("second_db").doesNotContain("first_db");

        catalog.initSession(aliceOne);
        assertThat(catalog.listDatabases()).contains("first_db").doesNotContain("second_db");
        assertThat(fileSystemFactory.createCalls()).hasValue(2);
    }

    @Test
    public void testCatalogDelegatesPaimonDefaultMethodsToCurrentCatalog()
            throws Exception
    {
        PaimonCatalog catalog = catalog();
        RecordingCatalog recordingCatalog = new RecordingCatalog();
        setCurrentCatalog(catalog, recordingCatalog.catalog());
        Identifier identifier = new Identifier("default", "test_table");

        catalog.listTablesPagedGlobally("default%", "test%", 10, "page");
        catalog.invalidateTable(identifier);
        catalog.repairCatalog();
        catalog.repairDatabase("default");
        catalog.repairTable(identifier);
        catalog.registerTable(identifier, "s3://warehouse/default.db/test_table");
        catalog.listConsumersPaged(identifier, 10, "page");
        catalog.resetConsumer(identifier, "consumer", 1L);
        catalog.rollbackSchema(identifier, 1);
        catalog.createBranch(identifier, "branch", "tag", true);
        catalog.listFunctionsPaged("default", 10, "page", "fn%");
        catalog.listFunctionsPagedGlobally("default%", "fn%", 10, "page");
        catalog.listFunctionDetailsPaged("default", 10, "page", "fn%");

        assertThat(recordingCatalog.calls()).containsExactly(
                "listTablesPagedGlobally",
                "invalidateTable",
                "repairCatalog",
                "repairDatabase",
                "repairTable",
                "registerTable",
                "listConsumersPaged",
                "resetConsumer",
                "rollbackSchema",
                "createBranch",
                "listFunctionsPaged",
                "listFunctionsPagedGlobally",
                "listFunctionDetailsPaged");
    }

    private PaimonCatalog catalog()
    {
        return catalog(new LocalFileSystemFactory(root));
    }

    private PaimonCatalog catalog(TrinoFileSystemFactory fileSystemFactory)
    {
        Options options = new Options();
        options.set(WAREHOUSE, "local:///warehouse");
        return new PaimonCatalog(options, fileSystemFactory);
    }

    private static View view(Identifier identifier, String query, String comment)
    {
        return new ViewImpl(
                identifier,
                List.of(DataTypes.FIELD(0, "id", DataTypes.BIGINT(), "id column")),
                query,
                Map.of("trino", query),
                comment,
                Map.of("comment", comment));
    }

    private static TestingConnectorSession session(String user)
    {
        return session(user, Map.of());
    }

    private static TestingConnectorSession session(String user, Map<String, String> extraCredentials)
    {
        return TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.forUser(user)
                        .withExtraCredentials(extraCredentials)
                        .build())
                .build();
    }

    @SuppressWarnings("unchecked")
    private static void setCurrentCatalog(PaimonCatalog catalog, Catalog currentCatalog)
            throws Exception
    {
        Field currentCatalogField = PaimonCatalog.class.getDeclaredField("currentCatalog");
        currentCatalogField.setAccessible(true);
        ((ThreadLocal<Catalog>) currentCatalogField.get(catalog)).set(currentCatalog);
    }

    private static final class RecordingCatalog
    {
        private final List<String> calls = new ArrayList<>();

        private Catalog catalog()
        {
            return (Catalog) Proxy.newProxyInstance(
                    PaimonCatalogTest.class.getClassLoader(),
                    new Class<?>[] {Catalog.class},
                    (proxy, method, args) -> {
                        if (method.getDeclaringClass() == Object.class) {
                            return method.invoke(this, args);
                        }
                        calls.add(method.getName());
                        if (method.getReturnType() == boolean.class) {
                            return false;
                        }
                        if (method.getReturnType() == Map.class) {
                            return Map.of();
                        }
                        if (method.getReturnType() == List.class) {
                            return List.of();
                        }
                        if (method.getReturnType() == org.apache.paimon.PagedList.class) {
                            return new org.apache.paimon.PagedList<>(List.of(), null);
                        }
                        return null;
                    });
        }

        private List<String> calls()
        {
            return calls;
        }
    }

    private static final class RecordingFileSystemFactory
            implements TrinoFileSystemFactory
    {
        private final Path root;
        private final AtomicInteger createCalls = new AtomicInteger();

        private RecordingFileSystemFactory(Path root)
        {
            this.root = root;
        }

        @Override
        public TrinoFileSystem create(ConnectorIdentity identity)
        {
            createCalls.incrementAndGet();
            Path userRoot = root.resolve(identity.getUser() + "-" + Integer.toHexString(identity.getExtraCredentials().hashCode()));
            try {
                Files.createDirectories(userRoot);
            }
            catch (java.io.IOException e) {
                throw new UncheckedIOException(e);
            }
            return new LocalFileSystemFactory(userRoot).create(identity);
        }

        public AtomicInteger createCalls()
        {
            return createCalls;
        }
    }
}
