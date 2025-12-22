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

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeSignature;
import io.trino.testing.TestingConnectorSession;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPaimonViewFieldIds
{
    @Test
    public void testCreateViewAssignsUniqueFieldIds()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(new Options(), identity -> (TrinoFileSystem) null);
        PaimonMetadata metadata = new PaimonMetadata(catalog, new SingleTypeManager());

        TypeId varcharId = VARCHAR.getTypeId();
        ConnectorViewDefinition definition = new ConnectorViewDefinition(
                "select 1",
                Optional.of("test_catalog"),
                Optional.of("test_schema"),
                List.of(
                        new ConnectorViewDefinition.ViewColumn("c1", varcharId, Optional.empty()),
                        new ConnectorViewDefinition.ViewColumn("c2", varcharId, Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                false,
                List.of());

        metadata.createView(TestingConnectorSession.SESSION, new SchemaTableName("test_schema", "test_view"), definition, Map.of(), false);

        assertThat(catalog.capturedView).isNotNull();
        List<DataField> fields = catalog.capturedView.rowType().getFields();
        assertThat(fields).hasSize(2);
        assertThat(fields.get(0).id()).isEqualTo(0);
        assertThat(fields.get(1).id()).isEqualTo(1);
        assertThat(fields.get(0).id()).isNotEqualTo(fields.get(1).id());
    }

    @Test
    public void testGetViewReturnsCatalogAndSchemaWhenStored()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(new Options(), identity -> (TrinoFileSystem) null);
        PaimonMetadata metadata = new PaimonMetadata(catalog, new SingleTypeManager());

        TypeId varcharId = VARCHAR.getTypeId();
        ConnectorViewDefinition definition = new ConnectorViewDefinition(
                "select 1",
                Optional.of("test_catalog"),
                Optional.of("test_schema"),
                List.of(new ConnectorViewDefinition.ViewColumn("c1", varcharId, Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                false,
                List.of());

        SchemaTableName viewName = new SchemaTableName("test_schema", "test_view");
        metadata.createView(TestingConnectorSession.SESSION, viewName, definition, Map.of(), false);

        ConnectorViewDefinition read = metadata.getView(TestingConnectorSession.SESSION, viewName).orElseThrow();
        assertThat(read.getCatalog()).contains("test_catalog");
        assertThat(read.getSchema()).contains("test_schema");
    }

    private static final class TestingPaimonCatalog
            extends PaimonCatalog
    {
        private org.apache.paimon.view.View capturedView;

        private TestingPaimonCatalog(Options options, TrinoFileSystemFactory factory)
        {
            super(options, factory);
        }

        @Override
        public void initSession(ConnectorSession connectorSession) {}

        @Override
        public void createView(Identifier identifier, org.apache.paimon.view.View view, boolean ignoreIfExists)
        {
            this.capturedView = view;
        }

        @Override
        public org.apache.paimon.view.View getView(Identifier identifier)
        {
            return capturedView;
        }
    }

    private static final class SingleTypeManager
            implements TypeManager
    {
        private final TypeOperators typeOperators = new TypeOperators();

        @Override
        public Type getType(TypeSignature signature)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Type fromSqlType(String type)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Type getType(TypeId id)
        {
            if (id.equals(VARCHAR.getTypeId())) {
                return VARCHAR;
            }
            throw new UnsupportedOperationException();
        }

        @Override
        public TypeOperators getTypeOperators()
        {
            return typeOperators;
        }
    }
}
