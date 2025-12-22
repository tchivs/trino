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
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeSignature;
import io.trino.testing.TestingConnectorSession;
import org.apache.paimon.options.Options;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestPaimonMetadataInitSession
{
    @Test
    public void testApplyLimitInitializesCatalog()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(new Options(), identity -> (TrinoFileSystem) null);
        PaimonMetadata metadata = new PaimonMetadata(catalog, new UnsupportedTypeManager());

        PaimonTableHandle tableHandle = new PaimonTableHandle("test", "table", java.util.Map.of());
        ConnectorSession session = TestingConnectorSession.SESSION;

        Optional<?> result = metadata.applyLimit(session, tableHandle, 10);
        assertThat(result).isPresent();
        assertThat(catalog.initSessionCalls).isEqualTo(1);
    }

    private static final class TestingPaimonCatalog
            extends PaimonCatalog
    {
        private int initSessionCalls;

        private TestingPaimonCatalog(Options options, TrinoFileSystemFactory factory)
        {
            super(options, factory);
        }

        @Override
        public void initSession(ConnectorSession connectorSession)
        {
            initSessionCalls++;
        }
    }

    private static final class UnsupportedTypeManager
            implements TypeManager
    {
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
            throw new UnsupportedOperationException();
        }

        @Override
        public TypeOperators getTypeOperators()
        {
            throw new UnsupportedOperationException();
        }
    }
}
