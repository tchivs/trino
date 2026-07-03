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

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.type.TypeManager;
import org.apache.paimon.options.Options;

import static java.util.Objects.requireNonNull;

public class PaimonMetadataFactory
{
    private final PaimonCatalog catalog;

    private final TypeManager typeManager;

    @Inject
    public PaimonMetadataFactory(Options options, TrinoFileSystemFactory fileSystemFactory, TypeManager typeManager,
            PaimonConfig config)
    {
        this(options, fileSystemFactory, typeManager,
                requireNonNull(config, "config is null").getCatalogSessionCacheMaximumSize());
    }

    public PaimonMetadataFactory(Options options, TrinoFileSystemFactory fileSystemFactory, TypeManager typeManager)
    {
        this(options, fileSystemFactory, typeManager, PaimonCatalog.DEFAULT_SESSION_CATALOG_CACHE_MAXIMUM_SIZE);
    }

    private PaimonMetadataFactory(Options options, TrinoFileSystemFactory fileSystemFactory, TypeManager typeManager,
            int sessionCatalogCacheMaximumSize)
    {
        this.catalog = new PaimonCatalog(
                requireNonNull(options, "options is null"),
                requireNonNull(fileSystemFactory, "fileSystemFactory is null"),
                sessionCatalogCacheMaximumSize);
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    public PaimonMetadata create()
    {
        return new PaimonMetadata(catalog, typeManager);
    }

    public TypeManager typeManager()
    {
        return typeManager;
    }
}
