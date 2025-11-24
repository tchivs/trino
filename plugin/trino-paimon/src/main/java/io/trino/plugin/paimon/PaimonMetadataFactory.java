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

public class PaimonMetadataFactory
{
    private final PaimonCatalog catalog;

    private final TypeManager typeManager;

    @Inject
    public PaimonMetadataFactory(Options options, TrinoFileSystemFactory fileSystemFactory, TypeManager typeManager)
    {
        this.catalog = new PaimonCatalog(options, fileSystemFactory);
        this.typeManager = typeManager;
    }

    public PaimonMetadata create()
    {
        return new PaimonMetadata(catalog, typeManager);
    }
}
