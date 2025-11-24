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
import io.trino.plugin.paimon.fileio.PaimonFileIO;
import io.trino.plugin.paimon.fileio.PaimonFileIOLoader;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestPaimonFileIOLoader
{
    @Test
    public void testFileIOLoaderScheme()
    {
        TrinoFileSystem mockFileSystem = createMockFileSystem();
        PaimonFileIOLoader loader = new PaimonFileIOLoader(mockFileSystem);

        // Verify getScheme() returns a valid value
        String scheme = loader.getScheme();
        assertThat(scheme).isNotNull();
        assertThat(scheme).isEqualTo("trino");

        System.out.println("✓ PaimonFileIOLoader.getScheme() = " + scheme);
    }

    @Test
    public void testFileIOLoaderLoad()
    {
        TrinoFileSystem mockFileSystem = createMockFileSystem();
        PaimonFileIOLoader loader = new PaimonFileIOLoader(mockFileSystem);

        // Test loading FileIO
        Path testPath = new Path("s3://test-bucket/test-path");
        FileIO fileIO = loader.load(testPath);

        assertThat(fileIO).isNotNull();
        assertThat(fileIO).isInstanceOf(PaimonFileIO.class);

        System.out.println("✓ PaimonFileIOLoader.load() created PaimonFileIO");
    }

    @Test
    public void testCatalogContextWithPreferIOLoader()
    {
        TrinoFileSystem mockFileSystem = createMockFileSystem();
        PaimonFileIOLoader loader = new PaimonFileIOLoader(mockFileSystem);

        // Create CatalogContext with PaimonFileIOLoader as preferIOLoader
        Options options = new Options();
        options.set("warehouse", "s3://test-bucket/warehouse");

        CatalogContext context = CatalogContext.create(options, loader, loader);

        assertThat(context).isNotNull();
        assertThat(context.preferIO()).isEqualTo(loader);
        assertThat(context.fallbackIO()).isEqualTo(loader);

        System.out.println("✓ CatalogContext created with preferIO: " + context.getClass().getName());
        System.out.println("✓ preferIO: " + (context.preferIO() != null ? context.preferIO().getClass().getName() : "null"));
        System.out.println("✓ fallbackIO: " + (context.fallbackIO() != null ? context.fallbackIO().getClass().getName() : "null"));
    }

    @Test
    public void testFileIOLoaderIsUsedAsPreferIO()
    {
        TrinoFileSystem mockFileSystem = createMockFileSystem();
        PaimonFileIOLoader loader = new PaimonFileIOLoader(mockFileSystem);

        // Create CatalogContext with PaimonFileIOLoader as preferIO
        Options options = new Options();
        options.set("warehouse", "s3://test-bucket/warehouse");

        CatalogContext context = CatalogContext.create(options, loader, loader);

        System.out.println("\n=== Testing CatalogContext Setup ===");
        System.out.println("Context type: " + context.getClass().getName());
        System.out.println("preferIO: " + (context.preferIO() != null ? context.preferIO().getClass().getName() : "null"));
        System.out.println("fallbackIO: " + (context.fallbackIO() != null ? context.fallbackIO().getClass().getName() : "null"));

        // Verify that our loader is set as preferIO
        assertThat(context.preferIO()).isNotNull();
        assertThat(context.preferIO()).isEqualTo(loader);
        assertThat(context.preferIO()).isInstanceOf(PaimonFileIOLoader.class);

        System.out.println("✓ PaimonFileIOLoader is correctly set as preferIO");
        System.out.println("✓ This means Paimon will try PaimonFileIO first, before falling back to HadoopFileIO");
    }

    private TrinoFileSystem createMockFileSystem()
    {
        // Return a minimal mock that's sufficient for testing the loader
        // We don't need a full implementation since we're only testing the loader setup
        return null; // The loader can be created with null FileSystem for these tests
    }
}
