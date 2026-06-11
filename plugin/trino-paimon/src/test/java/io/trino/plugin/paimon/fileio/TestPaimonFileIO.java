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
package io.trino.plugin.paimon.fileio;

import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.memory.MemoryFileSystem;
import org.apache.paimon.fs.Path;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class TestPaimonFileIO
{
    @Test
    public void testObjectStoreMkdirsCreatesDirectoryMarker()
            throws IOException
    {
        PaimonFileIO fileIO = new PaimonFileIO(new MemoryFileSystem(), null);
        Path databasePath = new Path("memory:///warehouse/minio_smoke.db");

        assertThat(fileIO.exists(databasePath)).isFalse();

        assertThat(fileIO.mkdirs(databasePath)).isTrue();

        assertThat(fileIO.exists(databasePath)).isTrue();
        assertThat(fileIO.getFileStatus(databasePath).isDir()).isTrue();
        assertThat(fileIO.listStatus(databasePath)).isEmpty();
    }

    @Test
    public void testObjectStoreRenameFileFallsBackToCopyAndDelete()
            throws IOException
    {
        PaimonFileIO fileIO = new PaimonFileIO(new NoRenameFileSystem(), null);
        Path source = new Path("memory:///warehouse/minio_smoke.db/orders/schema/.schema-0.tmp");
        Path target = new Path("memory:///warehouse/minio_smoke.db/orders/schema/schema-0");

        fileIO.writeFile(source, "schema", false);

        assertThat(fileIO.rename(source, target)).isTrue();

        assertThat(fileIO.exists(source)).isFalse();
        assertThat(fileIO.exists(target)).isTrue();
        assertThat(fileIO.readFileUtf8(target)).isEqualTo("schema");
    }

    private static class NoRenameFileSystem
            implements TrinoFileSystem
    {
        private final MemoryFileSystem delegate = new MemoryFileSystem();

        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            return delegate.newInputFile(location);
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length)
        {
            return delegate.newInputFile(location, length);
        }

        @Override
        public TrinoOutputFile newOutputFile(Location location)
        {
            return delegate.newOutputFile(location);
        }

        @Override
        public void deleteFile(Location location)
                throws IOException
        {
            delegate.deleteFile(location);
        }

        @Override
        public void deleteDirectory(Location location)
                throws IOException
        {
            delegate.deleteDirectory(location);
        }

        @Override
        public void deleteFiles(Collection<Location> locations)
                throws IOException
        {
            delegate.deleteFiles(locations);
        }

        @Override
        public void renameFile(Location source, Location target)
                throws IOException
        {
            throw new IOException("S3 does not support renames");
        }

        @Override
        public FileIterator listFiles(Location location)
                throws IOException
        {
            return delegate.listFiles(location);
        }

        @Override
        public Optional<Boolean> directoryExists(Location location)
                throws IOException
        {
            return delegate.directoryExists(location);
        }

        @Override
        public void createDirectory(Location location)
                throws IOException
        {
            delegate.createDirectory(location);
        }

        @Override
        public void renameDirectory(Location source, Location target)
                throws IOException
        {
            delegate.renameDirectory(source, target);
        }

        @Override
        public Set<Location> listDirectories(Location location)
                throws IOException
        {
            return delegate.listDirectories(location);
        }

        @Override
        public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
                throws IOException
        {
            return delegate.createTemporaryDirectory(targetPath, temporaryPrefix, relativePrefix);
        }
    }
}
