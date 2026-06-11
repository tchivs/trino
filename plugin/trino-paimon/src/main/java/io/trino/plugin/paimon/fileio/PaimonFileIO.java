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

import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.FileIOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class PaimonFileIO
        implements
        FileIO
{
    private static final String DIRECTORY_MARKER_FILE_NAME = "_trino_paimon_directory_marker";

    private final TrinoFileSystem trinoFileSystem;
    private final boolean objectStore;

    public PaimonFileIO(TrinoFileSystem trinoFileSystem, @Nullable Path path)
    {
        this.trinoFileSystem = trinoFileSystem;
        this.objectStore = path == null || checkObjectStore(path.toUri().getScheme());
    }

    private static boolean checkObjectStore(String scheme)
    {
        if (scheme == null) {
            return false;
        }
        return FileIOUtils.isObjectStore(scheme.toLowerCase(Locale.ENGLISH));
    }

    @Override
    public boolean isObjectStore()
    {
        return objectStore;
    }

    @Override
    public void configure(CatalogContext catalogContext)
    {
    }

    @Override
    public SeekableInputStream newInputStream(Path path)
            throws IOException
    {
        return new PaimonInputStreamWrapper(trinoFileSystem.newInputFile(Location.of(path.toString())).newStream());
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite)
            throws IOException
    {
        TrinoOutputFile trinoOutputFile = trinoFileSystem.newOutputFile(Location.of(path.toString()));

        try {
            return new PositionOutputStreamWrapper(trinoOutputFile.create());
        }
        catch (FileAlreadyExistsException e) {
            if (overwrite) {
                trinoFileSystem.deleteFile(Location.of(path.toString()));
                return new PositionOutputStreamWrapper(trinoOutputFile.create());
            }
            throw e;
        }
    }

    @Override
    public FileStatus getFileStatus(Path path)
            throws IOException
    {
        return status(path);
    }

    private FileStatus status(Path path)
            throws IOException
    {
        Location location = Location.of(path.toString());
        if (isDirectory(location)) {
            return new PaimonDirectoryFileStatus(path);
        }
        else {
            TrinoInputFile trinoInputFile = trinoFileSystem.newInputFile(location);
            return new PaimonFileStatus(trinoInputFile.length(), path, trinoInputFile.lastModified().getEpochSecond());
        }
    }

    @Override
    public FileStatus[] listStatus(Path path)
            throws IOException
    {
        List<FileStatus> fileStatusList = new ArrayList<>();
        Location location = Location.of(path.toString());
        if (isDirectory(location)) {
            FileIterator fileIterator = trinoFileSystem.listFiles(location);
            while (fileIterator.hasNext()) {
                FileEntry fileEntry = fileIterator.next();
                if (isDirectChild(location, fileEntry.location()) && !isDirectoryMarker(fileEntry.location())) {
                    fileStatusList.add(new PaimonFileStatus(fileEntry.length(), new Path(fileEntry.location().toString()),
                            fileEntry.lastModified().getEpochSecond()));
                }
            }
            trinoFileSystem.listDirectories(Location.of(path.toString()))
                    .forEach(l -> fileStatusList.add(new PaimonDirectoryFileStatus(new Path(l.toString()))));
        }
        return fileStatusList.toArray(new FileStatus[0]);
    }

    @Override
    public FileStatus[] listDirectories(Path path)
            throws IOException
    {
        return trinoFileSystem.listDirectories(Location.of(path.toString())).stream()
                .map(l -> new PaimonDirectoryFileStatus(new Path(l.toString()))).toArray(FileStatus[]::new);
    }

    @Override
    public boolean exists(Path path)
            throws IOException
    {
        Location location = Location.of(path.toString());
        return isDirectory(location) || existFile(location);
    }

    private boolean existFile(Location location)
            throws IOException
    {
        try {
            return trinoFileSystem.newInputFile(location).exists();
        }
        catch (IllegalArgumentException e) {
            return false;
        }
    }

    @Override
    public boolean delete(Path path, boolean recursive)
            throws IOException
    {
        Location location = Location.of(path.toString());
        if (isDirectory(location)) {
            if (!recursive) {
                if (hasChildForNonRecursiveDelete(location)) {
                    throw new IOException("Directory " + location + " is not empty");
                }
            }
            trinoFileSystem.deleteDirectory(location);
            return true;
        }
        else if (existFile(location)) {
            trinoFileSystem.deleteFile(location);
            return true;
        }

        return false;
    }

    @Override
    public boolean mkdirs(Path path)
            throws IOException
    {
        Location location = Location.of(path.toString());
        trinoFileSystem.createDirectory(location);
        if (objectStore) {
            trinoFileSystem.newOutputFile(directoryMarker(location)).createOrOverwrite(new byte[0]);
        }
        return true;
    }

    @Override
    public boolean rename(Path source, Path target)
            throws IOException
    {
        Location sourceLocation = Location.of(source.toString());
        Location targetLocation = Location.of(target.toString());
        if (isDirectory(sourceLocation)) {
            if (objectStore) {
                throw new IOException("S3 does not support directory renames");
            }
            trinoFileSystem.renameDirectory(sourceLocation, targetLocation);
        }
        else if (objectStore) {
            copyFile(source, target, false);
            trinoFileSystem.deleteFile(sourceLocation);
        }
        else {
            trinoFileSystem.renameFile(sourceLocation, targetLocation);
        }
        return true;
    }

    private boolean isDirectory(Location location)
            throws IOException
    {
        if (trinoFileSystem.directoryExists(location).orElse(false)) {
            return true;
        }
        return objectStore && directoryMarkerExists(location);
    }

    private boolean directoryMarkerExists(Location location)
            throws IOException
    {
        return trinoFileSystem.newInputFile(directoryMarker(location)).exists();
    }

    private boolean hasChildForNonRecursiveDelete(Location location)
            throws IOException
    {
        FileIterator fileIterator = trinoFileSystem.listFiles(location);
        while (fileIterator.hasNext()) {
            Location child = fileIterator.next().location();
            if (isDirectChild(location, child) && !isDirectoryMarker(child)) {
                return true;
            }
        }
        return !trinoFileSystem.listDirectories(location).isEmpty();
    }

    private static boolean isDirectChild(Location parent, Location child)
    {
        if (!parent.scheme().equals(child.scheme()) || !parent.host().equals(child.host())) {
            return false;
        }
        String parentPath = normalizeDirectoryPath(parent);
        String childPath = normalizeDirectoryPath(child);
        if (parentPath.isEmpty()) {
            return !childPath.isEmpty() && childPath.indexOf('/') < 0;
        }
        if (!childPath.startsWith(parentPath + "/")) {
            return false;
        }
        return childPath.indexOf('/', parentPath.length() + 1) < 0;
    }

    private static String normalizeDirectoryPath(Location location)
    {
        String path = location.path();
        if (path.endsWith("/") && path.length() > 1) {
            return path.substring(0, path.length() - 1);
        }
        return path;
    }

    private static Location directoryMarker(Location location)
    {
        return location.appendPath(DIRECTORY_MARKER_FILE_NAME);
    }

    private static boolean isDirectoryMarker(Location location)
    {
        return location.fileName().equals(DIRECTORY_MARKER_FILE_NAME);
    }
}
