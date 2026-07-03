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

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static java.util.Objects.requireNonNull;

public class PaimonFileIO
        implements
        FileIO
{
    private static final String DIRECTORY_MARKER_FILE_NAME = "_trino_paimon_directory_marker";

    private final TrinoFileSystem trinoFileSystem;
    private final boolean objectStore;

    public PaimonFileIO(TrinoFileSystem trinoFileSystem, Path path)
    {
        this.trinoFileSystem = requireNonNull(trinoFileSystem, "trinoFileSystem is null");
        this.objectStore = checkObjectStore(requireNonNull(path, "path is null").toUri().getScheme());
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
        if (objectStore) {
            IOException fileProbeFailure = null;
            try {
                if (existFile(location)) {
                    return fileStatus(location, path);
                }
            }
            catch (IOException e) {
                fileProbeFailure = e;
            }
            if (isDirectory(location, false)) {
                return new PaimonDirectoryFileStatus(path);
            }
            if (fileProbeFailure != null) {
                throw fileProbeFailure;
            }
            return fileStatus(location, path);
        }
        if (isDirectory(location)) {
            return new PaimonDirectoryFileStatus(path);
        }
        return fileStatus(location, path);
    }

    @Override
    public FileStatus[] listStatus(Path path)
            throws IOException
    {
        List<FileStatus> fileStatusList = new ArrayList<>();
        Location location = Location.of(path.toString());
        if (objectStore) {
            boolean fileProbeFailed = false;
            try {
                if (existFile(location)) {
                    fileStatusList.add(fileStatus(location, path));
                }
            }
            catch (IOException e) {
                fileProbeFailed = true;
            }
            if (fileStatusList.isEmpty() && isDirectory(location, false)) {
                addDirectoryEntries(fileStatusList, location);
            }
            if (fileProbeFailed && fileStatusList.isEmpty()) {
                return new FileStatus[0];
            }
        }
        else if (isDirectory(location)) {
            addDirectoryEntries(fileStatusList, location);
        }
        else if (existFile(location)) {
            fileStatusList.add(status(path));
        }
        return fileStatusList.toArray(new FileStatus[0]);
    }

    private void addDirectoryEntries(List<FileStatus> fileStatusList, Location location)
            throws IOException
    {
        FileIterator fileIterator = trinoFileSystem.listFiles(location);
        while (fileIterator.hasNext()) {
            FileEntry fileEntry = fileIterator.next();
            if (isDirectChild(location, fileEntry.location()) && !isDirectoryMarker(fileEntry.location())) {
                fileStatusList.add(new PaimonFileStatus(fileEntry.length(), new Path(fileEntry.location().toString()),
                        fileEntry.lastModified().getEpochSecond()));
            }
        }
        trinoFileSystem.listDirectories(location)
                .forEach(l -> fileStatusList.add(new PaimonDirectoryFileStatus(new Path(l.toString()))));
    }

    @Override
    public FileStatus[] listDirectories(Path path)
            throws IOException
    {
        Location location = Location.of(path.toString());
        if (!isDirectoryForObjectStorePrefix(location)) {
            return new FileStatus[0];
        }
        return trinoFileSystem.listDirectories(location).stream()
                .map(l -> new PaimonDirectoryFileStatus(new Path(l.toString()))).toArray(FileStatus[]::new);
    }

    @Override
    public boolean exists(Path path)
            throws IOException
    {
        Location location = Location.of(path.toString());
        if (objectStore) {
            try {
                if (existFile(location)) {
                    return true;
                }
            }
            catch (IOException e) {
                return isDirectory(location, false);
            }
            return isDirectory(location, false);
        }
        return isDirectory(location) || existFile(location);
    }

    @Override
    public void checkOrMkdirs(Path path)
            throws IOException
    {
        if (!objectStore) {
            FileIO.super.checkOrMkdirs(path);
            return;
        }

        Location location = Location.of(path.toString());
        if (isDirectory(location, false)) {
            return;
        }

        try {
            if (existFile(location)) {
                throw new IllegalArgumentException("The path '%s' should be a directory.".formatted(path));
            }
        }
        catch (IOException ignored) {
            // Some S3-compatible stores fail HEAD on absent directory-prefix objects instead of
            // returning a normal not-found response. Let mkdirs perform the real write/access check.
        }
        mkdirs(path);
    }

    private FileStatus fileStatus(Location location, Path path)
            throws IOException
    {
        TrinoInputFile trinoInputFile = trinoFileSystem.newInputFile(location);
        return new PaimonFileStatus(trinoInputFile.length(), path, trinoInputFile.lastModified().getEpochSecond());
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
        if (isDirectoryForObjectStorePrefix(location)) {
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
        boolean sourceIsDirectory = isDirectoryForObjectStorePrefix(sourceLocation);
        if (!sourceIsDirectory && !existFile(sourceLocation)) {
            return false;
        }

        if (sourceIsDirectory && objectStore) {
            throw new IOException("S3 does not support directory renames");
        }

        if (isDirectoryForObjectStorePrefix(targetLocation)) {
            targetLocation = targetLocation.appendPath(source.getName());
            target = new Path(targetLocation.toString());
        }
        if (isDirectoryForObjectStorePrefix(targetLocation) || existFile(targetLocation)) {
            return false;
        }

        if (sourceIsDirectory) {
            trinoFileSystem.renameDirectory(sourceLocation, targetLocation);
        }
        else if (objectStore) {
            try {
                copyFile(source, target, false);
            }
            catch (FileAlreadyExistsException e) {
                return false;
            }
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
        return isDirectory(location, true);
    }

    private boolean isDirectoryForObjectStorePrefix(Location location)
            throws IOException
    {
        if (!objectStore) {
            return isDirectory(location);
        }
        try {
            if (existFile(location)) {
                return false;
            }
        }
        catch (IOException ignored) {
            // Some S3-compatible stores fail HEAD for absent directory-prefix objects. Continue
            // with directory marker/list probes, which are the authoritative object-store checks.
        }
        return isDirectory(location, false);
    }

    private boolean isDirectory(Location location, boolean checkExactFile)
            throws IOException
    {
        if (checkExactFile && objectStore && existFile(location)) {
            return false;
        }
        if (trinoFileSystem.directoryExists(location).orElse(false)) {
            return true;
        }
        if (!objectStore) {
            return false;
        }
        return directoryMarkerExists(location)
                || !trinoFileSystem.listDirectories(location).isEmpty()
                || hasDirectChildFile(location);
    }

    private boolean directoryMarkerExists(Location location)
            throws IOException
    {
        try {
            return trinoFileSystem.newInputFile(directoryMarker(location)).exists();
        }
        catch (IOException e) {
            return false;
        }
    }

    private boolean hasChildForNonRecursiveDelete(Location location)
            throws IOException
    {
        if (hasDirectChildFile(location)) {
            return true;
        }
        return !trinoFileSystem.listDirectories(location).isEmpty();
    }

    private boolean hasDirectChildFile(Location location)
            throws IOException
    {
        FileIterator fileIterator = trinoFileSystem.listFiles(location);
        while (fileIterator.hasNext()) {
            Location child = fileIterator.next().location();
            if (isDirectChild(location, child) && !isDirectoryMarker(child)) {
                return true;
            }
        }
        return false;
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
