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

import jakarta.annotation.Nullable;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.crosspartition.IndexBootstrap;
import org.apache.paimon.crosspartition.KeyPartPartitionKeyExtractor;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReader.RecordIterator;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;
import org.apache.paimon.utils.TypeUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.paimon.CoreOptions.SCAN_MODE;
import static org.apache.paimon.CoreOptions.SCAN_SNAPSHOT_ID;
import static org.apache.paimon.CoreOptions.StartupMode.FROM_SNAPSHOT;
import static org.apache.paimon.CoreOptions.StartupMode.LATEST;
import static org.apache.paimon.io.SplitsParallelReadUtil.parallelExecute;

/** Shared, snapshot-pinned bootstrap files for Paimon KEY_DYNAMIC writers. */
final class PaimonKeyDynamicBootstrap
{
    private static final int MANIFEST_MAGIC = 0x504B4442;
    private static final int MANIFEST_VERSION = 1;
    private static final String ROOT_DIRECTORY = ".trino-key-dynamic-bootstrap";
    private static final String MANIFEST_FILE = "manifest";

    private PaimonKeyDynamicBootstrap() {}

    static Artifact open(
            FileStoreTable table,
            String queryId,
            OptionalSnapshot expectedSnapshot,
            int assignerParallelism)
            throws Exception
    {
        requireNonNull(table, "table is null");
        requireNonNull(queryId, "queryId is null");
        requireNonNull(expectedSnapshot, "expectedSnapshot is null");
        checkArgument(!queryId.isBlank(), "queryId is blank");
        checkArgument(assignerParallelism > 0, "assignerParallelism must be positive: %s", assignerParallelism);

        Long snapshot = snapshotForBootstrap(table, expectedSnapshot);
        FileIO fileIO = table.fileIO();
        Path root = artifactRoot(table, queryId, expectedSnapshot.pinned() ? snapshot : null, assignerParallelism);
        Path manifest = new Path(root, MANIFEST_FILE);
        if (!fileIO.exists(manifest)) {
            throw new IOException("Paimon KEY_DYNAMIC bootstrap artifact was not prepared by the coordinator: " + root);
        }
        return readManifest(fileIO, manifest, root, table, snapshot, assignerParallelism);
    }

    static void prepare(
            FileStoreTable table,
            String queryId,
            OptionalSnapshot expectedSnapshot,
            int assignerParallelism)
            throws Exception
    {
        requireNonNull(table, "table is null");
        requireNonNull(queryId, "queryId is null");
        requireNonNull(expectedSnapshot, "expectedSnapshot is null");
        checkArgument(!queryId.isBlank(), "queryId is blank");
        checkArgument(assignerParallelism > 0, "assignerParallelism must be positive: %s", assignerParallelism);

        Long snapshot = snapshotForBootstrap(table, expectedSnapshot);
        FileIO fileIO = table.fileIO();
        Path root = artifactRoot(table, queryId, expectedSnapshot.pinned() ? snapshot : null, assignerParallelism);
        Path manifest = new Path(root, MANIFEST_FILE);
        fileIO.mkdirs(root);
        if (!fileIO.exists(manifest)) {
            generate(fileIO, root, manifest, table, snapshot, assignerParallelism);
        }
        readManifest(fileIO, manifest, root, table, snapshot, assignerParallelism);
    }

    static void cleanup(FileStoreTable table, String queryId, OptionalSnapshot expectedSnapshot, int assignerParallelism)
    {
        try {
            Long snapshot = optionalSnapshotValue(expectedSnapshot.snapshotId());
            table.fileIO().delete(artifactRoot(table, queryId, snapshot, assignerParallelism), true);
        }
        catch (Exception ignored) {
            // Bootstrap artifacts are temporary. A later table cleanup can remove an artifact left by a failed query.
        }
    }

    static OptionalLong latestSnapshot(FileStoreTable table)
    {
        Long snapshot = latestSnapshotId(requireNonNull(table, "table is null"));
        return snapshot == null ? OptionalLong.empty() : OptionalLong.of(snapshot);
    }

    static void validateSnapshot(FileStoreTable table, OptionalSnapshot expectedSnapshot, String phase)
    {
        requireNonNull(table, "table is null");
        requireNonNull(expectedSnapshot, "expectedSnapshot is null");
        Long expected = expectedSnapshot.pinned() ? optionalSnapshotValue(expectedSnapshot.snapshotId()) : null;
        verifySnapshot(latestSnapshotId(table), expected, expectedSnapshot.pinned(), phase);
    }

    static OptionalSnapshot snapshotFor(PaimonTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        if (!tableHandle.isKeyDynamicBootstrapSnapshotPlanned()) {
            return OptionalSnapshot.unpinned();
        }
        return OptionalSnapshot.pinned(tableHandle.getKeyDynamicBootstrapSnapshot());
    }

    private static void generate(
            FileIO fileIO,
            Path root,
            Path manifest,
            FileStoreTable table,
            @Nullable Long snapshot,
            int assignerParallelism)
            throws Exception
    {
        Path attempt = new Path(root, "attempt-" + UUID.randomUUID());
        fileIO.mkdirs(attempt);
        List<ShardWriter> writers = new ArrayList<>();
        try {
            RowType bootstrapType = IndexBootstrap.bootstrapType(table.schema());
            for (int assigner = 0; assigner < assignerParallelism; assigner++) {
                writers.add(new ShardWriter(fileIO, new Path(attempt, "part-" + assigner), bootstrapType));
            }

            KeyPartPartitionKeyExtractor keyExtractor = new KeyPartPartitionKeyExtractor(table.schema());
            try (RecordReader<InternalRow> reader = SnapshotPinnedBootstrap.bootstrap(table, snapshot)) {
                RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    try {
                        InternalRow row;
                        while ((row = batch.next()) != null) {
                            BinaryRow key = keyExtractor.trimmedPrimaryKey(row);
                            int assigner = Math.abs(key.hashCode() % assignerParallelism);
                            writers.get(assigner).write(row);
                        }
                    }
                    finally {
                        batch.releaseBatch();
                    }
                }
            }
            closeWriters(writers);
            List<ShardMetadata> shardMetadata = writersFromClosed(writers);

            Path attemptManifest = new Path(attempt, MANIFEST_FILE);
            writeManifest(fileIO, attemptManifest, table.schema(), snapshot, assignerParallelism,
                    attempt.getName(), shardMetadata);
            if (!fileIO.rename(attemptManifest, manifest)) {
                fileIO.delete(attempt, true);
                return;
            }
        }
        catch (Exception e) {
            closeWriters(writers, e);
            fileIO.deleteDirectoryQuietly(attempt);
            throw e;
        }
    }

    private static List<ShardMetadata> writersFromClosed(List<ShardWriter> writers)
    {
        return writers.stream().map(ShardWriter::metadata).collect(Collectors.toUnmodifiableList());
    }

    private static void closeWriters(List<ShardWriter> writers)
            throws IOException
    {
        IOException failure = null;
        for (ShardWriter writer : writers) {
            try {
                writer.close();
            }
            catch (IOException e) {
                if (failure == null) {
                    failure = e;
                }
                else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static void closeWriters(List<ShardWriter> writers, Exception failure)
    {
        for (ShardWriter writer : writers) {
            try {
                writer.close();
            }
            catch (IOException e) {
                failure.addSuppressed(e);
            }
        }
    }

    private static void writeManifest(
            FileIO fileIO,
            Path path,
            TableSchema schema,
            @Nullable Long snapshot,
            int assignerParallelism,
            String attempt,
            List<ShardMetadata> shards)
            throws IOException
    {
        try (PositionOutputStream output = fileIO.newOutputStream(path, false);
                DataOutputStream data = new DataOutputStream(output)) {
            data.writeInt(MANIFEST_MAGIC);
            data.writeInt(MANIFEST_VERSION);
            data.writeBoolean(snapshot != null);
            if (snapshot != null) {
                data.writeLong(snapshot);
            }
            data.writeInt(assignerParallelism);
            data.writeUTF(schemaFingerprint(schema));
            data.writeUTF(attempt);
            data.writeInt(shards.size());
            for (ShardMetadata shard : shards) {
                data.writeLong(shard.records());
                data.writeLong(shard.length());
                data.writeUTF(shard.sha256());
            }
        }
    }

    private static Artifact readManifest(
            FileIO fileIO,
            Path manifest,
            Path root,
            FileStoreTable table,
            @Nullable Long expectedSnapshot,
            int expectedParallelism)
            throws IOException
    {
        ManifestData data;
        try (InputStream input = fileIO.newInputStream(manifest);
                DataInputStream stream = new DataInputStream(input)) {
            int magic = stream.readInt();
            int version = stream.readInt();
            if (magic != MANIFEST_MAGIC || version != MANIFEST_VERSION) {
                throw new IOException("Unsupported Paimon KEY_DYNAMIC bootstrap manifest: " + manifest);
            }
            boolean hasSnapshot = stream.readBoolean();
            Long snapshot = hasSnapshot ? stream.readLong() : null;
            int parallelism = stream.readInt();
            String schemaFingerprint = stream.readUTF();
            String attempt = stream.readUTF();
            int shardCount = stream.readInt();
            if (shardCount < 0 || shardCount > expectedParallelism) {
                throw new IOException("Invalid Paimon KEY_DYNAMIC bootstrap shard count: " + shardCount);
            }
            List<ShardMetadata> shards = new ArrayList<>(shardCount);
            for (int index = 0; index < shardCount; index++) {
                long records = stream.readLong();
                long length = stream.readLong();
                String checksum = stream.readUTF();
                if (records < 0 || length < 0 || !checksum.matches("[0-9a-f]{64}")) {
                    throw new IOException("Invalid Paimon KEY_DYNAMIC bootstrap shard metadata: " + manifest);
                }
                shards.add(new ShardMetadata(records, length, checksum));
            }
            if (attempt.isBlank() || attempt.contains("/") || attempt.contains("\\") || attempt.equals(".")
                    || attempt.equals("..")) {
                throw new IOException("Invalid Paimon KEY_DYNAMIC bootstrap attempt: " + attempt);
            }
            data = new ManifestData(snapshot, parallelism, schemaFingerprint, attempt, List.copyOf(shards));
        }

        if (!equalsNullable(expectedSnapshot, data.snapshot())
                || data.parallelism() != expectedParallelism
                || !schemaFingerprint(table.schema()).equals(data.schemaFingerprint())
                || data.shards().size() != expectedParallelism) {
            throw new IOException("Paimon KEY_DYNAMIC bootstrap manifest does not match the planned write: " + manifest);
        }
        return new Artifact(fileIO, root, data, IndexBootstrap.bootstrapType(table.schema()));
    }

    private static Long snapshotForBootstrap(FileStoreTable table, OptionalSnapshot expectedSnapshot)
    {
        Long currentSnapshot = latestSnapshotId(table);
        Long snapshot = expectedSnapshot.pinned()
                ? optionalSnapshotValue(expectedSnapshot.snapshotId())
                : currentSnapshot;
        verifySnapshot(currentSnapshot, snapshot, expectedSnapshot.pinned(), "before bootstrap");
        return snapshot;
    }

    private static Path artifactRoot(FileStoreTable table, String queryId, @Nullable Long snapshot, int parallelism)
    {
        // Query id and pinned snapshot provide the artifact identity. Keep the current schema out
        // of the path so a schema change cannot strand the old artifact before cleanup.
        String identity = table.location() + "\n" + queryId + "\n" + snapshot + "\n" + parallelism;
        return new Path(table.location(), ROOT_DIRECTORY + "/" + sha256(identity));
    }

    private static Long latestSnapshotId(FileStoreTable table)
    {
        return table.store().snapshotManager().latestSnapshotId();
    }

    private static void verifySnapshot(
            @Nullable Long actual,
            @Nullable Long expected,
            boolean pinned,
            String phase)
    {
        if (pinned && !equalsNullable(actual, expected)) {
            throw new IllegalStateException(
                    "Paimon KEY_DYNAMIC table snapshot changed " + phase + ": expected " + expected + ", actual " + actual);
        }
    }

    private static boolean equalsNullable(@Nullable Long left, @Nullable Long right)
    {
        return left == null ? right == null : left.equals(right);
    }

    @Nullable
    private static Long optionalSnapshotValue(java.util.OptionalLong snapshot)
    {
        return snapshot.isPresent() ? snapshot.getAsLong() : null;
    }

    private static String schemaFingerprint(TableSchema schema)
    {
        return sha256(schema.toString());
    }

    private static String sha256(String value)
    {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(UTF_8));
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte valueByte : digest) {
                result.append(String.format("%02x", valueByte));
            }
            return result.toString();
        }
        catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
    }

    record OptionalSnapshot(boolean pinned, OptionalLong snapshotId)
    {
        OptionalSnapshot
        {
            requireNonNull(snapshotId, "snapshotId is null");
            if (snapshotId.isPresent() && snapshotId.getAsLong() < 0) {
                throw new IllegalArgumentException("snapshotId must be non-negative");
            }
        }

        static OptionalSnapshot pinned(OptionalLong snapshotId)
        {
            return new OptionalSnapshot(true, snapshotId);
        }

        static OptionalSnapshot unpinned()
        {
            return new OptionalSnapshot(false, OptionalLong.empty());
        }
    }

    static final class Artifact
    {
        private final FileIO fileIO;
        private final Path root;
        private final ManifestData manifest;
        private final RowType bootstrapType;

        private Artifact(FileIO fileIO, Path root, ManifestData manifest, RowType bootstrapType)
        {
            this.fileIO = fileIO;
            this.root = root;
            this.manifest = manifest;
            this.bootstrapType = bootstrapType;
        }

        ShardReader openShard(int assigner)
                throws IOException
        {
            checkArgument(assigner >= 0 && assigner < manifest.shards().size(),
                    "assigner must be within bootstrap shard count: %s", assigner);
            ShardMetadata metadata = manifest.shards().get(assigner);
            Path path = new Path(new Path(root, manifest.attempt()), "part-" + assigner);
            if (fileIO.getFileStatus(path).getLen() != metadata.length()) {
                throw new IOException("Paimon KEY_DYNAMIC bootstrap shard length changed: " + path);
            }
            return new ShardReader(fileIO, path, metadata, bootstrapType);
        }

        List<Long> recordCounts()
        {
            return manifest.shards().stream().map(ShardMetadata::records).toList();
        }
    }

    static final class ShardReader
            implements AutoCloseable
    {
        private final DataInputViewStreamWrapper input;
        private final CountingInputStream countedInput;
        private final DigestInputStream digestInput;
        private final InternalRowSerializer serializer;
        private final ShardMetadata metadata;
        private long remaining;
        private boolean closed;

        private ShardReader(FileIO fileIO, Path path, ShardMetadata metadata, RowType rowType)
                throws IOException
        {
            this.digestInput = new DigestInputStream(fileIO.newInputStream(path), newDigest());
            this.countedInput = new CountingInputStream(digestInput);
            this.input = new DataInputViewStreamWrapper(countedInput);
            this.serializer = new InternalRowSerializer(rowType);
            this.metadata = metadata;
            this.remaining = metadata.records();
        }

        @Nullable
        InternalRow next()
                throws IOException
        {
            if (remaining == 0) {
                close();
                return null;
            }
            InternalRow row = serializer.deserialize(input);
            remaining--;
            return row;
        }

        @Override
        public void close()
                throws IOException
        {
            if (closed) {
                return;
            }
            closed = true;
            IOException failure = null;
            if (remaining == 0 && countedInput.count() != metadata.length()) {
                failure = new IOException("Paimon KEY_DYNAMIC bootstrap shard length does not match record data");
            }
            if (remaining == 0 && !metadata.sha256().equals(hex(digestInput.getMessageDigest().digest()))) {
                failure = new IOException("Paimon KEY_DYNAMIC bootstrap shard checksum mismatch");
            }
            try {
                input.close();
            }
            catch (IOException e) {
                if (failure == null) {
                    failure = e;
                }
                else {
                    failure.addSuppressed(e);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static final class ShardWriter
    {
        private final PositionOutputStream output;
        private final DataOutputViewStreamWrapper data;
        private final DigestOutputStream digestOutput;
        private long records;
        private ShardMetadata metadata;
        private boolean closed;

        private ShardWriter(FileIO fileIO, Path path, RowType rowType)
                throws IOException
        {
            this.output = fileIO.newOutputStream(path, false);
            this.digestOutput = new DigestOutputStream(output, newDigest());
            this.data = new DataOutputViewStreamWrapper(digestOutput);
            this.serializer = new InternalRowSerializer(rowType);
        }

        private final InternalRowSerializer serializer;

        private void write(InternalRow row)
                throws IOException
        {
            serializer.serialize(row, data);
            records++;
        }

        private void close()
                throws IOException
        {
            if (closed) {
                return;
            }
            closed = true;
            data.flush();
            long length = output.getPos();
            data.close();
            metadata = new ShardMetadata(records, length, hex(digestOutput.getMessageDigest().digest()));
        }

        private ShardMetadata metadata()
        {
            return requireNonNull(metadata, "shard writer is not closed");
        }
    }

    private record ShardMetadata(long records, long length, String sha256) {}

    private record ManifestData(
            @Nullable Long snapshot,
            int parallelism,
            String schemaFingerprint,
            String attempt,
            List<ShardMetadata> shards)
    {
    }

    private static MessageDigest newDigest()
    {
        try {
            return MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
    }

    private static String hex(byte[] bytes)
    {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(String.format("%02x", value));
        }
        return result.toString();
    }

    private static final class CountingInputStream
            extends InputStream
    {
        private final InputStream delegate;
        private long count;

        private CountingInputStream(InputStream delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public int read()
                throws IOException
        {
            int value = delegate.read();
            if (value >= 0) {
                count++;
            }
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length)
                throws IOException
        {
            int read = delegate.read(bytes, offset, length);
            if (read > 0) {
                count += read;
            }
            return read;
        }

        @Override
        public void close()
                throws IOException
        {
            delegate.close();
        }

        private long count()
        {
            return count;
        }
    }

    private static final class SnapshotPinnedBootstrap
    {
        private SnapshotPinnedBootstrap() {}

        private static RecordReader<InternalRow> bootstrap(FileStoreTable table, @Nullable Long snapshot)
                throws IOException
        {
            RowType rowType = table.rowType();
            List<String> fieldNames = rowType.getFieldNames();
            int[] keyProjection = table.schema().trimmedPrimaryKeys().stream()
                    .map(fieldNames::indexOf)
                    .mapToInt(Integer::intValue)
                    .toArray();

            Map<String, String> scanOptions = new HashMap<>();
            if (snapshot == null) {
                scanOptions.put(SCAN_MODE.key(), LATEST.toString());
            }
            else {
                scanOptions.put(SCAN_MODE.key(), FROM_SNAPSHOT.toString());
                scanOptions.put(SCAN_SNAPSHOT_ID.key(), snapshot.toString());
            }
            FileStoreTable scanTable = table.copy(scanOptions);
            ReadBuilder readBuilder = scanTable.newReadBuilder().withProjection(keyProjection);
            DataTableScan tableScan = (DataTableScan) readBuilder.newScan();
            List<Split> splits = tableScan
                    .withLevelFilter(level -> true)
                    .plan()
                    .splits();

            CoreOptions options = CoreOptions.fromMap(scanTable.options());
            Duration indexTtl = options.crossPartitionUpsertIndexTtl();
            if (indexTtl != null) {
                long ttlMillis = indexTtl.toMillis();
                long currentTime = System.currentTimeMillis();
                splits = splits.stream()
                        .filter(split -> filterSplit(split, ttlMillis, currentTime))
                        .collect(Collectors.toList());
            }

            RowDataToObjectArrayConverter partBucketConverter = new RowDataToObjectArrayConverter(
                    TypeUtils.concat(TypeUtils.project(rowType, table.partitionKeys()),
                            RowType.of(DataTypes.INT())));
            return parallelExecute(
                    TypeUtils.project(rowType, keyProjection),
                    split -> readBuilder.newRead().createReader(split),
                    splits,
                    options.pageSize(),
                    options.crossPartitionUpsertBootstrapParallelism(),
                    split -> {
                        DataSplit dataSplit = (DataSplit) split;
                        return partBucketConverter.toGenericRow(
                                new org.apache.paimon.data.JoinedRow(dataSplit.partition(),
                                        org.apache.paimon.data.GenericRow.of(dataSplit.bucket())));
                    },
                    (row, extra) -> new org.apache.paimon.data.JoinedRow().replace(row, extra));
        }

        private static boolean filterSplit(Split split, long indexTtl, long currentTime)
        {
            for (org.apache.paimon.io.DataFileMeta file : ((DataSplit) split).dataFiles()) {
                if (currentTime <= file.creationTimeEpochMillis() + indexTtl) {
                    return true;
                }
            }
            return false;
        }
    }
}
