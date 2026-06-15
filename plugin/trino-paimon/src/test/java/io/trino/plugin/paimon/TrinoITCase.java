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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.InnerTableWrite;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Files;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.time.ZoneOffset.UTC;
import static org.apache.paimon.data.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@Execution(ExecutionMode.SAME_THREAD)  // Disable concurrent execution to avoid table name conflicts
public class TrinoITCase
        extends
        AbstractTestQueryFramework
{
    private static final String CATALOG = "paimon";
    private static final String DB = "default";

    protected long t2FirstCommitTimestamp;

    // Cleanup method to ensure test isolation
    @AfterEach
    public void cleanupTestTables()
    {
        try {
            // Drop common test tables that may have been created
            sql("DROP TABLE IF EXISTS paimon.default.t5");
            sql("DROP TABLE IF EXISTS paimon.default.t6");
            sql("DROP TABLE IF EXISTS paimon.default.json_values");
            sql("DROP TABLE IF EXISTS paimon.default.vector_directive_values");
            sql("DROP TABLE IF EXISTS paimon.default.vector_directive_add_column");
            sql("DROP TABLE IF EXISTS paimon.default.blob_directive_values");
            sql("DROP TABLE IF EXISTS paimon.default.blob_directive_add_column");
            sql("DROP TABLE IF EXISTS paimon.default.comment_directive_values");
            sql("DROP TABLE IF EXISTS paimon.default.orders");
            sql("DROP TABLE IF EXISTS paimon.default.comment_values");
            sql("DROP TABLE IF EXISTS paimon.default.replace_values");
            sql("DROP TABLE IF EXISTS paimon.default.truncate_values");
            sql("DROP TABLE IF EXISTS paimon.default.hash_fixed_mutations");
            sql("DROP TABLE IF EXISTS paimon.default.drop_nn_values");
            sql("DROP TABLE IF EXISTS paimon.default.nested_field_values");
            sql("DROP TABLE IF EXISTS paimon.default.not_null_values");
            sql("DROP TABLE IF EXISTS paimon.default.time_orc_values");
            sql("DROP TABLE IF EXISTS paimon.default.time_travel_schema_evolution");
            sql("DROP TABLE IF EXISTS paimon.default.row_tracking_values");
            // Drop test schemas that may have been created
            sql("DROP SCHEMA IF EXISTS paimon.test CASCADE");
            sql("DROP SCHEMA IF EXISTS paimon.tpch CASCADE");
        }
        catch (Exception e) {
            // Ignore cleanup errors - table may not exist
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String warehouse = Files.createTempDirectory(UUID.randomUUID().toString()).toUri().toString();
        // flink sink
        Path tablePath1 = new Path(warehouse, DB + ".db/t1");
        SimpleTableTestHelper testHelper1 = createTestHelper(tablePath1);
        testHelper1.write(GenericRow.of(1, 2L, fromString("1"), fromString("1")));
        testHelper1.write(GenericRow.of(3, 4L, fromString("2"), fromString("2")));
        testHelper1.write(GenericRow.of(5, 6L, fromString("3"), fromString("3")));
        testHelper1.write(GenericRow.ofKind(RowKind.DELETE, 3, 4L, fromString("2"), fromString("2")));
        testHelper1.commit();

        Path tablePath2 = new Path(warehouse, "default.db/t2");
        SimpleTableTestHelper testHelper2 = createTestHelper(tablePath2);
        testHelper2.write(GenericRow.of(1, 2L, fromString("1"), fromString("1")));
        testHelper2.write(GenericRow.of(3, 4L, fromString("2"), fromString("2")));
        testHelper2.commit();
        testHelper2.createTag("1");
        t2FirstCommitTimestamp = System.currentTimeMillis();
        testHelper2.write(GenericRow.of(5, 6L, fromString("3"), fromString("3")));
        testHelper2.write(GenericRow.of(7, 8L, fromString("4"), fromString("4")));
        testHelper2.commit();
        testHelper2.createTag("tag-2");

        Path versionPrecedenceTablePath = new Path(warehouse, "default.db/t_version_precedence");
        SimpleTableTestHelper versionPrecedenceHelper = createTestHelper(versionPrecedenceTablePath);
        versionPrecedenceHelper.write(GenericRow.of(1, 2L, fromString("1"), fromString("1")));
        versionPrecedenceHelper.write(GenericRow.of(3, 4L, fromString("2"), fromString("2")));
        versionPrecedenceHelper.commit();
        versionPrecedenceHelper.write(GenericRow.of(5, 6L, fromString("3"), fromString("3")));
        versionPrecedenceHelper.write(GenericRow.of(7, 8L, fromString("4"), fromString("4")));
        versionPrecedenceHelper.commit();
        versionPrecedenceHelper.createTag("2", 1L);

        createSystemChangelogTable(new Path(warehouse, "default.db/system_changelog_values"));

        {
            Path tablePath3 = new Path(warehouse, "default.db/t3");
            RowType rowType = new RowType(Arrays.asList(new DataField(0, "pt", DataTypes.STRING()),
                    new DataField(1, "a", new IntType()), new DataField(2, "b", new BigIntType()),
                    new DataField(3, "c", new BigIntType()), new DataField(4, "d", new IntType())));
            new SchemaManager(LocalFileIO.create(), tablePath3).createTable(new Schema(rowType.getFields(),
                    Collections.singletonList("pt"), Collections.emptyList(), new HashMap<>(), ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath3);
            InnerTableWrite writer = table.newWrite("user");
            InnerTableCommit commit = table.newCommit("user");
            writer.write(GenericRow.of(fromString("1"), 1, 1L, 1L, 1));
            writer.write(GenericRow.of(fromString("1"), 1, 2L, 2L, 2));
            writer.write(GenericRow.of(fromString("2"), 3, 3L, 3L, 3));
            commit.commit(0, writer.prepareCommit(true, 0));
        }

        {
            Path tablePath = new Path(warehouse, "default.db/empty_t");
            RowType rowType = new RowType(
                    Arrays.asList(new DataField(1, "a", new IntType()), new DataField(2, "b", new BigIntType())));
            new SchemaManager(LocalFileIO.create(), tablePath).createTable(new Schema(rowType.getFields(),
                    Collections.emptyList(), Collections.emptyList(), new HashMap<>(), ""));
        }

        {
            Path tablePath4 = new Path(warehouse, "default.db/t4");
            List<DataField> innerRowFields = new ArrayList<>();
            innerRowFields.add(new DataField(4, "innercol1", new IntType()));
            innerRowFields.add(new DataField(5, "innercol2", new VarCharType(VarCharType.MAX_LENGTH)));
            RowType rowType = new RowType(Arrays.asList(new DataField(0, "i", new IntType()),
                    new DataField(1, "map",
                            new MapType(new VarCharType(VarCharType.MAX_LENGTH),
                                    new VarCharType(VarCharType.MAX_LENGTH))),
                    new DataField(2, "innerrow", new RowType(true, innerRowFields)),
                    new DataField(3, "array", new ArrayType(new IntType()))));
            new SchemaManager(LocalFileIO.create(), tablePath4)
                    .createTable(new Schema(rowType.getFields(), Collections.emptyList(),
                            Collections.singletonList("i"), Collections.singletonMap("bucket", "1"), ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath4);
            InnerTableWrite writer = table.newWrite("user");
            InnerTableCommit commit = table.newCommit("user");
            Map<Object, Object> map = new HashMap<>();
            map.put(fromString("1"), fromString("2"));
            writer.write(GenericRow.of(1, new GenericMap(map), GenericRow.of(2, fromString("male")),
                    new GenericArray(new int[]{1, 2, 3})));
            commit.commit(0, writer.prepareCommit(true, 0));
        }

        {
            Path tablePath6 = new Path(warehouse, "default.db/t99");
            RowType rowType = new RowType(Arrays.asList(new DataField(0, "boolean", DataTypes.BOOLEAN()),
                    new DataField(1, "tinyint", DataTypes.TINYINT()),
                    new DataField(2, "smallint", DataTypes.SMALLINT()), new DataField(3, "int", DataTypes.INT()),
                    new DataField(4, "bigint", DataTypes.BIGINT()), new DataField(5, "float", DataTypes.FLOAT()),
                    new DataField(6, "double", DataTypes.DOUBLE()), new DataField(7, "char", DataTypes.CHAR(5)),
                    new DataField(8, "varchar", DataTypes.VARCHAR(100)), new DataField(9, "date", DataTypes.DATE()),
                    new DataField(10, "timestamp_0", DataTypes.TIMESTAMP(0)),
                    new DataField(11, "timestamp_3", DataTypes.TIMESTAMP(3)),
                    new DataField(12, "timestamp_6", DataTypes.TIMESTAMP(6)),
                    new DataField(13, "timestamp_tz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)),
                    new DataField(14, "decimal", DataTypes.DECIMAL(10, 5)),
                    new DataField(15, "varbinary", DataTypes.VARBINARY(10)),
                    new DataField(16, "array", DataTypes.ARRAY(DataTypes.INT())),
                    new DataField(17, "map", DataTypes.MAP(DataTypes.INT(), DataTypes.INT())),
                    new DataField(18, "row", DataTypes.ROW(DataTypes.FIELD(100, "q1", DataTypes.INT()),
                            DataTypes.FIELD(101, "q2", DataTypes.INT())))));
            new SchemaManager(LocalFileIO.create(), tablePath6).createTable(new Schema(rowType.getFields(),
                    List.of("boolean", "tinyint", "smallint", "int", "bigint", "float", "double", "char", "varchar",
                            "date", "timestamp_0", "timestamp_3", "timestamp_6", "timestamp_tz", "decimal"),
                    List.of("boolean", "tinyint", "smallint", "int", "bigint", "float", "double", "char", "varchar",
                            "date", "timestamp_0", "timestamp_3", "timestamp_6", "timestamp_tz", "decimal",
                            "varbinary"),
                    Collections.singletonMap("bucket", "1"), ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath6);
            InnerTableWrite writer = table.newWrite("user");
            InnerTableCommit commit = table.newCommit("user");
            writer.write(GenericRow.of(true, (byte) 1, (short) 1, 1, 1L, 1.0f, 1.0d, BinaryString.fromString("char1"),
                    BinaryString.fromString("varchar1"), 0, Timestamp.fromMicros(1694505288000000L),
                    Timestamp.fromMicros(1694505288001000L), Timestamp.fromMicros(1694505288001001L),
                    Timestamp.fromMicros(1694505288002001L), Decimal.fromUnscaledLong(10000, 10, 5),
                    new byte[]{0x01, 0x02, 0x03}, new GenericArray(new int[]{1, 1, 1}), new GenericMap(Map.of(1, 1)),
                    GenericRow.of(1, 1)));
            commit.commit(0, writer.prepareCommit(true, 0));
        }

        {
            Path tablePath7 = new Path(warehouse, "default.db/t100");
            RowType rowType = new RowType(Arrays.asList(new DataField(0, "boolean", DataTypes.BOOLEAN()),
                    new DataField(1, "tinyint", DataTypes.TINYINT()),
                    new DataField(2, "smallint", DataTypes.SMALLINT()), new DataField(3, "int", DataTypes.INT()),
                    new DataField(4, "bigint", DataTypes.BIGINT()), new DataField(5, "float", DataTypes.FLOAT()),
                    new DataField(6, "double", DataTypes.DOUBLE()), new DataField(7, "char", DataTypes.CHAR(5)),
                    new DataField(8, "varchar", DataTypes.VARCHAR(100)), new DataField(9, "date", DataTypes.DATE()),
                    new DataField(10, "timestamp_0", DataTypes.TIMESTAMP(3)),
                    new DataField(11, "timestamp_3", DataTypes.TIMESTAMP(3)),
                    new DataField(12, "timestamp_6", DataTypes.TIMESTAMP(6)),
                    new DataField(13, "decimal", DataTypes.DECIMAL(10, 5)),
                    new DataField(14, "varbinary", DataTypes.VARBINARY(10)),
                    new DataField(15, "array", DataTypes.ARRAY(DataTypes.INT())),
                    new DataField(16, "map", DataTypes.MAP(DataTypes.INT(), DataTypes.INT())),
                    new DataField(17, "row", DataTypes.ROW(DataTypes.FIELD(100, "q1", DataTypes.INT()),
                            DataTypes.FIELD(101, "q2", DataTypes.INT())))));
            new SchemaManager(LocalFileIO.create(), tablePath7).createTable(new Schema(rowType.getFields(),
                    Collections.emptyList(), Collections.emptyList(), Collections.singletonMap("bucket", "-1"), ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath7);
            InnerTableWrite writer = table.newWrite("user");
            InnerTableCommit commit = table.newCommit("user");
            writer.write(GenericRow.of(true, (byte) 1, (short) 1, 1, 1L, 1.0f, 1.0d, BinaryString.fromString("char1"),
                    BinaryString.fromString("varchar1"), 0, Timestamp.fromMicros(1694505288000000L),
                    Timestamp.fromMicros(1694505288001000L), Timestamp.fromMicros(1694505288001001L),
                    Decimal.fromUnscaledLong(10000, 10, 5), new byte[]{0x01, 0x02, 0x03},
                    new GenericArray(new int[]{1, 1, 1}), new GenericMap(Map.of(1, 1)), GenericRow.of(1, 1)));
            commit.commit(0, writer.prepareCommit(true, 0));

            new SchemaManager(LocalFileIO.create(), tablePath7).commitChanges(SchemaChange.dropColumn("smallint"));
            table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath7);
            writer = table.newWrite("user");
            commit = table.newCommit("user");
            writer.write(GenericRow.of(true, (byte) 1, 1, 1L, 1.0f, 1.0d, BinaryString.fromString("char1"),
                    BinaryString.fromString("varchar1"), 0, Timestamp.fromMicros(1694505288000000L),
                    Timestamp.fromMicros(1694505288001000L), Timestamp.fromMicros(1694505288001001L),
                    Decimal.fromUnscaledLong(10000, 10, 5), new byte[]{0x01, 0x02, 0x03},
                    new GenericArray(new int[]{1, 1, 1}), new GenericMap(Map.of(1, 1)), GenericRow.of(1, 1)));
            commit.commit(1, writer.prepareCommit(true, 1));

            new SchemaManager(LocalFileIO.create(), tablePath7)
                    .commitChanges(SchemaChange.addColumn("smallint", DataTypes.SMALLINT()));
            table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath7);
            writer = table.newWrite("user");
            commit = table.newCommit("user");
            writer.write(GenericRow.of(true, (byte) 1, 1, 1L, 1.0f, 1.0d, BinaryString.fromString("char1"),
                    BinaryString.fromString("varchar1"), 0, Timestamp.fromMicros(1694505288000000L),
                    Timestamp.fromMicros(1694505288001000L), Timestamp.fromMicros(1694505288001001L),
                    Decimal.fromUnscaledLong(10000, 10, 5), new byte[]{0x01, 0x02, 0x03},
                    new GenericArray(new int[]{1, 1, 1}), new GenericMap(Map.of(1, 1)), GenericRow.of(1, 1),
                    (short) 1));
            commit.commit(1, writer.prepareCommit(true, 1));
        }

        {
            Path tablePath6 = new Path(warehouse, "default.db/t101");
            RowType rowType = new RowType(Arrays.asList(new DataField(0, "a", DataTypes.STRING()),
                    new DataField(1, "b", DataTypes.INT()), new DataField(2, "c", DataTypes.INT())));
            new SchemaManager(LocalFileIO.create(), tablePath6).createTable(
                    new Schema(rowType.getFields(), Collections.emptyList(), List.of("a"), new HashMap<>() {
                        {
                            put(CoreOptions.BUCKET.key(), "1");
                            put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
                        }
                    }, ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath6);
            InnerTableWrite writer = table.newWrite("user");
            writer.withIOManager(new IOManagerImpl("/tmp"));
            InnerTableCommit commit = table.newCommit("user");
            for (int i = 0; i < 10; i++) {
                writer.write(GenericRow.of(BinaryString.fromString("a" + i), i, i));
            }
            commit.commit(0, writer.prepareCommit(true, 0));

            writer.write(GenericRow.ofKind(RowKind.DELETE, BinaryString.fromString("a0"), 0, 0));
            commit.commit(1, writer.prepareCommit(true, 1));
        }

        {
            Path tablePath = new Path(warehouse, "default.db/t102");
            RowType rowType = new RowType(Arrays.asList(new DataField(0, "a", DataTypes.STRING()),
                    new DataField(1, "b", DataTypes.INT()), new DataField(2, "c", DataTypes.INT())));
            new SchemaManager(LocalFileIO.create(), tablePath).createTable(
                    new Schema(rowType.getFields(), Collections.emptyList(), Collections.emptyList(), new HashMap<>() {
                        {
                            put("file-index.bloom-filter.columns", "a,b,c");
                        }
                    }, ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
            InnerTableWrite writer = table.newWrite("user");
            writer.withIOManager(new IOManagerImpl("/tmp"));
            InnerTableCommit commit = table.newCommit("user");
            for (int i = 0; i < 100; i = i + 3) {
                writer.write(GenericRow.of(BinaryString.fromString("a" + i), i, i));
            }
            commit.commit(0, writer.prepareCommit(true, 0));

            for (int i = 1; i < 100; i = i + 3) {
                writer.write(GenericRow.of(BinaryString.fromString("a" + i), i, i));
            }
            commit.commit(1, writer.prepareCommit(true, 1));

            for (int i = 2; i < 100; i = i + 3) {
                writer.write(GenericRow.of(BinaryString.fromString("a" + i), i, i));
            }
            commit.commit(2, writer.prepareCommit(true, 2));
        }

        {
            Path tablePath = new Path(warehouse, "default.db/fixed_bucket_table_wi_pk");
            RowType rowType = new RowType(Arrays.asList(new DataField(0, "id", DataTypes.INT()),
                    new DataField(1, "name", DataTypes.STRING())));
            new SchemaManager(LocalFileIO.create(), tablePath).createTable(
                    new Schema(rowType.getFields(), Collections.emptyList(), Collections.emptyList(), new HashMap<>() {
                        {
                            put("file.format", "orc");
                            put("primary-key", "id");
                            put("bucket", "2");
                        }
                    }, ""));
        }

        {
            Path tablePath = new Path(warehouse, "default.db/fixed_bucket_table_wo_pk");
            RowType rowType = new RowType(Arrays.asList(new DataField(0, "id", DataTypes.INT()),
                    new DataField(1, "name", DataTypes.STRING())));
            new SchemaManager(LocalFileIO.create(), tablePath).createTable(
                    new Schema(rowType.getFields(), Collections.emptyList(), Collections.emptyList(), new HashMap<>() {
                        {
                            put("file.format", "orc");
                            put("bucket", "2");
                            put("bucket-key", "id");
                        }
                    }, ""));
        }

        {
            Path tablePath = new Path(warehouse, "default.db/unaware_table");
            RowType rowType = new RowType(Arrays.asList(new DataField(0, "id", DataTypes.INT()),
                    new DataField(1, "name", DataTypes.STRING())));
            new SchemaManager(LocalFileIO.create(), tablePath).createTable(
                    new Schema(rowType.getFields(), Collections.emptyList(), Collections.emptyList(), new HashMap<>() {
                        {
                            put("file.format", "orc");
                        }
                    }, ""));
        }

        {
            Path tablePath = new Path(warehouse, "default.db/vector_values");
            RowType rowType = new RowType(Arrays.asList(new DataField(0, "id", DataTypes.INT()),
                    new DataField(1, "embedding", DataTypes.VECTOR(3, DataTypes.FLOAT()))));
            new SchemaManager(LocalFileIO.create(), tablePath).createTable(new Schema(rowType.getFields(),
                    Collections.emptyList(), Collections.emptyList(), new HashMap<>() {
                        {
                            put(CoreOptions.FILE_FORMAT.key(), "json");
                            put(CoreOptions.FILE_COMPRESSION.key(), "none");
                        }
                    }, ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
            InnerTableWrite writer = table.newWrite("user");
            InnerTableCommit commit = table.newCommit("user");
            writer.write(GenericRow.of(1, BinaryVector.fromPrimitiveArray(new float[] {1.0f, 2.5f, 3.75f})));
            commit.commit(0, writer.prepareCommit(true, 0));
        }

        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(testSessionBuilder().setCatalog(CATALOG).setSchema(DB).build())
                    .build();
            queryRunner.installPlugin(new PaimonPlugin());
            Map<String, String> options = new HashMap<>();
            options.put("warehouse", warehouse);
            queryRunner.createCatalog(CATALOG, CATALOG, options);
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static SimpleTableTestHelper createTestHelper(Path tablePath)
            throws Exception
    {
        RowType rowType = new RowType(
                Arrays.asList(new DataField(0, "a", new IntType()), new DataField(1, "b", new BigIntType()),
                        // test field name has upper case
                        new DataField(2, "aCa", new VarCharType()), new DataField(3, "d", new CharType(1))));
        return new SimpleTableTestHelper(tablePath, rowType);
    }

    private static void createSystemChangelogTable(Path tablePath)
            throws Exception
    {
        Schema schema = Schema.newBuilder()
                .column("pk", DataTypes.INT())
                .column("pt", DataTypes.INT())
                .column("col1", DataTypes.INT())
                .partitionKeys("pt")
                .primaryKey("pk", "pt")
                .option(CoreOptions.CHANGELOG_PRODUCER.key(), "input")
                .option(CoreOptions.TABLE_READ_SEQUENCE_NUMBER_ENABLED.key(), "true")
                .option("bucket", "1")
                .build();
        new SchemaManager(LocalFileIO.create(), tablePath).createTable(schema);

        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
        InnerTableWrite writer = table.newWrite("user");
        InnerTableCommit commit = table.newCommit("user");
        writer.write(GenericRow.ofKind(RowKind.INSERT, 1, 1, 1));
        writer.write(GenericRow.ofKind(RowKind.DELETE, 1, 1, 1));
        writer.write(GenericRow.ofKind(RowKind.INSERT, 1, 2, 5));
        writer.write(GenericRow.ofKind(RowKind.UPDATE_BEFORE, 1, 2, 5));
        writer.write(GenericRow.ofKind(RowKind.UPDATE_AFTER, 1, 2, 6));
        writer.write(GenericRow.ofKind(RowKind.INSERT, 2, 3, 1));
        commit.commit(0, writer.prepareCommit(true, 0));
    }

    @Test
    public void testComplexTypes()
    {
        assertThat(sql("SELECT * FROM paimon.default.t4")).isEqualTo("[[1, {1=2}, [2, male], [1, 2, 3]]]");
    }

    @Test
    public void testEmptyTable()
    {
        assertThat(sql("SELECT * FROM paimon.default.empty_t")).isEqualTo("[]");
    }

    @Test
    public void testProjection()
    {
        assertThat(sql("SELECT * FROM paimon.default.t1")).isEqualTo("[[1, 2, 1, 1], [5, 6, 3, 3]]");
        assertThat(sql("SELECT a, aCa FROM paimon.default.t1")).isEqualTo("[[1, 1], [5, 3]]");
        assertThat(sql("SELECT SUM(b) FROM paimon.default.t1")).isEqualTo("[[8]]");
    }

    @Test
    public void testLimit()
    {
        assertThat(sql("SELECT * FROM paimon.default.t1 LIMIT 1")).isEqualTo("[[1, 2, 1, 1]]");
        assertThat(sql("SELECT * FROM paimon.default.t1 WHERE a = 5 LIMIT 1")).isEqualTo("[[5, 6, 3, 3]]");
    }

    @Test
    public void testSystemTable()
    {
        assertThat(sql("SELECT snapshot_id,schema_id,commit_user,commit_identifier,commit_kind FROM \"t1$snapshots\""))
                .isEqualTo("[[1, 0, user, 0, APPEND]]");
    }

    @Test
    public void testAuditLogSystemTable()
    {
        assertThat(sql("SHOW COLUMNS FROM paimon.default.\"system_changelog_values$audit_log\""))
                .isEqualTo("[[rowkind, varchar, , ], [_sequence_number, bigint, , ], [pk, integer, , ], [pt, integer, , ], [col1, integer, , ]]");
        assertThat(sql("SELECT rowkind, _sequence_number, pk, pt, col1 "
                + "FROM paimon.default.\"system_changelog_values$audit_log\" "
                + "ORDER BY _sequence_number"))
                .isEqualTo("[[+I, 0, 2, 3, 1], [-D, 1, 1, 1, 1], [+U, 2, 1, 2, 6]]");
    }

    @Test
    public void testBinlogSystemTable()
    {
        assertThat(sql("SHOW COLUMNS FROM paimon.default.\"system_changelog_values$binlog\""))
                .isEqualTo("[[rowkind, varchar, , ], [_sequence_number, bigint, , ], [pk, array(integer), , ], [pt, array(integer), , ], [col1, array(integer), , ]]");
        assertThat(sql("SELECT rowkind, _sequence_number, pk, pt, col1 "
                + "FROM paimon.default.\"system_changelog_values$binlog\" "
                + "ORDER BY _sequence_number"))
                .isEqualTo("[[+I, 0, [2], [3], [1]], [-D, 1, [1], [1], [1]], [+U, 2, [1], [2], [6]]]");
    }

    @Test
    public void testRowTrackingSystemTable()
    {
        sql("CREATE TABLE paimon.default.row_tracking_values ("
                + "id integer, "
                + "name varchar) "
                + "WITH (row_tracking_enabled = 'true')");
        sql("INSERT INTO paimon.default.row_tracking_values VALUES (11, 'alpha'), (22, 'beta')");

        assertThat(sql("SHOW COLUMNS FROM paimon.default.\"row_tracking_values$row_tracking\""))
                .isEqualTo("[[id, integer, , ], [name, varchar, , ], [_row_id, bigint, , ], [_sequence_number, bigint, , ]]");
        assertThat(sql("SELECT id, name, _row_id, _sequence_number "
                + "FROM paimon.default.\"row_tracking_values$row_tracking\" "
                + "ORDER BY id"))
                .isEqualTo("[[11, alpha, 0, 1], [22, beta, 1, 1]]");
    }

    @Test
    public void testRowTrackingHiddenColumnsOnBaseTable()
    {
        sql("CREATE TABLE paimon.default.row_tracking_values ("
                + "id integer, "
                + "name varchar) "
                + "WITH (row_tracking_enabled = 'true')");
        sql("INSERT INTO paimon.default.row_tracking_values VALUES (11, 'alpha'), (22, 'beta')");

        assertThat(sql("SHOW COLUMNS FROM paimon.default.row_tracking_values"))
                .isEqualTo("[[id, integer, , ], [name, varchar, , ]]");
        assertThat(sql("SELECT id, name, _row_id, _sequence_number "
                + "FROM paimon.default.row_tracking_values "
                + "ORDER BY id"))
                .isEqualTo("[[11, alpha, 0, 1], [22, beta, 1, 1]]");
        assertThat(sql("SELECT id, _row_id "
                + "FROM paimon.default.row_tracking_values "
                + "WHERE _row_id = BIGINT '0'"))
                .isEqualTo("[[11, 0]]");
        assertThat(sql("SELECT * FROM paimon.default.row_tracking_values ORDER BY id"))
                .isEqualTo("[[11, alpha], [22, beta]]");
    }

    @Test
    public void testFilter()
    {
        assertThat(sql("SELECT a, aCa FROM paimon.default.t2 WHERE a < 4")).isEqualTo("[[1, 1], [3, 2]]");
    }

    @Test
    public void testGroupByWithCast()
    {
        assertThat(sql("SELECT pt, a, SUM(b), SUM(d) FROM paimon.default.t3 GROUP BY pt, a ORDER BY pt, a"))
                .isEqualTo("[[1, 1, 3, 3], [2, 3, 3, 3]]");
    }

    @Test
    public void testLimitWithPartition()
    {
        assertThat(sql("SELECT * FROM paimon.default.t3 WHERE pt = '1' LIMIT 1")).isEqualTo("[[1, 1, 1, 1, 1]]");

        assertThat(sql("SELECT * FROM paimon.default.t3 WHERE pt = '1' AND b = 2 LIMIT 1"))
                .isEqualTo("[[1, 1, 2, 2, 2]]");
    }

    @Test
    public void testShowCreateTable()
    {
        assertThat(sql("SHOW CREATE TABLE paimon.default.t3"))
                .isEqualTo("[[CREATE TABLE paimon.default.t3 (\n" + "   pt varchar,\n" + "   a integer,\n"
                        + "   b bigint,\n" + "   c bigint,\n" + "   d integer\n" + ")]]");
    }

    @Test
    public void testCreateSchema()
    {
        sql("CREATE SCHEMA paimon.test");
        assertThat(sql("SHOW SCHEMAS FROM paimon")).isEqualTo("[[default], [information_schema], [sys], [test]]");
        sql("DROP SCHEMA paimon.test");
    }

    @Test
    public void testDropSchema()
    {
        sql("CREATE SCHEMA paimon.tpch");
        sql("DROP SCHEMA paimon.tpch");
        assertThat(sql("SHOW SCHEMAS FROM paimon")).isEqualTo("[[default], [information_schema], [sys]]");
    }

    @Test
    public void testGlobalSystemTables()
    {
        assertThat(sql("SHOW TABLES FROM paimon.sys"))
                .isEqualTo("[[all_table_options], [catalog_options], [partitions], [tables]]");
        assertThat(sql("SHOW COLUMNS FROM paimon.sys.catalog_options"))
                .isEqualTo("[[key, varchar, , ], [value, varchar, , ]]");
    }

    @Test
    public void testCreateTable()
    {
        sql("CREATE TABLE orders (" + "  order_key bigint," + "  order_status varchar," + "  total_price double,"
                + "  order_date date" + ")" + "WITH (" + "file_format = 'ORC',"
                + "primary_key = ARRAY['order_key','order_date']," + "partitioned_by = ARRAY['order_date'],"
                + "bucket = '2'," + "bucket_key = 'order_key'," + "changelog_producer = 'input'" + ")");
        assertThat(sql("SHOW TABLES FROM paimon.default")).contains("orders");
        sql("DROP TABLE IF EXISTS paimon.default.orders");
    }

    @Test
    public void testRenameTable()
    {
        sql("CREATE TABLE t5 (" + "  order_key bigint," + "  order_status varchar," + "  total_price double,"
                + "  order_date date" + ")" + "WITH (" + "file_format = 'ORC',"
                + "primary_key = ARRAY['order_key','order_date']," + "partitioned_by = ARRAY['order_date'],"
                + "bucket = '2'," + "bucket_key = 'order_key'," + "changelog_producer = 'input'" + ")");
        sql("ALTER TABLE paimon.default.t5 RENAME TO t6");
        String result = sql("SHOW TABLES FROM paimon.default");
        assertThat(result).doesNotContain("t5").contains("t6");
        sql("DROP TABLE IF EXISTS paimon.default.t6");
    }

    @Test
    public void testDropTable()
    {
        sql("CREATE TABLE t5 (" + "  order_key bigint," + "  order_status varchar," + "  total_price double,"
                + "  order_date date" + ")" + "WITH (" + "file_format = 'ORC',"
                + "primary_key = ARRAY['order_key','order_date']," + "partitioned_by = ARRAY['order_date'],"
                + "bucket = '2'," + "bucket_key = 'order_key'," + "changelog_producer = 'input'" + ")");
        sql("DROP TABLE IF EXISTS paimon.default.t5");
        assertThat(sql("SHOW TABLES FROM paimon.default")).doesNotContain("t5");
    }

    @Test
    public void testTruncateTable()
    {
        sql("CREATE TABLE paimon.default.truncate_values (id integer, name varchar) WITH (bucket = '-1')");
        sql("INSERT INTO paimon.default.truncate_values VALUES (1, 'one'), (2, 'two')");

        assertThat(sql("SELECT count(*) FROM paimon.default.truncate_values")).isEqualTo("[[2]]");

        sql("TRUNCATE TABLE paimon.default.truncate_values");

        assertThat(sql("SELECT count(*) FROM paimon.default.truncate_values")).isEqualTo("[[0]]");
    }

    @Test
    public void testTableAndColumnComments()
    {
        sql("CREATE TABLE paimon.default.comment_values ("
                + "id integer COMMENT 'identifier', "
                + "name varchar) "
                + "COMMENT 'table comment' "
                + "WITH (bucket = '-1')");

        assertThat(sql("SELECT comment FROM system.metadata.table_comments "
                + "WHERE catalog_name = 'paimon' AND schema_name = 'default' AND table_name = 'comment_values'"))
                .isEqualTo("[[table comment]]");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.comment_values"))
                .isEqualTo("[[id, integer, , identifier], [name, varchar, , ]]");

        sql("COMMENT ON TABLE paimon.default.comment_values IS 'updated table comment'");
        assertThat(sql("SELECT comment FROM system.metadata.table_comments "
                + "WHERE catalog_name = 'paimon' AND schema_name = 'default' AND table_name = 'comment_values'"))
                .isEqualTo("[[updated table comment]]");
        assertThat(sql("SHOW CREATE TABLE paimon.default.comment_values"))
                .contains("COMMENT 'updated table comment'");

        sql("COMMENT ON COLUMN paimon.default.comment_values.name IS 'display name'");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.comment_values"))
                .isEqualTo("[[id, integer, , identifier], [name, varchar, , display name]]");

        sql("ALTER TABLE paimon.default.comment_values ADD COLUMN detail varchar COMMENT 'detail column'");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.comment_values"))
                .isEqualTo("[[id, integer, , identifier], [name, varchar, , display name], [detail, varchar, , detail column]]");

        sql("COMMENT ON COLUMN paimon.default.comment_values.name IS NULL");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.comment_values"))
                .isEqualTo("[[id, integer, , identifier], [name, varchar, , ], [detail, varchar, , detail column]]");

        sql("COMMENT ON TABLE paimon.default.comment_values IS NULL");
        assertThat(sql("SELECT comment IS NULL FROM system.metadata.table_comments "
                + "WHERE catalog_name = 'paimon' AND schema_name = 'default' AND table_name = 'comment_values'"))
                .isEqualTo("[[true]]");
    }

    @Test
    public void testColumnCommentDirectiveDoesNotChangeExistingLogicalType()
    {
        sql("CREATE TABLE paimon.default.comment_directive_values ("
                + "id integer, "
                + "embedding array(real), "
                + "picture varbinary) "
                + "WITH (file_format = 'json', file_compression = 'none')");

        sql("COMMENT ON COLUMN paimon.default.comment_directive_values.embedding IS '__VECTOR_FIELD;3; display vector'");
        sql("COMMENT ON COLUMN paimon.default.comment_directive_values.picture IS '__BLOB_FIELD; display blob'");

        assertThat(sql("SHOW COLUMNS FROM paimon.default.comment_directive_values")).isEqualTo(
                "[[id, integer, , ], [embedding, array(real), , __VECTOR_FIELD;3; display vector], [picture, varbinary, , __BLOB_FIELD; display blob]]");
        sql("INSERT INTO paimon.default.comment_directive_values VALUES "
                + "(1, ARRAY[CAST(1.0 AS real), CAST(2.0 AS real)], X'CAFE')");
        assertThat(sql("SELECT id, embedding, to_hex(picture) FROM paimon.default.comment_directive_values"))
                .isEqualTo("[[1, [1.0, 2.0], CAFE]]");
    }

    @Test
    public void testCreateOrReplaceTable()
    {
        sql("CREATE OR REPLACE TABLE paimon.default.replace_values (id integer, name varchar) WITH (bucket = '-1')");
        assertThat(sql("SELECT count(*) FROM paimon.default.replace_values")).isEqualTo("[[0]]");

        sql("INSERT INTO paimon.default.replace_values VALUES (1, 'one'), (2, 'two')");
        assertThat(sql("SELECT count(*) FROM paimon.default.replace_values")).isEqualTo("[[2]]");

        sql("CREATE OR REPLACE TABLE paimon.default.replace_values AS SELECT 3 id, 'three' name");
        assertThat(sql("SELECT * FROM paimon.default.replace_values")).isEqualTo("[[3, three]]");
    }

    @Test
    public void testHashFixedDeleteAndMerge()
    {
        sql("CREATE TABLE paimon.default.hash_fixed_mutations ("
                + "id integer, "
                + "name varchar, "
                + "score integer) "
                + "WITH (primary_key = ARRAY['id'], bucket = '1', bucket_key = 'id')");
        sql("INSERT INTO paimon.default.hash_fixed_mutations VALUES "
                + "(1, 'one', 10), (2, 'two', 20), (3, 'three', 30)");

        sql("DELETE FROM paimon.default.hash_fixed_mutations WHERE id = 2");
        assertThat(sql("SELECT * FROM paimon.default.hash_fixed_mutations ORDER BY id"))
                .isEqualTo("[[1, one, 10], [3, three, 30]]");

        sql("MERGE INTO paimon.default.hash_fixed_mutations t "
                + "USING (VALUES (1, 'one-updated', 11), (3, 'three-deleted', -1), (4, 'four', 40)) "
                + "AS s(id, name, score) "
                + "ON (t.id = s.id) "
                + "WHEN MATCHED AND s.score < 0 THEN DELETE "
                + "WHEN MATCHED THEN UPDATE SET name = s.name, score = s.score "
                + "WHEN NOT MATCHED THEN INSERT (id, name, score) VALUES (s.id, s.name, s.score)");

        assertThat(sql("SELECT * FROM paimon.default.hash_fixed_mutations ORDER BY id"))
                .isEqualTo("[[1, one-updated, 11], [4, four, 40]]");
    }

    @Test
    public void testNotNullInsertValidation()
    {
        sql("CREATE TABLE paimon.default.not_null_values ("
                + "nullable_col integer, "
                + "not_null_col integer NOT NULL) "
                + "WITH (bucket = '-1')");

        assertThat(sql("SHOW CREATE TABLE paimon.default.not_null_values"))
                .contains("not_null_col integer NOT NULL");
        sql("INSERT INTO paimon.default.not_null_values (not_null_col) VALUES (2)");
        assertThat(sql("SELECT nullable_col, not_null_col FROM paimon.default.not_null_values"))
                .isEqualTo("[[null, 2]]");

        assertThatExceptionOfType(QueryFailedException.class)
                .isThrownBy(() -> sql("INSERT INTO paimon.default.not_null_values (nullable_col) VALUES (1)"))
                .withMessageContaining("not_null_col");
        assertThatExceptionOfType(QueryFailedException.class)
                .isThrownBy(() -> sql("INSERT INTO paimon.default.not_null_values "
                        + "(not_null_col, nullable_col) VALUES (NULL, 3)"))
                .withMessageContaining("NULL value not allowed for NOT NULL column: not_null_col");
    }

    @Test
    public void testAddNotNullColumnFailsFast()
    {
        sql("CREATE TABLE paimon.default.not_null_values (id integer) WITH (bucket = '-1')");

        assertThatExceptionOfType(QueryFailedException.class)
                .isThrownBy(() -> sql("ALTER TABLE paimon.default.not_null_values "
                        + "ADD COLUMN required_value integer NOT NULL"))
                .withMessageContaining("This connector does not support adding not null columns");
    }

    @Test
    public void testAddColumn()
    {
        sql("CREATE TABLE t5 (" + "  order_key bigint," + "  order_status varchar," + "  total_price double,"
                + "  order_date date" + ")" + "WITH (" + "file_format = 'ORC',"
                + "primary_key = ARRAY['order_key','order_date']," + "partitioned_by = ARRAY['order_date'],"
                + "bucket = '2'," + "bucket_key = 'order_key'," + "changelog_producer = 'input'" + ")");
        sql("INSERT INTO paimon.default.t5 (order_key, order_status, total_price, order_date) "
                + "VALUES (1, 'old', 11.0, DATE '2026-06-11')");
        sql("ALTER TABLE paimon.default.t5 ADD COLUMN zip varchar");
        sql("INSERT INTO paimon.default.t5 (order_key, order_status, total_price, order_date, zip) "
                + "VALUES (2, 'new', 22.0, DATE '2026-06-12', '94107')");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.t5")).isEqualTo(
                "[[order_key, bigint, , ], [order_status, varchar, , ], [total_price, double, , ], [order_date, date, , ], [zip, varchar, , ]]");
        assertThat(sql("SELECT order_key, order_status, zip FROM paimon.default.t5 ORDER BY order_key"))
                .isEqualTo("[[1, old, null], [2, new, 94107]]");
        sql("DROP TABLE IF EXISTS paimon.default.t5");
    }

    @Test
    public void testRenameColumn()
    {
        sql("CREATE TABLE t5 (" + "  order_key bigint," + "  order_status varchar," + "  total_price double,"
                + "  order_date date" + ")" + "WITH (" + "file_format = 'ORC',"
                + "primary_key = ARRAY['order_key','order_date']," + "partitioned_by = ARRAY['order_date'],"
                + "bucket = '2'," + "bucket_key = 'order_key'," + "changelog_producer = 'input'" + ")");
        sql("ALTER TABLE paimon.default.t5 RENAME COLUMN order_status to g");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.t5")).isEqualTo(
                "[[order_key, bigint, , ], [g, varchar, , ], [total_price, double, , ], [order_date, date, , ]]");
        sql("DROP TABLE IF EXISTS paimon.default.t5");
    }

    @Test
    public void testDropColumn()
    {
        sql("CREATE TABLE t5 (" + "  order_key bigint," + "  order_status varchar," + "  total_price double,"
                + "  order_date date" + ")" + "WITH (" + "file_format = 'ORC',"
                + "primary_key = ARRAY['order_key','order_date']," + "partitioned_by = ARRAY['order_date'],"
                + "bucket = '2'," + "bucket_key = 'order_key'," + "changelog_producer = 'input'" + ")");
        sql("ALTER TABLE paimon.default.t5 DROP COLUMN order_status");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.t5"))
                .isEqualTo("[[order_key, bigint, , ], [total_price, double, , ], [order_date, date, , ]]");
        sql("DROP TABLE IF EXISTS paimon.default.t5");
    }

    @Test
    public void testSetTableProperties()
    {
        sql("CREATE TABLE t5 (" + "  order_key bigint," + "  order_status varchar," + "  total_price double,"
                + "  order_date date" + ")" + "WITH (" + "file_format = 'ORC',"
                + "primary_key = ARRAY['order_key','order_date']," + "partitioned_by = ARRAY['order_date'],"
                + "bucket = '2'," + "bucket_key = 'order_key'," + "changelog_producer = 'input'" + ")");
        sql("ALTER TABLE paimon.default.t5 SET PROPERTIES bucket = '4',snapshot_time_retained = '4h'");
        sql("DROP TABLE IF EXISTS paimon.default.t5");
    }

    @Test
    public void testDropNotNullConstraint()
    {
        sql("CREATE TABLE paimon.default.drop_nn_values ("
                + "id integer, "
                + "required_col integer NOT NULL) "
                + "WITH (bucket = '-1')");

        // Verify NOT NULL is enforced
        assertThatExceptionOfType(QueryFailedException.class)
                .isThrownBy(() -> sql("INSERT INTO paimon.default.drop_nn_values (id) VALUES (1)"))
                .withMessageContaining("required_col");

        // Drop the NOT NULL constraint
        sql("ALTER TABLE paimon.default.drop_nn_values ALTER COLUMN required_col DROP NOT NULL");

        // Now null values should be accepted
        sql("INSERT INTO paimon.default.drop_nn_values (id) VALUES (1)");
        assertThat(sql("SELECT id, required_col FROM paimon.default.drop_nn_values"))
                .isEqualTo("[[1, null]]");
    }

    @Test
    public void testNestedFieldOperations()
    {
        sql("CREATE TABLE paimon.default.nested_field_values ("
                + "id integer, "
                + "info row(name varchar, age integer, city varchar)) "
                + "WITH (bucket = '-1')");
        sql("INSERT INTO paimon.default.nested_field_values VALUES "
                + "(1, ROW('alice', 30, 'NYC'))");

        // Verify initial state
        assertThat(sql("SELECT id, info.name, info.age, info.city FROM paimon.default.nested_field_values"))
                .isEqualTo("[[1, alice, 30, NYC]]");

        // Drop nested field: dropField
        sql("ALTER TABLE paimon.default.nested_field_values DROP COLUMN info.city");
        assertThat(sql("SELECT id, info.name, info.age FROM paimon.default.nested_field_values"))
                .isEqualTo("[[1, alice, 30]]");

        // Rename nested field: renameField (verify schema change, not data migration)
        sql("ALTER TABLE paimon.default.nested_field_values RENAME COLUMN info.name TO full_name");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.nested_field_values"))
                .contains("[info, row(full_name varchar, age integer), , ]");

        // Insert data with new schema to verify rename works for new writes
        sql("INSERT INTO paimon.default.nested_field_values VALUES (2, ROW('bob', 25))");
        assertThat(sql("SELECT id, info.full_name, info.age FROM paimon.default.nested_field_values WHERE id = 2"))
                .isEqualTo("[[2, bob, 25]]");

        // Change nested field type: setFieldType
        sql("ALTER TABLE paimon.default.nested_field_values ALTER COLUMN info.age SET DATA TYPE bigint");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.nested_field_values"))
                .contains("[info, row(full_name varchar, age bigint), , ]");

        // Add nested field: addField
        sql("ALTER TABLE paimon.default.nested_field_values ADD COLUMN info.email varchar");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.nested_field_values"))
                .contains("[info, row(full_name varchar, age bigint, email varchar), , ]");

        // Verify new writes work with full schema
        sql("INSERT INTO paimon.default.nested_field_values VALUES (3, ROW('charlie', 35, 'charlie@test.com'))");
        assertThat(sql("SELECT id, info.full_name, info.age, info.email FROM paimon.default.nested_field_values WHERE id = 3"))
                .isEqualTo("[[3, charlie, 35, charlie@test.com]]");
    }

    @Test
    public void testAllType()
    {
        assertThat(sql("SELECT boolean, tinyint, smallint,int,bigint,float,double,char,varchar, date,timestamp_0, "
                + "timestamp_3, timestamp_6, decimal, to_hex(varbinary), array, map, row FROM paimon.default.t99"))
                .isEqualTo("[[true, 1, 1, 1, 1, 1.0, 1.0, char1, varchar1, 1970-01-01, "
                        + "2023-09-12T07:54:48, 2023-09-12T07:54:48.001, 2023-09-12T07:54:48.001001, "
                        + "0.10000, 010203, [1, 1, 1], {1=1}, [1, 1]]]");
    }

    @Test
    public void testOrcTimeType()
    {
        sql("CREATE TABLE paimon.default.time_orc_values ("
                + "id integer, "
                + "time_value time(3)) "
                + "WITH (file_format = 'ORC')");
        sql("INSERT INTO paimon.default.time_orc_values VALUES "
                + "(1, TIME '00:00:12.345'), "
                + "(2, TIME '23:59:59.999')");

        assertThat(sql("SELECT id, CAST(time_value AS varchar) FROM paimon.default.time_orc_values ORDER BY id"))
                .isEqualTo("[[1, 00:00:12.345], [2, 23:59:59.999]]");
    }

    @Test
    public void testJsonVariantType()
    {
        sql("CREATE TABLE paimon.default.json_values ("
                + "id integer, "
                + "payload json, "
                + "nested array(json)) "
                + "WITH (file_format = 'PARQUET')");
        sql("INSERT INTO paimon.default.json_values VALUES "
                + "(1, JSON '{\"name\":\"alice\",\"numbers\":[1,2,3]}', ARRAY[JSON '{\"kind\":\"home\"}', JSON '42']), "
                + "(2, JSON '{\"name\":\"bob\",\"active\":true}', ARRAY[JSON '{\"kind\":\"work\"}', JSON 'null'])");

        assertThat(sql("SHOW COLUMNS FROM paimon.default.json_values")).isEqualTo(
                "[[id, integer, , ], [payload, json, , ], [nested, array(json), , ]]");
        assertThat(sql("SELECT id, json_extract_scalar(payload, '$.name'), json_format(nested[1]) "
                + "FROM paimon.default.json_values ORDER BY id"))
                .isEqualTo("[[1, alice, {\"kind\":\"home\"}], [2, bob, {\"kind\":\"work\"}]]");
        assertThat(sql("SELECT id, json_extract_scalar(nested[1], '$.kind'), json_format(nested[2]) "
                + "FROM paimon.default.json_values ORDER BY id"))
                .isEqualTo("[[1, home, 42], [2, work, null]]");
    }

    @Test
    public void testDirectReadFilterOnUnprojectedColumnFallsBackToPaimonReader()
    {
        sql("CREATE TABLE paimon.default.direct_filter_values ("
                + "id integer, "
                + "category varchar, "
                + "payload varchar) "
                + "WITH (file_format = 'PARQUET')");
        sql("INSERT INTO paimon.default.direct_filter_values VALUES "
                + "(1, 'keep', 'alpha'), "
                + "(2, 'drop', 'beta'), "
                + "(3, 'keep', 'gamma')");

        assertThat(sql("SELECT id, payload FROM paimon.default.direct_filter_values "
                + "WHERE category = 'keep' ORDER BY id"))
                .isEqualTo("[[1, alpha], [3, gamma]]");
    }

    @Test
    public void testSchemaEvolutionFilterOnAddedColumnSkipsOldFiles()
    {
        sql("CREATE TABLE paimon.default.direct_filter_schema_evolution ("
                + "id integer, "
                + "payload varchar) "
                + "WITH (file_format = 'PARQUET')");
        sql("INSERT INTO paimon.default.direct_filter_schema_evolution VALUES "
                + "(1, 'alpha'), "
                + "(2, 'beta')");
        sql("ALTER TABLE paimon.default.direct_filter_schema_evolution ADD COLUMN category varchar");
        sql("INSERT INTO paimon.default.direct_filter_schema_evolution VALUES "
                + "(3, 'gamma', 'keep'), "
                + "(4, 'delta', 'drop')");

        assertThat(sql("SELECT id, payload FROM paimon.default.direct_filter_schema_evolution "
                + "WHERE category = 'keep' ORDER BY id"))
                .isEqualTo("[[3, gamma]]");
        assertThat(sql("SELECT id FROM paimon.default.direct_filter_schema_evolution "
                + "WHERE category = 'missing'"))
                .isEqualTo("[]");
    }

    @Test
    public void testFilesystemCatalogViewCreateFailsFast()
    {
        assertThatExceptionOfType(QueryFailedException.class)
                .isThrownBy(() -> sql("CREATE VIEW paimon.default.order_view AS SELECT 1 value"))
                .withMessageContaining("This connector does not support creating views")
                .withMessageContaining("Paimon catalog does not support view create operations");
    }

    @Test
    public void testVectorType()
    {
        assertThat(sql("SHOW COLUMNS FROM paimon.default.vector_values")).isEqualTo(
                "[[id, integer, , ], [embedding, array(real), , ]]");
        assertThat(sql("SELECT id, embedding FROM paimon.default.vector_values"))
                .isEqualTo("[[1, [1.0, 2.5, 3.75]]]");

        sql("INSERT INTO paimon.default.vector_values VALUES "
                + "(2, ARRAY[CAST(4.0 AS real), CAST(5.5 AS real), CAST(6.25 AS real)])");

        assertThat(sql("SELECT id, embedding FROM paimon.default.vector_values ORDER BY id"))
                .isEqualTo("[[1, [1.0, 2.5, 3.75]], [2, [4.0, 5.5, 6.25]]]");
    }

    @Test
    public void testVectorColumnDirectiveOnCreateTable()
    {
        sql("CREATE TABLE paimon.default.vector_directive_values ("
                + "id integer, "
                + "embedding array(real) COMMENT '__VECTOR_FIELD;3; embedding vector') "
                + "WITH (file_format = 'json', file_compression = 'none')");
        sql("INSERT INTO paimon.default.vector_directive_values VALUES "
                + "(1, ARRAY[CAST(1.0 AS real), CAST(2.5 AS real), CAST(3.75 AS real)])");

        assertThat(sql("SHOW COLUMNS FROM paimon.default.vector_directive_values")).isEqualTo(
                "[[id, integer, , ], [embedding, array(real), , embedding vector]]");
        assertThat(sql("SELECT id, embedding FROM paimon.default.vector_directive_values"))
                .isEqualTo("[[1, [1.0, 2.5, 3.75]]]");
        assertThatExceptionOfType(QueryFailedException.class)
                .isThrownBy(() -> sql("INSERT INTO paimon.default.vector_directive_values VALUES "
                        + "(2, ARRAY[CAST(1.0 AS real), CAST(2.5 AS real)])"))
                .withMessageContaining("Paimon VECTOR length mismatch: expected 3, got 2");
    }

    @Test
    public void testVectorColumnDirectiveOnAddColumn()
    {
        sql("CREATE TABLE paimon.default.vector_directive_add_column ("
                + "id integer) WITH (file_format = 'json', file_compression = 'none')");
        sql("ALTER TABLE paimon.default.vector_directive_add_column "
                + "ADD COLUMN embedding array(real) COMMENT '__VECTOR_FIELD;3; added embedding'");
        sql("INSERT INTO paimon.default.vector_directive_add_column VALUES "
                + "(1, ARRAY[CAST(1.0 AS real), CAST(2.5 AS real), CAST(3.75 AS real)])");

        assertThat(sql("SHOW COLUMNS FROM paimon.default.vector_directive_add_column")).isEqualTo(
                "[[id, integer, , ], [embedding, array(real), , added embedding]]");
        assertThat(sql("SELECT id, embedding FROM paimon.default.vector_directive_add_column"))
                .isEqualTo("[[1, [1.0, 2.5, 3.75]]]");
        assertThatExceptionOfType(QueryFailedException.class)
                .isThrownBy(() -> sql("INSERT INTO paimon.default.vector_directive_add_column VALUES "
                        + "(2, ARRAY[CAST(1.0 AS real), CAST(2.5 AS real)])"))
                .withMessageContaining("Paimon VECTOR length mismatch: expected 3, got 2");
    }

    @Test
    public void testBlobColumnDirectiveOnCreateTable()
    {
        sql("CREATE TABLE paimon.default.blob_directive_values ("
                + "id integer, "
                + "picture varbinary COMMENT '__BLOB_FIELD; profile picture') "
                + "WITH (data_evolution_enabled = 'true', row_tracking_enabled = 'true')");
        sql("INSERT INTO paimon.default.blob_directive_values VALUES "
                + "(1, X'48656C6C6F')");

        assertThat(sql("SHOW COLUMNS FROM paimon.default.blob_directive_values")).isEqualTo(
                "[[id, integer, , ], [picture, varbinary, , profile picture]]");
        assertThat(sql("SELECT id, to_hex(picture) FROM paimon.default.blob_directive_values"))
                .isEqualTo("[[1, 48656C6C6F]]");
    }

    @Test
    public void testBlobColumnDirectiveOnAddColumn()
    {
        sql("CREATE TABLE paimon.default.blob_directive_add_column ("
                + "id integer) WITH (data_evolution_enabled = 'true', row_tracking_enabled = 'true')");
        sql("ALTER TABLE paimon.default.blob_directive_add_column "
                + "ADD COLUMN picture varbinary COMMENT '__BLOB_FIELD; added picture'");
        sql("INSERT INTO paimon.default.blob_directive_add_column VALUES "
                + "(1, X'5945')");

        assertThat(sql("SHOW COLUMNS FROM paimon.default.blob_directive_add_column")).isEqualTo(
                "[[id, integer, , ], [picture, varbinary, , added picture]]");
        assertThat(sql("SELECT id, to_hex(picture) FROM paimon.default.blob_directive_add_column"))
                .isEqualTo("[[1, 5945]]");
    }

    @Test
    public void testTimeTravel()
    {
        assertThat(sql("SELECT * FROM paimon.default.t2 FOR VERSION AS OF 1"))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2]]");
        assertThat(sql("SELECT * FROM paimon.default.t2 FOR VERSION AS OF 2"))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2], [5, 6, 3, 3], [7, 8, 4, 4]]");

        assertThat(sql("SELECT * FROM paimon.default.t2 FOR TIMESTAMP AS OF TIMESTAMP "
                + timestampLiteral(t2FirstCommitTimestamp, 6))).isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2]]");
        assertThat(sql("SELECT * FROM paimon.default.t2 FOR TIMESTAMP AS OF TIMESTAMP "
                + timestampLiteral(System.currentTimeMillis(), 6)))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2], [5, 6, 3, 3], [7, 8, 4, 4]]");
    }

    @Test
    public void testIncrementalRead()
    {
        assertThatExceptionOfType(QueryFailedException.class).isThrownBy(
                () -> sql("SELECT * FROM TABLE(paimon.system.table_changes(schema_name=>'default',table_name=>'t2'))"))
                .withMessage("Either INCREMENTAL_BETWEEN or INCREMENTAL_BETWEEN_TIMESTAMP must be provided");
        assertThat(sql(
                "SELECT * FROM TABLE(paimon.system.table_changes(schema_name=>'default',table_name=>'t2',incremental_between=>'1,2'))"))
                .isEqualTo("[[5, 6, 3, 3], [7, 8, 4, 4]]");
        assertThat(sql(
                "SELECT * FROM TABLE(paimon.system.table_changes(schema_name=>'default',table_name=>'t2',incremental_between=>'1,tag-2'))"))
                .isEqualTo("[[5, 6, 3, 3], [7, 8, 4, 4]]");
        assertThat(sql(
                "SELECT * FROM TABLE(paimon.system.table_changes(schema_name=>'default',table_name=>'t2',incremental_between_timestamp=>'%s,%s'))"
                        .formatted(t2FirstCommitTimestamp, System.currentTimeMillis())))
                .isEqualTo("[[5, 6, 3, 3], [7, 8, 4, 4]]");
    }

    @Test
    public void testTimeTravelWithTag()
    {
        // tag or snapshotId is string
        assertThat(sql("SELECT * FROM paimon.default.t2 FOR VERSION AS OF '1'"))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2]]");
        assertThat(sql("SELECT * FROM paimon.default.t2 FOR VERSION AS OF 'tag-2'"))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2], [5, 6, 3, 3], [7, 8, 4, 4]]");
        // tag or snapshotId is int
        assertThat(sql("SELECT * FROM paimon.default.t2 FOR VERSION AS OF 1"))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2]]");
    }

    @Test
    public void testTimeTravelVersionPrefersTagOverSnapshotIdWithSameToken()
    {
        assertThat(sql("SELECT * FROM paimon.default.t_version_precedence FOR VERSION AS OF 2"))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2]]");
        assertThat(sql("SELECT * FROM paimon.default.t_version_precedence FOR VERSION AS OF '2'"))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2]]");
    }

    @Test
    public void testTimeTravelUsesHistoricalSchemaAfterAddColumn()
    {
        sql("CREATE TABLE paimon.default.time_travel_schema_evolution (id integer, name varchar)");
        sql("INSERT INTO paimon.default.time_travel_schema_evolution VALUES (1, 'hello'), (2, 'paimon')");
        sql("ALTER TABLE paimon.default.time_travel_schema_evolution ADD COLUMN dt varchar");
        sql("INSERT INTO paimon.default.time_travel_schema_evolution VALUES (3, 'trino', '0401'), (4, 'spark', '0402')");

        assertThat(sql("SELECT * FROM paimon.default.time_travel_schema_evolution"))
                .isEqualTo("[[1, hello, null], [2, paimon, null], [3, trino, 0401], [4, spark, 0402]]");
        assertThat(sql("SELECT * FROM paimon.default.time_travel_schema_evolution FOR VERSION AS OF 1"))
                .isEqualTo("[[1, hello], [2, paimon]]");
    }

    @Test
    public void testSchemaEvolution()
    {
        assertThat(sql("SELECT boolean, tinyint, smallint, int, bigint,float,double,char,varchar, date,timestamp_0, "
                + "timestamp_3, timestamp_6, decimal, to_hex(varbinary), array, map, row FROM paimon.default.t100 "
                + "ORDER BY smallint NULLS FIRST"))
                .isEqualTo(
                        "[[true, 1, null, 1, 1, 1.0, 1.0, char1, varchar1, 1970-01-01, 2023-09-12T07:54:48, 2023-09-12T07:54:48.001, 2023-09-12T07:54:48.001001, 0.10000, 010203, [1, 1, 1], {1=1}, [1, 1]], "
                                + "[true, 1, null, 1, 1, 1.0, 1.0, char1, varchar1, 1970-01-01, 2023-09-12T07:54:48, 2023-09-12T07:54:48.001, 2023-09-12T07:54:48.001001, 0.10000, 010203, [1, 1, 1], {1=1}, [1, 1]], "
                                + "[true, 1, 1, 1, 1, 1.0, 1.0, char1, varchar1, 1970-01-01, 2023-09-12T07:54:48, 2023-09-12T07:54:48.001, 2023-09-12T07:54:48.001001, 0.10000, 010203, [1, 1, 1], {1=1}, [1, 1]]]");
    }

    @Test
    public void testDeletionFile()
    {
        assertThat(sql("SELECT * FROM paimon.default.t101")).isEqualTo(
                "[[a1, 1, 1], [a2, 2, 2], [a3, 3, 3], [a4, 4, 4], [a5, 5, 5], [a6, 6, 6], [a7, 7, 7], [a8, 8, 8], [a9, 9, 9]]");
    }

    @Test
    public void testFileIndex()
    {
        assertThat(sql("SELECT * FROM paimon.default.t102 where c = 2")).isEqualTo("[[a2, 2, 2]]");
    }

    @Test
    public void testInsertIntoFixedBucketTableWiPk()
    {
        sql("INSERT INTO paimon.default.fixed_bucket_table_wi_pk VALUES (1,'1'),(2,'2'),(3,'3'),(4,'4'),(5,'5'),(6,'6')");
        assertThat(sql("SELECT * FROM paimon.default.fixed_bucket_table_wi_pk order by id asc"))
                .isEqualTo("[[1, 1], [2, 2], [3, 3], [4, 4], [5, 5], [6, 6]]");
    }

    @Test
    public void testInsertIntoFixedBucketTableWoPk()
    {
        sql("INSERT INTO paimon.default.fixed_bucket_table_wo_pk VALUES (1,'1'),(2,'2'),(3,'3'),(4,'4'),(1,'1'),(2,'2'),(3,'3'),(4,'4')");
        assertThat(sql("SELECT * FROM paimon.default.fixed_bucket_table_wo_pk order by id asc"))
                .isEqualTo("[[1, 1], [1, 1], [2, 2], [2, 2], [3, 3], [3, 3], [4, 4], [4, 4]]");
    }

    @Test
    public void testInsertIntoUnawareTable()
    {
        sql("INSERT INTO paimon.default.unaware_table VALUES (1,'1'),(2,'2'),(3,'3'),(4,'4'),(1,'1'),(2,'2'),(3,'3'),(4,'4')");
        assertThat(sql("SELECT * FROM paimon.default.unaware_table order by id asc"))
                .isEqualTo("[[1, 1], [1, 1], [2, 2], [2, 2], [3, 3], [3, 3], [4, 4], [4, 4]]");
    }

    protected String sql(String sql)
    {
        MaterializedResult result = getQueryRunner().execute(sql);
        return result.getMaterializedRows().toString();
    }

    protected static String timestampLiteral(long epochMilliSeconds, int precision)
    {
        return DateTimeFormatter.ofPattern("''yyyy-MM-dd HH:mm:ss." + "S".repeat(precision) + " VV''")
                .format(Instant.ofEpochMilli(epochMilliSeconds).atZone(UTC));
    }
}
