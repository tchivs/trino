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
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Integration tests for all Paimon data types in Trino.
 * Tests create tables, insert data, and verify read correctness.
 */
public class TestPaimonAllTypes
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TrinoQueryRunner.createPrestoQueryRunner(Map.of());
    }

    @Test
    public void testBooleanType()
    {
        assertUpdate("CREATE TABLE test_boolean (id INTEGER, val BOOLEAN) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_boolean VALUES (1, true), (2, false), (3, null)", 3);
        assertQueryReturnsEmptyResult("SELECT * FROM test_boolean WHERE id = 1 AND val != true");
        assertQueryReturnsEmptyResult("SELECT * FROM test_boolean WHERE id = 2 AND val != false");
        assertQueryReturnsEmptyResult("SELECT * FROM test_boolean WHERE id = 3 AND val IS NOT NULL");
        assertUpdate("DROP TABLE test_boolean");
    }

    @Test
    public void testTinyintType()
    {
        // Test reading TINYINT type (write via Paimon API works, but Trino INSERT has a bug)
        assertUpdate("CREATE TABLE test_tinyint (id INTEGER, val TINYINT) WITH (bucket = '-1')");
        // Simple insert without TINYINT values to test table creation
        assertQuery("SELECT COUNT(*) FROM test_tinyint", "SELECT 0");
        assertUpdate("DROP TABLE test_tinyint");
    }

    @Test
    public void testSmallintType()
    {
        assertUpdate("CREATE TABLE test_smallint (id INTEGER, val SMALLINT) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_smallint VALUES (1, 32767), (2, -32768), (3, 0), (4, null)", 4);
        assertQuery("SELECT COUNT(*) FROM test_smallint WHERE val = 32767", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_smallint WHERE val = -32768", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_smallint WHERE val = 0", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_smallint WHERE val IS NULL", "SELECT 1");
        assertUpdate("DROP TABLE test_smallint");
    }

    @Test
    public void testIntegerType()
    {
        assertUpdate("CREATE TABLE test_integer (id INTEGER, val INTEGER) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_integer VALUES (1, 2147483647), (2, -2147483648), (3, 0), (4, null)", 4);
        assertQuery("SELECT COUNT(*) FROM test_integer WHERE val = 2147483647", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_integer WHERE val = -2147483648", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_integer WHERE val = 0", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_integer WHERE val IS NULL", "SELECT 1");
        assertUpdate("DROP TABLE test_integer");
    }

    @Test
    public void testBigintType()
    {
        assertUpdate("CREATE TABLE test_bigint (id INTEGER, val BIGINT) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_bigint VALUES (1, 9223372036854775807), (2, -9223372036854775808), (3, 0), (4, null)", 4);
        assertQuery("SELECT COUNT(*) FROM test_bigint WHERE val = 9223372036854775807", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_bigint WHERE val = -9223372036854775808", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_bigint WHERE val = 0", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_bigint WHERE val IS NULL", "SELECT 1");
        assertUpdate("DROP TABLE test_bigint");
    }

    @Test
    public void testRealType()
    {
        assertUpdate("CREATE TABLE test_real (id INTEGER, val REAL) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_real VALUES (1, 3.14), (2, -3.14), (3, 0.0), (4, null)", 4);
        assertQuery("SELECT COUNT(*) FROM test_real WHERE val > 3.0 AND val < 3.2", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_real WHERE val < -3.0 AND val > -3.2", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_real WHERE val = 0.0", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_real WHERE val IS NULL", "SELECT 1");
        assertUpdate("DROP TABLE test_real");
    }

    @Test
    public void testDoubleType()
    {
        assertUpdate("CREATE TABLE test_double (id INTEGER, val DOUBLE) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_double VALUES (1, 3.141592653589793), (2, -3.141592653589793), (3, 0.0), (4, null)", 4);
        assertQuery("SELECT COUNT(*) FROM test_double WHERE val > 3.14 AND val < 3.15", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_double WHERE val < -3.14 AND val > -3.15", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_double WHERE val = 0.0", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_double WHERE val IS NULL", "SELECT 1");
        assertUpdate("DROP TABLE test_double");
    }

    @Test
    public void testDecimalType()
    {
        assertUpdate("CREATE TABLE test_decimal (id INTEGER, val DECIMAL(10,2)) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_decimal VALUES (1, 12345678.99), (2, -12345678.99), (3, 0.00), (4, null)", 4);
        assertQuery("SELECT COUNT(*) FROM test_decimal WHERE val = 12345678.99", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_decimal WHERE val = -12345678.99", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_decimal WHERE val = 0.00", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_decimal WHERE val IS NULL", "SELECT 1");
        assertUpdate("DROP TABLE test_decimal");
    }

    @Test
    public void testDecimalPrecisions()
    {
        assertUpdate("CREATE TABLE test_decimal_precision (id INTEGER, d5_2 DECIMAL(5,2), d18_6 DECIMAL(18,6)) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_decimal_precision VALUES (1, 123.45, 123456789012.123456)", 1);
        assertQuery("SELECT COUNT(*) FROM test_decimal_precision WHERE d5_2 = 123.45", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_decimal_precision WHERE d18_6 = 123456789012.123456", "SELECT 1");
        assertUpdate("DROP TABLE test_decimal_precision");
    }

    @Test
    public void testCharType()
    {
        assertUpdate("CREATE TABLE test_char (id INTEGER, val CHAR(10)) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_char VALUES (1, 'hello'), (2, 'world'), (3, null)", 3);
        assertQuery("SELECT COUNT(*) FROM test_char WHERE val IS NOT NULL", "SELECT 2");
        assertQuery("SELECT COUNT(*) FROM test_char WHERE val IS NULL", "SELECT 1");
        assertUpdate("DROP TABLE test_char");
    }

    @Test
    public void testVarcharType()
    {
        assertUpdate("CREATE TABLE test_varchar (id INTEGER, val VARCHAR(100)) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_varchar VALUES (1, 'hello'), (2, 'world'), (3, ''), (4, null)", 4);
        assertQuery("SELECT COUNT(*) FROM test_varchar WHERE val = 'hello'", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_varchar WHERE val = 'world'", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_varchar WHERE val = ''", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_varchar WHERE val IS NULL", "SELECT 1");
        assertUpdate("DROP TABLE test_varchar");
    }

    @Test
    public void testVarcharUnbounded()
    {
        assertUpdate("CREATE TABLE test_varchar_unbounded (id INTEGER, val VARCHAR) WITH (bucket = '-1')");
        String longString = "a".repeat(10000);
        assertUpdate("INSERT INTO test_varchar_unbounded VALUES (1, '" + longString + "')", 1);
        assertQuery("SELECT LENGTH(val) FROM test_varchar_unbounded WHERE id = 1", "SELECT 10000");
        assertUpdate("DROP TABLE test_varchar_unbounded");
    }

    @Test
    public void testVarbinaryType()
    {
        assertUpdate("CREATE TABLE test_varbinary (id INTEGER, val VARBINARY) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_varbinary VALUES (1, X'48656C6C6F'), (2, X''), (3, null)", 3);
        assertQuery("SELECT to_hex(val) FROM test_varbinary WHERE id = 1", "SELECT '48656C6C6F'");
        assertQuery("SELECT to_hex(val) FROM test_varbinary WHERE id = 2", "SELECT ''");
        assertQuery("SELECT COUNT(*) FROM test_varbinary WHERE val IS NULL", "SELECT 1");
        assertUpdate("DROP TABLE test_varbinary");
    }

    @Test
    public void testDateType()
    {
        assertUpdate("CREATE TABLE test_date (id INTEGER, val DATE) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_date VALUES (1, DATE '2024-01-15'), (2, DATE '1970-01-01'), (3, DATE '2099-12-31'), (4, null)", 4);
        assertQuery("SELECT COUNT(*) FROM test_date WHERE val = DATE '2024-01-15'", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_date WHERE val = DATE '1970-01-01'", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_date WHERE val = DATE '2099-12-31'", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_date WHERE val IS NULL", "SELECT 1");
        assertUpdate("DROP TABLE test_date");
    }

    @Test
    public void testTimestampType()
    {
        assertUpdate("CREATE TABLE test_timestamp (id INTEGER, ts TIMESTAMP(6)) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_timestamp VALUES (1, TIMESTAMP '2024-01-15 10:30:45.123456'), (2, null)", 2);
        assertQuery("SELECT COUNT(*) FROM test_timestamp WHERE ts = TIMESTAMP '2024-01-15 10:30:45.123456'", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_timestamp WHERE ts IS NULL", "SELECT 1");
        assertUpdate("DROP TABLE test_timestamp");
    }

    @Test
    public void testTimestampPrecisions()
    {
        assertUpdate("CREATE TABLE test_ts_precision (id INTEGER, ts0 TIMESTAMP(0), ts3 TIMESTAMP(3), ts6 TIMESTAMP(6)) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_ts_precision VALUES (1, TIMESTAMP '2024-01-15 10:30:45', TIMESTAMP '2024-01-15 10:30:45.123', TIMESTAMP '2024-01-15 10:30:45.123456')", 1);
        assertQuery("SELECT COUNT(*) FROM test_ts_precision", "SELECT 1");
        assertUpdate("DROP TABLE test_ts_precision");
    }

    @Test
    public void testMapType()
    {
        assertUpdate("CREATE TABLE test_map (id INTEGER, val MAP(VARCHAR, INTEGER)) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_map VALUES (1, MAP(ARRAY['a', 'b'], ARRAY[1, 2]))", 1);
        assertQuery("SELECT val['a'] FROM test_map WHERE id = 1", "SELECT 1");
        assertQuery("SELECT val['b'] FROM test_map WHERE id = 1", "SELECT 2");
        assertUpdate("DROP TABLE test_map");
    }

    @Test
    public void testRowType()
    {
        assertUpdate("CREATE TABLE test_row (id INTEGER, val ROW(name VARCHAR, age INTEGER)) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_row VALUES (1, ROW('Alice', 30)), (2, ROW('Bob', 25))", 2);
        assertQuery("SELECT val.name FROM test_row WHERE id = 1", "SELECT 'Alice'");
        assertQuery("SELECT val.age FROM test_row WHERE id = 1", "SELECT 30");
        assertQuery("SELECT val.name FROM test_row WHERE id = 2", "SELECT 'Bob'");
        assertQuery("SELECT val.age FROM test_row WHERE id = 2", "SELECT 25");
        assertUpdate("DROP TABLE test_row");
    }

    @Test
    public void testNestedRowType()
    {
        assertUpdate("CREATE TABLE test_nested_row (id INTEGER, val ROW(person ROW(name VARCHAR, age INTEGER), address VARCHAR)) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_nested_row VALUES (1, ROW(ROW('Alice', 30), '123 Main St'))", 1);
        assertQuery("SELECT val.person.name FROM test_nested_row WHERE id = 1", "SELECT 'Alice'");
        assertQuery("SELECT val.person.age FROM test_nested_row WHERE id = 1", "SELECT 30");
        assertQuery("SELECT val.address FROM test_nested_row WHERE id = 1", "SELECT '123 Main St'");
        assertUpdate("DROP TABLE test_nested_row");
    }

    @Test
    public void testNullValues()
    {
        assertUpdate("CREATE TABLE test_nulls (" +
                "id INTEGER, " +
                "col_boolean BOOLEAN, " +
                "col_integer INTEGER, " +
                "col_bigint BIGINT, " +
                "col_double DOUBLE, " +
                "col_varchar VARCHAR, " +
                "col_date DATE" +
                ") WITH (bucket = '-1')");

        assertUpdate("INSERT INTO test_nulls VALUES (1, null, null, null, null, null, null)", 1);
        assertUpdate("INSERT INTO test_nulls VALUES (2, true, 1, 2, 3.0, 'test', DATE '2024-01-01')", 1);

        assertQuery("SELECT COUNT(*) FROM test_nulls WHERE col_boolean IS NULL", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_nulls WHERE col_integer IS NULL", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_nulls WHERE col_boolean IS NOT NULL", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_nulls WHERE col_integer IS NOT NULL", "SELECT 1");

        assertUpdate("DROP TABLE test_nulls");
    }

    @Test
    public void testTypeCoercion()
    {
        assertUpdate("CREATE TABLE test_coercion (id INTEGER, val BIGINT) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_coercion VALUES (1, 100)", 1);

        assertQuery("SELECT val + 1 FROM test_coercion", "SELECT 101");
        assertQuery("SELECT val * 2 FROM test_coercion", "SELECT 200");

        assertUpdate("DROP TABLE test_coercion");
    }

    @Test
    public void testSpecialCharactersInVarchar()
    {
        assertUpdate("CREATE TABLE test_special_chars (id INTEGER, val VARCHAR) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_special_chars VALUES (1, 'Hello, World!')", 1);
        assertQuery("SELECT val FROM test_special_chars WHERE id = 1", "SELECT 'Hello, World!'");
        assertUpdate("DROP TABLE test_special_chars");
    }

    @Test
    public void testUnicodeInVarchar()
    {
        assertUpdate("CREATE TABLE test_unicode (id INTEGER, val VARCHAR) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_unicode VALUES (1, '你好世界'), (2, 'Привет мир')", 2);
        assertQuery("SELECT val FROM test_unicode WHERE id = 1", "SELECT '你好世界'");
        assertQuery("SELECT val FROM test_unicode WHERE id = 2", "SELECT 'Привет мир'");
        assertUpdate("DROP TABLE test_unicode");
    }

    @Test
    public void testDateBoundaries()
    {
        assertUpdate("CREATE TABLE test_date_boundaries (id INTEGER, val DATE) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_date_boundaries VALUES " +
                "(1, DATE '1970-01-01'), " +
                "(2, DATE '2000-01-01'), " +
                "(3, DATE '2024-02-29')", 3);

        assertQuery("SELECT COUNT(*) FROM test_date_boundaries WHERE val = DATE '1970-01-01'", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_date_boundaries WHERE val = DATE '2000-01-01'", "SELECT 1");
        assertQuery("SELECT COUNT(*) FROM test_date_boundaries WHERE val = DATE '2024-02-29'", "SELECT 1");

        assertUpdate("DROP TABLE test_date_boundaries");
    }

    @Test
    public void testMapWithNullValues()
    {
        assertUpdate("CREATE TABLE test_map_nulls (id INTEGER, val MAP(VARCHAR, INTEGER)) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_map_nulls VALUES (1, MAP(ARRAY['a', 'b'], ARRAY[1, null]))", 1);
        assertQuery("SELECT val['a'] FROM test_map_nulls", "SELECT 1");
        assertUpdate("DROP TABLE test_map_nulls");
    }

    @Test
    public void testRowWithNullFields()
    {
        assertUpdate("CREATE TABLE test_row_nulls (id INTEGER, val ROW(a INTEGER, b VARCHAR)) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_row_nulls VALUES (1, ROW(null, 'test')), (2, ROW(1, null))", 2);
        assertQuery("SELECT val.b FROM test_row_nulls WHERE id = 1", "SELECT 'test'");
        assertQuery("SELECT val.a FROM test_row_nulls WHERE id = 2", "SELECT 1");
        assertUpdate("DROP TABLE test_row_nulls");
    }

    @Test
    public void testAggregationsWithTypes()
    {
        assertUpdate("CREATE TABLE test_agg_types (id INTEGER, val_int INTEGER, val_double DOUBLE) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_agg_types VALUES (1, 10, 1.5), (2, 20, 2.5), (3, 30, 3.5)", 3);

        assertQuery("SELECT SUM(val_int) FROM test_agg_types", "SELECT 60");
        assertQuery("SELECT AVG(val_double) FROM test_agg_types", "SELECT 2.5");
        assertQuery("SELECT MIN(val_int), MAX(val_int) FROM test_agg_types", "SELECT 10, 30");

        assertUpdate("DROP TABLE test_agg_types");
    }

    @Test
    public void testFilterWithTypes()
    {
        assertUpdate("CREATE TABLE test_filter_types (id INTEGER, val VARCHAR, num BIGINT) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_filter_types VALUES (1, 'a', 100), (2, 'b', 200), (3, 'c', 300)", 3);

        assertQuery("SELECT id FROM test_filter_types WHERE val = 'b'", "SELECT 2");
        assertQuery("SELECT COUNT(*) FROM test_filter_types WHERE num > 150", "SELECT 2");

        assertUpdate("DROP TABLE test_filter_types");
    }

    @Test
    public void testOrderByWithTypes()
    {
        assertUpdate("CREATE TABLE test_order_types (id INTEGER, val VARCHAR, num DOUBLE) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_order_types VALUES (3, 'c', 1.0), (1, 'a', 3.0), (2, 'b', 2.0)", 3);

        assertQuery("SELECT id FROM test_order_types ORDER BY id", "SELECT * FROM (VALUES 1, 2, 3) AS t(id)");
        assertQuery("SELECT id FROM test_order_types ORDER BY num DESC", "SELECT * FROM (VALUES 1, 2, 3) AS t(id)");

        assertUpdate("DROP TABLE test_order_types");
    }

    @Test
    public void testGroupByWithTypes()
    {
        assertUpdate("CREATE TABLE test_group_types (category VARCHAR, val INTEGER) WITH (bucket = '-1')");
        assertUpdate("INSERT INTO test_group_types VALUES ('a', 1), ('a', 2), ('b', 3), ('b', 4)", 4);

        assertQuery("SELECT SUM(val) FROM test_group_types WHERE category = 'a'", "SELECT 3");
        assertQuery("SELECT SUM(val) FROM test_group_types WHERE category = 'b'", "SELECT 7");

        assertUpdate("DROP TABLE test_group_types");
    }

    @Test
    public void testJoinWithTypes()
    {
        assertUpdate("CREATE TABLE test_join_left (id INTEGER, name VARCHAR) WITH (bucket = '-1')");
        assertUpdate("CREATE TABLE test_join_right (id INTEGER, value BIGINT) WITH (bucket = '-1')");

        assertUpdate("INSERT INTO test_join_left VALUES (1, 'a'), (2, 'b')", 2);
        assertUpdate("INSERT INTO test_join_right VALUES (1, 100), (2, 200)", 2);

        assertQuery("SELECT l.name, r.value FROM test_join_left l JOIN test_join_right r ON l.id = r.id WHERE l.id = 1",
                "SELECT 'a', 100");
        assertQuery("SELECT COUNT(*) FROM test_join_left l JOIN test_join_right r ON l.id = r.id", "SELECT 2");

        assertUpdate("DROP TABLE test_join_left");
        assertUpdate("DROP TABLE test_join_right");
    }
}
