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

import io.airlift.json.JsonCodec;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

public class TrinoTableHandleTest
{
    private final JsonCodec<PaimonTableHandle> codec = JsonCodec.jsonCodec(PaimonTableHandle.class);

    @Test
    public void testPrestoTableHandle()
    {
        PaimonTableHandle expected = new PaimonTableHandle("test", "user", Collections.emptyMap(), TupleDomain.all(),
                Optional.empty(), OptionalLong.empty());
        testRoundTrip(expected);
    }

    @Test
    public void testTableWithDynamicOptionsMergesHandleAndSessionOptions()
            throws Exception
    {
        Map<String, String> handleOptions = Map.of(
                CoreOptions.SCAN_TAG_NAME.key(), "tag-1",
                CoreOptions.SCAN_SNAPSHOT_ID.key(), "5");
        PaimonTableHandle handle = new PaimonTableHandle("test", "user", handleOptions, TupleDomain.all(),
                Optional.empty(), OptionalLong.empty());

        AtomicReference<Map<String, String>> copiedOptions = new AtomicReference<>();
        Table table = capturingTable(copiedOptions);
        Field tableField = PaimonTableHandle.class.getDeclaredField("table");
        tableField.setAccessible(true);
        tableField.set(handle, table);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        PaimonSessionProperties.SCAN_TIMESTAMP, 1234L,
                        PaimonSessionProperties.SCAN_SNAPSHOT, 9L))
                .build();

        assertThat(handle.tableWithDynamicOptions(null, session)).isSameAs(table);
        assertThat(copiedOptions.get())
                .containsEntry(CoreOptions.SCAN_TAG_NAME.key(), "tag-1")
                .containsEntry(CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), "1234")
                .containsEntry(CoreOptions.SCAN_SNAPSHOT_ID.key(), "9");
    }

    private void testRoundTrip(PaimonTableHandle expected)
    {
        String json = codec.toJson(expected);
        PaimonTableHandle actual = codec.fromJson(json);
        assertThat(actual).isEqualTo(expected);
        assertThat(actual.getSchemaName()).isEqualTo(expected.getSchemaName());
        assertThat(actual.getTableName()).isEqualTo(expected.getTableName());
        assertThat(actual.getFilter()).isEqualTo(expected.getFilter());
        assertThat(actual.getProjectedColumns()).isEqualTo(expected.getProjectedColumns());
        assertThat(actual.getWriteColumns()).isEqualTo(expected.getWriteColumns());
        assertThat(actual.getLimit()).isEqualTo(expected.getLimit());
    }

    @Test
    public void testWriteColumnsRoundTrip()
    {
        List<ColumnHandle> writeColumns = List.of(
                PaimonColumnHandle.of("id", DataTypes.INT()),
                PaimonColumnHandle.of("name", DataTypes.STRING()));
        PaimonTableHandle expected = new PaimonTableHandle("test", "user", Collections.emptyMap(), TupleDomain.all(),
                Optional.empty(), Optional.empty(), OptionalLong.empty())
                .withWriteColumns(writeColumns);

        testRoundTrip(expected);
    }

    @Test
    public void testLegacyJsonMissingOptionalFields()
    {
        PaimonTableHandle actual = codec.fromJson("""
                {
                  "schemaName": "test",
                  "tableName": "user"
                }
                """);

        assertThat(actual.getSchemaName()).isEqualTo("test");
        assertThat(actual.getTableName()).isEqualTo("user");
        assertThat(actual.getDynamicOptions()).isEmpty();
        assertThat(actual.getFilter()).isEqualTo(TupleDomain.all());
        assertThat(actual.getProjectedColumns()).isEmpty();
        assertThat(actual.getWriteColumns()).isEmpty();
        assertThat(actual.getLimit()).isEmpty();
    }

    private static Table capturingTable(AtomicReference<Map<String, String>> copiedOptions)
    {
        AtomicReference<Table> tableReference = new AtomicReference<>();
        Table table = (Table) Proxy.newProxyInstance(Table.class.getClassLoader(), new Class<?>[] {Table.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("copy")) {
                        copiedOptions.set(Map.copyOf((Map<String, String>) args[0]));
                        return tableReference.get();
                    }
                    if (method.getName().equals("toString")) {
                        return "capturingTable";
                    }
                    if (method.getName().equals("hashCode")) {
                        return System.identityHashCode(proxy);
                    }
                    if (method.getName().equals("equals")) {
                        return proxy == args[0];
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        tableReference.set(table);
        return table;
    }
}
