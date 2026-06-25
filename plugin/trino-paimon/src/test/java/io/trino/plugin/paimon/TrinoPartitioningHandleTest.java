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
import io.trino.spi.connector.ConnectorPartitioningHandle;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InstantiationUtil;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TrinoPartitioningHandleTest
{
    private final JsonCodec<PaimonPartitioningHandle> codec = JsonCodec.jsonCodec(PaimonPartitioningHandle.class);

    @Test
    public void testTrinoPartitioningHandle()
            throws Exception
    {
        byte[] schemaData = serializedTestSchema();
        PaimonPartitioningHandle expected = new PaimonPartitioningHandle(schemaData);
        testRoundTrip(expected);
        assertThat(expected.isSingleNode()).isFalse();
    }

    @Test
    public void testSingleNodePartitioningHandle()
            throws Exception
    {
        byte[] schemaData = serializedTestSchema();
        PaimonPartitioningHandle expected = new PaimonPartitioningHandle(schemaData, true);

        testRoundTrip(expected);
        assertThat(expected.isSingleNode()).isTrue();
    }

    @Test
    public void testPartitioningHandleRejectsMissingSchema()
    {
        assertThatThrownBy(() -> codec.fromJson("{}"))
                .rootCause()
                .hasMessageContaining("Missing required creator property 'schema'");
    }

    @Test
    public void testPartitioningHandleRejectsEmptySchema()
    {
        assertThatThrownBy(() -> new PaimonPartitioningHandle(new byte[0]))
                .hasMessage("schema is empty");

        assertThatThrownBy(() -> codec.fromJson("{\"schema\":\"\"}"))
                .hasRootCauseMessage("schema is empty");
    }

    @Test
    public void testPartitioningHandleRejectsUnknownJsonFields()
            throws Exception
    {
        byte[] schemaData = serializedTestSchema();
        String json = appendJsonField(codec.toJson(new PaimonPartitioningHandle(schemaData)),
                "\"unexpectedField\":true");

        assertThatThrownBy(() -> codec.fromJson(json))
                .hasRootCauseMessage("Unknown PaimonPartitioningHandle JSON field: unexpectedField");
    }

    @Test
    public void testPartitioningHandleAcceptsTrinoTypedJsonField()
            throws Exception
    {
        byte[] schemaData = serializedTestSchema();
        PaimonPartitioningHandle expected = new PaimonPartitioningHandle(schemaData);
        String json = appendJsonField(codec.toJson(expected), "\"@type\":\"%s\"".formatted(typedHandleId(PaimonPartitioningHandle.class)));

        assertThat(codec.fromJson(json)).isEqualTo(expected);
    }

    @Test
    public void testPartitioningHandleRejectsInvalidTrinoTypedJsonField()
            throws Exception
    {
        byte[] schemaData = serializedTestSchema();
        PaimonPartitioningHandle expected = new PaimonPartitioningHandle(schemaData);
        String json = appendJsonField(codec.toJson(expected), "\"@type\":true");

        assertThatThrownBy(() -> codec.fromJson(json))
                .hasRootCauseMessage("Invalid PaimonPartitioningHandle JSON @type field");
    }

    @Test
    public void testPartitioningHandleRejectsConnectorNameOnlyTypedJsonField()
            throws Exception
    {
        byte[] schemaData = serializedTestSchema();
        PaimonPartitioningHandle expected = new PaimonPartitioningHandle(schemaData);
        String json = appendJsonField(codec.toJson(expected), "\"@type\":\"paimon\"");

        assertThatThrownBy(() -> codec.fromJson(json))
                .hasRootCauseMessage("Invalid PaimonPartitioningHandle JSON @type field");
    }

    @Test
    public void testPartitioningHandleDefensivelyCopiesSchema()
            throws Exception
    {
        byte[] schemaData = serializedTestSchema();
        byte[] expectedSchema = schemaData.clone();
        PaimonPartitioningHandle handle = new PaimonPartitioningHandle(schemaData);

        schemaData[0] = (byte) (schemaData[0] + 1);
        byte[] returnedSchema = handle.schema();
        returnedSchema[0] = (byte) (returnedSchema[0] + 1);

        assertThat(handle.schema()).isEqualTo(expectedSchema);
    }

    @Test
    public void testPartitioningHandleRejectsSerializedNonTableSchema()
            throws Exception
    {
        byte[] schemaData = InstantiationUtil.serializeObject("test_schema");

        assertThatThrownBy(() -> new PaimonPartitioningHandle(schemaData))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("schema must contain a serialized Paimon TableSchema");
        assertThatThrownBy(() -> codec.fromJson("{\"schema\":\"%s\"}".formatted(java.util.Base64.getEncoder().encodeToString(schemaData))))
                .hasRootCauseMessage("schema must contain a serialized Paimon TableSchema");
    }

    @Test
    public void testPartitioningHandleRejectsMalformedSerializedSchema()
    {
        assertThatThrownBy(() -> new PaimonPartitioningHandle(new byte[] {1, 2, 3}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("schema must contain a serialized Paimon TableSchema");

        assertThatThrownBy(() -> codec.fromJson("{\"schema\":\"AQID\"}"))
                .hasRootCauseMessage("schema must contain a serialized Paimon TableSchema");
    }

    @Test
    public void testNodePartitioningProviderRequiresPaimonPartitioningHandle()
            throws Exception
    {
        byte[] schemaData = serializedTestSchema();
        PaimonPartitioningHandle handle = new PaimonPartitioningHandle(schemaData);

        assertThat(PaimonNodePartitioningProvider.getPartitioningHandle(handle)).isSameAs(handle);

        assertThatThrownBy(() -> PaimonNodePartitioningProvider.getPartitioningHandle(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("partitioningHandle is null");

        ConnectorPartitioningHandle wrongHandle = new ConnectorPartitioningHandle() {};
        assertThatThrownBy(() -> PaimonNodePartitioningProvider.getPartitioningHandle(wrongHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon node partitioning requires PaimonPartitioningHandle, got: %s",
                        wrongHandle.getClass().getName());
    }

    @Test
    public void testNodePartitioningProviderRejectsMalformedInputs()
            throws Exception
    {
        PaimonNodePartitioningProvider provider = new PaimonNodePartitioningProvider();
        PaimonPartitioningHandle handle = new PaimonPartitioningHandle(serializedTestSchema());

        assertThatThrownBy(() -> provider.getBucketFunction(null, null, handle, null, 1))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("partitionChannelTypes is null");
        assertThatThrownBy(() -> provider.getBucketFunction(null, null, handle, singletonList(null), 1))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("partitionChannelTypes contains null type");
        assertThatThrownBy(() -> provider.getBucketFunction(null, null, handle, List.of(BIGINT), 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("workerCount must be positive: 0");
        assertThatThrownBy(() -> provider.getBucketFunction(null, null, handle, List.of(BIGINT), -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("workerCount must be positive: -1");
    }

    @Test
    public void testSingleNodePartitioningProviderRoutesAllRowsToSingleBucket()
            throws Exception
    {
        PaimonNodePartitioningProvider provider = new PaimonNodePartitioningProvider();
        PaimonPartitioningHandle handle = new PaimonPartitioningHandle(serializedTestSchema(), true);

        assertThat(provider.getBucketNodeMapping(null, null, handle))
                .get()
                .extracting(mapping -> mapping.getBucketCount())
                .isEqualTo(1);
        assertThat(provider.getBucketFunction(null, null, handle, List.of(BIGINT), 8)
                .getBucket(null, 0))
                .isEqualTo(0);
    }

    private void testRoundTrip(PaimonPartitioningHandle expected)
    {
        String json = codec.toJson(expected);
        PaimonPartitioningHandle actual = codec.fromJson(json);
        assertThat(actual).isEqualTo(expected);
        assertThat(actual.schema()).isEqualTo(expected.schema());
    }

    private static String appendJsonField(String json, String field)
    {
        return json.substring(0, json.length() - 1) + "," + field + "}";
    }

    private static String typedHandleId(Class<?> handleClass)
    {
        return "paimon:" + handleClass.getName();
    }

    private static byte[] serializedTestSchema()
            throws Exception
    {
        return InstantiationUtil.serializeObject(testSchema());
    }

    private static TableSchema testSchema()
    {
        return TableSchema.create(1, new Schema(
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT())).getFields(),
                List.of(),
                List.of(),
                Map.of(),
                ""));
    }
}
