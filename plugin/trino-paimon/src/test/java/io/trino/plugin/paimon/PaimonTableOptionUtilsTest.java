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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PaimonTableOptionUtilsTest
{
    @Test
    public void testLatestFileFormatOptionsArePassedThroughAsStrings()
    {
        Schema.Builder builder = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("embedding", DataTypes.ARRAY(DataTypes.FLOAT()));

        PaimonTableOptionUtils.buildOptions(builder, Map.ofEntries(
                entry("file_format", "mosaic"),
                entry("vector_file_format", "lance"),
                entry("variant_shredding_schema", "{\"type\":\"object\"}"),
                entry("variant_infer_shredding_schema", "true"),
                entry("variant_shredding_max_schema_width", "64"),
                entry("variant_shredding_max_schema_depth", "8"),
                entry("variant_shredding_min_field_cardinality_ratio", "0.25"),
                entry("variant_shredding_max_infer_buffer_row", "512"),
                entry("blob_descriptor_field", "payload"),
                entry("blob_view_field", "thumbnail"),
                entry("blob_external_storage_field", "payload"),
                entry("blob_external_storage_path", "file:/tmp/blob-external"),
                entry("vector_field", "embedding")));

        assertThat(builder.build().options())
                .containsEntry(CoreOptions.FILE_FORMAT.key(), "mosaic")
                .containsEntry(CoreOptions.VECTOR_FILE_FORMAT.key(), "lance")
                .containsEntry(CoreOptions.VARIANT_SHREDDING_SCHEMA.key(), "{\"type\":\"object\"}")
                .containsEntry(CoreOptions.VARIANT_INFER_SHREDDING_SCHEMA.key(), "true")
                .containsEntry(CoreOptions.VARIANT_SHREDDING_MAX_SCHEMA_WIDTH.key(), "64")
                .containsEntry(CoreOptions.VARIANT_SHREDDING_MAX_SCHEMA_DEPTH.key(), "8")
                .containsEntry(CoreOptions.VARIANT_SHREDDING_MIN_FIELD_CARDINALITY_RATIO.key(), "0.25")
                .containsEntry(CoreOptions.VARIANT_SHREDDING_MAX_INFER_BUFFER_ROW.key(), "512")
                .containsEntry(CoreOptions.BLOB_DESCRIPTOR_FIELD.key(), "payload")
                .containsEntry(CoreOptions.BLOB_VIEW_FIELD.key(), "thumbnail")
                .containsEntry(CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD.key(), "payload")
                .containsEntry(CoreOptions.BLOB_EXTERNAL_STORAGE_PATH.key(), "file:/tmp/blob-external")
                .containsEntry(CoreOptions.VECTOR_FIELD.key(), "embedding");
    }

    @Test
    public void testCamelCasePaimonOptionsAreExposedAsSnakeCase()
    {
        assertThat(PaimonTableOptionUtils.convertOptionKey(CoreOptions.VARIANT_SHREDDING_SCHEMA.key()))
                .isEqualTo("variant_shredding_schema");
        assertThat(PaimonTableOptionUtils.convertOptionKey(CoreOptions.VARIANT_INFER_SHREDDING_SCHEMA.key()))
                .isEqualTo("variant_infer_shredding_schema");
        assertThat(PaimonTableOptionUtils.convertOptionKey(CoreOptions.VARIANT_SHREDDING_MAX_SCHEMA_WIDTH.key()))
                .isEqualTo("variant_shredding_max_schema_width");
        assertThat(PaimonTableOptionUtils.convertOptionKey(CoreOptions.VARIANT_SHREDDING_MIN_FIELD_CARDINALITY_RATIO.key()))
                .isEqualTo("variant_shredding_min_field_cardinality_ratio");
    }

    @Test
    public void testTrinoTableOptionKeysMapBackToPaimonKeys()
    {
        assertThat(PaimonTableOptionUtils.toPaimonOptionKey("variant_shredding_max_schema_width"))
                .isEqualTo(CoreOptions.VARIANT_SHREDDING_MAX_SCHEMA_WIDTH.key());
        assertThat(PaimonTableOptionUtils.toPaimonOptionKey("vector_file_format"))
                .isEqualTo(CoreOptions.VECTOR_FILE_FORMAT.key());
        assertThat(PaimonTableOptionUtils.toPaimonOptionKey("scan_fallback_branch"))
                .isEqualTo(CoreOptions.SCAN_FALLBACK_BRANCH.key());
        assertThat(PaimonTableOptionUtils.toPaimonOptionKey("custom.option"))
                .isEqualTo("custom.option");
    }

    @Test
    public void testPaimonOptionsAreExposedAsStrings()
    {
        PaimonTableOptions tableOptions = new PaimonTableOptions();

        assertThat(tableOptions.getTableProperties())
                .filteredOn(property -> property.getName().equals("merge_engine"))
                .singleElement()
                .satisfies(property -> {
                    assertThat(property.getSqlType()).isEqualTo(VARCHAR);
                    assertThat(property.getJavaType()).isEqualTo(String.class);
                });
        assertThat(tableOptions.getTableProperties())
                .filteredOn(property -> property.getName().equals("vector_field"))
                .singleElement()
                .satisfies(property -> {
                    assertThat(property.getSqlType()).isEqualTo(VARCHAR);
                    assertThat(property.getJavaType()).isEqualTo(String.class);
                });
        assertThat(tableOptions.getTableProperties())
                .filteredOn(property -> property.getName().equals("scan_fallback_branch"))
                .singleElement()
                .satisfies(property -> {
                    assertThat(property.getSqlType()).isEqualTo(VARCHAR);
                    assertThat(property.getJavaType()).isEqualTo(String.class);
                });
        assertThat(tableOptions.getTableProperties())
                .filteredOn(property -> property.getName().equals("blob_external_storage_path"))
                .singleElement()
                .satisfies(property -> {
                    assertThat(property.getSqlType()).isEqualTo(VARCHAR);
                    assertThat(property.getJavaType()).isEqualTo(String.class);
                });
    }

    @Test
    public void testBlankTableOptionsAreRejected()
    {
        Schema.Builder builder = Schema.newBuilder()
                .column("id", DataTypes.INT());

        assertThatThrownBy(() -> PaimonTableOptionUtils.buildOptions(builder, Map.of("file_format", " ")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("properties value for property 'file_format' is blank");
    }

    @Test
    public void testBuildOptionsRejectsNonStringOptionValues()
    {
        Schema.Builder builder = Schema.newBuilder()
                .column("id", DataTypes.INT());

        assertThatThrownBy(() -> PaimonTableOptionUtils.buildOptions(builder, Map.of("bucket", List.of("4"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("properties value for property 'bucket' must be a string");
    }

    @Test
    public void testBuildOptionsRejectsNullInputs()
    {
        Schema.Builder builder = Schema.newBuilder()
                .column("id", DataTypes.INT());

        assertThatThrownBy(() -> PaimonTableOptionUtils.buildOptions(null, Map.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("builder is null");
        assertThatThrownBy(() -> PaimonTableOptionUtils.buildOptions(builder, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("properties is null");
    }

    @Test
    public void testBuildOptionsRejectsBlankOptionKeys()
    {
        Schema.Builder builder = Schema.newBuilder()
                .column("id", DataTypes.INT());

        assertThatThrownBy(() -> PaimonTableOptionUtils.buildOptions(builder,
                Collections.singletonMap(null, "value")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("properties contains null option key");
        assertThatThrownBy(() -> PaimonTableOptionUtils.buildOptions(builder, Map.of(" ", "value")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("properties contains blank option key");
    }

    @Test
    public void testOptionKeyConversionRejectsMalformedInputs()
    {
        assertThatThrownBy(() -> PaimonTableOptionUtils.convertOptionKey(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("key is null");
        assertThatThrownBy(() -> PaimonTableOptionUtils.convertOptionKey(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("key is blank");
        assertThatThrownBy(() -> PaimonTableOptionUtils.toPaimonOptionKey(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("trinoOptionKey is null");
        assertThatThrownBy(() -> PaimonTableOptionUtils.toPaimonOptionKey(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("trinoOptionKey is blank");
    }

    @Test
    public void testOptionValueTypeDoesNotLeakBetweenCoreOptions()
    {
        List<PaimonTableOptionUtils.OptionInfo> optionInfos = PaimonTableOptionUtils.getOptionInfos();

        assertThat(optionInfos)
                .filteredOn(option -> option.paimonOptionKey.equals(CoreOptions.FILE_FORMAT_PER_LEVEL.key()))
                .singleElement()
                .satisfies(option -> {
                    assertThat(option.type).isEmpty();
                });
    }

    @Test
    public void testReflectedOptionKeysAreUnique()
    {
        List<PaimonTableOptionUtils.OptionInfo> optionInfos = PaimonTableOptionUtils.getOptionInfos();

        assertThat(optionInfos.stream()
                .map(option -> option.trinoOptionKey)
                .distinct()
                .count())
                .isEqualTo(optionInfos.size());
        assertThat(optionInfos.stream()
                .map(option -> option.paimonOptionKey)
                .distinct()
                .count())
                .isEqualTo(optionInfos.size());
    }

    @Test
    public void testTablePropertiesReflectSchemaOptionsAndLayoutProperties()
    {
        Map<String, Object> properties = PaimonTableOptionUtils.tableProperties(
                Map.of(
                        CoreOptions.BUCKET.key(), "7",
                        CoreOptions.BUCKET_KEY.key(), "id",
                        CoreOptions.VECTOR_FILE_FORMAT.key(), "lance",
                        CoreOptions.BLOB_EXTERNAL_STORAGE_PATH.key(), "file:/tmp/blob-external"),
                List.of("id"),
                List.of("pt"));

        assertThat(properties)
                .containsEntry(PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, List.of("id"))
                .containsEntry(PaimonTableOptions.PARTITIONED_BY_PROPERTY, List.of("pt"))
                .containsEntry("bucket", "7")
                .containsEntry("bucket_key", "id")
                .containsEntry("vector_file_format", "lance")
                .containsEntry("blob_external_storage_path", "file:/tmp/blob-external");
    }

    @Test
    public void testTablePropertiesRejectNullInputs()
    {
        assertThatThrownBy(() -> PaimonTableOptionUtils.tableProperties(null, List.of(), List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("options is null");
        assertThatThrownBy(() -> PaimonTableOptionUtils.tableProperties(Map.of(), null, List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("primaryKeys is null");
        assertThatThrownBy(() -> PaimonTableOptionUtils.tableProperties(Map.of(), List.of(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("partitionKeys is null");
    }

    @Test
    public void testOptionInfoValidationRejectsMalformedAndDuplicateOptions()
    {
        assertThatThrownBy(() -> PaimonTableOptionUtils.validateOptionInfos(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("optionInfos is null");
        assertThatThrownBy(() -> PaimonTableOptionUtils.validateOptionInfos(Collections.singletonList(null)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("optionInfo is null");
        assertThatThrownBy(() -> PaimonTableOptionUtils.validateOptionInfos(List.of(
                new PaimonTableOptionUtils.OptionInfo(null, "paimon.key", "String"))))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("trinoOptionKey is null");
        assertThatThrownBy(() -> PaimonTableOptionUtils.validateOptionInfos(List.of(
                new PaimonTableOptionUtils.OptionInfo("trino_key", null, "String"))))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("paimonOptionKey is null");
        assertThatThrownBy(() -> PaimonTableOptionUtils.validateOptionInfos(List.of(
                new PaimonTableOptionUtils.OptionInfo(" ", "paimon.key", "String"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("trinoOptionKey is blank");
        assertThatThrownBy(() -> PaimonTableOptionUtils.validateOptionInfos(List.of(
                new PaimonTableOptionUtils.OptionInfo("trino_key", " ", "String"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("paimonOptionKey is blank");
        assertThatThrownBy(() -> PaimonTableOptionUtils.validateOptionInfos(List.of(
                new PaimonTableOptionUtils.OptionInfo("same_key", "paimon.first", "String"),
                new PaimonTableOptionUtils.OptionInfo("same_key", "paimon.second", "String"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Duplicate Trino table option key 'same_key' maps to Paimon keys 'paimon.first' and 'paimon.second'");
        assertThatThrownBy(() -> PaimonTableOptionUtils.validateOptionInfos(List.of(
                new PaimonTableOptionUtils.OptionInfo("first_key", "paimon.same", "String"),
                new PaimonTableOptionUtils.OptionInfo("second_key", "paimon.same", "String"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Duplicate Paimon table option key 'paimon.same' maps to Trino keys 'first_key' and 'second_key'");
    }

    @Test
    public void testPrimaryAndPartitionKeysUseExplicitDefaults()
    {
        assertThat(PaimonTableOptions.getPrimaryKeys(Map.of())).isEmpty();
        assertThat(PaimonTableOptions.getPartitionedKeys(Map.of())).isEmpty();
    }

    @Test
    public void testPrimaryAndPartitionKeysRequireTableProperties()
    {
        assertThatThrownBy(() -> PaimonTableOptions.getPrimaryKeys(null))
                .hasMessage("tableProperties is null");
        assertThatThrownBy(() -> PaimonTableOptions.getPartitionedKeys(null))
                .hasMessage("tableProperties is null");
    }

    @Test
    public void testPrimaryAndPartitionKeysRejectNullValues()
    {
        assertThatThrownBy(() -> PaimonTableOptions.getPrimaryKeys(Map.of(
                PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, Collections.singletonList(null))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("primary_key contains null value");

        assertThatThrownBy(() -> PaimonTableOptions.getPartitionedKeys(Map.of(
                PaimonTableOptions.PARTITIONED_BY_PROPERTY, Collections.singletonList(null))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("partitioned_by contains null value");
    }

    @Test
    public void testPrimaryAndPartitionKeysRejectNonListValues()
    {
        assertThatThrownBy(() -> PaimonTableOptions.getPrimaryKeys(Map.of(
                PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, "id")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("primary_key must be a list of strings");

        assertThatThrownBy(() -> PaimonTableOptions.getPartitionedKeys(Map.of(
                PaimonTableOptions.PARTITIONED_BY_PROPERTY, "dt")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("partitioned_by must be a list of strings");
    }

    @Test
    public void testPrimaryAndPartitionKeysRejectNonStringValues()
    {
        assertThatThrownBy(() -> PaimonTableOptions.getPrimaryKeys(Map.of(
                PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, List.of(1))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("primary_key contains non-string value");

        assertThatThrownBy(() -> PaimonTableOptions.getPartitionedKeys(Map.of(
                PaimonTableOptions.PARTITIONED_BY_PROPERTY, List.of(1))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("partitioned_by contains non-string value");
    }

    @Test
    public void testPrimaryAndPartitionKeysRejectBlankValues()
    {
        assertThatThrownBy(() -> PaimonTableOptions.getPrimaryKeys(Map.of(
                PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, List.of(" "))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("primary_key contains blank value");

        assertThatThrownBy(() -> PaimonTableOptions.getPartitionedKeys(Map.of(
                PaimonTableOptions.PARTITIONED_BY_PROPERTY, List.of(" "))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("partitioned_by contains blank value");
    }
}
