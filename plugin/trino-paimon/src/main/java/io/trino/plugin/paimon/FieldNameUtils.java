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

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public class FieldNameUtils
{
    private FieldNameUtils()
    {
    }

    /**
     * Get field names from RowType in lowercase.
     * Paimon stores column names in lowercase in ORC/Parquet files.
     */
    public static List<String> fieldNames(RowType rowType)
    {
        return rowType.getFields().stream().map(DataField::name).map(FieldNameUtils::toLowerCase).collect(Collectors.toList());
    }

    /**
     * Convert a single field name to lowercase.
     * Uses Locale.ENGLISH for consistent behavior across different system locales.
     */
    public static String toLowerCase(String fieldName)
    {
        return fieldName.toLowerCase(Locale.ENGLISH);
    }

    /**
     * Convert a list of field names to lowercase.
     * Uses Locale.ENGLISH for consistent behavior across different system locales.
     */
    public static List<String> toLowerCase(List<String> fieldNames)
    {
        return fieldNames.stream().map(FieldNameUtils::toLowerCase).collect(Collectors.toList());
    }
}
