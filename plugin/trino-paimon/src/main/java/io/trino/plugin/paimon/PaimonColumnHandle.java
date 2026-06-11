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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.JsonSerdeUtil;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class PaimonColumnHandle
        implements
        ColumnHandle
{
    public static final String TRINO_ROW_ID_NAME = "$row_id";
    private final String columnName;
    private final String typeString;
    private final Type trinoType;
    private final boolean isRowId;

    @JsonCreator
    public PaimonColumnHandle(@JsonProperty("columnName") String columnName,
            @JsonProperty("typeString") String typeString)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.typeString = requireNonNull(typeString, "columnType is null");
        this.trinoType = PaimonTypeUtils.fromPaimonType(logicalType());
        this.isRowId = TRINO_ROW_ID_NAME.equals(columnName);
    }

    public static PaimonColumnHandle of(String columnName, DataType columnType)
    {
        return new PaimonColumnHandle(columnName, JsonSerdeUtil.toJson(columnType));
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public String getTypeString()
    {
        return typeString;
    }

    @JsonIgnore
    public Type getTrinoType()
    {
        return trinoType;
    }

    @JsonProperty
    public boolean isRowId()
    {
        return isRowId;
    }

    public DataType logicalType()
    {
        return JsonSerdeUtil.fromJson(typeString, DataType.class);
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(columnName, trinoType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, typeString);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        PaimonColumnHandle other = (PaimonColumnHandle) obj;
        return columnName.equals(other.columnName) && typeString.equals(other.typeString);
    }

    @Override
    public String toString()
    {
        return "{" + "columnName='" + columnName + '\'' + ", typeString='" + typeString + '\'' + ", trinoType="
                + trinoType + '}';
    }
}
