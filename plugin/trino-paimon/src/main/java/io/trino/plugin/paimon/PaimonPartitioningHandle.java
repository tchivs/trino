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

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.utils.InstantiationUtil;

import java.io.IOException;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record PaimonPartitioningHandle(byte[] schema) implements ConnectorPartitioningHandle
{
    @JsonCreator
    public PaimonPartitioningHandle(@JsonProperty(value = "schema", required = true) byte[] schema)
    {
        requireNonNull(schema, "schema is null");
        checkArgument(schema.length > 0, "schema is empty");
        byte[] schemaCopy = schema.clone();
        deserializeTableSchema(schemaCopy);
        this.schema = schemaCopy;
    }

    @JsonAnySetter
    public void rejectUnknownJsonField(String name, Object value)
    {
        PaimonHandleJsonUtils.rejectUnknownHandleJsonField("PaimonPartitioningHandle", name, value);
    }

    @Override
    @JsonProperty
    public byte[] schema()
    {
        return schema.clone();
    }

    @JsonIgnore
    public TableSchema getOriginalSchema()
    {
        return deserializeTableSchema(schema);
    }

    private static TableSchema deserializeTableSchema(byte[] schema)
    {
        try {
            Object deserialized = InstantiationUtil.deserializeObject(schema, PaimonPartitioningHandle.class.getClassLoader());
            checkArgument(deserialized instanceof TableSchema, "schema must contain a serialized Paimon TableSchema");
            return (TableSchema) deserialized;
        }
        catch (IOException | ClassNotFoundException e) {
            throw new IllegalArgumentException("schema must contain a serialized Paimon TableSchema");
        }
    }

    @Override
    @JsonIgnore
    public boolean isSingleNode()
    {
        return ConnectorPartitioningHandle.super.isSingleNode();
    }

    @Override
    @JsonIgnore
    public boolean isCoordinatorOnly()
    {
        return ConnectorPartitioningHandle.super.isCoordinatorOnly();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PaimonPartitioningHandle that = (PaimonPartitioningHandle) o;
        return Arrays.equals(schema, that.schema);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(schema);
    }
}
