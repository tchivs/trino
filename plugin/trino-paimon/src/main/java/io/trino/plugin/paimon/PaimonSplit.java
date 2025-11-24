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
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;
import org.apache.paimon.table.source.Split;

import java.util.Collections;
import java.util.List;

public record PaimonSplit(String splitSerialized, Double weight) implements ConnectorSplit
{
    @JsonCreator
    public PaimonSplit(@JsonProperty("splitSerialized") String splitSerialized, @JsonProperty("weight") Double weight)
    {
        this.splitSerialized = splitSerialized;
        this.weight = weight;
    }

    public static PaimonSplit fromSplit(Split split, Double weight)
    {
        return new PaimonSplit(EncodingUtils.encodeObjectToString(split), weight);
    }

    public Split decodeSplit()
    {
        return EncodingUtils.decodeStringToObject(splitSerialized);
    }

    @Override
    @JsonProperty
    public String splitSerialized()
    {
        return splitSerialized;
    }

    @Override
    @JsonProperty
    public Double weight()
    {
        return weight;
    }

    @Override
    @JsonIgnore
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    @JsonIgnore
    public List<HostAddress> getAddresses()
    {
        return Collections.emptyList();
    }

    @Override
    @JsonIgnore
    public SplitWeight getSplitWeight()
    {
        return SplitWeight.fromProportion(weight);
    }

    @Override
    @JsonIgnore
    public long getRetainedSizeInBytes()
    {
        return ConnectorSplit.super.getRetainedSizeInBytes();
    }
}
