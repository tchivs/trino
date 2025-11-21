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

import io.airlift.configuration.Config;
import org.apache.paimon.options.Options;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class for Paimon connector to declare configuration properties
 * so that Airlift Bootstrap framework knows they are being consumed.
 */
public class PaimonConfig
{
    private String warehouse;
    private String s3Endpoint;
    private String s3AccessKey;
    private String s3SecretKey;
    private Boolean s3PathStyleAccess;
    private String s3Region;
    private Boolean fsNativeS3Enabled;
    private Boolean fsHadoopEnabled;

    public String getWarehouse()
    {
        return warehouse;
    }

    @Config("warehouse")
    public PaimonConfig setWarehouse(String warehouse)
    {
        this.warehouse = warehouse;
        return this;
    }

    public String getS3Endpoint()
    {
        return s3Endpoint;
    }

    @Config("s3.endpoint")
    public PaimonConfig setS3Endpoint(String s3Endpoint)
    {
        this.s3Endpoint = s3Endpoint;
        return this;
    }

    public String getS3AccessKey()
    {
        return s3AccessKey;
    }

    @Config("s3.access-key")
    public PaimonConfig setS3AccessKey(String s3AccessKey)
    {
        this.s3AccessKey = s3AccessKey;
        return this;
    }

    public String getS3SecretKey()
    {
        return s3SecretKey;
    }

    @Config("s3.secret-key")
    public PaimonConfig setS3SecretKey(String s3SecretKey)
    {
        this.s3SecretKey = s3SecretKey;
        return this;
    }

    public Boolean getS3PathStyleAccess()
    {
        return s3PathStyleAccess;
    }

    @Config("s3.path-style-access")
    public PaimonConfig setS3PathStyleAccess(Boolean s3PathStyleAccess)
    {
        this.s3PathStyleAccess = s3PathStyleAccess;
        return this;
    }

    public String getS3Region()
    {
        return s3Region;
    }

    @Config("s3.region")
    public PaimonConfig setS3Region(String s3Region)
    {
        this.s3Region = s3Region;
        return this;
    }

    public Boolean getFsNativeS3Enabled()
    {
        return fsNativeS3Enabled;
    }

    @Config("fs.native-s3.enabled")
    public PaimonConfig setFsNativeS3Enabled(Boolean fsNativeS3Enabled)
    {
        this.fsNativeS3Enabled = fsNativeS3Enabled;
        return this;
    }

    public Boolean getFsHadoopEnabled()
    {
        return fsHadoopEnabled;
    }

    @Config("fs.hadoop.enabled")
    public PaimonConfig setFsHadoopEnabled(Boolean fsHadoopEnabled)
    {
        this.fsHadoopEnabled = fsHadoopEnabled;
        return this;
    }

    /**
     * Convert this configuration to Paimon Options. This method creates a Map of
     * all non-null configuration properties and returns it as a Paimon Options
     * object.
     */
    public Options toOptions()
    {
        Map<String, String> options = new HashMap<>();

        if (warehouse != null) {
            options.put("warehouse", warehouse);
        }
        if (s3Endpoint != null) {
            options.put("s3.endpoint", s3Endpoint);
        }
        if (s3AccessKey != null) {
            options.put("s3.access-key", s3AccessKey);
        }
        if (s3SecretKey != null) {
            options.put("s3.secret-key", s3SecretKey);
        }
        if (s3PathStyleAccess != null) {
            options.put("s3.path-style-access", s3PathStyleAccess.toString());
        }
        if (s3Region != null) {
            options.put("s3.region", s3Region);
        }
        if (fsNativeS3Enabled != null) {
            options.put("fs.native-s3.enabled", fsNativeS3Enabled.toString());
        }
        if (fsHadoopEnabled != null) {
            options.put("fs.hadoop.enabled", fsHadoopEnabled.toString());
        }

        return new Options(options);
    }
}
