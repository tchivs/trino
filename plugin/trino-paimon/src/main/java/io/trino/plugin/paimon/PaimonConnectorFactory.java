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

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.airlift.log.Logger;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.plugin.base.ConnectorContextModule;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorMetadata;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;
import org.apache.paimon.utils.StringUtils;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.weakref.jmx.guice.MBeanModule;

import javax.xml.parsers.DocumentBuilderFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PaimonConnectorFactory
        implements
        ConnectorFactory
{
    private static final Logger LOG = Logger.get(PaimonConnectorFactory.class);

    // see
    // https://trino.io/docs/current/connector/hive.html#hive-general-configuration-properties
    private static final String HADOOP_CONF_FILES_KEY = "hive.config.resources";
    // see org.apache.paimon.utils.HadoopUtils
    private static final String HADOOP_CONF_PREFIX = "hadoop.";

    private static void readHadoopXml(String path, Map<String, String> config)
            throws Exception
    {
        path = path.trim();
        if (path.isEmpty()) {
            return;
        }

        File xmlFile = new File(path);
        NodeList propertyNodes = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(xmlFile)
                .getElementsByTagName("property");
        for (int i = 0; i < propertyNodes.getLength(); i++) {
            Node propertyNode = propertyNodes.item(i);
            if (propertyNode.getNodeType() == 1) {
                Element propertyElement = (Element) propertyNode;
                String key = propertyElement.getElementsByTagName("name").item(0).getTextContent();
                String value = propertyElement.getElementsByTagName("value").item(0).getTextContent();
                if (!StringUtils.isNullOrWhitespaceOnly(value)) {
                    config.putIfAbsent(HADOOP_CONF_PREFIX + key, value);
                }
            }
        }
    }

    @Override
    public String getName()
    {
        return "paimon";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return create(catalogName, config, context, new EmptyModule());
    }

    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context, Module module)
    {
        config = new HashMap<>(config);
        if (config.containsKey(HADOOP_CONF_FILES_KEY)) {
            for (String hadoopXml : config.get(HADOOP_CONF_FILES_KEY).split(",")) {
                try {
                    readHadoopXml(hadoopXml, config);
                }
                catch (Exception e) {
                    LOG.warn("Failed to read hadoop xml file " + hadoopXml + ", skipping this file.", e);
                }
            }
        }

        ClassLoader classLoader = PaimonConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(new MBeanModule(),
                    new ConnectorObjectNameGeneratorModule("org.apache.paimon.trino", "paimon.trino"),
                    new JsonModule(),
                    new PaimonModule(),
                    new MBeanServerModule(),
                    new FileSystemModule(catalogName, context, false),
                    new ConnectorContextModule(catalogName, context),
                    binder -> binder.bind(ClassLoader.class).toInstance(classLoader),
                    module);

            Injector injector = app.doNotInitializeLogging().setRequiredConfigurationProperties(config).initialize();

            PaimonMetadata paimonMetadata = injector.getInstance(PaimonMetadataFactory.class).create();
            PaimonSplitManager paimonSplitManager = injector.getInstance(PaimonSplitManager.class);
            PaimonPageSourceProvider paimonPageSourceProvider = injector.getInstance(PaimonPageSourceProvider.class);
            PaimonPageSinkProvider paimonPageSinkProvider = injector.getInstance(PaimonPageSinkProvider.class);
            PaimonNodePartitioningProvider paimonNodePartitioningProvider = injector
                    .getInstance(PaimonNodePartitioningProvider.class);
            PaimonSessionProperties paimonSessionProperties = injector.getInstance(PaimonSessionProperties.class);
            PaimonTableOptions paimonTableOptions = injector.getInstance(PaimonTableOptions.class);
            Set<ConnectorTableFunction> connectorTableFunctions = injector.getInstance(new Key<>() {
            });
            FunctionProvider functionProvider = injector.getInstance(FunctionProvider.class);

            return new PaimonConnector(new ClassLoaderSafeConnectorMetadata(paimonMetadata, classLoader),
                    new ClassLoaderSafeConnectorSplitManager(paimonSplitManager, classLoader),
                    new ClassLoaderSafeConnectorPageSourceProvider(paimonPageSourceProvider, classLoader),
                    new ClassLoaderSafeConnectorPageSinkProvider(paimonPageSinkProvider, classLoader),
                    paimonNodePartitioningProvider, paimonTableOptions, paimonSessionProperties, connectorTableFunctions,
                    functionProvider);
        }
    }

    /** Empty module for paimon connector factory. */
    public static class EmptyModule
            implements
            Module
    {
        @Override
        public void configure(Binder binder)
        {
        }
    }
}
