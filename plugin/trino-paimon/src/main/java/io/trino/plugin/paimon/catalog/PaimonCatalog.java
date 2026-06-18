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
package io.trino.plugin.paimon.catalog;

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.paimon.ClassLoaderUtils;
import io.trino.plugin.paimon.fileio.PaimonFileIOLoader;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.SelectedRole;
import jakarta.annotation.Nullable;
import org.apache.paimon.PagedList;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.format.FileFormatProvider;
import org.apache.paimon.function.Function;
import org.apache.paimon.function.FunctionChange;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.rest.responses.GetTagResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Instant;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.utils.SnapshotNotExistException;
import org.apache.paimon.view.View;
import org.apache.paimon.view.ViewChange;

import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.trino.plugin.paimon.format.TrinoPaimonFileFormatProvider.IDENTIFIER;
import static java.util.Objects.requireNonNull;
import static org.apache.paimon.options.CatalogOptions.RESOLVING_FILE_IO_ENABLED;

public class PaimonCatalog
        implements
        Catalog
{
    private final Options options;

    private final TrinoFileSystemFactory paimonFileSystemFactory;

    private final ConcurrentMap<SessionCatalogKey, Catalog> catalogs = new ConcurrentHashMap<>();
    private final ThreadLocal<Catalog> currentCatalog = new ThreadLocal<>();

    public PaimonCatalog(Options options, TrinoFileSystemFactory paimonFileSystemFactory)
    {
        this.options = requireNonNull(options, "options is null");
        this.paimonFileSystemFactory = requireNonNull(paimonFileSystemFactory, "paimonFileSystemFactory is null");
    }

    public void initSession(ConnectorSession connectorSession)
    {
        Catalog catalog = forSession(connectorSession);
        currentCatalog.set(catalog);
    }

    public Catalog forSession(ConnectorSession connectorSession)
    {
        requireNonNull(connectorSession, "connectorSession is null");
        return catalogs.computeIfAbsent(
                SessionCatalogKey.from(requireNonNull(connectorSession.getIdentity(), "connectorSession identity is null")),
                ignored -> createCatalog(connectorSession));
    }

    private Catalog createCatalog(ConnectorSession connectorSession)
    {
        return ClassLoaderUtils.runWithContextClassLoader(() -> {
            TrinoFileSystem trinoFileSystem = paimonFileSystemFactory.create(connectorSession);
            // Avoid referencing HadoopUtils.HADOOP_LOAD_DEFAULT_CONFIG directly, as loading
            // HadoopUtils triggers NoClassDefFoundError for org.apache.hadoop.conf.Configuration
            // when Hadoop is not on the classpath. Use the string key instead.
            Map<String, String> catalogOptionMap = new HashMap<>(options.toMap());
            catalogOptionMap.put("hadoop-load-default-config", "false");
            catalogOptionMap.put(
                    Catalog.TABLE_RUNTIME_OPTION_PREFIX + FileFormatProvider.VALIDATION_FORMAT_PROVIDER,
                    IDENTIFIER);
            Options catalogOptions = Options.fromMap(catalogOptionMap);
            catalogOptions.set(RESOLVING_FILE_IO_ENABLED, false);
            CatalogContext catalogContext = CatalogContext.create(catalogOptions,
                    new PaimonFileIOLoader(trinoFileSystem), null);
            return CatalogFactory.createCatalog(catalogContext);
        }, this.getClass().getClassLoader());
    }

    @Override
    public Map<String, String> options()
    {
        return current().options();
    }

    @Override
    public CatalogLoader catalogLoader()
    {
        return current().catalogLoader();
    }

    @Override
    public boolean caseSensitive()
    {
        return current().caseSensitive();
    }

    @Override
    public List<String> listDatabases()
    {
        return current().listDatabases();
    }

    @Override
    public PagedList<String> listDatabasesPaged(@Nullable Integer maxResults, @Nullable String pageToken,
            @Nullable String databaseNamePattern)
    {
        return current().listDatabasesPaged(maxResults, pageToken, databaseNamePattern);
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
            throws DatabaseAlreadyExistException
    {
        current().createDatabase(name, ignoreIfExists, properties);
    }

    @Override
    public Database getDatabase(String name)
            throws DatabaseNotExistException
    {
        return current().getDatabase(name);
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException,
            DatabaseNotEmptyException
    {
        current().dropDatabase(name, ignoreIfNotExists, cascade);
    }

    @Override
    public void alterDatabase(String name, List<PropertyChange> changes, boolean ignoreIfNotExists)
            throws DatabaseNotExistException
    {
        current().alterDatabase(name, changes, ignoreIfNotExists);
    }

    @Override
    public Table getTable(Identifier identifier)
            throws TableNotExistException
    {
        return current().getTable(identifier);
    }

    @Override
    public Table getTableById(String tableId)
            throws TableIdNotExistException
    {
        return current().getTableById(tableId);
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException
    {
        return current().listTables(databaseName);
    }

    @Override
    public PagedList<String> listTablesPaged(String databaseName, @Nullable Integer maxResults,
            @Nullable String pageToken, @Nullable String tableNamePattern, @Nullable String tableType)
            throws DatabaseNotExistException
    {
        return current().listTablesPaged(databaseName, maxResults, pageToken, tableNamePattern, tableType);
    }

    @Override
    public PagedList<Table> listTableDetailsPaged(String databaseName, @Nullable Integer maxResults,
            @Nullable String pageToken, @Nullable String tableNamePattern, @Nullable String tableType)
            throws DatabaseNotExistException
    {
        return current().listTableDetailsPaged(databaseName, maxResults, pageToken, tableNamePattern, tableType);
    }

    @Override
    public List<Table> listTableDetails(String databaseName)
            throws DatabaseNotExistException
    {
        return current().listTableDetails(databaseName);
    }

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws TableNotExistException
    {
        current().dropTable(identifier, ignoreIfNotExists);
    }

    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException,
            DatabaseNotExistException
    {
        current().createTable(identifier, schema, ignoreIfExists);
    }

    @Override
    public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
            throws TableNotExistException,
            TableAlreadyExistException
    {
        current().renameTable(fromTable, toTable, ignoreIfNotExists);
    }

    @Override
    public void alterTable(Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException,
            ColumnAlreadyExistException,
            ColumnNotExistException
    {
        current().alterTable(identifier, changes, ignoreIfNotExists);
    }

    @Override
    public View getView(Identifier identifier)
            throws ViewNotExistException
    {
        return current().getView(identifier);
    }

    @Override
    public void dropView(Identifier identifier, boolean ignoreIfNotExists)
            throws ViewNotExistException
    {
        current().dropView(identifier, ignoreIfNotExists);
    }

    @Override
    public void createView(Identifier identifier, View view, boolean ignoreIfExists)
            throws ViewAlreadyExistException,
            DatabaseNotExistException
    {
        current().createView(identifier, view, ignoreIfExists);
    }

    @Override
    public List<String> listViews(String databaseName)
            throws DatabaseNotExistException
    {
        return current().listViews(databaseName);
    }

    @Override
    public PagedList<String> listViewsPaged(String databaseName, @Nullable Integer maxResults,
            @Nullable String pageToken, @Nullable String viewNamePattern)
            throws DatabaseNotExistException
    {
        return current().listViewsPaged(databaseName, maxResults, pageToken, viewNamePattern);
    }

    @Override
    public PagedList<View> listViewDetailsPaged(String databaseName, @Nullable Integer maxResults,
            @Nullable String pageToken, @Nullable String viewNamePattern)
            throws DatabaseNotExistException
    {
        return current().listViewDetailsPaged(databaseName, maxResults, pageToken, viewNamePattern);
    }

    @Override
    public PagedList<Identifier> listViewsPagedGlobally(@Nullable String databaseNamePattern,
            @Nullable String viewNamePattern, @Nullable Integer maxResults, @Nullable String pageToken)
    {
        return current().listViewsPagedGlobally(databaseNamePattern, viewNamePattern, maxResults, pageToken);
    }

    @Override
    public void renameView(Identifier fromView, Identifier toView, boolean ignoreIfNotExists)
            throws ViewNotExistException,
            ViewAlreadyExistException
    {
        current().renameView(fromView, toView, ignoreIfNotExists);
    }

    @Override
    public void alterView(Identifier view, List<ViewChange> viewChanges, boolean ignoreIfNotExists)
            throws ViewNotExistException,
            DialectAlreadyExistException,
            DialectNotExistException
    {
        current().alterView(view, viewChanges, ignoreIfNotExists);
    }

    @Override
    public List<Partition> listPartitions(Identifier identifier)
            throws TableNotExistException
    {
        return current().listPartitions(identifier);
    }

    @Override
    public PagedList<Partition> listPartitionsPaged(Identifier identifier, @Nullable Integer maxResults,
            @Nullable String pageToken, @Nullable String partitionNamePattern)
            throws TableNotExistException
    {
        return current().listPartitionsPaged(identifier, maxResults, pageToken, partitionNamePattern);
    }

    @Override
    public List<Partition> listPartitionsByNames(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException
    {
        return current().listPartitionsByNames(identifier, partitions);
    }

    @Override
    public boolean supportsListObjectsPaged()
    {
        return current().supportsListObjectsPaged();
    }

    @Override
    public boolean supportsListByPattern()
    {
        return current().supportsListByPattern();
    }

    @Override
    public boolean supportsListTableByType()
    {
        return current().supportsListTableByType();
    }

    @Override
    public boolean supportsVersionManagement()
    {
        return current().supportsVersionManagement();
    }

    @Override
    public boolean commitSnapshot(Identifier identifier, @Nullable String tableUuid, Snapshot snapshot,
            List<PartitionStatistics> statistics)
            throws TableNotExistException
    {
        return current().commitSnapshot(identifier, tableUuid, snapshot, statistics);
    }

    @Override
    public Optional<TableSnapshot> loadSnapshot(Identifier identifier)
            throws TableNotExistException
    {
        return current().loadSnapshot(identifier);
    }

    @Override
    public Optional<Snapshot> loadSnapshot(Identifier identifier, String version)
            throws TableNotExistException
    {
        return current().loadSnapshot(identifier, version);
    }

    @Override
    public PagedList<Snapshot> listSnapshotsPaged(Identifier identifier, @Nullable Integer maxResults,
            @Nullable String pageToken)
            throws TableNotExistException
    {
        return current().listSnapshotsPaged(identifier, maxResults, pageToken);
    }

    @Override
    public void rollbackTo(Identifier identifier, Instant instant)
            throws TableNotExistException
    {
        current().rollbackTo(identifier, instant);
    }

    @Override
    public void rollbackTo(Identifier identifier, Instant instant, @Nullable Long fromSnapshot)
            throws TableNotExistException
    {
        current().rollbackTo(identifier, instant, fromSnapshot);
    }

    @Override
    public void createBranch(Identifier identifier, String branch, @Nullable String fromTag)
            throws TableNotExistException,
            BranchAlreadyExistException,
            TagNotExistException
    {
        current().createBranch(identifier, branch, fromTag);
    }

    @Override
    public void dropBranch(Identifier identifier, String branch)
            throws BranchNotExistException
    {
        current().dropBranch(identifier, branch);
    }

    @Override
    public void renameBranch(Identifier identifier, String fromBranch, String toBranch)
            throws BranchNotExistException,
            BranchAlreadyExistException
    {
        current().renameBranch(identifier, fromBranch, toBranch);
    }

    @Override
    public void fastForward(Identifier identifier, String branch)
            throws BranchNotExistException
    {
        current().fastForward(identifier, branch);
    }

    @Override
    public List<String> listBranches(Identifier identifier)
            throws TableNotExistException
    {
        return current().listBranches(identifier);
    }

    @Override
    public GetTagResponse getTag(Identifier identifier, String tagName)
            throws TableNotExistException,
            TagNotExistException
    {
        return current().getTag(identifier, tagName);
    }

    @Override
    public void createTag(Identifier identifier, String tagName, @Nullable Long snapshotId,
            @Nullable String timeRetained, boolean ignoreIfExists)
            throws TableNotExistException,
            SnapshotNotExistException,
            TagAlreadyExistException
    {
        current().createTag(identifier, tagName, snapshotId, timeRetained, ignoreIfExists);
    }

    @Override
    public PagedList<String> listTagsPaged(Identifier identifier, @Nullable Integer maxResults,
            @Nullable String pageToken, @Nullable String tagNamePrefix)
            throws TableNotExistException
    {
        return current().listTagsPaged(identifier, maxResults, pageToken, tagNamePrefix);
    }

    @Override
    public void deleteTag(Identifier identifier, String tagName)
            throws TableNotExistException,
            TagNotExistException
    {
        current().deleteTag(identifier, tagName);
    }

    @Override
    public boolean supportsPartitionModification()
    {
        return current().supportsPartitionModification();
    }

    @Override
    public void createPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException
    {
        current().createPartitions(identifier, partitions);
    }

    @Override
    public void dropPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException
    {
        current().dropPartitions(identifier, partitions);
    }

    @Override
    public void alterPartitions(Identifier identifier, List<PartitionStatistics> partitions)
            throws TableNotExistException
    {
        current().alterPartitions(identifier, partitions);
    }

    @Override
    public List<String> listFunctions(String databaseName)
            throws DatabaseNotExistException
    {
        return current().listFunctions(databaseName);
    }

    @Override
    public Function getFunction(Identifier identifier)
            throws FunctionNotExistException
    {
        return current().getFunction(identifier);
    }

    @Override
    public void createFunction(Identifier identifier, Function function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException,
            DatabaseNotExistException
    {
        current().createFunction(identifier, function, ignoreIfExists);
    }

    @Override
    public void dropFunction(Identifier identifier, boolean ignoreIfNotExists)
            throws FunctionNotExistException
    {
        current().dropFunction(identifier, ignoreIfNotExists);
    }

    @Override
    public void alterFunction(Identifier identifier, List<FunctionChange> changes, boolean ignoreIfNotExists)
            throws FunctionNotExistException,
            DefinitionAlreadyExistException,
            DefinitionNotExistException
    {
        current().alterFunction(identifier, changes, ignoreIfNotExists);
    }

    @Override
    public TableQueryAuthResult authTableQuery(Identifier identifier, @Nullable List<String> select)
            throws TableNotExistException
    {
        return current().authTableQuery(identifier, select);
    }

    @Override
    public void close()
            throws Exception
    {
        currentCatalog.remove();
        Exception failure = null;
        for (Catalog catalog : catalogs.values()) {
            try {
                catalog.close();
            }
            catch (Exception e) {
                if (failure == null) {
                    failure = e;
                }
                else {
                    failure.addSuppressed(e);
                }
            }
        }
        catalogs.clear();
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException
    {
        current().createDatabase(name, ignoreIfExists);
    }

    @Override
    public void alterTable(Identifier identifier, SchemaChange change, boolean ignoreIfNotExists)
            throws TableNotExistException,
            ColumnAlreadyExistException,
            ColumnNotExistException
    {
        current().alterTable(identifier, change, ignoreIfNotExists);
    }

    @Override
    public void markDonePartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException
    {
        current().markDonePartitions(identifier, partitions);
    }

    private Catalog current()
    {
        Catalog catalog = currentCatalog.get();
        if (catalog == null) {
            throw new IllegalStateException("Paimon catalog has not been initialized for a Trino session");
        }
        return catalog;
    }

    private record SessionCatalogKey(
            String user,
            Set<String> groups,
            Optional<PrincipalKey> principal,
            Set<String> enabledSystemRoles,
            Optional<SelectedRole> connectorRole,
            Map<String, String> extraCredentials)
    {
        private SessionCatalogKey
        {
            user = requireNonNull(user, "user is null");
            groups = Set.copyOf(requireNonNull(groups, "groups is null"));
            principal = requireNonNull(principal, "principal is null");
            enabledSystemRoles = Set.copyOf(requireNonNull(enabledSystemRoles, "enabledSystemRoles is null"));
            connectorRole = requireNonNull(connectorRole, "connectorRole is null");
            extraCredentials = Map.copyOf(requireNonNull(extraCredentials, "extraCredentials is null"));
        }

        static SessionCatalogKey from(ConnectorIdentity identity)
        {
            requireNonNull(identity, "identity is null");
            return new SessionCatalogKey(
                    identity.getUser(),
                    identity.getGroups(),
                    identity.getPrincipal().map(PrincipalKey::from),
                    identity.getEnabledSystemRoles(),
                    identity.getConnectorRole(),
                    identity.getExtraCredentials());
        }
    }

    private record PrincipalKey(String className, String name)
    {
        private PrincipalKey
        {
            className = requireNonNull(className, "className is null");
            name = requireNonNull(name, "name is null");
        }

        static PrincipalKey from(Principal principal)
        {
            requireNonNull(principal, "principal is null");
            return new PrincipalKey(principal.getClass().getName(), principal.getName());
        }
    }
}
