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

import io.airlift.units.Duration;
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.testing.TestingConnectorSession;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.LongStream;

import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_CANNOT_OPEN_SPLIT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.DynamicFilter.NOT_BLOCKED;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.paimon.options.Options.fromMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DynamicFilteringTrinoSplitSourceTest
{
    @Test
    public void testComplexDynamicPredicateIsCompactedWithoutDroppingStaticPredicate()
    {
        PaimonColumnHandle idColumn = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        PaimonColumnHandle regionColumn = PaimonColumnHandle.of("region", DataTypes.BIGINT());
        TupleDomain<PaimonColumnHandle> staticPredicate = TupleDomain.withColumnDomains(Map.of(
                regionColumn, Domain.singleValue(BIGINT, 7L)));
        TupleDomain<PaimonColumnHandle> dynamicPredicate = TupleDomain.withColumnDomains(Map.of(
                idColumn, Domain.multipleValues(BIGINT, LongStream.range(0, 10).boxed().toList())));

        TupleDomain<PaimonColumnHandle> combined = DynamicFilteringTrinoSplitSource.combinePredicates(
                staticPredicate, dynamicPredicate, 3);

        assertThat(combined.getDomains().orElseThrow()).containsEntry(regionColumn, Domain.singleValue(BIGINT, 7L));
        assertThat(combined.getDomains().orElseThrow()).containsEntry(idColumn,
                Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 0L, true, 9L, true)), false));
    }

    @Test
    public void testDynamicPredicateIsPreservedWhenBelowCompactionThreshold()
    {
        PaimonColumnHandle idColumn = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        TupleDomain<PaimonColumnHandle> dynamicPredicate = TupleDomain.withColumnDomains(Map.of(
                idColumn, Domain.multipleValues(BIGINT, List.of(1L, 2L))));

        TupleDomain<PaimonColumnHandle> combined = DynamicFilteringTrinoSplitSource.combinePredicates(
                TupleDomain.all(), dynamicPredicate, 3);

        assertThat(combined).isEqualTo(dynamicPredicate);
    }

    @Test
    public void testNoneDynamicPredicateIsPreserved()
    {
        PaimonColumnHandle regionColumn = PaimonColumnHandle.of("region", DataTypes.BIGINT());
        TupleDomain<PaimonColumnHandle> staticPredicate = TupleDomain.withColumnDomains(Map.of(
                regionColumn, Domain.singleValue(BIGINT, 7L)));

        TupleDomain<PaimonColumnHandle> combined = DynamicFilteringTrinoSplitSource.combinePredicates(
                staticPredicate, TupleDomain.none(), 3);

        assertThat(combined).isEqualTo(TupleDomain.none());
    }

    @Test
    public void testNonAwaitableDynamicPredicateIsStillAppliedBySplitManager()
    {
        PaimonColumnHandle idColumn = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        PaimonColumnHandle regionColumn = PaimonColumnHandle.of("region", DataTypes.BIGINT());
        TupleDomain<PaimonColumnHandle> staticPredicate = TupleDomain.withColumnDomains(Map.of(
                regionColumn, Domain.singleValue(BIGINT, 7L)));
        TupleDomain<ColumnHandle> dynamicPredicate = TupleDomain.withColumnDomains(Map.of(
                (ColumnHandle) idColumn, Domain.singleValue(BIGINT, 11L)));
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Collections.emptyMap(),
                staticPredicate,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        TupleDomain<PaimonColumnHandle> effectivePredicate = PaimonSplitManager.effectivePredicate(
                tableHandle,
                dynamicFilter(dynamicPredicate, false));

        assertThat(effectivePredicate.getDomains().orElseThrow())
                .containsEntry(regionColumn, Domain.singleValue(BIGINT, 7L))
                .containsEntry(idColumn, Domain.singleValue(BIGINT, 11L));
    }

    @Test
    public void testDynamicPredicateIsIgnoredWhenLimitAlreadyAccepted()
    {
        PaimonColumnHandle idColumn = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        PaimonColumnHandle regionColumn = PaimonColumnHandle.of("region", DataTypes.BIGINT());
        TupleDomain<PaimonColumnHandle> staticPredicate = TupleDomain.withColumnDomains(Map.of(
                regionColumn, Domain.singleValue(BIGINT, 7L)));
        TupleDomain<ColumnHandle> dynamicPredicate = TupleDomain.withColumnDomains(Map.of(
                (ColumnHandle) idColumn, Domain.singleValue(BIGINT, 11L)));
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Collections.emptyMap(),
                staticPredicate,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(5));

        TupleDomain<PaimonColumnHandle> effectivePredicate = PaimonSplitManager.effectivePredicate(
                tableHandle,
                dynamicFilter(dynamicPredicate, false));

        assertThat(effectivePredicate).isEqualTo(staticPredicate);
    }

    @Test
    public void testDynamicRowIdPredicateIsConvertedToRowRanges()
    {
        PaimonColumnHandle rowIdColumn = PaimonColumnHandle.of("_row_id", org.apache.paimon.table.SpecialFields.ROW_ID.type());
        TupleDomain<PaimonColumnHandle> combinedPredicate = DynamicFilteringTrinoSplitSource.combinePredicates(
                TupleDomain.all(),
                TupleDomain.withColumnDomains(Map.of(rowIdColumn, Domain.singleValue(BIGINT, 11L))),
                3);

        assertThat(PaimonRowRangeExtractor.extractRowIdRanges(combinedPredicate))
                .hasValue(List.of(new org.apache.paimon.utils.Range(11, 11)));
        assertThat(PaimonRowRangeExtractor.removeRowIdPredicate(combinedPredicate)).isEqualTo(TupleDomain.all());
    }

    @Test
    public void testConstructorRejectsNullDependencies()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        PaimonCatalog catalog = new PaimonCatalog(fromMap(Map.of()), identity -> {
            throw new UnsupportedOperationException("not used");
        });
        DynamicFilter dynamicFilter = dynamicFilter(TupleDomain.all(), false);
        Duration waitTimeout = new Duration(0, MILLISECONDS);

        assertThatThrownBy(() -> new DynamicFilteringTrinoSplitSource(null, TestingConnectorSession.SESSION, catalog, dynamicFilter, waitTimeout))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableHandle is null");
        assertThatThrownBy(() -> new DynamicFilteringTrinoSplitSource(tableHandle, null, catalog, dynamicFilter, waitTimeout))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> new DynamicFilteringTrinoSplitSource(tableHandle, TestingConnectorSession.SESSION, null, dynamicFilter, waitTimeout))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("paimonCatalog is null");
        assertThatThrownBy(() -> new DynamicFilteringTrinoSplitSource(tableHandle, TestingConnectorSession.SESSION, catalog, null, waitTimeout))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("dynamicFilter is null");
        assertThatThrownBy(() -> new DynamicFilteringTrinoSplitSource(tableHandle, TestingConnectorSession.SESSION, catalog, dynamicFilter, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("dynamicFilteringWaitTimeout is null");
    }

    @Test
    public void testGetNextBatchRejectsNonPositiveBatchSizeBeforeWaitingForDynamicFilter()
    {
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Collections.emptyMap(),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                TestingConnectorSession.SESSION,
                new PaimonCatalog(fromMap(Map.of()), identity -> {
                    throw new UnsupportedOperationException("not used");
                }),
                dynamicFilter(TupleDomain.all(), true),
                new Duration(1, SECONDS));

        assertThatThrownBy(() -> splitSource.getNextBatch(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot fetch a batch of zero size");
        assertThatThrownBy(() -> splitSource.getNextBatch(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot fetch a batch of zero size");
    }

    @Test
    public void testPlanningInitializesCatalogBeforeLoadingTable()
            throws Exception
    {
        RecordingCatalog catalog = new RecordingCatalog(false);
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Collections.emptyMap(),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                        .build(),
                catalog,
                dynamicFilter(TupleDomain.all(), false),
                new Duration(0, MILLISECONDS));

        ConnectorSplitSource.ConnectorSplitBatch batch = splitSource.getNextBatch(100).get();

        assertThat(catalog.initialized()).isTrue();
        assertThat(catalog.tableLoaded()).isTrue();
        assertThat(batch.getSplits()).isEmpty();
        assertThat(batch.isNoMoreSplits()).isTrue();
    }

    @Test
    public void testDynamicSplitPlanningRefreshesLatestFileStoreSchema()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        RecordingCatalog catalog = new RecordingCatalog(false, staleFileStoreTable(copiedWithLatestSchema));
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Collections.emptyMap(),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                        .build(),
                catalog,
                dynamicFilter(TupleDomain.all(), false),
                new Duration(0, MILLISECONDS));

        ConnectorSplitSource.ConnectorSplitBatch batch = splitSource.getNextBatch(100).get();

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(batch.getSplits()).isEmpty();
        assertThat(batch.isNoMoreSplits()).isTrue();
    }

    @Test
    public void testDynamicSplitPlanningMapsUnsupportedReadFeaturesToNotSupported()
    {
        RecordingCatalog catalog = new RecordingCatalog(false, unsupportedPlanningTable());
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Collections.emptyMap(),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                        .build(),
                catalog,
                dynamicFilter(TupleDomain.all(), false),
                new Duration(0, MILLISECONDS));

        assertThatThrownBy(() -> splitSource.getNextBatch(100))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon table read uses features which are not supported by the Trino connector");
                    assertThat(exception.getCause()).isInstanceOf(UnsupportedOperationException.class)
                            .hasMessage("unsupported scan mode");
                });
    }

    @Test
    public void testDynamicTableChangesSplitPlanningMapsUnsupportedReadFeaturesToNotSupported()
    {
        RecordingCatalog catalog = new RecordingCatalog(false, unsupportedPlanningTable());
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Map.of(org.apache.paimon.CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2"),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                        .build(),
                catalog,
                dynamicFilter(TupleDomain.all(), false),
                new Duration(0, MILLISECONDS));

        assertThatThrownBy(() -> splitSource.getNextBatch(100))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon system.table_changes uses features which are not supported by the Trino connector");
                    assertThat(exception.getCause()).isInstanceOf(UnsupportedOperationException.class)
                            .hasMessage("unsupported scan mode");
                });
    }

    @Test
    public void testDynamicSplitPlanningMapsWrappedRuntimeIoFailuresToCannotOpenSplit()
    {
        RecordingCatalog catalog = new RecordingCatalog(false, failingPlanningTable("dynamic split planning failed"));
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Collections.emptyMap(),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                        .build(),
                catalog,
                dynamicFilter(TupleDomain.all(), false),
                new Duration(0, MILLISECONDS));

        assertThatThrownBy(() -> splitSource.getNextBatch(100))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_CANNOT_OPEN_SPLIT.toErrorCode());
                    assertThat(exception).hasMessage("Failed to plan Paimon splits");
                    assertThat(exception.getCause()).isInstanceOf(IOException.class)
                            .hasMessage("dynamic split planning failed");
                });
    }

    @Test
    public void testDynamicTableChangesSplitPlanningMapsWrappedRuntimeIoFailuresToCannotOpenSplit()
    {
        RecordingCatalog catalog = new RecordingCatalog(false, failingPlanningTable("dynamic table_changes planning failed"));
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Map.of(org.apache.paimon.CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2"),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                        .build(),
                catalog,
                dynamicFilter(TupleDomain.all(), false),
                new Duration(0, MILLISECONDS));

        assertThatThrownBy(() -> splitSource.getNextBatch(100))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_CANNOT_OPEN_SPLIT.toErrorCode());
                    assertThat(exception).hasMessage("Failed to plan Paimon table_changes splits");
                    assertThat(exception.getCause()).isInstanceOf(IOException.class)
                            .hasMessage("dynamic table_changes planning failed");
                });
    }

    @Test
    public void testEmptyPlanningDoesNotInitializeCatalog()
            throws Exception
    {
        RecordingCatalog catalog = new RecordingCatalog(true);
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Collections.emptyMap(),
                        TupleDomain.none(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                        .build(),
                catalog,
                dynamicFilter(TupleDomain.all(), false),
                new Duration(0, MILLISECONDS));

        ConnectorSplitSource.ConnectorSplitBatch batch = splitSource.getNextBatch(100).get();

        assertThat(catalog.initialized()).isFalse();
        assertThat(catalog.tableLoaded()).isFalse();
        assertThat(batch.getSplits()).isEmpty();
        assertThat(batch.isNoMoreSplits()).isTrue();
    }

    @Test
    public void testCloseBeforePlanningDoesNotInitializeCatalog()
            throws Exception
    {
        RecordingCatalog catalog = new RecordingCatalog(true);
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Collections.emptyMap(),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                        .build(),
                catalog,
                dynamicFilter(TupleDomain.all(), false),
                new Duration(0, MILLISECONDS));

        splitSource.close();

        ConnectorSplitSource.ConnectorSplitBatch batch = splitSource.getNextBatch(100).get();

        assertThat(splitSource.isFinished()).isTrue();
        assertThat(catalog.initialized()).isFalse();
        assertThat(catalog.tableLoaded()).isFalse();
        assertThat(batch.getSplits()).isEmpty();
        assertThat(batch.isNoMoreSplits()).isTrue();
    }

    @Test
    public void testAcceptedLimitSkipsAwaitingDynamicFilter()
            throws Exception
    {
        RecordingCatalog catalog = new RecordingCatalog(false);
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Collections.emptyMap(),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.of(5)),
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                        .build(),
                catalog,
                blockingDynamicFilter(TupleDomain.none()),
                new Duration(1, SECONDS));

        ConnectorSplitSource.ConnectorSplitBatch batch = splitSource.getNextBatch(100).get();

        assertThat(catalog.initialized()).isTrue();
        assertThat(catalog.tableLoaded()).isTrue();
        assertThat(batch.getSplits()).isEmpty();
        assertThat(batch.isNoMoreSplits()).isTrue();
    }

    @Test
    public void testCombinePredicatesRejectsNullInputs()
    {
        assertThatThrownBy(() -> DynamicFilteringTrinoSplitSource.combinePredicates(null, TupleDomain.all(), 3))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("staticPredicate is null");
        assertThatThrownBy(() -> DynamicFilteringTrinoSplitSource.combinePredicates(TupleDomain.all(), (TupleDomain<PaimonColumnHandle>) null, 3))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("dynamicPredicate is null");
        assertThatThrownBy(() -> DynamicFilteringTrinoSplitSource.combinePredicates(TupleDomain.all(), (DynamicFilter) null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("dynamicFilter is null");
    }

    @Test
    public void testCombinePredicatesRejectsNonPositiveCompactionThreshold()
    {
        assertThatThrownBy(() -> DynamicFilteringTrinoSplitSource.combinePredicates(TupleDomain.all(), TupleDomain.all(), 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("domainCompactionThreshold must be positive");
        assertThatThrownBy(() -> DynamicFilteringTrinoSplitSource.combinePredicates(TupleDomain.all(), TupleDomain.all(), -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("domainCompactionThreshold must be positive");
    }

    @Test
    public void testDynamicFilterPredicateRequiresPaimonColumnHandles()
    {
        ColumnHandle wrongColumn = new ColumnHandle() {};
        TupleDomain<ColumnHandle> dynamicPredicate = TupleDomain.withColumnDomains(Map.of(
                wrongColumn, Domain.singleValue(BIGINT, 11L)));

        assertThatThrownBy(() -> DynamicFilteringTrinoSplitSource.combinePredicates(
                TupleDomain.all(), dynamicFilter(dynamicPredicate, false)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon dynamic filter requires PaimonColumnHandle, got: %s",
                        wrongColumn.getClass().getName());
    }

    private static DynamicFilter dynamicFilter(TupleDomain<ColumnHandle> predicate, boolean awaitable)
    {
        return new DynamicFilter()
        {
            @Override
            public Set<ColumnHandle> getColumnsCovered()
            {
                return predicate.getDomains()
                        .map(Map::keySet)
                        .orElse(Set.of());
            }

            @Override
            public CompletableFuture<?> isBlocked()
            {
                return NOT_BLOCKED;
            }

            @Override
            public boolean isComplete()
            {
                return !awaitable;
            }

            @Override
            public boolean isAwaitable()
            {
                return awaitable;
            }

            @Override
            public TupleDomain<ColumnHandle> getCurrentPredicate()
            {
                return predicate;
            }
        };
    }

    private static DynamicFilter blockingDynamicFilter(TupleDomain<ColumnHandle> predicate)
    {
        return new DynamicFilter()
        {
            @Override
            public Set<ColumnHandle> getColumnsCovered()
            {
                return predicate.getDomains()
                        .map(Map::keySet)
                        .orElse(Set.of());
            }

            @Override
            public CompletableFuture<?> isBlocked()
            {
                return new CompletableFuture<>();
            }

            @Override
            public boolean isComplete()
            {
                return false;
            }

            @Override
            public boolean isAwaitable()
            {
                return true;
            }

            @Override
            public TupleDomain<ColumnHandle> getCurrentPredicate()
            {
                return predicate;
            }
        };
    }

    private static Table table()
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));
        return (Table) Proxy.newProxyInstance(
                DynamicFilteringTrinoSplitSourceTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "newReadBuilder" -> readBuilder(rowType);
                    case "rowType" -> rowType;
                    case "toString" -> "testing-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static FileStoreTable staleFileStoreTable(AtomicBoolean copiedWithLatestSchema)
    {
        RowType latestRowType = DataTypes.ROW(DataTypes.FIELD(0, "new_id", DataTypes.BIGINT()));
        FileStoreTable latestTable = (FileStoreTable) Proxy.newProxyInstance(
                DynamicFilteringTrinoSplitSourceTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "copyWithLatestSchema" -> proxy;
                    case "newReadBuilder" -> readBuilder(latestRowType);
                    case "coreOptions" -> new org.apache.paimon.CoreOptions(new org.apache.paimon.options.Options());
                    case "rowType" -> latestRowType;
                    case "toString" -> "latest-dynamic-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        return (FileStoreTable) Proxy.newProxyInstance(
                DynamicFilteringTrinoSplitSourceTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield latestTable;
                    }
                    case "coreOptions" -> new org.apache.paimon.CoreOptions(new org.apache.paimon.options.Options());
                    case "newReadBuilder" -> throw new AssertionError("stale table must not be used for dynamic split planning");
                    case "rowType" -> throw new AssertionError("stale rowType must not be used for dynamic split planning");
                    case "toString" -> "stale-dynamic-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static ReadBuilder readBuilder(RowType rowType)
    {
        return (ReadBuilder) Proxy.newProxyInstance(
                DynamicFilteringTrinoSplitSourceTest.class.getClassLoader(),
                new Class<?>[] {ReadBuilder.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "dropStats", "withFilter", "withLimit" -> proxy;
                    case "newScan" -> tableScan();
                    case "readType" -> rowType;
                    case "tableName" -> "testing-table";
                    case "toString" -> "testing-read-builder";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Table unsupportedPlanningTable()
    {
        return (Table) Proxy.newProxyInstance(
                DynamicFilteringTrinoSplitSourceTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "newReadBuilder" -> unsupportedPlanningReadBuilder();
                    case "rowType" -> DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));
                    case "toString" -> "unsupported-dynamic-planning-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Table failingPlanningTable(String message)
    {
        return (Table) Proxy.newProxyInstance(
                DynamicFilteringTrinoSplitSourceTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "newReadBuilder" -> failingPlanningReadBuilder(message);
                    case "rowType" -> DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));
                    case "toString" -> "failing-dynamic-planning-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static ReadBuilder unsupportedPlanningReadBuilder()
    {
        return (ReadBuilder) Proxy.newProxyInstance(
                DynamicFilteringTrinoSplitSourceTest.class.getClassLoader(),
                new Class<?>[] {ReadBuilder.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "dropStats", "withFilter", "withLimit" -> proxy;
                    case "newScan" -> throw new UnsupportedOperationException("unsupported scan mode");
                    case "readType" -> DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));
                    case "tableName" -> "unsupported-dynamic-planning-table";
                    case "toString" -> "unsupported-dynamic-planning-read-builder";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static ReadBuilder failingPlanningReadBuilder(String message)
    {
        return (ReadBuilder) Proxy.newProxyInstance(
                DynamicFilteringTrinoSplitSourceTest.class.getClassLoader(),
                new Class<?>[] {ReadBuilder.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "dropStats", "withFilter", "withLimit" -> proxy;
                    case "newScan" -> throw new UncheckedIOException(new IOException(message));
                    case "readType" -> DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));
                    case "tableName" -> "failing-dynamic-planning-table";
                    case "toString" -> "failing-dynamic-planning-read-builder";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static TableScan tableScan()
    {
        return (TableScan) Proxy.newProxyInstance(
                DynamicFilteringTrinoSplitSourceTest.class.getClassLoader(),
                new Class<?>[] {TableScan.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "plan" -> (TableScan.Plan) () -> List.of();
                    case "toString" -> "testing-table-scan";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static class RecordingCatalog
            extends PaimonCatalog
    {
        private final boolean failIfInitialized;
        private final Table table;
        private final AtomicBoolean initialized = new AtomicBoolean();
        private final AtomicBoolean tableLoaded = new AtomicBoolean();

        private RecordingCatalog(boolean failIfInitialized)
        {
            this(failIfInitialized, table());
        }

        private RecordingCatalog(boolean failIfInitialized, Table table)
        {
            super(new Options(), session -> {
                throw new AssertionError("filesystem should not be used by dynamic filtering split source test");
            });
            this.failIfInitialized = failIfInitialized;
            this.table = table;
        }

        @Override
        public void initSession(io.trino.spi.connector.ConnectorSession connectorSession)
        {
            if (failIfInitialized) {
                throw new AssertionError("catalog should not be initialized for empty split planning");
            }
            initialized.set(true);
        }

        @Override
        public Catalog forSession(io.trino.spi.connector.ConnectorSession connectorSession)
        {
            if (failIfInitialized) {
                throw new AssertionError("catalog should not be initialized for empty split planning");
            }
            initialized.set(true);
            return this;
        }

        @Override
        public Table getTable(Identifier identifier)
        {
            if (!initialized.get()) {
                throw new AssertionError("table loaded before catalog session initialization");
            }
            tableLoaded.set(true);
            assertThat(identifier.getDatabaseName()).isEqualTo("schema");
            assertThat(identifier.getObjectName()).isEqualTo("table");
            return table;
        }

        private boolean initialized()
        {
            return initialized.get();
        }

        private boolean tableLoaded()
        {
            return tableLoaded.get();
        }
    }
}
