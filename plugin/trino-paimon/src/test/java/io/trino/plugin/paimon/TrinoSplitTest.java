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
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_CANNOT_OPEN_SPLIT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TrinoSplitTest
{
    private static final io.trino.spi.connector.ConnectorSession SESSION = io.trino.testing.TestingConnectorSession.builder()
            .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
            .build();

    private final JsonCodec<PaimonSplit> codec = JsonCodec.jsonCodec(PaimonSplit.class);

    @Test
    public void testJsonRoundTrip()
            throws Exception
    {
        PaimonSplit expected = PaimonSplit.fromSplit(new TestingSplit(100), 0.1);
        String json = codec.toJson(expected);
        PaimonSplit actual = codec.fromJson(json);
        assertThat(actual.splitSerialized()).isEqualTo(expected.splitSerialized());
    }

    @Test
    public void testJsonMissingWeightFailsFast()
    {
        assertThatThrownBy(() -> codec.fromJson("""
                    {
                      "splitSerialized": "missing-weight"
                    }
                    """))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid JSON string")
                .rootCause()
                .hasMessageContaining("Missing required creator property 'weight'");
    }

    @Test
    public void testBlankSplitSerializedFailsFast()
    {
        assertThatThrownBy(() -> new PaimonSplit(" ", 0.1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("splitSerialized is blank");

        assertThatThrownBy(() -> codec.fromJson("""
                    {
                      "splitSerialized": "",
                      "weight": 0.1
                    }
                    """))
                .hasRootCauseMessage("splitSerialized is blank");
    }

    @Test
    public void testJsonUnknownFieldsFailFast()
    {
        String json = appendJsonField(codec.toJson(PaimonSplit.fromSplit(new TestingSplit(100), 0.1)), "\"unexpectedField\":true");

        assertThatThrownBy(() -> codec.fromJson(json))
                .hasRootCauseMessage("Unknown PaimonSplit JSON field: unexpectedField");
    }

    @Test
    public void testJsonAcceptsTrinoTypedJsonField()
    {
        PaimonSplit expected = PaimonSplit.fromSplit(new TestingSplit(100), 0.1);
        String json = appendJsonField(codec.toJson(expected), "\"@type\":\"%s\"".formatted(typedHandleId(PaimonSplit.class)));

        assertThat(codec.fromJson(json)).isEqualTo(expected);
    }

    @Test
    public void testJsonRejectsInvalidTrinoTypedJsonField()
    {
        PaimonSplit expected = PaimonSplit.fromSplit(new TestingSplit(100), 0.1);
        String json = appendJsonField(codec.toJson(expected), "\"@type\":true");

        assertThatThrownBy(() -> codec.fromJson(json))
                .hasRootCauseMessage("Invalid PaimonSplit JSON @type field");
    }

    @Test
    public void testJsonRejectsConnectorNameOnlyTypedJsonField()
    {
        PaimonSplit expected = PaimonSplit.fromSplit(new TestingSplit(100), 0.1);
        String json = appendJsonField(codec.toJson(expected), "\"@type\":\"paimon\"");

        assertThatThrownBy(() -> codec.fromJson(json))
                .hasRootCauseMessage("Invalid PaimonSplit JSON @type field");
    }

    @Test
    public void testJsonRejectsMalformedSerializedSplit()
    {
        assertThatThrownBy(() -> codec.fromJson("""
                    {
                      "splitSerialized": "not-a-serialized-split",
                      "weight": 0.1
                    }
                    """))
                .hasRootCauseMessage("splitSerialized must contain a serialized Paimon Split");
    }

    @Test
    public void testInvalidSplitWeightFailsFast()
    {
        assertThatThrownBy(() -> new PaimonSplit("split", Double.NaN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("weight must be in the range (0, 1]");
        assertThatThrownBy(() -> new PaimonSplit("split", 0.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("weight must be in the range (0, 1]");
        assertThatThrownBy(() -> new PaimonSplit("split", 1.1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("weight must be in the range (0, 1]");
    }

    @Test
    public void testSplitRetainedSizeIncludesSerializedSplit()
    {
        PaimonSplit shortSplit = new PaimonSplit("serialized", 0.1);
        PaimonSplit longSplit = new PaimonSplit("serialized-with-extra-payload", 0.1);

        assertThat(shortSplit.getRetainedSizeInBytes()).isPositive();
        assertThat(longSplit.getRetainedSizeInBytes()).isGreaterThan(shortSplit.getRetainedSizeInBytes());
    }

    @Test
    public void testFromSplitRejectsNullSplit()
    {
        assertThatThrownBy(() -> PaimonSplit.fromSplit(null, 0.1))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("split is null");
    }

    @Test
    public void testZeroRowSplitUsesMinimumWeight()
    {
        double minimumSplitWeight = 0.05;

        double weight = PaimonSplitManager.calculateSplitWeight(new TestingSplit(0), 0, minimumSplitWeight);

        assertThat(weight).isEqualTo(minimumSplitWeight);
        assertThat(new PaimonSplit("ignored", weight).getSplitWeight())
                .isEqualTo(SplitWeight.fromProportion(minimumSplitWeight));
    }

    @Test
    public void testSplitWeightIsBoundedByMinimumAndStandardWeight()
    {
        assertThat(PaimonSplitManager.calculateSplitWeight(new TestingSplit(1), 100, 0.05)).isEqualTo(0.05);
        assertThat(PaimonSplitManager.calculateSplitWeight(new TestingSplit(200), 100, 0.05)).isEqualTo(1.0);
    }

    @Test
    public void testSplitWeightUsesMergedRowCountWhenAvailable()
    {
        Split split = new TestingSplit(100, 25L);

        assertThat(PaimonSplitManager.splitWeightRowCount(split)).isEqualTo(25);
        assertThat(PaimonSplitManager.calculateSplitWeight(split, 100, 0.05)).isEqualTo(0.25);
    }

    @Test
    public void testInvalidMinimumSplitWeightFailsFast()
    {
        assertThatThrownBy(() -> PaimonSplitManager.calculateSplitWeight(new TestingSplit(1), 100, Double.NaN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minimumSplitWeight must be in the range (0, 1]");
        assertThatThrownBy(() -> PaimonSplitManager.calculateSplitWeight(new TestingSplit(1), 100, 0.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minimumSplitWeight must be in the range (0, 1]");
        assertThatThrownBy(() -> PaimonSplitManager.calculateSplitWeight(new TestingSplit(1), 100, -0.1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minimumSplitWeight must be in the range (0, 1]");
        assertThatThrownBy(() -> PaimonSplitManager.calculateSplitWeight(new TestingSplit(1), 100, 1.1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minimumSplitWeight must be in the range (0, 1]");
    }

    @Test
    public void testNonePredicateUsesEmptySplitSource()
            throws Exception
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of(),
                TupleDomain.none(), Optional.empty(), Optional.empty(), OptionalLong.empty());
        PaimonSplitSource splitSource = PaimonSplitManager.emptySplitSource(tableHandle);

        ConnectorSplitSource.ConnectorSplitBatch batch = splitSource.getNextBatch(100).get();

        assertThat(batch.getSplits()).isEmpty();
        assertThat(batch.isNoMoreSplits()).isTrue();
        assertThat(splitSource.isFinished()).isTrue();
    }

    @Test
    public void testLimitZeroUsesEmptySplitSource()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of(),
                TupleDomain.all(), Optional.empty(), Optional.empty(), OptionalLong.of(0));

        assertThat(PaimonSplitManager.isEmptySplit(TupleDomain.all(), tableHandle)).isTrue();
        assertThat(PaimonSplitManager.isEmptySplit(TupleDomain.none(), tableHandle)).isTrue();
    }

    @Test
    public void testPushLimitRejectsNullInputs()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of(),
                TupleDomain.all(), Optional.empty(), Optional.empty(), OptionalLong.empty());

        assertThatThrownBy(() -> PaimonSplitManager.pushLimit(null, tableHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("readBuilder is null");
        assertThatThrownBy(() -> PaimonSplitManager.pushLimit(unusedReadBuilder(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableHandle is null");
    }

    @Test
    public void testPushLimitAppliesOnlySafePaimonIntLimits()
    {
        TestingReadBuilder readBuilder = new TestingReadBuilder();

        PaimonSplitManager.pushLimit(readBuilder, new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(Integer.MAX_VALUE)));

        assertThat(readBuilder.limit()).hasValue(Integer.MAX_VALUE);
    }

    @Test
    public void testPushLimitSkipsOverflowingTrinoLimits()
    {
        TestingReadBuilder readBuilder = new TestingReadBuilder();

        PaimonSplitManager.pushLimit(readBuilder, new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of((long) Integer.MAX_VALUE + 1)));

        assertThat(readBuilder.limit()).isEmpty();
    }

    @Test
    public void testPushPredicateUsesRowRangesForHiddenRowId()
    {
        PaimonColumnHandle rowIdColumn = PaimonColumnHandle.of("_row_id", org.apache.paimon.table.SpecialFields.ROW_ID.type());
        TupleDomain<PaimonColumnHandle> predicate = TupleDomain.withColumnDomains(Map.of(
                rowIdColumn, Domain.singleValue(BIGINT, 7L)));
        TestingReadBuilder readBuilder = new TestingReadBuilder();

        PaimonSplitManager.pushPredicate(readBuilder, table(), predicate);

        assertThat(readBuilder.rowRanges()).containsExactly(new org.apache.paimon.utils.Range(7, 7));
        assertThat(readBuilder.filterApplied()).isFalse();
    }

    @Test
    public void testPushPredicatePreservesNonRowIdFilterAlongsideRowRanges()
    {
        PaimonColumnHandle rowIdColumn = PaimonColumnHandle.of("_row_id", org.apache.paimon.table.SpecialFields.ROW_ID.type());
        PaimonColumnHandle idColumn = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        TupleDomain<PaimonColumnHandle> predicate = TupleDomain.withColumnDomains(Map.of(
                rowIdColumn, Domain.singleValue(BIGINT, 7L),
                idColumn, Domain.singleValue(BIGINT, 11L)));
        TestingReadBuilder readBuilder = new TestingReadBuilder();

        PaimonSplitManager.pushPredicate(readBuilder, table(), predicate);

        assertThat(readBuilder.rowRanges()).containsExactly(new org.apache.paimon.utils.Range(7, 7));
        assertThat(readBuilder.filterApplied()).isTrue();
    }

    @Test
    public void testSplitPlanningRequiresPaimonTableHandle()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThat(PaimonSplitManager.getTableHandle(tableHandle)).isSameAs(tableHandle);

        assertThatThrownBy(() -> PaimonSplitManager.getTableHandle(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableHandle is null");

        ConnectorTableHandle wrongTableHandle = new ConnectorTableHandle() {};
        assertThatThrownBy(() -> PaimonSplitManager.getTableHandle(wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon split planning requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
    }

    @Test
    public void testTableFunctionSplitPlanningRequiresPaimonTableHandle()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThat(PaimonSplitManager.getTableFunctionHandle(tableHandle)).isSameAs(tableHandle);

        assertThatThrownBy(() -> PaimonSplitManager.getTableFunctionHandle(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("functionHandle is null");

        ConnectorTableFunctionHandle wrongFunctionHandle = new ConnectorTableFunctionHandle() {};
        assertThatThrownBy(() -> PaimonSplitManager.getTableFunctionHandle(wrongFunctionHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon table function split planning requires PaimonTableHandle, got: %s",
                        wrongFunctionHandle.getClass().getName());
    }

    @Test
    public void testSplitManagerRejectsNullEntryPointDependencies()
    {
        PaimonSplitManager splitManager = splitManager();
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of(),
                TupleDomain.none(), Optional.empty(), Optional.empty(), OptionalLong.empty());

        assertThatThrownBy(() -> splitManager.getSplits(null, null, tableHandle, DynamicFilter.EMPTY, Constraint.alwaysTrue()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> splitManager.getSplits(null, SESSION, tableHandle, null, Constraint.alwaysTrue()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("dynamicFilter is null");
        assertThatThrownBy(() -> splitManager.getSplits(null, SESSION, tableHandle, DynamicFilter.EMPTY, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("constraint is null");
        assertThatThrownBy(() -> splitManager.getSplits(null, null, tableHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
    }

    @Test
    public void testSplitPlanningInitializesCatalogBeforeLoadingTable()
            throws Exception
    {
        RecordingCatalog catalog = new RecordingCatalog(false);
        PaimonSplitManager splitManager = splitManager(catalog);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of(),
                TupleDomain.all(), Optional.empty(), Optional.empty(), OptionalLong.empty());

        ConnectorSplitSource splitSource = splitManager.getSplits(null, SESSION, tableHandle, DynamicFilter.EMPTY,
                Constraint.alwaysTrue());
        ConnectorSplitSource.ConnectorSplitBatch batch = splitSource.getNextBatch(100).get();

        assertThat(catalog.initialized()).isTrue();
        assertThat(catalog.tableLoaded()).isTrue();
        assertThat(batch.getSplits()).isEmpty();
        assertThat(batch.isNoMoreSplits()).isTrue();
    }

    @Test
    public void testSplitPlanningRefreshesLatestFileStoreSchema()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        RecordingCatalog catalog = new RecordingCatalog(false, staleFileStoreTable(copiedWithLatestSchema));
        PaimonSplitManager splitManager = splitManager(catalog);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of(),
                TupleDomain.all(), Optional.empty(), Optional.empty(), OptionalLong.empty());

        ConnectorSplitSource splitSource = splitManager.getSplits(null, SESSION, tableHandle, DynamicFilter.EMPTY,
                Constraint.alwaysTrue());
        ConnectorSplitSource.ConnectorSplitBatch batch = splitSource.getNextBatch(100).get();

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(batch.getSplits()).isEmpty();
        assertThat(batch.isNoMoreSplits()).isTrue();
    }

    @Test
    public void testSplitPlanningMapsUnsupportedReadFeaturesToNotSupported()
    {
        RecordingCatalog catalog = new RecordingCatalog(false, unsupportedPlanningTable());
        PaimonSplitManager splitManager = splitManager(catalog);

        assertThatThrownBy(() -> splitManager.getSplits(
                null,
                SESSION,
                new PaimonTableHandle("schema", "table", Map.of()),
                DynamicFilter.EMPTY,
                Constraint.alwaysTrue()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon table read uses features which are not supported by the Trino connector");
                    assertThat(exception.getCause()).isInstanceOf(UnsupportedOperationException.class)
                            .hasMessage("unsupported scan mode");
                });
    }

    @Test
    public void testTableChangesSplitPlanningMapsUnsupportedReadFeaturesToNotSupported()
    {
        RecordingCatalog catalog = new RecordingCatalog(false, unsupportedPlanningTable());
        PaimonSplitManager splitManager = splitManager(catalog);
        PaimonTableHandle tableChangesHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(org.apache.paimon.CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2"),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThatThrownBy(() -> splitManager.getSplits(null, SESSION, tableChangesHandle))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon system.table_changes uses features which are not supported by the Trino connector");
                    assertThat(exception.getCause()).isInstanceOf(UnsupportedOperationException.class)
                            .hasMessage("unsupported scan mode");
                });
    }

    @Test
    public void testAutoTagTableChangesSplitPlanningMapsUnsupportedReadFeaturesToNotSupported()
    {
        RecordingCatalog catalog = new RecordingCatalog(false, unsupportedPlanningTable());
        PaimonSplitManager splitManager = splitManager(catalog);
        PaimonTableHandle tableChangesHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(org.apache.paimon.CoreOptions.INCREMENTAL_TO_AUTO_TAG.key(), "2024-12-04"),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThatThrownBy(() -> splitManager.getSplits(null, SESSION, tableChangesHandle))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon system.table_changes uses features which are not supported by the Trino connector");
                    assertThat(exception.getCause()).isInstanceOf(UnsupportedOperationException.class)
                            .hasMessage("unsupported scan mode");
                });
    }

    @Test
    public void testSplitPlanningMapsWrappedRuntimeIoFailuresToCannotOpenSplit()
    {
        RecordingCatalog catalog = new RecordingCatalog(false, failingPlanningTable("split planning failed"));
        PaimonSplitManager splitManager = splitManager(catalog);

        assertThatThrownBy(() -> splitManager.getSplits(
                null,
                SESSION,
                new PaimonTableHandle("schema", "table", Map.of()),
                DynamicFilter.EMPTY,
                Constraint.alwaysTrue()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_CANNOT_OPEN_SPLIT.toErrorCode());
                    assertThat(exception).hasMessage("Failed to plan Paimon splits");
                    assertThat(exception.getCause()).isInstanceOf(IOException.class)
                            .hasMessage("split planning failed");
                });
    }

    @Test
    public void testTableChangesSplitPlanningMapsWrappedRuntimeIoFailuresToCannotOpenSplit()
    {
        RecordingCatalog catalog = new RecordingCatalog(false, failingPlanningTable("table_changes planning failed"));
        PaimonSplitManager splitManager = splitManager(catalog);
        PaimonTableHandle tableChangesHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(org.apache.paimon.CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2"),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThatThrownBy(() -> splitManager.getSplits(null, SESSION, tableChangesHandle))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_CANNOT_OPEN_SPLIT.toErrorCode());
                    assertThat(exception).hasMessage("Failed to plan Paimon table_changes splits");
                    assertThat(exception.getCause()).isInstanceOf(IOException.class)
                            .hasMessage("table_changes planning failed");
                });
    }

    @Test
    public void testAutoTagTableChangesSplitPlanningMapsWrappedRuntimeIoFailuresToCannotOpenSplit()
    {
        RecordingCatalog catalog = new RecordingCatalog(false, failingPlanningTable("auto-tag table_changes planning failed"));
        PaimonSplitManager splitManager = splitManager(catalog);
        PaimonTableHandle tableChangesHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(org.apache.paimon.CoreOptions.INCREMENTAL_TO_AUTO_TAG.key(), "2024-12-04"),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThatThrownBy(() -> splitManager.getSplits(null, SESSION, tableChangesHandle))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_CANNOT_OPEN_SPLIT.toErrorCode());
                    assertThat(exception).hasMessage("Failed to plan Paimon table_changes splits");
                    assertThat(exception.getCause()).isInstanceOf(IOException.class)
                            .hasMessage("auto-tag table_changes planning failed");
                });
    }

    @Test
    public void testEmptySplitPlanningDoesNotInitializeCatalog()
            throws Exception
    {
        RecordingCatalog catalog = new RecordingCatalog(true);
        PaimonSplitManager splitManager = splitManager(catalog);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of(),
                TupleDomain.none(), Optional.empty(), Optional.empty(), OptionalLong.empty());

        ConnectorSplitSource splitSource = splitManager.getSplits(null, io.trino.testing.TestingConnectorSession.SESSION,
                tableHandle, DynamicFilter.EMPTY, Constraint.alwaysTrue());
        ConnectorSplitSource.ConnectorSplitBatch batch = splitSource.getNextBatch(100).get();

        assertThat(catalog.initialized()).isFalse();
        assertThat(catalog.tableLoaded()).isFalse();
        assertThat(batch.getSplits()).isEmpty();
        assertThat(batch.isNoMoreSplits()).isTrue();
    }

    @Test
    public void testSplitSourceRejectsNonPositiveBatchSize()
            throws Exception
    {
        PaimonSplit split = new PaimonSplit("serialized", 0.1);
        PaimonSplitSource splitSource = new PaimonSplitSource(List.of(split), OptionalLong.empty());

        assertThatThrownBy(() -> splitSource.getNextBatch(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot fetch a batch of zero size");
        assertThatThrownBy(() -> splitSource.getNextBatch(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot fetch a batch of zero size");
        assertThat(queuedSplitCount(splitSource)).isEqualTo(1);
    }

    @Test
    public void testSplitSourceDoesNotPollWhenLimitAlreadyReached()
            throws Exception
    {
        PaimonSplit split = new PaimonSplit("serialized", 0.1);
        PaimonSplitSource splitSource = new PaimonSplitSource(List.of(split), OptionalLong.of(0));

        ConnectorSplitSource.ConnectorSplitBatch batch = splitSource.getNextBatch(100).get();

        assertThat(batch.getSplits()).isEmpty();
        assertThat(batch.isNoMoreSplits()).isTrue();
        assertThat(queuedSplitCount(splitSource)).isEqualTo(1);
    }

    @Test
    public void testSplitSourceDoesNotDecodeSplitsWithoutLimit()
            throws Exception
    {
        PaimonSplit split = new PaimonSplit("serialized", 0.1);
        PaimonSplitSource splitSource = new PaimonSplitSource(List.of(split), OptionalLong.empty());

        ConnectorSplitSource.ConnectorSplitBatch batch = splitSource.getNextBatch(100).get();

        assertThat(batch.getSplits()).containsExactly(split);
        assertThat(batch.isNoMoreSplits()).isTrue();
        assertThat(queuedSplitCount(splitSource)).isEqualTo(0);
    }

    @Test
    public void testSplitSourceLimitCountsOnlyMergedRowCount()
            throws Exception
    {
        PaimonSplit first = PaimonSplit.fromSplit(new TestingSplit(100, 3L), 0.1);
        PaimonSplit second = PaimonSplit.fromSplit(new TestingSplit(100, 3L), 0.1);
        PaimonSplit third = PaimonSplit.fromSplit(new TestingSplit(100, null), 0.1);
        PaimonSplitSource splitSource = new PaimonSplitSource(List.of(first, second, third), OptionalLong.of(5));

        ConnectorSplitSource.ConnectorSplitBatch firstBatch = splitSource.getNextBatch(10).get();
        ConnectorSplitSource.ConnectorSplitBatch secondBatch = splitSource.getNextBatch(10).get();

        assertThat(firstBatch.getSplits()).containsExactly(first, second);
        assertThat(firstBatch.isNoMoreSplits()).isTrue();
        assertThat(secondBatch.getSplits()).isEmpty();
        assertThat(queuedSplitCount(splitSource)).isEqualTo(1);
    }

    @Test
    public void testSplitSourceLimitMergedRowCountSaturatesOnOverflow()
            throws Exception
    {
        PaimonSplit first = PaimonSplit.fromSplit(new TestingSplit(100, Long.MAX_VALUE - 1), 0.1);
        PaimonSplit second = PaimonSplit.fromSplit(new TestingSplit(100, 100L), 0.1);
        PaimonSplit third = PaimonSplit.fromSplit(new TestingSplit(100, 1L), 0.1);
        PaimonSplitSource splitSource = new PaimonSplitSource(List.of(first, second, third), OptionalLong.of(Long.MAX_VALUE));

        ConnectorSplitSource.ConnectorSplitBatch firstBatch = splitSource.getNextBatch(10).get();
        ConnectorSplitSource.ConnectorSplitBatch secondBatch = splitSource.getNextBatch(10).get();

        assertThat(firstBatch.getSplits()).containsExactly(first, second);
        assertThat(firstBatch.isNoMoreSplits()).isTrue();
        assertThat(secondBatch.getSplits()).isEmpty();
        assertThat(queuedSplitCount(splitSource)).isEqualTo(1);
    }

    @Test
    public void testSplitSourceDoesNotUsePossiblyDuplicateRowCountForLimit()
            throws Exception
    {
        PaimonSplit first = PaimonSplit.fromSplit(new TestingSplit(100, null), 0.1);
        PaimonSplit second = PaimonSplit.fromSplit(new TestingSplit(100, null), 0.1);
        PaimonSplitSource splitSource = new PaimonSplitSource(List.of(first, second), OptionalLong.of(5));

        ConnectorSplitSource.ConnectorSplitBatch batch = splitSource.getNextBatch(10).get();

        assertThat(batch.getSplits()).containsExactly(first, second);
        assertThat(batch.isNoMoreSplits()).isTrue();
        assertThat(queuedSplitCount(splitSource)).isEqualTo(0);
    }

    @Test
    public void testSplitSourceRejectsNullSplits()
    {
        assertThatThrownBy(() -> new PaimonSplitSource(Arrays.asList(new PaimonSplit("serialized", 0.1), null), OptionalLong.empty()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("splits contains null split");
    }

    @Test
    public void testSplitSourceRejectsNegativeLimit()
    {
        assertThatThrownBy(() -> new PaimonSplitSource(List.of(), OptionalLong.of(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("limit must be non-negative");
    }

    @Test
    public void testSplitSourceCloseMarksFinishedAndClearsQueuedSplits()
            throws Exception
    {
        PaimonSplit first = new PaimonSplit("serialized-1", 0.1);
        PaimonSplit second = new PaimonSplit("serialized-2", 0.1);
        PaimonSplitSource splitSource = new PaimonSplitSource(List.of(first, second), OptionalLong.empty());

        splitSource.close();

        ConnectorSplitSource.ConnectorSplitBatch batch = splitSource.getNextBatch(10).get();

        assertThat(splitSource.isFinished()).isTrue();
        assertThat(batch.getSplits()).isEmpty();
        assertThat(batch.isNoMoreSplits()).isTrue();
        assertThat(queuedSplitCount(splitSource)).isEqualTo(0);
    }

    private record TestingSplit(long rowCount, Long mergedRowCountValue) implements Split
    {
        private TestingSplit(long rowCount)
        {
            this(rowCount, null);
        }

        @Override
        public OptionalLong mergedRowCount()
        {
            return mergedRowCountValue == null ? OptionalLong.empty() : OptionalLong.of(mergedRowCountValue);
        }

        @Override
        public Optional<List<RawFile>> convertToRawFiles()
        {
            return Optional.empty();
        }
    }

    private static String appendJsonField(String json, String field)
    {
        return json.substring(0, json.length() - 1) + "," + field + "}";
    }

    private static String typedHandleId(Class<?> handleClass)
    {
        return "paimon:" + handleClass.getName();
    }

    private static int queuedSplitCount(PaimonSplitSource splitSource)
            throws Exception
    {
        Field splits = PaimonSplitSource.class.getDeclaredField("splits");
        splits.setAccessible(true);
        return ((java.util.Queue<?>) splits.get(splitSource)).size();
    }

    private static ReadBuilder unusedReadBuilder()
    {
        return (ReadBuilder) Proxy.newProxyInstance(
                ReadBuilder.class.getClassLoader(),
                new Class<?>[] {ReadBuilder.class},
                (proxy, method, args) -> {
                    throw new UnsupportedOperationException("not used");
                });
    }

    private static final class TestingReadBuilder
            implements ReadBuilder
    {
        private List<org.apache.paimon.utils.Range> rowRanges = List.of();
        private boolean filterApplied;
        private OptionalInt limit = OptionalInt.empty();

        @Override
        public String tableName()
        {
            return "testing-table";
        }

        @Override
        public RowType readType()
        {
            return table().rowType();
        }

        @Override
        public ReadBuilder withFilter(org.apache.paimon.predicate.Predicate predicate)
        {
            filterApplied = true;
            return this;
        }

        @Override
        public ReadBuilder withPartitionFilter(Map<String, String> partitionSpec)
        {
            return this;
        }

        @Override
        public ReadBuilder withPartitionFilter(org.apache.paimon.partition.PartitionPredicate partitionPredicate)
        {
            return this;
        }

        @Override
        public ReadBuilder withBucket(int bucket)
        {
            return this;
        }

        @Override
        public ReadBuilder withBucketFilter(org.apache.paimon.utils.Filter<Integer> bucketFilter)
        {
            return this;
        }

        @Override
        public ReadBuilder withReadType(RowType readType)
        {
            return this;
        }

        @Override
        public ReadBuilder withProjection(int[] projection)
        {
            return this;
        }

        @Override
        public ReadBuilder withLimit(int limit)
        {
            this.limit = OptionalInt.of(limit);
            return this;
        }

        @Override
        public ReadBuilder withTopN(org.apache.paimon.predicate.TopN topN)
        {
            return this;
        }

        @Override
        public ReadBuilder withShard(int indexOfThisSubtask, int numberOfParallelSubtasks)
        {
            return this;
        }

        @Override
        public ReadBuilder withRowRanges(List<org.apache.paimon.utils.Range> rowRanges)
        {
            this.rowRanges = List.copyOf(rowRanges);
            return this;
        }

        @Override
        public ReadBuilder withRowRangeIndex(org.apache.paimon.utils.RowRangeIndex rowRangeIndex)
        {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public ReadBuilder dropStats()
        {
            return this;
        }

        @Override
        public TableScan newScan()
        {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public org.apache.paimon.table.source.StreamTableScan newStreamScan()
        {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public org.apache.paimon.table.source.TableRead newRead()
        {
            throw new UnsupportedOperationException("not used");
        }

        private List<org.apache.paimon.utils.Range> rowRanges()
        {
            return rowRanges;
        }

        private boolean filterApplied()
        {
            return filterApplied;
        }

        private OptionalInt limit()
        {
            return limit;
        }
    }

    private static PaimonSplitManager splitManager()
    {
        return splitManager(null);
    }

    private static PaimonSplitManager splitManager(RecordingCatalog catalog)
    {
        return new PaimonSplitManager(new PaimonMetadataFactory(new Options(),
                session -> {
                    throw new AssertionError("filesystem should not be used by split planning test");
                },
                TESTING_TYPE_MANAGER)
        {
            @Override
            public PaimonMetadata create()
            {
                if (catalog != null) {
                    return new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
                }
                return super.create();
            }
        });
    }

    private static Table table()
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));
        return (Table) Proxy.newProxyInstance(
                TrinoSplitTest.class.getClassLoader(),
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
                TrinoSplitTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "copyWithLatestSchema" -> proxy;
                    case "newReadBuilder" -> readBuilder(latestRowType);
                    case "coreOptions" -> new org.apache.paimon.CoreOptions(new org.apache.paimon.options.Options());
                    case "rowType" -> latestRowType;
                    case "toString" -> "latest-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        return (FileStoreTable) Proxy.newProxyInstance(
                TrinoSplitTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield latestTable;
                    }
                    case "coreOptions" -> new org.apache.paimon.CoreOptions(new org.apache.paimon.options.Options());
                    case "newReadBuilder" -> throw new AssertionError("stale table must not be used for split planning");
                    case "rowType" -> throw new AssertionError("stale rowType must not be used for split planning");
                    case "toString" -> "stale-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static ReadBuilder readBuilder(RowType rowType)
    {
        return (ReadBuilder) Proxy.newProxyInstance(
                TrinoSplitTest.class.getClassLoader(),
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

    private static TableScan tableScan()
    {
        return (TableScan) Proxy.newProxyInstance(
                TrinoSplitTest.class.getClassLoader(),
                new Class<?>[] {TableScan.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "plan" -> (TableScan.Plan) () -> List.of();
                    case "toString" -> "testing-table-scan";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Table unsupportedPlanningTable()
    {
        return (Table) Proxy.newProxyInstance(
                TrinoSplitTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "newReadBuilder" -> unsupportedPlanningReadBuilder();
                    case "rowType" -> DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));
                    case "toString" -> "unsupported-planning-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Table failingPlanningTable(String message)
    {
        return (Table) Proxy.newProxyInstance(
                TrinoSplitTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "newReadBuilder" -> failingPlanningReadBuilder(message);
                    case "rowType" -> DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));
                    case "toString" -> "failing-planning-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static ReadBuilder unsupportedPlanningReadBuilder()
    {
        return (ReadBuilder) Proxy.newProxyInstance(
                TrinoSplitTest.class.getClassLoader(),
                new Class<?>[] {ReadBuilder.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "dropStats", "withFilter", "withLimit" -> proxy;
                    case "newScan" -> {
                        throw new UnsupportedOperationException("unsupported scan mode");
                    }
                    case "readType" -> DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));
                    case "tableName" -> "unsupported-planning-table";
                    case "toString" -> "unsupported-planning-read-builder";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static ReadBuilder failingPlanningReadBuilder(String message)
    {
        return (ReadBuilder) Proxy.newProxyInstance(
                TrinoSplitTest.class.getClassLoader(),
                new Class<?>[] {ReadBuilder.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "dropStats", "withFilter", "withLimit" -> proxy;
                    case "newScan" -> throw new UncheckedIOException(new IOException(message));
                    case "readType" -> DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));
                    case "tableName" -> "failing-planning-table";
                    case "toString" -> "failing-planning-read-builder";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static class RecordingCatalog
            extends io.trino.plugin.paimon.catalog.PaimonCatalog
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
                throw new AssertionError("filesystem should not be used by split planning test");
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
        public org.apache.paimon.catalog.Catalog forSession(io.trino.spi.connector.ConnectorSession connectorSession)
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
