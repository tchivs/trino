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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.StringUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static org.apache.paimon.fileindex.FileIndexCommon.toMapKey;

public class PaimonFilterExtractor
{
    public static final String TRINO_MAP_ELEMENT_AT_FUNCTION_NAME = "element_at";
    private static final String DERIVED_PARTITION_COLUMNS_OPTION = "trino.derived-partition-columns";
    private static final DateTimeFormatter YEAR_MONTH_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd");

    private PaimonFilterExtractor()
    {
    }

    /**
     * Extract filter from trino, include ExpressionFilter.
     *
     * @param catalog
     *            the Trino catalog
     * @param paimonTableHandle
     *            the Trino table handle
     * @param constraint
     *            the constraint to extract filters from
     * @return an Optional containing the extracted TrinoFilter, or empty if no new
     *         filters
     */
    public static Optional<TrinoFilter> extract(PaimonCatalog catalog, PaimonTableHandle paimonTableHandle,
            Constraint constraint)
    {
        org.apache.paimon.table.Table table = paimonTableHandle.table(catalog);
        RowType rowType = table.rowType();
        TupleDomain<PaimonColumnHandle> oldFilter = paimonTableHandle.getFilter();
        TupleDomain<PaimonColumnHandle> newFilter = constraint.getSummary().transformKeys(PaimonColumnHandle.class::cast)
                .intersect(oldFilter);

        Map<PaimonColumnHandle, Domain> trinoColumnHandleForExpressionFilter = extractTrinoColumnHandleForExpressionFilter(
                constraint);
        List<PaimonLikeFilter> likeFilters = extractLikeFilters(constraint);
        List<PaimonLikeFilter> mergedLikeFilters = mergeLikeFilters(paimonTableHandle.getLikeFilters(), likeFilters);
        Map<PaimonColumnHandle, Domain> likeDomains = extractLikeDomains(rowType, mergedLikeFilters);
        if (!likeDomains.isEmpty()) {
            newFilter = newFilter.intersect(TupleDomain.withColumnDomains(likeDomains));
        }
        // Infer partition filters from timestamp predicates (e.g., jjsj >= '2025-10-01' => dt IN ('2025-10', '2025-11'))
        newFilter = inferDerivedPartitionFilters(
                newFilter,
                rowType,
                table.partitionKeys(),
                table.options().get(DERIVED_PARTITION_COLUMNS_OPTION));

        newFilter = removeRedundantDerivedPartitionFilters(
                newFilter,
                table.partitionKeys(),
                table.options().get(DERIVED_PARTITION_COLUMNS_OPTION));

        if (oldFilter.equals(newFilter)
                && trinoColumnHandleForExpressionFilter.isEmpty()
                && mergedLikeFilters.equals(paimonTableHandle.getLikeFilters())) {
            return Optional.empty();
        }

        LinkedHashMap<PaimonColumnHandle, Domain> acceptedDomains = new LinkedHashMap<>();
        LinkedHashMap<PaimonColumnHandle, Domain> unsupportedDomains = new LinkedHashMap<>();
        new PaimonFilterConverter(rowType).convert(newFilter, acceptedDomains,
                unsupportedDomains);

        List<String> partitionKeys = paimonTableHandle.table(catalog).partitionKeys();
        LinkedHashMap<PaimonColumnHandle, Domain> unenforcedDomains = new LinkedHashMap<>();
        acceptedDomains.forEach((columnHandle, domain) -> {
            if (!partitionKeys.contains(columnHandle.getColumnName())) {
                unenforcedDomains.put(columnHandle, domain);
            }
        });

        acceptedDomains.putAll(trinoColumnHandleForExpressionFilter);

        @SuppressWarnings({"unchecked", "rawtypes"})
        TupleDomain<ColumnHandle> remain = (TupleDomain) TupleDomain.withColumnDomains(unsupportedDomains)
                .intersect(TupleDomain.withColumnDomains(unenforcedDomains));

        ConnectorExpression remainingExpression = computeRemainingExpression(constraint, remain);

        return Optional.of(new TrinoFilter(
                TupleDomain.withColumnDomains(acceptedDomains),
                remain,
                remainingExpression,
                mergedLikeFilters));
    }

    @VisibleForTesting
    static ConnectorExpression computeRemainingExpression(Constraint constraint, TupleDomain<ColumnHandle> remain)
    {
        // Always preserve the original expression to ensure unsupported predicates (like LIKE)
        // are evaluated by Trino. Only return TRUE if the original expression was already TRUE.
        if (constraint.getExpression().equals(Constant.TRUE) && remain.isAll()) {
            return Constant.TRUE;
        }
        return constraint.getExpression();
    }

    /**
     * Extract Expression filter from trino Constraint. Extract Trino Expression
     * filter ( e.g. element_at(jsonmap, 'a') = '1' ) to PaimonColumnHandle.
     *
     * @param constraint
     *            the constraint to extract expression filters from
     * @return a map of PaimonColumnHandle to Domain representing the extracted
     *         expression filters
     */
    public static Map<PaimonColumnHandle, Domain> extractTrinoColumnHandleForExpressionFilter(Constraint constraint)
    {
        Map<PaimonColumnHandle, Domain> expressionPredicates = Collections.emptyMap();

        if (constraint.getExpression() instanceof Call expression) {
            Map<String, ColumnHandle> assignments = constraint.getAssignments();

            if (expression.getFunctionName().equals(EQUAL_OPERATOR_FUNCTION_NAME)) {
                expressionPredicates = handleExpressionEqualOrIn(assignments, expression, false);
            }
            else if (expression.getFunctionName().equals(IN_PREDICATE_FUNCTION_NAME)) {
                expressionPredicates = handleExpressionEqualOrIn(assignments, expression, true);
            }
            else if (expression.getFunctionName().equals(AND_FUNCTION_NAME)) {
                expressionPredicates = handleAndArguments(assignments, expression);
            }
            else if (expression.getFunctionName().equals(OR_FUNCTION_NAME)) {
                expressionPredicates = handleOrArguments(assignments, expression);
            }
        }
        return expressionPredicates;
    }

    @VisibleForTesting
    static List<PaimonLikeFilter> extractLikeFilters(Constraint constraint)
    {
        return extractLikeFilters(constraint.getExpression(), constraint.getAssignments());
    }

    /** Expression filter support the case of "AND" and "IN". */
    private static Map<PaimonColumnHandle, Domain> handleAndArguments(Map<String, ColumnHandle> assignments,
            Call expression)
    {
        Map<PaimonColumnHandle, Domain> expressionPredicates = new HashMap<>();

        for (ConnectorExpression argument : expression.getArguments()) {
            if (!(argument instanceof Call call)) {
                continue;
            }
            if (call.getFunctionName().equals(EQUAL_OPERATOR_FUNCTION_NAME)) {
                mergeConjunct(expressionPredicates, handleExpressionEqualOrIn(assignments, call, false));
            }
            else if (call.getFunctionName().equals(IN_PREDICATE_FUNCTION_NAME)) {
                mergeConjunct(expressionPredicates, handleExpressionEqualOrIn(assignments, call, true));
            }
            else if (call.getFunctionName().equals(AND_FUNCTION_NAME)) {
                mergeConjunct(expressionPredicates, handleAndArguments(assignments, call));
            }
        }

        return expressionPredicates;
    }

    private static List<PaimonLikeFilter> extractLikeFilters(ConnectorExpression expression,
            Map<String, ColumnHandle> assignments)
    {
        if (!(expression instanceof Call call)) {
            return List.of();
        }
        if (call.getFunctionName().equals(AND_FUNCTION_NAME)) {
            List<PaimonLikeFilter> filters = new ArrayList<>();
            for (ConnectorExpression argument : call.getArguments()) {
                filters.addAll(extractLikeFilters(argument, assignments));
            }
            return filters;
        }
        if (call.getFunctionName().equals(LIKE_FUNCTION_NAME)) {
            return tryExtractLikePredicate(call, assignments)
                    .map(List::of)
                    .orElseGet(List::of);
        }
        return List.of();
    }

    /**
     * Expression filter support for "OR" clause. Handles OR expressions by
     * combining domains for the same column.
     */
    private static Map<PaimonColumnHandle, Domain> handleOrArguments(Map<String, ColumnHandle> assignments,
            Call expression)
    {
        Map<PaimonColumnHandle, Domain> combinedPredicates = new HashMap<>();
        PaimonColumnHandle commonColumn = null;

        // Collect all predicates from OR arguments
        for (ConnectorExpression argument : expression.getArguments()) {
            if (!(argument instanceof Call call)) {
                // Cannot handle non-Call arguments in OR, return empty map
                return Collections.emptyMap();
            }

            Map<PaimonColumnHandle, Domain> argumentPredicates;

            if (call.getFunctionName().equals(EQUAL_OPERATOR_FUNCTION_NAME)) {
                argumentPredicates = handleExpressionEqualOrIn(assignments, call, false);
            }
            else if (call.getFunctionName().equals(IN_PREDICATE_FUNCTION_NAME)) {
                argumentPredicates = handleExpressionEqualOrIn(assignments, call, true);
            }
            else {
                // Unsupported function in OR, return empty map
                return Collections.emptyMap();
            }

            // OR pushdown is only safe when every disjunct constrains the same single column.
            // Otherwise, translating into TupleDomain would effectively AND the constraints and can cause data loss.
            if (argumentPredicates.size() != 1) {
                return Collections.emptyMap();
            }

            Map.Entry<PaimonColumnHandle, Domain> entry = argumentPredicates.entrySet().iterator().next();
            if (commonColumn == null) {
                commonColumn = entry.getKey();
            }
            else if (!commonColumn.equals(entry.getKey())) {
                return Collections.emptyMap();
            }

            combinedPredicates.merge(commonColumn, entry.getValue(), Domain::union);
        }

        return combinedPredicates;
    }

    private static Optional<PaimonLikeFilter> tryExtractLikePredicate(Call expression, Map<String, ColumnHandle> assignments)
    {
        List<ConnectorExpression> arguments = expression.getArguments();
        if (arguments.size() < 2 || arguments.size() > 3) {
            return Optional.empty();
        }
        if (!(arguments.get(0) instanceof Variable variable)) {
            return Optional.empty();
        }
        if (!(arguments.get(1) instanceof Constant patternConstant)) {
            return Optional.empty();
        }
        Object patternValue = patternConstant.getValue();
        if (!(patternValue instanceof Slice patternSlice)) {
            return Optional.empty();
        }

        Optional<String> escape = Optional.empty();
        if (arguments.size() == 3) {
            if (!(arguments.get(2) instanceof Constant escapeConstant)) {
                return Optional.empty();
            }
            Object escapeValue = escapeConstant.getValue();
            if (!(escapeValue instanceof Slice escapeSlice)) {
                return Optional.empty();
            }
            String escapeString = escapeSlice.toStringUtf8();
            if (escapeString.length() != 1) {
                return Optional.empty();
            }
            if (!"\\".equals(escapeString)) {
                return Optional.empty();
            }
            escape = Optional.of(escapeString);
        }

        ColumnHandle columnHandle = assignments.get(variable.getName());
        if (!(columnHandle instanceof PaimonColumnHandle paimonColumn)) {
            return Optional.empty();
        }

        Type type = paimonColumn.getTrinoType();
        if (!(type instanceof VarcharType || type instanceof CharType)) {
            return Optional.empty();
        }

        return Optional.of(new PaimonLikeFilter(paimonColumn.getColumnName(), patternSlice.toStringUtf8(), escape));
    }

    private static Map<PaimonColumnHandle, Domain> handleExpressionEqualOrIn(Map<String, ColumnHandle> assignments,
            Call expression, boolean inClause)
    {
        if (expression.getArguments().size() != 2) {
            return Collections.emptyMap();
        }

        ConnectorExpression left = expression.getArguments().get(0);
        ConnectorExpression right = expression.getArguments().get(1);

        Optional<Map<PaimonColumnHandle, Domain>> extracted = inClause
                ? tryExtractInPredicate(assignments, left, right).or(() -> tryExtractInPredicate(assignments, right, left))
                : tryExtractEqualityPredicate(assignments, left, right).or(() -> tryExtractEqualityPredicate(assignments, right, left));

        return extracted.orElseGet(Collections::emptyMap);
    }

    /**
     * Using paimon, trino only supports element_at function to extract values from
     * map type.
     */
    private static Map<PaimonColumnHandle, Domain> handleElementAtArguments(Map<String, ColumnHandle> assignments,
            String columnName, String nestedName, Type elementType, List<Range> ranges)
    {
        if (ranges.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<PaimonColumnHandle, Domain> expressionPredicates = new HashMap<>();

        ColumnHandle columnHandle = assignments.get(columnName);
        if (!(columnHandle instanceof PaimonColumnHandle paimonColumnHandle)) {
            return Collections.emptyMap();
        }

        Type trinoType = paimonColumnHandle.getTrinoType();
        if (trinoType instanceof MapType) {
            String mapKey = toMapKey(columnName, nestedName);
            expressionPredicates.put(
                    PaimonColumnHandle.of(mapKey, PaimonTypeUtils.toPaimonType(trinoType)),
                    Domain.create(SortedRangeSet.copyOf(elementType, ranges), false));
        }
        return expressionPredicates;
    }

    private static Optional<Map<PaimonColumnHandle, Domain>> tryExtractEqualityPredicate(
            Map<String, ColumnHandle> assignments,
            ConnectorExpression elementAtCandidate,
            ConnectorExpression valueCandidate)
    {
        Optional<ElementAtArguments> elementAt = tryExtractElementAt(elementAtCandidate);
        if (elementAt.isEmpty()) {
            return Optional.empty();
        }

        if (!(valueCandidate instanceof Constant valueConstant)) {
            return Optional.empty();
        }

        if (valueConstant.getValue() == null) {
            return Optional.empty();
        }

        if (!valueConstant.getType().equals(elementAt.get().elementType())) {
            return Optional.empty();
        }

        return Optional.of(handleElementAtArguments(
                assignments,
                elementAt.get().columnName(),
                elementAt.get().nestedName(),
                elementAt.get().elementType(),
                List.of(Range.equal(valueConstant.getType(), valueConstant.getValue()))));
    }

    private static Optional<Map<PaimonColumnHandle, Domain>> tryExtractInPredicate(
            Map<String, ColumnHandle> assignments,
            ConnectorExpression elementAtCandidate,
            ConnectorExpression arrayCandidate)
    {
        Optional<ElementAtArguments> elementAt = tryExtractElementAt(elementAtCandidate);
        if (elementAt.isEmpty()) {
            return Optional.empty();
        }

        if (!(arrayCandidate instanceof Call arrayConstructorCall)) {
            return Optional.empty();
        }

        if (!arrayConstructorCall.getFunctionName().equals(ARRAY_CONSTRUCTOR_FUNCTION_NAME)) {
            return Optional.empty();
        }

        if (!(arrayConstructorCall.getType() instanceof ArrayType arrayType)) {
            return Optional.empty();
        }

        Type elementType = arrayType.getElementType();
        if (!elementType.equals(elementAt.get().elementType())) {
            return Optional.empty();
        }

        List<Range> ranges = new ArrayList<>();
        for (ConnectorExpression argument : arrayConstructorCall.getArguments()) {
            if (!(argument instanceof Constant constant)) {
                return Optional.empty();
            }
            if (constant.getValue() == null) {
                continue;
            }
            if (!constant.getType().equals(elementType)) {
                return Optional.empty();
            }
            ranges.add(Range.equal(elementType, constant.getValue()));
        }

        return Optional.of(handleElementAtArguments(
                assignments,
                elementAt.get().columnName(),
                elementAt.get().nestedName(),
                elementType,
                ranges));
    }

    private static Optional<ElementAtArguments> tryExtractElementAt(ConnectorExpression expression)
    {
        if (!(expression instanceof Call call)) {
            return Optional.empty();
        }

        if (!TRINO_MAP_ELEMENT_AT_FUNCTION_NAME.equals(call.getFunctionName().getName())) {
            return Optional.empty();
        }

        if (call.getArguments().size() != 2) {
            return Optional.empty();
        }

        if (!(call.getArguments().get(0) instanceof Variable variable)) {
            return Optional.empty();
        }

        if (!(call.getArguments().get(1) instanceof Constant constantKey)) {
            return Optional.empty();
        }

        Object keyValue = constantKey.getValue();
        if (!(keyValue instanceof Slice slice)) {
            return Optional.empty();
        }

        return Optional.of(new ElementAtArguments(variable.getName(), slice.toStringUtf8(), call.getType()));
    }

    private static void mergeConjunct(Map<PaimonColumnHandle, Domain> target, Map<PaimonColumnHandle, Domain> addition)
    {
        addition.forEach((column, domain) -> target.merge(column, domain, Domain::intersect));
    }

    private static List<PaimonLikeFilter> mergeLikeFilters(List<PaimonLikeFilter> existing, List<PaimonLikeFilter> incoming)
    {
        if (incoming.isEmpty()) {
            return existing;
        }
        if (existing.isEmpty()) {
            return incoming;
        }
        LinkedHashMap<PaimonLikeFilter, Boolean> merged = new LinkedHashMap<>();
        existing.forEach(filter -> merged.put(filter, Boolean.TRUE));
        incoming.forEach(filter -> merged.put(filter, Boolean.TRUE));
        return List.copyOf(merged.keySet());
    }

    /**
     * Infer partition filters from timestamp predicates based on derived partition column configuration.
     * For example, if dt=jjsj:yyyy-MM and query has jjsj >= '2025-10-01' AND jjsj < '2025-12-01',
     * this method will infer dt IN ('2025-10', '2025-11').
     */
    @VisibleForTesting
    static TupleDomain<PaimonColumnHandle> inferDerivedPartitionFilters(
            TupleDomain<PaimonColumnHandle> filter,
            RowType rowType,
            List<String> partitionKeys,
            String derivedPartitionColumns)
    {
        if (filter.isAll() || filter.isNone() || StringUtils.isNullOrWhitespaceOnly(derivedPartitionColumns)) {
            return filter;
        }
        Optional<Map<PaimonColumnHandle, Domain>> domains = filter.getDomains();
        if (domains.isEmpty()) {
            return filter;
        }
        List<DerivedPartitionColumn> mappings = parseDerivedPartitionColumns(derivedPartitionColumns);
        if (mappings.isEmpty()) {
            return filter;
        }

        Set<String> normalizedPartitionKeys = partitionKeys.stream()
                .map(FieldNameUtils::toLowerCase)
                .collect(java.util.stream.Collectors.toSet());

        Map<PaimonColumnHandle, Domain> updated = new LinkedHashMap<>(domains.get());
        boolean changed = false;

        for (DerivedPartitionColumn mapping : mappings) {
            if (!normalizedPartitionKeys.contains(mapping.partitionColumn())) {
                continue;
            }
            // Skip if partition filter already exists
            Optional<PaimonColumnHandle> existingPartitionHandle = findHandle(updated, mapping.partitionColumn());
            if (existingPartitionHandle.isPresent()) {
                continue;
            }
            // Find source column handle
            Optional<PaimonColumnHandle> sourceHandle = findHandle(updated, mapping.sourceColumn());
            if (sourceHandle.isEmpty()) {
                continue;
            }
            Domain sourceDomain = updated.get(sourceHandle.get());
            Optional<Domain> inferredPartitionDomain = inferPartitionDomainFromTimestamp(
                    sourceDomain, mapping.pattern(), rowType, mapping.partitionColumn());
            if (inferredPartitionDomain.isPresent()) {
                PaimonColumnHandle partitionHandle = createPartitionColumnHandle(rowType, mapping.partitionColumn());
                if (partitionHandle != null) {
                    updated.put(partitionHandle, inferredPartitionDomain.get());
                    changed = true;
                }
            }
        }

        if (!changed) {
            return filter;
        }
        return TupleDomain.withColumnDomains(updated);
    }

    private static PaimonColumnHandle createPartitionColumnHandle(RowType rowType, String columnName)
    {
        List<String> fieldNames = FieldNameUtils.fieldNames(rowType);
        List<String> originFieldNames = rowType.getFieldNames();
        int index = fieldNames.indexOf(columnName);
        if (index < 0) {
            return null;
        }
        return PaimonColumnHandle.of(originFieldNames.get(index), rowType.getTypeAt(index));
    }

    private static Optional<Domain> inferPartitionDomainFromTimestamp(
            Domain sourceDomain,
            String pattern,
            RowType rowType,
            String partitionColumn)
    {
        Type sourceType = sourceDomain.getType();
        if (!(sourceType instanceof TimestampType timestampType)) {
            return Optional.empty();
        }

        // Extract all ranges from the source domain
        if (sourceDomain.getValues().isAll() || sourceDomain.getValues().isNone()) {
            return Optional.empty();
        }

        List<Range> ranges = sourceDomain.getValues().getRanges().getOrderedRanges();
        if (ranges.isEmpty()) {
            return Optional.empty();
        }

        // Collect all partition values from all ranges
        List<String> allPartitionValues = new ArrayList<>();
        for (Range range : ranges) {
            List<String> partitionValues = computePartitionValuesFromRange(range, timestampType, pattern);
            allPartitionValues.addAll(partitionValues);
        }

        if (allPartitionValues.isEmpty()) {
            return Optional.empty();
        }

        // Remove duplicates and create domain
        List<String> uniqueValues = allPartitionValues.stream().distinct().toList();

        // Find partition column type
        Type partitionType = findPartitionType(rowType, partitionColumn);
        if (partitionType == null) {
            partitionType = VarcharType.VARCHAR;
        }

        List<Range> partitionRanges = new ArrayList<>();
        for (String value : uniqueValues) {
            partitionRanges.add(Range.equal(partitionType, Slices.utf8Slice(value)));
        }

        return Optional.of(Domain.create(SortedRangeSet.copyOf(partitionType, partitionRanges), false));
    }

    private static Type findPartitionType(RowType rowType, String columnName)
    {
        List<String> fieldNames = FieldNameUtils.fieldNames(rowType);
        int index = fieldNames.indexOf(columnName);
        if (index < 0) {
            return null;
        }
        return PaimonTypeUtils.fromPaimonType(rowType.getTypeAt(index));
    }

    private static List<String> computePartitionValuesFromRange(Range range, TimestampType timestampType, String pattern)
    {
        List<String> values = new ArrayList<>();

        OptionalLong lowMicros = OptionalLong.empty();
        OptionalLong highMicros = OptionalLong.empty();

        if (!range.isLowUnbounded()) {
            lowMicros = toEpochMicros(timestampType, range.getLowBoundedValue());
        }
        if (!range.isHighUnbounded()) {
            highMicros = toEpochMicros(timestampType, range.getHighBoundedValue());
        }

        // If both bounds are unbounded, we can't infer partition values
        if (lowMicros.isEmpty() && highMicros.isEmpty()) {
            return values;
        }

        // Convert micros to LocalDateTime
        LocalDateTime lowDateTime = lowMicros.isPresent()
                ? LocalDateTime.ofEpochSecond(lowMicros.getAsLong() / MICROSECONDS_PER_SECOND,
                        (int) ((lowMicros.getAsLong() % MICROSECONDS_PER_SECOND) * 1000),
                        ZoneOffset.UTC)
                : null;
        LocalDateTime highDateTime = highMicros.isPresent()
                ? LocalDateTime.ofEpochSecond(highMicros.getAsLong() / MICROSECONDS_PER_SECOND,
                        (int) ((highMicros.getAsLong() % MICROSECONDS_PER_SECOND) * 1000),
                        ZoneOffset.UTC)
                : null;

        // Adjust for exclusive bounds
        if (lowDateTime != null && !range.isLowInclusive()) {
            lowDateTime = lowDateTime.plusNanos(1);
        }
        if (highDateTime != null && range.isHighInclusive()) {
            highDateTime = highDateTime.plusNanos(1);
        }

        // Generate partition values based on pattern
        if ("yyyy-MM".equals(pattern)) {
            values.addAll(generateYearMonthValues(lowDateTime, highDateTime));
        }
        else if ("yyyy-MM-dd".equals(pattern)) {
            values.addAll(generateDateValues(lowDateTime, highDateTime));
        }

        return values;
    }

    private static List<String> generateYearMonthValues(LocalDateTime lowDateTime, LocalDateTime highDateTime)
    {
        List<String> values = new ArrayList<>();

        // Default bounds if one side is unbounded (limit to reasonable range)
        if (lowDateTime == null) {
            lowDateTime = highDateTime.minusYears(1);
        }
        if (highDateTime == null) {
            highDateTime = lowDateTime.plusYears(1);
        }

        YearMonth start = YearMonth.from(lowDateTime);
        // For exclusive upper bound, if highDateTime is exactly at month start, exclude that month
        YearMonth end = YearMonth.from(highDateTime);
        if (highDateTime.getDayOfMonth() == 1 && highDateTime.getHour() == 0
                && highDateTime.getMinute() == 0 && highDateTime.getSecond() == 0
                && highDateTime.getNano() == 0) {
            end = end.minusMonths(1);
        }

        // Limit to prevent excessive partition values (max 24 months)
        int maxMonths = 24;
        int count = 0;

        YearMonth current = start;
        while (!current.isAfter(end) && count < maxMonths) {
            values.add(current.format(YEAR_MONTH_FORMATTER));
            current = current.plusMonths(1);
            count++;
        }

        return values;
    }

    private static List<String> generateDateValues(LocalDateTime lowDateTime, LocalDateTime highDateTime)
    {
        List<String> values = new ArrayList<>();

        // Default bounds if one side is unbounded (limit to reasonable range)
        if (lowDateTime == null) {
            lowDateTime = highDateTime.minusDays(30);
        }
        if (highDateTime == null) {
            highDateTime = lowDateTime.plusDays(30);
        }

        LocalDate start = lowDateTime.toLocalDate();
        LocalDate end = highDateTime.toLocalDate();

        // Limit to prevent excessive partition values (max 366 days)
        int maxDays = 366;
        int count = 0;

        LocalDate current = start;
        while (!current.isAfter(end) && count < maxDays) {
            values.add(current.format(DATE_FORMATTER));
            current = current.plusDays(1);
            count++;
        }

        return values;
    }

    static TupleDomain<PaimonColumnHandle> removeRedundantDerivedPartitionFilters(
            TupleDomain<PaimonColumnHandle> filter,
            List<String> partitionKeys,
            String derivedPartitionColumns)
    {
        if (filter.isAll() || filter.isNone() || StringUtils.isNullOrWhitespaceOnly(derivedPartitionColumns)) {
            return filter;
        }
        Optional<Map<PaimonColumnHandle, Domain>> domains = filter.getDomains();
        if (domains.isEmpty()) {
            return filter;
        }
        List<DerivedPartitionColumn> mappings = parseDerivedPartitionColumns(derivedPartitionColumns);
        if (mappings.isEmpty()) {
            return filter;
        }

        Set<String> normalizedPartitionKeys = partitionKeys.stream()
                .map(FieldNameUtils::toLowerCase)
                .collect(java.util.stream.Collectors.toSet());

        Map<PaimonColumnHandle, Domain> updated = new LinkedHashMap<>(domains.get());
        boolean changed = false;
        for (DerivedPartitionColumn mapping : mappings) {
            if (!normalizedPartitionKeys.contains(mapping.partitionColumn())) {
                continue;
            }
            Optional<PaimonColumnHandle> partitionHandle = findHandle(updated, mapping.partitionColumn());
            Optional<PaimonColumnHandle> sourceHandle = findHandle(updated, mapping.sourceColumn());
            if (partitionHandle.isEmpty() || sourceHandle.isEmpty()) {
                continue;
            }
            Domain partitionDomain = updated.get(partitionHandle.get());
            Domain sourceDomain = updated.get(sourceHandle.get());
            if (isRedundantDerivedPartition(partitionDomain, sourceDomain, mapping.pattern())) {
                updated.remove(sourceHandle.get());
                changed = true;
            }
        }

        if (!changed) {
            return filter;
        }
        if (updated.isEmpty()) {
            return TupleDomain.all();
        }
        return TupleDomain.withColumnDomains(updated);
    }

    private static List<DerivedPartitionColumn> parseDerivedPartitionColumns(String derivedPartitionColumns)
    {
        if (StringUtils.isNullOrWhitespaceOnly(derivedPartitionColumns)) {
            return List.of();
        }
        List<DerivedPartitionColumn> mappings = new ArrayList<>();
        for (String entry : derivedPartitionColumns.split(",")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int equalsIndex = trimmed.indexOf('=');
            if (equalsIndex <= 0 || equalsIndex == trimmed.length() - 1) {
                continue;
            }
            String partitionColumn = trimmed.substring(0, equalsIndex).trim();
            String remainder = trimmed.substring(equalsIndex + 1).trim();
            int patternIndex = remainder.indexOf(':');
            if (patternIndex <= 0 || patternIndex == remainder.length() - 1) {
                continue;
            }
            String sourceColumn = remainder.substring(0, patternIndex).trim();
            String pattern = remainder.substring(patternIndex + 1).trim();
            if (partitionColumn.isEmpty() || sourceColumn.isEmpty() || pattern.isEmpty()) {
                continue;
            }
            mappings.add(new DerivedPartitionColumn(
                    FieldNameUtils.toLowerCase(partitionColumn),
                    FieldNameUtils.toLowerCase(sourceColumn),
                    pattern));
        }
        return mappings;
    }

    private static Optional<PaimonColumnHandle> findHandle(Map<PaimonColumnHandle, Domain> domains, String columnName)
    {
        return domains.keySet().stream()
                .filter(handle -> FieldNameUtils.toLowerCase(handle.getColumnName()).equals(columnName))
                .findFirst();
    }

    private static boolean isRedundantDerivedPartition(Domain partitionDomain, Domain sourceDomain, String pattern)
    {
        if (!partitionDomain.isSingleValue()) {
            return false;
        }
        Type partitionType = partitionDomain.getType();
        if (!(partitionType instanceof VarcharType || partitionType instanceof CharType)) {
            return false;
        }
        Object partitionValue = partitionDomain.getSingleValue();
        if (!(partitionValue instanceof Slice partitionSlice)) {
            return false;
        }
        Optional<PartitionRange> range = parsePartitionRange(partitionSlice.toStringUtf8(), pattern);
        if (range.isEmpty()) {
            return false;
        }

        Type sourceType = sourceDomain.getType();
        if (!(sourceType instanceof TimestampType timestampType)) {
            return false;
        }
        Optional<Range> sourceRange = extractSingleRange(sourceDomain);
        if (sourceRange.isEmpty()) {
            return false;
        }
        Range rangeValue = sourceRange.get();
        if (rangeValue.isLowUnbounded() || rangeValue.isHighUnbounded()) {
            return false;
        }
        if (!rangeValue.isLowInclusive() || rangeValue.isHighInclusive()) {
            return false;
        }

        OptionalLong low = toEpochMicros(timestampType, rangeValue.getLowBoundedValue());
        OptionalLong high = toEpochMicros(timestampType, rangeValue.getHighBoundedValue());
        if (low.isEmpty() || high.isEmpty()) {
            return false;
        }
        return low.getAsLong() == range.get().startMicros()
                && high.getAsLong() == range.get().endMicros();
    }

    private static Optional<Range> extractSingleRange(Domain domain)
    {
        if (!domain.getValues().isAll() && !domain.getValues().isNone()) {
            if (domain.getValues().getType().isOrderable()) {
                List<Range> ranges = domain.getValues().getRanges().getOrderedRanges();
                if (ranges.size() == 1) {
                    return Optional.of(ranges.get(0));
                }
            }
        }
        return Optional.empty();
    }

    private static Optional<PartitionRange> parsePartitionRange(String value, String pattern)
    {
        try {
            if ("yyyy-MM".equals(pattern)) {
                YearMonth yearMonth = YearMonth.parse(value, YEAR_MONTH_FORMATTER);
                LocalDateTime start = yearMonth.atDay(1).atStartOfDay();
                LocalDateTime end = yearMonth.plusMonths(1).atDay(1).atStartOfDay();
                return Optional.of(new PartitionRange(toEpochMicros(start), toEpochMicros(end)));
            }
            if ("yyyy-MM-dd".equals(pattern)) {
                LocalDate date = LocalDate.parse(value, DATE_FORMATTER);
                LocalDateTime start = date.atStartOfDay();
                LocalDateTime end = date.plusDays(1).atStartOfDay();
                return Optional.of(new PartitionRange(toEpochMicros(start), toEpochMicros(end)));
            }
        }
        catch (DateTimeParseException ignored) {
            return Optional.empty();
        }
        return Optional.empty();
    }

    private static long toEpochMicros(LocalDateTime dateTime)
    {
        long epochSecond = dateTime.toEpochSecond(ZoneOffset.UTC);
        return Math.addExact(Math.multiplyExact(epochSecond, MICROSECONDS_PER_SECOND), dateTime.getNano() / 1_000);
    }

    private static OptionalLong toEpochMicros(TimestampType timestampType, Object value)
    {
        if (timestampType.isShort()) {
            if (!(value instanceof Long)) {
                return OptionalLong.empty();
            }
            return OptionalLong.of((long) value);
        }
        if (!(value instanceof LongTimestamp longTimestamp)) {
            return OptionalLong.empty();
        }
        if (longTimestamp.getPicosOfMicro() != 0) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(longTimestamp.getEpochMicros());
    }

    static Map<PaimonColumnHandle, Domain> extractLikeDomains(RowType rowType, List<PaimonLikeFilter> likeFilters)
    {
        if (likeFilters.isEmpty()) {
            return Map.of();
        }
        Map<PaimonColumnHandle, Domain> domains = new LinkedHashMap<>();
        List<String> fieldNames = FieldNameUtils.fieldNames(rowType);
        List<String> originFieldNames = rowType.getFieldNames();
        for (PaimonLikeFilter likeFilter : likeFilters) {
            Optional<ParsedLikePattern> parsed = parseLikePattern(likeFilter.pattern(), likeFilter.escape());
            if (parsed.isEmpty()) {
                continue;
            }
            String fieldName = FieldNameUtils.toLowerCase(likeFilter.columnName());
            int index = fieldNames.indexOf(fieldName);
            if (index < 0) {
                continue;
            }
            PaimonColumnHandle columnHandle = PaimonColumnHandle.of(originFieldNames.get(index), rowType.getTypeAt(index));
            Optional<Domain> domain = toDomainForLike(columnHandle.getTrinoType(), parsed.get());
            domain.ifPresent(value -> domains.merge(columnHandle, value, Domain::intersect));
        }
        return domains;
    }

    static Optional<ParsedLikePattern> parseLikePattern(String pattern, Optional<String> escape)
    {
        char escapeChar = escape.map(value -> value.charAt(0)).orElse((char) 0);
        StringBuilder literal = new StringBuilder();
        for (int i = 0; i < pattern.length(); i++) {
            char current = pattern.charAt(i);
            if (escapeChar != 0 && current == escapeChar) {
                if (i + 1 >= pattern.length()) {
                    return Optional.empty();
                }
                literal.append(pattern.charAt(i + 1));
                i++;
                continue;
            }
            if (current == '_') {
                return Optional.empty();
            }
            if (current == '%') {
                if (i == pattern.length() - 1) {
                    return Optional.of(new ParsedLikePattern(literal.toString(), true));
                }
                return Optional.empty();
            }
            literal.append(current);
        }
        return Optional.of(new ParsedLikePattern(literal.toString(), false));
    }

    static Optional<Domain> toDomainForLike(Type type, ParsedLikePattern pattern)
    {
        if (!(type instanceof VarcharType || type instanceof CharType)) {
            return Optional.empty();
        }
        Slice literal = Slices.utf8Slice(pattern.literal());
        if (!pattern.hasTrailingWildcard()) {
            return Optional.of(Domain.singleValue(type, literal));
        }
        if (pattern.literal().isEmpty()) {
            return Optional.empty();
        }
        Range range = nextPrefix(literal)
                .map(next -> Range.range(type, literal, true, next, false))
                .orElseGet(() -> Range.greaterThanOrEqual(type, literal));
        return Optional.of(Domain.create(ValueSet.ofRanges(range), false));
    }

    static Optional<Slice> nextPrefix(Slice slice)
    {
        byte[] bytes = slice.getBytes();
        for (int i = bytes.length - 1; i >= 0; i--) {
            int current = bytes[i] & 0xFF;
            if (current != 0xFF) {
                byte[] next = Arrays.copyOf(bytes, i + 1);
                next[i] = (byte) (current + 1);
                return Optional.of(Slices.wrappedBuffer(next));
            }
        }
        return Optional.empty();
    }

    private record ElementAtArguments(String columnName, String nestedName, Type elementType) {}

    record ParsedLikePattern(String literal, boolean hasTrailingWildcard) {}

    private record DerivedPartitionColumn(String partitionColumn, String sourceColumn, String pattern) {}

    private record PartitionRange(long startMicros, long endMicros) {}

    /** TrinoFilter for paimon trinoMetadata applyFilter. */
    public record TrinoFilter(TupleDomain<PaimonColumnHandle> filter, TupleDomain<ColumnHandle> remainFilter,
                              ConnectorExpression remainingExpression, List<PaimonLikeFilter> likeFilters)
    {
    }
}
