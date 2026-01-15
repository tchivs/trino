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
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static org.apache.paimon.fileindex.FileIndexCommon.toMapKey;

public class PaimonFilterExtractor
{
    public static final String TRINO_MAP_ELEMENT_AT_FUNCTION_NAME = "element_at";

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
        RowType rowType = paimonTableHandle.table(catalog).rowType();
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

    /** TrinoFilter for paimon trinoMetadata applyFilter. */
    public record TrinoFilter(TupleDomain<PaimonColumnHandle> filter, TupleDomain<ColumnHandle> remainFilter,
                              ConnectorExpression remainingExpression, List<PaimonLikeFilter> likeFilters)
    {
    }
}
