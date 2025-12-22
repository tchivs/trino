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
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import org.apache.paimon.annotation.VisibleForTesting;

import java.util.ArrayList;
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
        TupleDomain<PaimonColumnHandle> oldFilter = paimonTableHandle.getFilter();
        TupleDomain<PaimonColumnHandle> newFilter = constraint.getSummary().transformKeys(PaimonColumnHandle.class::cast)
                .intersect(oldFilter);

        Map<PaimonColumnHandle, Domain> trinoColumnHandleForExpressionFilter = extractTrinoColumnHandleForExpressionFilter(
                constraint);

        if (oldFilter.equals(newFilter) && trinoColumnHandleForExpressionFilter.isEmpty()) {
            return Optional.empty();
        }

        LinkedHashMap<PaimonColumnHandle, Domain> acceptedDomains = new LinkedHashMap<>();
        LinkedHashMap<PaimonColumnHandle, Domain> unsupportedDomains = new LinkedHashMap<>();
        new PaimonFilterConverter(paimonTableHandle.table(catalog).rowType()).convert(newFilter, acceptedDomains,
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

        return Optional
                .of(new TrinoFilter(TupleDomain.withColumnDomains(acceptedDomains), remain, remainingExpression));
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

    private record ElementAtArguments(String columnName, String nestedName, Type elementType) {}

    /** TrinoFilter for paimon trinoMetadata applyFilter. */
    public record TrinoFilter(TupleDomain<PaimonColumnHandle> filter, TupleDomain<ColumnHandle> remainFilter,
                              ConnectorExpression remainingExpression)
    {
    }
}
