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
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
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

        if (oldFilter.equals(newFilter)) {
            return Optional.empty();
        }

        Map<PaimonColumnHandle, Domain> trinoColumnHandleForExpressionFilter = extractTrinoColumnHandleForExpressionFilter(
                constraint);

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

        // Determine remaining expression based on whether all filters were pushed down
        ConnectorExpression remainingExpression = remain.isAll() ? Constant.TRUE : constraint.getExpression();

        return Optional
                .of(new TrinoFilter(TupleDomain.withColumnDomains(acceptedDomains), remain, remainingExpression));
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

        expression.getArguments().stream().map(argument -> (Call) argument).forEach(argument -> {
            if (argument.getFunctionName().equals(EQUAL_OPERATOR_FUNCTION_NAME)) {
                expressionPredicates.putAll(handleExpressionEqualOrIn(assignments, argument, false));
            }
            else if (argument.getFunctionName().equals(IN_PREDICATE_FUNCTION_NAME)) {
                expressionPredicates.putAll(handleExpressionEqualOrIn(assignments, argument, true));
            }
        });

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

            // Merge domains for the same columns
            for (Map.Entry<PaimonColumnHandle, Domain> entry : argumentPredicates.entrySet()) {
                PaimonColumnHandle column = entry.getKey();
                Domain domain = entry.getValue();

                combinedPredicates.merge(column, domain, Domain::union);
            }
        }

        return combinedPredicates;
    }

    private static Map<PaimonColumnHandle, Domain> handleExpressionEqualOrIn(Map<String, ColumnHandle> assignments,
            Call expression, boolean inClause)
    {
        Call elementAtExpression = (Call) expression.getArguments().get(0);

        String functionName = elementAtExpression.getFunctionName().getName();

        switch (functionName) {
            case TRINO_MAP_ELEMENT_AT_FUNCTION_NAME : {
                Variable columnExpression = (Variable) elementAtExpression.getArguments().get(0);
                Constant columnKey = (Constant) elementAtExpression.getArguments().get(1);

                Constant elementAtValue = (Constant) expression.getArguments().get(1);
                List<Range> values;
                Type elementType;
                if (inClause) {
                    elementType = ((ArrayType) elementAtValue.getType()).getElementType();
                    values = elementAtValue.getChildren().stream().filter(a -> ((Constant) a).getValue() != null)
                            .map(arguemnt -> Range.equal(arguemnt.getType(), ((Constant) arguemnt).getValue()))
                            .collect(Collectors.toList());
                }
                else {
                    elementType = elementAtValue.getType();
                    values = elementAtValue.getValue() == null
                            ? Collections.emptyList()
                            : ImmutableList.of(Range.equal(elementAtValue.getType(), elementAtValue.getValue()));
                }
                if (columnKey.getValue() == null) {
                    throw new RuntimeException("Expression pares failed: " + expression);
                }

                return handleElementAtArguments(assignments, columnExpression.getName(),
                        ((Slice) columnKey.getValue()).toStringUtf8(), elementType, values);
            }
            default : {
                return Collections.emptyMap();
            }
        }
    }

    /**
     * Using paimon, trino only supports element_at function to extract values from
     * map type.
     */
    private static Map<PaimonColumnHandle, Domain> handleElementAtArguments(Map<String, ColumnHandle> assignments,
            String columnName, String nestedName, Type elementType, List<Range> ranges)
    {
        Map<PaimonColumnHandle, Domain> expressionPredicates = Maps.newHashMap();
        PaimonColumnHandle paimonColumnHandle = (PaimonColumnHandle) assignments.get(columnName);
        Type trinoType = paimonColumnHandle.getTrinoType();
        if (trinoType instanceof MapType) {
            expressionPredicates.put(
                    PaimonColumnHandle.of(toMapKey(columnName, nestedName), PaimonTypeUtils.toPaimonType(trinoType)),
                    Domain.create(SortedRangeSet.copyOf(elementType, ranges), false));
        }
        return expressionPredicates;
    }

    /** TrinoFilter for paimon trinoMetadata applyFilter. */
    public record TrinoFilter(TupleDomain<PaimonColumnHandle> filter, TupleDomain<ColumnHandle> remainFilter,
                              ConnectorExpression remainingExpression)
    {
    }
}
