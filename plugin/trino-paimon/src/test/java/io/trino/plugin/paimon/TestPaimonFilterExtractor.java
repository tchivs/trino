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

import io.airlift.slice.Slices;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.expression.Constant.TRUE;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.paimon.fileindex.FileIndexCommon.toMapKey;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPaimonFilterExtractor
{
    private static final MapType MAP_VARCHAR_VARCHAR = new MapType(VARCHAR, VARCHAR, new TypeOperators());

    @Test
    public void testExtractEqualityOnElementAt()
    {
        PaimonColumnHandle mapColumn = PaimonColumnHandle.of("m", PaimonTypeUtils.toPaimonType(MAP_VARCHAR_VARCHAR));

        Call elementAt = new Call(
                VARCHAR,
                new FunctionName(PaimonFilterExtractor.TRINO_MAP_ELEMENT_AT_FUNCTION_NAME),
                List.of(new Variable("m", MAP_VARCHAR_VARCHAR), new Constant(Slices.utf8Slice("a"), VARCHAR)));
        Call equality = new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, List.of(elementAt, new Constant(Slices.utf8Slice("1"), VARCHAR)));

        Constraint constraint = new Constraint(TupleDomain.all(), equality, Map.of("m", mapColumn));
        Map<PaimonColumnHandle, Domain> extracted = PaimonFilterExtractor.extractTrinoColumnHandleForExpressionFilter(constraint);

        PaimonColumnHandle expectedHandle = PaimonColumnHandle.of(toMapKey("m", "a"), PaimonTypeUtils.toPaimonType(mapColumn.getTrinoType()));
        Domain expectedDomain = Domain.create(
                SortedRangeSet.copyOf(VARCHAR, List.of(Range.equal(VARCHAR, Slices.utf8Slice("1")))),
                false);

        assertThat(extracted).containsOnlyKeys(expectedHandle);
        assertThat(extracted.get(expectedHandle)).isEqualTo(expectedDomain);
    }

    @Test
    public void testExtractInOnElementAt()
    {
        PaimonColumnHandle mapColumn = PaimonColumnHandle.of("m", PaimonTypeUtils.toPaimonType(MAP_VARCHAR_VARCHAR));

        Call elementAt = new Call(
                VARCHAR,
                new FunctionName(PaimonFilterExtractor.TRINO_MAP_ELEMENT_AT_FUNCTION_NAME),
                List.of(new Variable("m", MAP_VARCHAR_VARCHAR), new Constant(Slices.utf8Slice("a"), VARCHAR)));

        Call arrayConstructor = new Call(
                new ArrayType(VARCHAR),
                ARRAY_CONSTRUCTOR_FUNCTION_NAME,
                List.of(new Constant(Slices.utf8Slice("1"), VARCHAR), new Constant(Slices.utf8Slice("2"), VARCHAR)));

        Call inPredicate = new Call(BOOLEAN, IN_PREDICATE_FUNCTION_NAME, List.of(elementAt, arrayConstructor));

        Constraint constraint = new Constraint(TupleDomain.all(), inPredicate, Map.of("m", mapColumn));
        Map<PaimonColumnHandle, Domain> extracted = PaimonFilterExtractor.extractTrinoColumnHandleForExpressionFilter(constraint);

        PaimonColumnHandle expectedHandle = PaimonColumnHandle.of(toMapKey("m", "a"), PaimonTypeUtils.toPaimonType(mapColumn.getTrinoType()));
        List<Range> ranges = List.of(
                Range.equal(VARCHAR, Slices.utf8Slice("1")),
                Range.equal(VARCHAR, Slices.utf8Slice("2")));
        Domain expectedDomain = Domain.create(SortedRangeSet.copyOf(VARCHAR, ranges), false);

        assertThat(extracted).containsOnlyKeys(expectedHandle);
        assertThat(extracted.get(expectedHandle)).isEqualTo(expectedDomain);
    }

    @Test
    public void testExtractOrOnlyWhenSameColumn()
    {
        PaimonColumnHandle mapColumn = PaimonColumnHandle.of("m", PaimonTypeUtils.toPaimonType(MAP_VARCHAR_VARCHAR));

        Call elementAtA = new Call(
                VARCHAR,
                new FunctionName(PaimonFilterExtractor.TRINO_MAP_ELEMENT_AT_FUNCTION_NAME),
                List.of(new Variable("m", MAP_VARCHAR_VARCHAR), new Constant(Slices.utf8Slice("a"), VARCHAR)));
        Call equal1 = new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, List.of(elementAtA, new Constant(Slices.utf8Slice("1"), VARCHAR)));
        Call equal2 = new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, List.of(elementAtA, new Constant(Slices.utf8Slice("2"), VARCHAR)));
        Call orSameKey = new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(equal1, equal2));

        Constraint constraintSame = new Constraint(TupleDomain.all(), orSameKey, Map.of("m", mapColumn));
        Map<PaimonColumnHandle, Domain> extractedSame = PaimonFilterExtractor.extractTrinoColumnHandleForExpressionFilter(constraintSame);

        PaimonColumnHandle expectedHandle = PaimonColumnHandle.of(toMapKey("m", "a"), PaimonTypeUtils.toPaimonType(mapColumn.getTrinoType()));
        List<Range> ranges = List.of(
                Range.equal(VARCHAR, Slices.utf8Slice("1")),
                Range.equal(VARCHAR, Slices.utf8Slice("2")));
        Domain expectedDomain = Domain.create(SortedRangeSet.copyOf(VARCHAR, ranges), false);

        assertThat(extractedSame).containsOnlyKeys(expectedHandle);
        assertThat(extractedSame.get(expectedHandle)).isEqualTo(expectedDomain);

        Call elementAtB = new Call(
                VARCHAR,
                new FunctionName(PaimonFilterExtractor.TRINO_MAP_ELEMENT_AT_FUNCTION_NAME),
                List.of(new Variable("m", MAP_VARCHAR_VARCHAR), new Constant(Slices.utf8Slice("b"), VARCHAR)));
        Call orDifferentKey = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(equal1, new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, List.of(elementAtB, new Constant(Slices.utf8Slice("3"), VARCHAR)))));

        Constraint constraintDifferent = new Constraint(TupleDomain.all(), orDifferentKey, Map.of("m", mapColumn));
        assertThat(PaimonFilterExtractor.extractTrinoColumnHandleForExpressionFilter(constraintDifferent)).isEmpty();
    }

    @Test
    public void testExtractAndSkipsNonCallArguments()
    {
        PaimonColumnHandle mapColumn = PaimonColumnHandle.of("m", PaimonTypeUtils.toPaimonType(MAP_VARCHAR_VARCHAR));

        Call elementAt = new Call(
                VARCHAR,
                new FunctionName(PaimonFilterExtractor.TRINO_MAP_ELEMENT_AT_FUNCTION_NAME),
                List.of(new Variable("m", MAP_VARCHAR_VARCHAR), new Constant(Slices.utf8Slice("a"), VARCHAR)));
        Call equality = new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, List.of(elementAt, new Constant(Slices.utf8Slice("1"), VARCHAR)));
        Call andPredicate = new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(equality, TRUE));

        Constraint constraint = new Constraint(TupleDomain.all(), andPredicate, Map.of("m", mapColumn));
        Map<PaimonColumnHandle, Domain> extracted = PaimonFilterExtractor.extractTrinoColumnHandleForExpressionFilter(constraint);

        PaimonColumnHandle expectedHandle = PaimonColumnHandle.of(toMapKey("m", "a"), PaimonTypeUtils.toPaimonType(mapColumn.getTrinoType()));
        assertThat(extracted).containsOnlyKeys(expectedHandle);
    }

    @Test
    public void testComputeRemainingExpressionKeepsUnsupportedPredicate()
    {
        Call like = new Call(
                BOOLEAN,
                LIKE_FUNCTION_NAME,
                List.of(new Variable("bjnr", VARCHAR), new Constant(Slices.utf8Slice("%ionia%"), VARCHAR)));

        Constraint constraint = new Constraint(TupleDomain.all(), like, Map.of("bjnr", mockColumnHandle()));
        assertThat(PaimonFilterExtractor.computeRemainingExpression(constraint, TupleDomain.all())).isEqualTo(like);
    }

    @Test
    public void testExtractLikeFilters()
    {
        PaimonColumnHandle column = PaimonColumnHandle.of("bjnr", PaimonTypeUtils.toPaimonType(VARCHAR));
        Call like = new Call(
                BOOLEAN,
                LIKE_FUNCTION_NAME,
                List.of(new Variable("bjnr", VARCHAR), new Constant(Slices.utf8Slice("abc%"), VARCHAR)));

        Constraint constraint = new Constraint(TupleDomain.all(), like, Map.of("bjnr", column));
        List<PaimonLikeFilter> filters = PaimonFilterExtractor.extractLikeFilters(constraint);

        assertThat(filters).containsExactly(new PaimonLikeFilter("bjnr", "abc%", Optional.empty()));
    }

    @Test
    public void testExtractLikeFiltersSkipsUnsupportedEscape()
    {
        PaimonColumnHandle column = PaimonColumnHandle.of("bjnr", PaimonTypeUtils.toPaimonType(VARCHAR));
        Call like = new Call(
                BOOLEAN,
                LIKE_FUNCTION_NAME,
                List.of(
                        new Variable("bjnr", VARCHAR),
                        new Constant(Slices.utf8Slice("a!%"), VARCHAR),
                        new Constant(Slices.utf8Slice("!"), VARCHAR)));

        Constraint constraint = new Constraint(TupleDomain.all(), like, Map.of("bjnr", column));
        assertThat(PaimonFilterExtractor.extractLikeFilters(constraint)).isEmpty();
    }

    @Test
    public void testExtractLikeFiltersSkipsOr()
    {
        PaimonColumnHandle column = PaimonColumnHandle.of("bjnr", PaimonTypeUtils.toPaimonType(VARCHAR));
        Call like1 = new Call(
                BOOLEAN,
                LIKE_FUNCTION_NAME,
                List.of(new Variable("bjnr", VARCHAR), new Constant(Slices.utf8Slice("abc%"), VARCHAR)));
        Call like2 = new Call(
                BOOLEAN,
                LIKE_FUNCTION_NAME,
                List.of(new Variable("bjnr", VARCHAR), new Constant(Slices.utf8Slice("def%"), VARCHAR)));
        Call orCall = new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(like1, like2));

        Constraint constraint = new Constraint(TupleDomain.all(), orCall, Map.of("bjnr", column));
        assertThat(PaimonFilterExtractor.extractLikeFilters(constraint)).isEmpty();
    }

    @Test
    public void testExtractLikeDomainsForPrefix()
    {
        RowType rowType = new RowType(List.of(new DataField(0, "name", DataTypes.STRING())));
        List<PaimonLikeFilter> filters = List.of(new PaimonLikeFilter("name", "abc%", Optional.empty()));

        Map<PaimonColumnHandle, Domain> domains = PaimonFilterExtractor.extractLikeDomains(rowType, filters);

        PaimonColumnHandle handle = PaimonColumnHandle.of("name", rowType.getTypeAt(0));
        assertThat(domains).containsOnlyKeys(handle);

        Domain domain = domains.get(handle);
        assertThat(domain).isNotNull();
        assertThat(domain.getValues().getRanges().getRangeCount()).isEqualTo(1);

        Domain expected = Domain.create(
                ValueSet.ofRanges(Range.range(
                        handle.getTrinoType(),
                        Slices.utf8Slice("abc"),
                        true,
                        PaimonFilterExtractor.nextPrefix(Slices.utf8Slice("abc")).orElseThrow(),
                        false)),
                false);
        assertThat(domain).isEqualTo(expected);
    }

    @Test
    public void testExtractLikeDomainsForExactMatch()
    {
        RowType rowType = new RowType(List.of(new DataField(0, "name", DataTypes.STRING())));
        List<PaimonLikeFilter> filters = List.of(new PaimonLikeFilter("name", "abc", Optional.empty()));

        Map<PaimonColumnHandle, Domain> domains = PaimonFilterExtractor.extractLikeDomains(rowType, filters);

        PaimonColumnHandle handle = PaimonColumnHandle.of("name", rowType.getTypeAt(0));
        assertThat(domains).containsOnlyKeys(handle);
        assertThat(domains.get(handle)).isEqualTo(Domain.singleValue(handle.getTrinoType(), Slices.utf8Slice("abc")));
    }

    @Test
    public void testExtractLikeDomainsSkipsNonPrefixPatterns()
    {
        RowType rowType = new RowType(List.of(new DataField(0, "name", DataTypes.STRING())));
        List<PaimonLikeFilter> filters = List.of(new PaimonLikeFilter("name", "a_c", Optional.empty()));

        assertThat(PaimonFilterExtractor.extractLikeDomains(rowType, filters)).isEmpty();
    }

    @Test
    public void testComputeRemainingExpressionReturnsTrueOnlyWhenAlreadyTrue()
    {
        Constraint constraint = new Constraint(TupleDomain.all(), TRUE, Map.of());
        assertThat(PaimonFilterExtractor.computeRemainingExpression(constraint, TupleDomain.all())).isEqualTo(TRUE);

        // even if remain is not ALL, keep the original expression
        assertThat(PaimonFilterExtractor.computeRemainingExpression(constraint, TupleDomain.none())).isEqualTo(TRUE);
    }

    private static ColumnHandle mockColumnHandle()
    {
        // assignments are only required to satisfy Constraint construction for these tests
        return new ColumnHandle() {};
    }
}
