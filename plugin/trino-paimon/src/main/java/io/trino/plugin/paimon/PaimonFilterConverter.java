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

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.RowType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.apache.paimon.predicate.PredicateBuilder.and;
import static org.apache.paimon.predicate.PredicateBuilder.or;

public class PaimonFilterConverter
{
    private static final Logger LOG = Logger.get(PaimonFilterConverter.class);

    private final RowType rowType;
    private final PredicateBuilder builder;

    public PaimonFilterConverter(RowType rowType)
    {
        this.rowType = rowType;
        this.builder = new PredicateBuilder(rowType);
    }

    public Optional<Predicate> convert(TupleDomain<PaimonColumnHandle> tupleDomain)
    {
        return convert(tupleDomain, new LinkedHashMap<>(), new LinkedHashMap<>());
    }

    // TODO: Enhancement - SUPPORTS_DEREFERENCE_PUSHDOWN
    // Current implementation doesn't support nested field predicates (e.g., WHERE
    // address.city = 'Beijing').
    // To support dereference pushdown:
    // 1. Parse nested paths from column names (e.g., ["address", "city"])
    // 2. Use Paimon's nested field predicate API with proper path resolution
    // 3. Ensure correct type conversion for nested ROW types
    // Reference:
    // tmp-docs/PUSHDOWN_OPTIMIZATION_GUIDE.md#4-supports_dereference_pushdown
    // Paimon API:
    // /opt/source/paimon/paimon-common/src/main/java/org/apache/paimon/types/RowType.java
    // Estimated effort: 8-12 hours
    // Priority: P2 (Medium value, moderate cost)
    public Optional<Predicate> convert(TupleDomain<PaimonColumnHandle> tupleDomain,
            HashMap<PaimonColumnHandle, Domain> acceptedDomains, HashMap<PaimonColumnHandle, Domain> unsupportedDomains)
    {
        if (tupleDomain.isAll()) {
            // alwaysTrue - no filtering needed, return empty to skip filter
            return Optional.empty();
        }

        if (tupleDomain.isNone()) {
            // alwaysFalse - filter out all rows
            return Optional.of(AlwaysFalsePredicate.INSTANCE);
        }

        Map<PaimonColumnHandle, Domain> domainMap = tupleDomain.getDomains().get();
        List<Predicate> conjuncts = new ArrayList<>();
        List<String> fieldNames = FieldNameUtils.fieldNames(rowType);
        for (Map.Entry<PaimonColumnHandle, Domain> entry : domainMap.entrySet()) {
            PaimonColumnHandle columnHandle = entry.getKey();
            Domain domain = entry.getValue();
            String field = columnHandle.getColumnName();
            Optional<Integer> nestedColumn = FileIndexOptions.topLevelIndexOfNested(field);
            if (nestedColumn.isPresent()) {
                int position = nestedColumn.get();
                field = field.substring(0, position);
            }
            // Fix case-sensitivity issue: fieldNames are lowercase, so convert field to lowercase for lookup
            int index = fieldNames.indexOf(FieldNameUtils.toLowerCase(field));
            if (index != -1) {
                try {
                    conjuncts
                            .add(toPredicate(index, columnHandle.getColumnName(), columnHandle.getTrinoType(), domain));
                    acceptedDomains.put(columnHandle, domain);
                    continue;
                }
                catch (UnsupportedOperationException exception) {
                    LOG.warn("Unsupported predicate, maybe the type of column is not supported yet.", exception);
                }
            }
            unsupportedDomains.put(columnHandle, domain);
        }

        if (conjuncts.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(and(conjuncts));
    }

    private Predicate toPredicate(int columnIndex, String field, Type type, Domain domain)
    {
        if (domain.isAll()) {
            // alwaysTrue for this column - no predicate needed, throw to skip
            throw new UnsupportedOperationException("Domain is ALL, no predicate needed for column: " + field);
        }
        if (domain.getValues().isNone()) {
            if (domain.isNullAllowed()) {
                return builder.isNull((columnIndex));
            }
            // alwaysFalse - no values match and null not allowed
            return AlwaysFalsePredicate.INSTANCE;
        }

        if (domain.getValues().isAll()) {
            if (domain.isNullAllowed()) {
                // alwaysTrue - all values including null are allowed, no predicate needed
                throw new UnsupportedOperationException("Domain allows all values including null for column: " + field);
            }
            return builder.isNotNull((columnIndex));
        }

        // Structural types (Array, Row, Map) support for NULL checks only
        // For non-NULL predicates, we throw UnsupportedOperationException to fail fast.
        // Ignoring these expressions could lead to data loss in case of deletions.
        if (type instanceof ArrayType || type instanceof io.trino.spi.type.RowType) {
            if (domain.getValues().isNone()) {
                // Only NULL values allowed - already handled above at line 141-146
                // This should not be reached, but keeping for safety
                if (domain.isNullAllowed()) {
                    return builder.isNull(columnIndex);
                }
                return AlwaysFalsePredicate.INSTANCE;
            }

            if (domain.getValues().isAll()) {
                // All non-NULL values allowed - already handled above at line 148-154
                // This should not be reached, but keeping for safety
                if (!domain.isNullAllowed()) {
                    return builder.isNotNull(columnIndex);
                }
                throw new UnsupportedOperationException("Domain allows all values including null for column: " + field);
            }

            // For structural types, we cannot push down predicates beyond NULL checks
            // because Paimon's predicate system doesn't support value-based filtering on
            // complex types
            throw new UnsupportedOperationException(
                    "Value-based predicates on structural types are not supported: " + type);
        }

        if (type instanceof MapType) {
            List<Range> orderedRanges = domain.getValues().getRanges().getOrderedRanges();
            List<Object> values = new ArrayList<>();
            List<Predicate> predicates = new ArrayList<>();
            for (Range range : orderedRanges) {
                if (range.isSingleValue()) {
                    values.add(getLiteralValue(((MapType) type).getValueType(), range.getLowBoundedValue()));
                }
            }
            if (!values.isEmpty()) {
                Predicate predicate = new LeafPredicate(In.INSTANCE, PaimonTypeUtils.toPaimonType(type), columnIndex,
                        field, values);
                predicates.add(predicate);
            }
            return or(predicates);
        }

        if (type.isOrderable()) {
            List<Range> orderedRanges = domain.getValues().getRanges().getOrderedRanges();
            List<Object> values = new ArrayList<>();
            List<Predicate> predicates = new ArrayList<>();
            for (Range range : orderedRanges) {
                if (range.isSingleValue()) {
                    values.add(getLiteralValue(type, range.getLowBoundedValue()));
                }
                else {
                    predicates.add(toPredicate(columnIndex, range));
                }
            }

            if (!values.isEmpty()) {
                predicates.add(builder.in(columnIndex, values));
            }

            if (domain.isNullAllowed()) {
                predicates.add(builder.isNull(columnIndex));
            }
            return or(predicates);
        }

        throw new UnsupportedOperationException();
    }

    private Predicate toPredicate(int columnIndex, Range range)
    {
        Type type = range.getType();

        if (range.isSingleValue()) {
            Object value = getLiteralValue(type, range.getSingleValue());
            return builder.equal(columnIndex, value);
        }

        // TODO: Enhancement - SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY
        // Current implementation supports VARCHAR equality (name = 'Alice') but not
        // inequality.
        // To support VARCHAR inequality predicates (name > 'Alice', name LIKE 'A%'):
        // 1. VARCHAR range queries already work via builder.greaterThan/lessThan (lines
        // 238-254)
        // 2. Need to add LIKE pattern support by detecting startsWith patterns
        // 3. Verify Paimon statistics support VARCHAR range filtering for effective
        // pruning
        // Reference:
        // tmp-docs/PUSHDOWN_OPTIMIZATION_GUIDE.md#2-supports_predicate_pushdown_with_varchar_inequality
        // Estimated effort: 6-8 hours
        // Priority: P1 (High value, moderate cost)
        List<Predicate> conjuncts = new ArrayList<>(2);
        if (!range.isLowUnbounded()) {
            Object low = getLiteralValue(type, range.getLowBoundedValue());
            Predicate lowBound;
            if (range.isLowInclusive()) {
                lowBound = builder.greaterOrEqual(columnIndex, low);
            }
            else {
                lowBound = builder.greaterThan(columnIndex, low);
            }
            conjuncts.add(lowBound);
        }

        if (!range.isHighUnbounded()) {
            Object high = getLiteralValue(type, range.getHighBoundedValue());
            Predicate highBound;
            if (range.isHighInclusive()) {
                highBound = builder.lessOrEqual(columnIndex, high);
            }
            else {
                highBound = builder.lessThan(columnIndex, high);
            }
            conjuncts.add(highBound);
        }

        return and(conjuncts);
    }

    private Object getLiteralValue(Type type, Object trinoNativeValue)
    {
        requireNonNull(trinoNativeValue, "trinoNativeValue is null");

        if (type instanceof BooleanType) {
            return trinoNativeValue;
        }

        if (type instanceof TinyintType) {
            return ((Long) trinoNativeValue).byteValue();
        }

        if (type instanceof SmallintType) {
            return ((Long) trinoNativeValue).shortValue();
        }

        if (type instanceof IntegerType) {
            return toIntExact((long) trinoNativeValue);
        }

        if (type instanceof BigintType) {
            return trinoNativeValue;
        }

        if (type instanceof RealType) {
            return intBitsToFloat(toIntExact((long) trinoNativeValue));
        }

        if (type instanceof DoubleType) {
            return trinoNativeValue;
        }

        if (type instanceof DateType) {
            return toIntExact(((Long) trinoNativeValue));
        }

        if (type.equals(TIME_MILLIS)) {
            return (int) ((long) trinoNativeValue / PICOSECONDS_PER_MILLISECOND);
        }

        if (type.equals(TIMESTAMP_MILLIS)) {
            return Timestamp.fromEpochMillis((long) trinoNativeValue / 1000);
        }

        if (type.equals(TIMESTAMP_TZ_MILLIS)) {
            if (trinoNativeValue instanceof Long) {
                return trinoNativeValue;
            }
            return Timestamp.fromEpochMillis(((LongTimestampWithTimeZone) trinoNativeValue).getEpochMillis());
        }

        if (type instanceof VarcharType || type instanceof CharType) {
            return BinaryString.fromBytes(((Slice) trinoNativeValue).getBytes());
        }

        if (type instanceof VarbinaryType) {
            return ((Slice) trinoNativeValue).getBytes();
        }

        if (type instanceof DecimalType decimalType) {
            BigDecimal bigDecimal;
            if (trinoNativeValue instanceof Long) {
                bigDecimal = BigDecimal.valueOf((long) trinoNativeValue).movePointLeft(decimalType.getScale());
            }
            else {
                bigDecimal = new BigDecimal(DecimalUtils.toBigInteger(trinoNativeValue), decimalType.getScale());
            }
            return Decimal.fromBigDecimal(bigDecimal, decimalType.getPrecision(), decimalType.getScale());
        }

        throw new UnsupportedOperationException("Unsupported type: " + type);
    }
}
