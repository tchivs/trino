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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.utils.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class PaimonTableOptionUtils
{
    private PaimonTableOptionUtils()
    {
    }

    public static void buildOptions(Schema.Builder builder, Map<String, Object> properties)
    {
        requireNonNull(builder, "builder is null");
        requireNonNull(properties, "properties is null");
        properties.keySet().forEach(PaimonTableOptionUtils::validatePropertyKey);
        List<OptionInfo> optionInfos = PaimonTableOptionUtils.getOptionInfos();
        for (OptionInfo optionInfo : optionInfos) {
            Object rawValue = properties.get(optionInfo.trinoOptionKey);
            if (rawValue != null) {
                builder.option(optionInfo.paimonOptionKey,
                        requireNonBlankStringOptionValue(optionInfo.trinoOptionKey, rawValue));
            }
        }
    }

    static String requireNonBlankStringOptionValue(String propertyName, Object rawValue)
    {
        requireNonNull(propertyName, "propertyName is null");
        if (!(rawValue instanceof String optionValue)) {
            throw new IllegalArgumentException(
                    "properties value for property '%s' must be a string".formatted(propertyName));
        }
        if (StringUtils.isNullOrWhitespaceOnly(optionValue)) {
            throw new IllegalArgumentException(
                    "properties value for property '%s' is blank".formatted(propertyName));
        }
        return optionValue;
    }

    public static String toPaimonOptionKey(String trinoOptionKey)
    {
        requireNonNull(trinoOptionKey, "trinoOptionKey is null");
        if (StringUtils.isNullOrWhitespaceOnly(trinoOptionKey)) {
            throw new IllegalArgumentException("trinoOptionKey is blank");
        }
        for (OptionInfo optionInfo : getOptionInfos()) {
            if (optionInfo.trinoOptionKey.equals(trinoOptionKey)) {
                return optionInfo.paimonOptionKey;
            }
        }
        return trinoOptionKey;
    }

    private static void validatePropertyKey(String propertyKey)
    {
        requireNonNull(propertyKey, "properties contains null option key");
        if (StringUtils.isNullOrWhitespaceOnly(propertyKey)) {
            throw new IllegalArgumentException("properties contains blank option key");
        }
    }

    public static List<OptionInfo> getOptionInfos()
    {
        List<OptionInfo> optionInfos = new ArrayList<>();
        List<OptionWithMetaInfo> optionWithMetaInfos = extractConfigOptions(CoreOptions.class);
        for (OptionWithMetaInfo optionWithMetaInfo : optionWithMetaInfos) {
            if (shouldSkip(optionWithMetaInfo.field.getName())) {
                continue;
            }

            String className = optionValueClassName(optionWithMetaInfo.field);
            optionInfos.add(new OptionInfo(convertOptionKey(optionWithMetaInfo.option.key()),
                    optionWithMetaInfo.option.key(), className));
        }
        validateOptionInfos(optionInfos);
        return optionInfos;
    }

    static void validateOptionInfos(List<OptionInfo> optionInfos)
    {
        requireNonNull(optionInfos, "optionInfos is null");
        Map<String, String> trinoToPaimonKeys = new LinkedHashMap<>();
        Map<String, String> paimonToTrinoKeys = new LinkedHashMap<>();
        for (OptionInfo optionInfo : optionInfos) {
            requireNonNull(optionInfo, "optionInfo is null");
            String trinoOptionKey = requireNonNull(optionInfo.trinoOptionKey, "trinoOptionKey is null");
            String paimonOptionKey = requireNonNull(optionInfo.paimonOptionKey, "paimonOptionKey is null");
            if (StringUtils.isNullOrWhitespaceOnly(trinoOptionKey)) {
                throw new IllegalArgumentException("trinoOptionKey is blank");
            }
            if (StringUtils.isNullOrWhitespaceOnly(paimonOptionKey)) {
                throw new IllegalArgumentException("paimonOptionKey is blank");
            }

            String previousPaimonOptionKey = trinoToPaimonKeys.putIfAbsent(trinoOptionKey, paimonOptionKey);
            if (previousPaimonOptionKey != null) {
                throw new IllegalStateException(
                        "Duplicate Trino table option key '%s' maps to Paimon keys '%s' and '%s'"
                                .formatted(trinoOptionKey, previousPaimonOptionKey, paimonOptionKey));
            }
            String previousTrinoOptionKey = paimonToTrinoKeys.putIfAbsent(paimonOptionKey, trinoOptionKey);
            if (previousTrinoOptionKey != null) {
                throw new IllegalStateException(
                        "Duplicate Paimon table option key '%s' maps to Trino keys '%s' and '%s'"
                                .formatted(paimonOptionKey, previousTrinoOptionKey, trinoOptionKey));
            }
        }
    }

    private static String optionValueClassName(Field field)
    {
        String className = "";
        Type genericType = field.getGenericType();
        if (genericType instanceof ParameterizedType parameterizedType) {
            Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
            for (Type actualTypeArgument : actualTypeArguments) {
                if (actualTypeArgument instanceof Class<?>) {
                    className = ((Class<?>) actualTypeArgument).getSimpleName();
                }
            }
        }
        return className;
    }

    private static boolean shouldSkip(String fieldName)
    {
        switch (fieldName) {
            case "PRIMARY_KEY" :
            case "PARTITION" :
            case "FILE_COMPRESSION_PER_LEVEL" :
            case "STREAMING_COMPACT" :
                return true;
            default :
                return false;
        }
    }

    public static String convertOptionKey(String key)
    {
        requireNonNull(key, "key is null");
        if (StringUtils.isNullOrWhitespaceOnly(key)) {
            throw new IllegalArgumentException("key is blank");
        }
        Pattern camelCaseBoundary = Pattern.compile("([a-z0-9])([A-Z])");
        Matcher camelCaseMatcher = camelCaseBoundary.matcher(key);
        String snakeCaseKey = camelCaseMatcher.replaceAll("$1_$2");
        Pattern separator = Pattern.compile("[.\\-]");
        Matcher separatorMatcher = separator.matcher(snakeCaseKey.toLowerCase(Locale.ENGLISH));
        return separatorMatcher.replaceAll("_");
    }

    private static List<OptionWithMetaInfo> extractConfigOptions(Class<?> clazz)
    {
        try {
            List<OptionWithMetaInfo> configOptions = new ArrayList<>(8);
            Field[] fields = clazz.getFields();
            for (Field field : fields) {
                if (isConfigOption(field)) {
                    configOptions.add(new OptionWithMetaInfo((ConfigOption<?>) field.get(null), field));
                }
            }
            return configOptions;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to extract config options from class " + clazz + '.', e);
        }
    }

    private static boolean isConfigOption(Field field)
    {
        return field.getType().equals(ConfigOption.class);
    }

    record OptionWithMetaInfo(ConfigOption<?> option, Field field)
    {
    }

    static class OptionInfo<T>
    {
        String trinoOptionKey;
        String paimonOptionKey;
        String type;

        public OptionInfo(String trinoOptionKey, String paimonOptionKey, String type)
        {
            this.trinoOptionKey = trinoOptionKey;
            this.paimonOptionKey = paimonOptionKey;
            this.type = type;
        }
    }
}
