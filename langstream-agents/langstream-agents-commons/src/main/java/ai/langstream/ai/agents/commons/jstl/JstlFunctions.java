/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.langstream.ai.agents.commons.jstl;

import ai.langstream.ai.agents.commons.TransformContext;
import ai.langstream.ai.agents.commons.jstl.predicate.JstlPredicate;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.el.ELException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.el.util.MessageFactory;

/** Provides convenience methods to use in jstl expression. All functions should be static. */
@Slf4j
public class JstlFunctions {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Setter private static Clock clock = Clock.systemUTC();

    public static String uppercase(Object input) {
        return input == null ? null : toString(input).toUpperCase();
    }

    public static String lowercase(Object input) {
        return input == null ? null : toString(input).toLowerCase();
    }

    public static String toJson(Object input) throws Exception {
        return MAPPER.writeValueAsString(input);
    }

    public static Object fromJson(Object input) throws Exception {
        if (input == null) {
            return null;
        }
        String s = toString(input);
        if (s.isEmpty()) {
            return null;
        }
        return MAPPER.readValue(s, Object.class);
    }

    public static Object toDouble(Object input) {
        if (input == null) {
            return null;
        }
        return JstlTypeConverter.INSTANCE.coerceToDouble(input);
    }

    public static Object toInt(Object input) {
        if (input == null) {
            return null;
        }
        return JstlTypeConverter.INSTANCE.coerceToInteger(input);
    }

    public static Object toLong(Object input) {
        if (input == null) {
            return null;
        }
        return JstlTypeConverter.INSTANCE.coerceToLong(input);
    }

    public static List<Float> toListOfFloat(Object input) {
        if (input == null) {
            return null;
        }
        if (input instanceof Collection<?> collection) {
            List<Float> result = new ArrayList<>(collection.size());
            for (Object o : collection) {
                result.add(JstlTypeConverter.INSTANCE.coerceToFloat(o));
            }
            return result;
        } else {
            throw new IllegalArgumentException("Cannot convert " + input + " to list of float");
        }
    }

    public static List<String> split(Object input, Object separatorExpression) {
        if (input == null) {
            return null;
        }
        String s = toString(input);
        if (s.isEmpty()) {
            // special case, split would return a list with an empty string
            return List.of();
        }
        String separator = toString(separatorExpression);
        return Stream.of(s.split(separator)).collect(Collectors.toList());
    }

    public static Map<String, Object> unpack(Object input, Object fieldsExpression) {
        if (input == null) {
            return null;
        }
        String fields = toString(fieldsExpression);
        Map<String, Object> result = new HashMap<>();
        List<Object> values;
        if (input instanceof String) {
            String s = (String) input;
            if (s.isEmpty()) {
                values = List.of();
            } else {
                values = Stream.of(s.split(",")).collect(Collectors.toList());
            }
        } else if (input instanceof List) {
            values = (List<Object>) input;
        } else if (input instanceof Collection) {
            values = new ArrayList<>((Collection<Object>) input);
        } else if (input.getClass().isArray()) {
            values = new ArrayList<>(Array.getLength(input));
            for (int i = 0; i < Array.getLength(input); i++) {
                values.add(Array.get(input, i));
            }
        } else {
            throw new IllegalArgumentException(
                    "fn:unpack cannot unpack object of type " + input.getClass().getName());
        }
        List<String> headers = Stream.of(fields.split(",")).collect(Collectors.toList());
        for (int i = 0; i < headers.size(); i++) {
            String header = headers.get(i);
            if (i < values.size()) {
                result.put(header, values.get(i));
            } else {
                result.put(header, null);
            }
        }
        return result;
    }

    public static List<Object> emptyList() {
        return List.of();
    }

    public static Map<String, Object> emptyMap() {
        return Map.of();
    }

    public static List<Object> mapToListOfStructs(Object object, String fields) {
        if (object == null) {
            throw new IllegalArgumentException("listOf doesn't allow a null value");
        }
        if (object instanceof Map map) {
            Map<String, Object> result = new HashMap<>();
            for (String field : fields.split(",")) {
                result.put(field, map.get(field));
            }
            return List.of(result);
        }
        throw new IllegalArgumentException(
                "listOf doesn't allow a non-map value, found a " + object.getClass());
    }

    public static List<Object> listToListOfStructs(Object object, String field) {
        if (object == null) {
            throw new IllegalArgumentException("listOf doesn't allow a null value");
        }
        List<Object> result = new ArrayList<>();
        if (object instanceof Collection col) {
            for (Object item : col) {
                Map<String, Object> map = new HashMap<>();
                map.put(field, item);
                result.add(map);
            }
            return result;
        }
        throw new IllegalArgumentException(
                "listToListOfStructs doesn't allow a non-list value, found a " + object.getClass());
    }

    public static List<Object> listAdd(Object object, Object value) {
        if (object == null) {
            throw new IllegalArgumentException("listOf doesn't allow a null value");
        }
        List<Object> result = new ArrayList<>();
        if (object instanceof Collection col) {
            result.addAll(col);
            result.add(value);
            return result;
        }
        throw new IllegalArgumentException(
                "listOf doesn't allow a non-map value, found a " + object.getClass());
    }

    public static List<Object> addAll(Object one, Object two) {
        List<Object> results = new ArrayList<>();
        if (one != null) {
            if (one instanceof Collection col) {
                results.addAll(col);
            } else {
                throw new IllegalArgumentException(
                        "First argument of fn:addAll is not a list, it is a " + one.getClass());
            }
        }
        if (two != null) {
            if (two instanceof Collection col) {
                results.addAll(col);
            } else {
                throw new IllegalArgumentException(
                        "Second argument of fn:addAll is not a list, it is a " + one.getClass());
            }
        }
        return results;
    }

    public static List<Object> filter(Object input, String expression) {
        if (input == null) {
            return null;
        }
        List<Object> source;
        if (input instanceof List) {
            source = (List<Object>) input;
        } else if (input instanceof Collection) {
            source = new ArrayList<>((Collection<?>) input);
        } else if (input.getClass().isArray()) {
            source = new ArrayList<>(Array.getLength(input));
            for (int i = 0; i < Array.getLength(input); i++) {
                source.add(Array.get(input, i));
            }
        } else {
            throw new IllegalArgumentException(
                    "fn:filter cannot filter object of type " + input.getClass().getName());
        }
        List<Object> result = new ArrayList<>();
        JstlPredicate predicate = new JstlPredicate(expression);
        for (Object o : source) {
            if (log.isDebugEnabled()) {
                log.info("Filtering object {}", o);
            }
            // nulls are always filtered out
            if (o != null) {
                TransformContext context = new TransformContext();
                context.setRecordObject(o);
                boolean evaluate = predicate.test(context);
                if (log.isDebugEnabled()) {
                    log.debug("Result of evaluation is {}", evaluate);
                }
                if (evaluate) {
                    result.add(o);
                }
            }
        }
        return result;
    }

    public static boolean contains(Object input, Object value) {
        if (input == null || value == null) {
            return false;
        }
        return toString(input).contains(toString(value));
    }

    public static String trim(Object input) {
        return input == null ? null : toString(input).trim();
    }

    public static String concat(Object first, Object second) {
        return toString(first) + toString(second);
    }

    public static String concat3(Object first, Object second, Object third) {
        return toString(first) + toString(second) + toString(third);
    }

    public static Object coalesce(Object value, Object valueIfNull) {
        return value == null ? valueIfNull : value;
    }

    public static String replace(Object input, Object regex, Object replacement) {
        if (input == null) {
            return null;
        }
        if (regex == null || replacement == null) {
            return input.toString();
        }
        return toString(input).replaceAll(toString(regex), toString(replacement));
    }

    public static String toString(Object input) {
        return JstlTypeConverter.INSTANCE.coerceToString(input);
    }

    public static BigDecimal toBigDecimal(Object value, Object scale) {
        final BigInteger bigInteger = JstlTypeConverter.INSTANCE.coerceToBigInteger(value);
        final int scaleInteger = JstlTypeConverter.INSTANCE.coerceToInteger(scale);
        return new BigDecimal(bigInteger, scaleInteger);
    }

    public static BigDecimal toBigDecimal(Object value) {
        final double d = JstlTypeConverter.INSTANCE.coerceToDouble(value);
        return BigDecimal.valueOf(d);
    }

    public static Instant now() {
        return Instant.now(clock);
    }

    public static Instant timestampAdd(Object input, Object delta, Object unit) {
        if (input == null || unit == null) {
            throw new ELException(MessageFactory.get("error.method.notypes"));
        }

        ChronoUnit chronoUnit;
        switch (JstlTypeConverter.INSTANCE.coerceToString(unit)) {
            case "years":
                chronoUnit = ChronoUnit.YEARS;
                break;
            case "months":
                chronoUnit = ChronoUnit.MONTHS;
                break;
            case "days":
                chronoUnit = ChronoUnit.DAYS;
                break;
            case "hours":
                chronoUnit = ChronoUnit.HOURS;
                break;
            case "minutes":
                chronoUnit = ChronoUnit.MINUTES;
                break;
            case "seconds":
                chronoUnit = ChronoUnit.SECONDS;
                break;
            case "millis":
                chronoUnit = ChronoUnit.MILLIS;
                break;
            case "nanos":
                chronoUnit = ChronoUnit.NANOS;
                break;
            default:
                throw new IllegalArgumentException(
                        "Invalid unit: "
                                + unit
                                + ". Should be one of [years, months, days, hours, minutes, seconds, millis]");
        }
        if (chronoUnit == ChronoUnit.MONTHS || chronoUnit == ChronoUnit.YEARS) {
            return JstlTypeConverter.INSTANCE
                    .coerceToOffsetDateTime(input)
                    .plus(JstlTypeConverter.INSTANCE.coerceToLong(delta), chronoUnit)
                    .toInstant();
        }
        return JstlTypeConverter.INSTANCE
                .coerceToInstant(input)
                .plus(JstlTypeConverter.INSTANCE.coerceToLong(delta), chronoUnit);
    }

    @Deprecated
    public static long dateadd(Object input, Object delta, Object unit) {
        return timestampAdd(input, delta, unit).toEpochMilli();
    }
}
