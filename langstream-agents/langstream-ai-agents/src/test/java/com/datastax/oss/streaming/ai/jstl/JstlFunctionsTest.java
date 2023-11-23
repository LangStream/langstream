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
package com.datastax.oss.streaming.ai.jstl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.ai.agents.commons.jstl.JstlFunctions;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class JstlFunctionsTest {

    @Test
    void testUpperCase() {
        assertEquals("UPPERCASE", JstlFunctions.uppercase("uppercase"));
    }

    @Test
    void testUpperCaseInteger() {}

    @Test
    void testUpperCaseNull() {
        assertNull(JstlFunctions.uppercase(null));
    }

    @Test
    void testLowerCase() {
        assertEquals("lowercase", JstlFunctions.lowercase("LOWERCASE"));
    }

    @Test
    void testLowerCaseInteger() {
        assertEquals("10", JstlFunctions.uppercase(10));
    }

    @Test
    void testLowerCaseNull() {
        assertNull(JstlFunctions.uppercase(null));
    }

    @Test
    void testContains() {
        assertTrue(JstlFunctions.contains("full text", "l t"));
        assertTrue(JstlFunctions.contains("full text", "full"));
        assertTrue(JstlFunctions.contains("full text", "text"));

        assertFalse(JstlFunctions.contains("full text", "lt"));
        assertFalse(JstlFunctions.contains("full text", "fll"));
        assertFalse(JstlFunctions.contains("full text", "txt"));
    }

    @Test
    void testContainsInteger() {
        assertTrue(JstlFunctions.contains(123, "2"));
        assertTrue(JstlFunctions.contains("123", 3));
        assertTrue(JstlFunctions.contains(123, 3));
        assertTrue(JstlFunctions.contains("123", "3"));

        assertFalse(JstlFunctions.contains(123, "4"));
        assertFalse(JstlFunctions.contains("123", 4));
        assertFalse(JstlFunctions.contains(123, 4));
        assertFalse(JstlFunctions.contains("123", "4"));
    }

    @Test
    void testContainsNull() {
        assertFalse(JstlFunctions.contains("null", null));
        assertFalse(JstlFunctions.contains(null, "null"));
        assertFalse(JstlFunctions.contains(null, null));
    }

    @Test
    void testConcat() {
        assertEquals("full text", JstlFunctions.concat("full ", "text"));
    }

    @Test
    void testConcatInteger() {
        assertEquals("12", JstlFunctions.concat(1, 2));
        assertEquals("12", JstlFunctions.concat("1", 2));
        assertEquals("12", JstlFunctions.concat(1, "2"));
    }

    @Test
    void testConcatNull() {
        assertEquals("text", JstlFunctions.concat(null, "text"));
        assertEquals("full ", JstlFunctions.concat("full ", null));
        assertEquals("", JstlFunctions.concat(null, null));
    }

    @Test
    void testNow() {
        Instant fixedInstant = Instant.now();
        Clock clock = Clock.fixed(fixedInstant, ZoneOffset.UTC);
        JstlFunctions.setClock(clock);
        assertEquals(fixedInstant, JstlFunctions.now());
    }

    @ParameterizedTest
    @MethodSource("millisTimestampAddProvider")
    void testAddDateMillis(long input, int delta, String unit, Instant expected) {
        assertEquals(expected, JstlFunctions.timestampAdd(input, delta, unit));
    }

    @ParameterizedTest
    @MethodSource("utcTimestampAddProvider")
    void testAddDateUTC(String input, int delta, String unit, Instant expected) {
        assertEquals(expected, JstlFunctions.timestampAdd(input, delta, unit));
    }

    @ParameterizedTest
    @MethodSource("nonUtcTimestampAddProvider")
    void testAddDateNonUTC(String input, int delta, String unit, Instant expected) {
        assertEquals(expected, JstlFunctions.timestampAdd(input, delta, unit));
    }

    @Test
    void testAddDateDeltaConversion() {
        assertEquals(
                Instant.parse("2022-10-02T02:02:03Z"),
                JstlFunctions.timestampAdd(
                        "2022-10-02T01:02:03Z", LocalTime.of(1, 0, 0), "millis"));
    }

    @Test
    void testAddDateUnitConversion() {
        assertEquals(
                Instant.parse("2022-10-02T02:02:03Z"),
                JstlFunctions.timestampAdd(
                        "2022-10-02T01:02:03Z", 1, "hours".getBytes(StandardCharsets.UTF_8)));
    }

    @ParameterizedTest
    @MethodSource("toBigDecimalProvider")
    void testToBigDecimal(Object value, Object scale, BigDecimal expected) {
        assertEquals(expected, JstlFunctions.toBigDecimal(value, scale));
    }

    @ParameterizedTest
    @MethodSource("toBigDecimalWithoutScaleProvider")
    void testToBigDecimalWithoutScale(Object value, BigDecimal expected) {
        assertEquals(expected, JstlFunctions.toBigDecimal(value));
    }

    @Test
    void testInvalidAddDate() {
        assertEquals(
                "Cannot convert [7] of type [class java.lang.Byte] to [class java.time.Instant]",
                assertThrows(
                                jakarta.el.ELException.class,
                                () -> {
                                    JstlFunctions.dateadd((byte) 7, 0, "days");
                                })
                        .getMessage());
    }

    /**
     * @return {"value convertible to BigInteger, scale, "expected BigDecimal value"}
     */
    public static Object[][] toBigDecimalProvider() {
        BigDecimal bigDecimal = new BigDecimal("12.34567890123456789012345678901234567890");
        BigDecimal smallDecimal = new BigDecimal("1234.5678");
        return new Object[][] {
            {
                new BigInteger("1234567890123456789012345678901234567890").toByteArray(),
                38,
                bigDecimal
            },
            {"1234567890123456789012345678901234567890", "38", bigDecimal},
            {12345678, 4, smallDecimal},
            {12345678L, 4, smallDecimal},
        };
    }

    /**
     * @return {"value convertible to double, "expected BigDecimal value"}
     */
    public static Object[][] toBigDecimalWithoutScaleProvider() {
        BigDecimal bigDecimal = BigDecimal.valueOf(12.34567890123456789012345678901234567890d);
        BigDecimal smallDecimal = BigDecimal.valueOf(1234.5678f);
        BigDecimal unscaledDecimal = BigDecimal.valueOf(1234567d);
        return new Object[][] {
            {"12.34567890123456789012345678901234567890", bigDecimal},
            {12.34567890123456789012345678901234567890d, bigDecimal},
            {1234.5678f, smallDecimal},
            {1234567, unscaledDecimal},
            {1234567L, unscaledDecimal},
        };
    }

    /**
     * @return {"input date in epoch millis", "delta", "unit", "expected value (in epoch millis)"}
     */
    public static Object[][] millisTimestampAddProvider() {
        Instant instant = Instant.parse("2022-10-02T01:02:03Z");
        long millis = instant.toEpochMilli();
        return new Object[][] {
            {millis, 0, "years", instant},
            {millis, 5, "years", Instant.parse("2027-10-02T01:02:03Z")},
            {millis, -3, "years", Instant.parse("2019-10-02T01:02:03Z")},
            {millis, 0, "months", instant},
            {millis, 5, "months", Instant.parse("2023-03-02T01:02:03Z")},
            {millis, -3, "months", Instant.parse("2022-07-02T01:02:03Z")},
            {millis, 0, "days", instant},
            {millis, 5, "days", Instant.parse("2022-10-07T01:02:03Z")},
            {millis, -3, "days", Instant.parse("2022-09-29T01:02:03Z")},
            {millis, 0, "hours", instant},
            {millis, 5, "hours", Instant.parse("2022-10-02T06:02:03Z")},
            {millis, -3, "hours", Instant.parse("2022-10-01T22:02:03Z")},
            {millis, 0, "minutes", instant},
            {millis, 5, "minutes", Instant.parse("2022-10-02T01:07:03Z")},
            {millis, -3, "minutes", Instant.parse("2022-10-02T00:59:03Z")},
            {millis, 0, "seconds", instant},
            {millis, 5, "seconds", Instant.parse("2022-10-02T01:02:08Z")},
            {millis, -3, "seconds", Instant.parse("2022-10-02T01:02:00Z")},
            {millis, 0, "millis", instant},
            {millis, 5, "millis", Instant.parse("2022-10-02T01:02:03.005Z")},
            {millis, -3, "millis", Instant.parse("2022-10-02T01:02:02.997Z")},
            {millis, 0, "nanos", instant},
            {millis, 5_000_000, "nanos", Instant.parse("2022-10-02T01:02:03.005Z")},
            {millis, -3_000_000, "nanos", Instant.parse("2022-10-02T01:02:02.997Z")},
        };
    }

    /**
     * @return {"input date", "delta", "unit", "expected value"}
     */
    public static Object[][] utcTimestampAddProvider() {
        String utcDateTime = "2022-10-02T01:02:03Z";
        Instant instant = Instant.parse("2022-10-02T01:02:03Z");
        return new Object[][] {
            {utcDateTime, 0, "years", instant},
            {utcDateTime, 5, "years", Instant.parse("2027-10-02T01:02:03Z")},
            {utcDateTime, -3, "years", Instant.parse("2019-10-02T01:02:03Z")},
            {utcDateTime, 0, "months", instant},
            {utcDateTime, 5, "months", Instant.parse("2023-03-02T01:02:03Z")},
            {utcDateTime, -3, "months", Instant.parse("2022-07-02T01:02:03Z")},
            {utcDateTime, 0, "days", instant},
            {utcDateTime, 5, "days", Instant.parse("2022-10-07T01:02:03Z")},
            {utcDateTime, -3, "days", Instant.parse("2022-09-29T01:02:03Z")},
            {utcDateTime, 0, "hours", instant},
            {utcDateTime, 5, "hours", Instant.parse("2022-10-02T06:02:03Z")},
            {utcDateTime, -3, "hours", Instant.parse("2022-10-01T22:02:03Z")},
            {utcDateTime, 0, "minutes", instant},
            {utcDateTime, 5, "minutes", Instant.parse("2022-10-02T01:07:03Z")},
            {utcDateTime, -3, "minutes", Instant.parse("2022-10-02T00:59:03Z")},
            {utcDateTime, 0, "seconds", instant},
            {utcDateTime, 5, "seconds", Instant.parse("2022-10-02T01:02:08Z")},
            {utcDateTime, -3, "seconds", Instant.parse("2022-10-02T01:02:00Z")},
            {utcDateTime, 0, "millis", instant},
            {utcDateTime, 5, "millis", Instant.parse("2022-10-02T01:02:03.005Z")},
            {utcDateTime, -3, "millis", Instant.parse("2022-10-02T01:02:02.997Z")},
            {utcDateTime, 0, "nanos", instant},
            {utcDateTime, 5_000_000, "nanos", Instant.parse("2022-10-02T01:02:03.005Z")},
            {utcDateTime, -3_000_000, "nanos", Instant.parse("2022-10-02T01:02:02.997Z")},
        };
    }

    /**
     * @return {"input date", "delta", "unit", "expected value (in epoch millis)"}
     */
    public static Object[][] nonUtcTimestampAddProvider() {
        String nonUtcDateTime = "2022-10-02T01:02:03+02:00";
        long twoHoursMillis = Duration.ofHours(2).toMillis();
        Instant instant = Instant.parse("2022-10-02T01:02:03Z");
        return new Object[][] {
            {nonUtcDateTime, 0, "years", instant.minusMillis(twoHoursMillis)},
            {
                nonUtcDateTime,
                5,
                "years",
                Instant.parse("2027-10-02T01:02:03Z").minusMillis(twoHoursMillis)
            },
            {
                nonUtcDateTime,
                -3,
                "years",
                Instant.parse("2019-10-02T01:02:03Z").minusMillis(twoHoursMillis)
            },
            {nonUtcDateTime, 0, "months", instant.minusMillis(twoHoursMillis)},
            {
                nonUtcDateTime,
                5,
                "months",
                Instant.parse("2023-03-02T01:02:03Z").minusMillis(twoHoursMillis)
            },
            {
                nonUtcDateTime,
                -3,
                "months",
                Instant.parse("2022-07-02T01:02:03Z").minusMillis(twoHoursMillis)
            },
            {nonUtcDateTime, 0, "days", instant.minusMillis(twoHoursMillis)},
            {
                nonUtcDateTime,
                5,
                "days",
                Instant.parse("2022-10-07T01:02:03Z").minusMillis(twoHoursMillis)
            },
            {
                nonUtcDateTime,
                -3,
                "days",
                Instant.parse("2022-09-29T01:02:03Z").minusMillis(twoHoursMillis)
            },
            {nonUtcDateTime, 0, "hours", instant.minusMillis(twoHoursMillis)},
            {
                nonUtcDateTime,
                5,
                "hours",
                Instant.parse("2022-10-02T06:02:03Z").minusMillis(twoHoursMillis)
            },
            {
                nonUtcDateTime,
                -3,
                "hours",
                Instant.parse("2022-10-01T22:02:03Z").minusMillis(twoHoursMillis)
            },
            {nonUtcDateTime, 0, "minutes", instant.minusMillis(twoHoursMillis)},
            {
                nonUtcDateTime,
                5,
                "minutes",
                Instant.parse("2022-10-02T01:07:03Z").minusMillis(twoHoursMillis)
            },
            {
                nonUtcDateTime,
                -3,
                "minutes",
                Instant.parse("2022-10-02T00:59:03Z").minusMillis(twoHoursMillis)
            },
            {nonUtcDateTime, 0, "seconds", instant.minusMillis(twoHoursMillis)},
            {
                nonUtcDateTime,
                5,
                "seconds",
                Instant.parse("2022-10-02T01:02:08Z").minusMillis(twoHoursMillis)
            },
            {
                nonUtcDateTime,
                -3,
                "seconds",
                Instant.parse("2022-10-02T01:02:00Z").minusMillis(twoHoursMillis)
            },
            {nonUtcDateTime, 0, "millis", instant.minusMillis(twoHoursMillis)},
            {
                nonUtcDateTime,
                5,
                "millis",
                Instant.parse("2022-10-02T01:02:03.005Z").minusMillis(twoHoursMillis)
            },
            {
                nonUtcDateTime,
                -3,
                "millis",
                Instant.parse("2022-10-02T01:02:02.997Z").minusMillis(twoHoursMillis)
            },
            {nonUtcDateTime, 0, "nanos", instant.minusMillis(twoHoursMillis)},
            {
                nonUtcDateTime,
                5_000_000,
                "nanos",
                Instant.parse("2022-10-02T01:02:03.005Z").minusMillis(twoHoursMillis)
            },
            {
                nonUtcDateTime,
                -3_000_000,
                "nanos",
                Instant.parse("2022-10-02T01:02:02.997Z").minusMillis(twoHoursMillis)
            },
        };
    }

    @Test
    void testAddDateInvalidUnit() {
        assertEquals(
                "Invalid unit: lightyear. Should be one of [years, months, days, hours, minutes, seconds, millis]",
                assertThrows(
                                IllegalArgumentException.class,
                                () -> {
                                    JstlFunctions.timestampAdd(0L, 0, "lightyear");
                                })
                        .getMessage());
    }

    @Test
    void testCast() {
        assertEquals(1.2, JstlFunctions.toDouble("1.2"));
        assertEquals(null, JstlFunctions.toDouble(null));
        assertEquals(1, JstlFunctions.toInt("1.2"));
        assertEquals(null, JstlFunctions.toInt(null));
    }

    @Test
    void testSplit() {
        assertEquals(List.of("1", "2"), JstlFunctions.split("1,2", ","));
        assertEquals(List.of(), JstlFunctions.split("", ","));
        assertEquals(null, JstlFunctions.split(null, ","));
    }

    @Test
    void testUnpack() {
        assertEquals(
                map("field1", "1", "field2", "2"), JstlFunctions.unpack("1,2", "field1,field2"));
        assertEquals(
                map("field1", null, "field2", null), JstlFunctions.unpack("", "field1,field2"));
        assertEquals(null, JstlFunctions.unpack(null, "field1,field2"));

        assertEquals(
                map("field1", "1", "field2", "2", "field3", null),
                JstlFunctions.unpack("1,2", "field1,field2,field3"));
        assertEquals(
                map("field1", "1", "field2", null), JstlFunctions.unpack("1", "field1,field2"));

        assertEquals(
                map("field1", "1", "field2", "2"),
                JstlFunctions.unpack(JstlFunctions.split("1:2", ":"), "field1,field2"));

        assertEquals(
                map("field1", 1f, "field2", 2f),
                JstlFunctions.unpack(List.of(1f, 2f), "field1,field2"));
    }

    @Test
    void testToJson() throws Exception {
        assertEquals("{\"field1\":1}", JstlFunctions.toJson(Map.of("field1", 1)));
        assertEquals("null", JstlFunctions.toJson(null));
        assertEquals("\"\"", JstlFunctions.toJson(""));
        assertEquals("[1,2,3]", JstlFunctions.toJson(List.of(1, 2, 3)));
    }

    @Test
    void testFromJson() throws Exception {
        assertEquals(Map.of("field1", 1), JstlFunctions.fromJson("{\"field1\":1}"));
        assertEquals(null, JstlFunctions.fromJson(null));
        assertEquals(null, JstlFunctions.fromJson("null"));
        assertEquals(null, JstlFunctions.fromJson(""));
        assertEquals("", JstlFunctions.fromJson("\"\""));
        assertEquals(List.of(1, 2, 3), JstlFunctions.fromJson("[1,2,3]"));
    }

    private static Map<String, Object> map(
            String key, Object nullableValue, String key2, Object nullableValue2) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, nullableValue);
        map.put(key2, nullableValue2);
        return map;
    }

    private static Map<String, Object> map(
            String key,
            Object nullableValue,
            String key2,
            Object nullableValue2,
            String key3,
            Object nullableValue3) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, nullableValue);
        map.put(key2, nullableValue2);
        map.put(key3, nullableValue3);
        return map;
    }

    @Test
    void testFilterQueryResults() {

        // the query step always produces a list<map<string,string>> result
        // this test verifies that it is possible to filter the result using the JSTL filter
        // function
        List<Map<String, String>> queryResult = new ArrayList<>();
        queryResult.add(Map.of("name", "product1", "price", "1.2", "similarity", "0.9"));
        queryResult.add(Map.of("name", "product2", "price", "1.7", "similarity", "0.1"));

        {
            List<Object> filter =
                    JstlFunctions.filter(queryResult, "fn:toDouble(record.similarity) >= 0.5");
            assertEquals(1, filter.size());
            assertEquals("product1", ((Map<String, String>) filter.get(0)).get("name"));
        }

        {
            List<Object> filter =
                    JstlFunctions.filter(queryResult, "fn:toDouble(record.similarity) < 0.5");
            assertEquals(1, filter.size());
            assertEquals("product2", ((Map<String, String>) filter.get(0)).get("name"));
        }

        {
            List<Object> filter = JstlFunctions.filter(queryResult, "false");
            assertEquals(0, filter.size());
        }

        {
            List<Object> filter = JstlFunctions.filter(queryResult, "true");
            assertEquals(2, filter.size());
        }
    }

    @Test
    void testFilterQueryResultsWithContext() {

        MutableRecord record = new MutableRecord();
        record.setValueObject(Map.of("threshold", "0.5"));

        try (JstlFunctions.FilterContextHandle context =
                JstlFunctions.FilterContextHandle.start(record)) {

            // the query step always produces a list<map<string,string>> result
            // this test verifies that it is possible to filter the result using the JSTL filter
            // function
            List<Map<String, String>> queryResult = new ArrayList<>();
            queryResult.add(Map.of("name", "product1", "price", "1.2", "similarity", "0.9"));
            queryResult.add(Map.of("name", "product2", "price", "1.7", "similarity", "0.1"));

            {
                List<Object> filter =
                        JstlFunctions.filter(
                                queryResult, "fn:toDouble(record.similarity) >= value.threshold");
                assertEquals(1, filter.size());
                assertEquals("product1", ((Map<String, String>) filter.get(0)).get("name"));
            }

            {
                List<Object> filter =
                        JstlFunctions.filter(
                                queryResult, "fn:toDouble(record.similarity) < value.threshold");
                assertEquals(1, filter.size());
                assertEquals("product2", ((Map<String, String>) filter.get(0)).get("name"));
            }

            {
                List<Object> filter = JstlFunctions.filter(queryResult, "false");
                assertEquals(0, filter.size());
            }

            {
                List<Object> filter = JstlFunctions.filter(queryResult, "true");
                assertEquals(2, filter.size());
            }
        }
    }
}
