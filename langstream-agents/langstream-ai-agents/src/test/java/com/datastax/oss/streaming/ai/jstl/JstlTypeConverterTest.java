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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import ai.langstream.ai.agents.commons.jstl.JstlTypeConverter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.TimeZone;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class JstlTypeConverterTest {

    private static final JstlTypeConverter converter = JstlTypeConverter.INSTANCE;

    @Test
    void testNullConversion() {
        assertNull(converter.coerceToType(null, byte[].class));
        assertNull(converter.coerceToType(null, String.class));
        assertNull(converter.coerceToType(null, int.class));
        assertNull(converter.coerceToType(null, long.class));
        assertNull(converter.coerceToType(null, float.class));
        assertNull(converter.coerceToType(null, double.class));
        assertNull(converter.coerceToType(null, boolean.class));
        assertNull(converter.coerceToType(null, Date.class));
        assertNull(converter.coerceToType(null, Timestamp.class));
        assertNull(converter.coerceToType(null, Time.class));
        assertNull(converter.coerceToType(null, LocalDateTime.class));
        assertNull(converter.coerceToType(null, LocalDate.class));
        assertNull(converter.coerceToType(null, LocalTime.class));
        assertNull(converter.coerceToType(null, Instant.class));
        assertNull(converter.coerceToType(null, OffsetDateTime.class));
        assertNull(converter.coerceToType(null, BigDecimal.class));
    }

    public static Object[][] conversions() {
        TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.UTC));
        byte byteValue = (byte) 42;
        short shortValue = (short) 42;
        int intValue = 42;
        long longValue = 42L;
        float floatValue = 42.8F;
        double doubleValue = 42.8D;
        long dateTimeMillis = 1672700645000L;
        long midnightMillis = 1672617600000L;
        long numberOfDays = 19359L;
        long timeMillis = 83045000L;
        double timeMillisWithNanos = 83045000.000006D;
        Date date = new Date(dateTimeMillis);
        Instant instant = Instant.ofEpochSecond(1672700645, 6);
        Timestamp timestamp = Timestamp.from(instant);
        OffsetDateTime offsetDateTime = OffsetDateTime.of(2023, 1, 2, 23, 4, 5, 6, ZoneOffset.UTC);
        LocalDateTime localDateTime = LocalDateTime.of(2023, 1, 2, 23, 4, 5, 6);
        LocalDate localDate = LocalDate.of(2023, 1, 2);
        LocalTime localTime = LocalTime.of(23, 4, 5, 6);
        Time time = new Time(timeMillis);
        LocalDateTime localDateTimeWithoutNanos =
                LocalDateTime.ofEpochSecond(dateTimeMillis / 1000, 0, ZoneOffset.UTC);
        BigInteger bigInteger = new BigInteger("345781342432523452345");
        BigDecimal bigDecimal = new BigDecimal("435897983457.83421");
        return new Object[][] {
            // Bytes
            {new byte[] {1, 2, 3}, byte[].class, new byte[] {1, 2, 3}},
            {"test", byte[].class, "test".getBytes(StandardCharsets.UTF_8)},
            {true, byte[].class, new byte[] {1}},
            {byteValue, byte[].class, new byte[] {byteValue}},
            {shortValue, byte[].class, Schema.INT16.encode(shortValue)},
            {intValue, byte[].class, Schema.INT32.encode(intValue)},
            {longValue, byte[].class, Schema.INT64.encode(longValue)},
            {floatValue, byte[].class, Schema.FLOAT.encode(floatValue)},
            {doubleValue, byte[].class, Schema.DOUBLE.encode(doubleValue)},
            {date, byte[].class, Schema.DATE.encode(date)},
            {timestamp, byte[].class, Schema.TIMESTAMP.encode(timestamp)},
            {time, byte[].class, Schema.TIME.encode(time)},
            {localDateTime, byte[].class, Schema.LOCAL_DATE_TIME.encode(localDateTime)},
            {instant, byte[].class, Schema.INSTANT.encode(instant)},
            {offsetDateTime, byte[].class, Schema.INSTANT.encode(instant)},
            {localDate, byte[].class, Schema.LOCAL_DATE.encode(localDate)},
            {localTime, byte[].class, Schema.LOCAL_TIME.encode(localTime)},

            // String
            {"test".getBytes(StandardCharsets.UTF_8), String.class, "test"},
            {"test", String.class, "test"},
            {true, String.class, "true"},
            {byteValue, String.class, "42"},
            {shortValue, String.class, "42"},
            {intValue, String.class, "42"},
            {longValue, String.class, "42"},
            {floatValue, String.class, "42.8"},
            {doubleValue, String.class, "42.8"},
            {date, String.class, "2023-01-02T23:04:05Z"},
            {timestamp, String.class, "2023-01-02T23:04:05.000000006Z"},
            {time, String.class, "23:04:05"},
            {localDateTime, String.class, "2023-01-02T23:04:05.000000006"},
            {instant, String.class, "2023-01-02T23:04:05.000000006Z"},
            {offsetDateTime, String.class, "2023-01-02T23:04:05.000000006Z"},
            {localDate, String.class, "2023-01-02"},
            {localTime, String.class, "23:04:05.000000006"},

            // Utf8String
            {new Utf8("test"), String.class, "test"},
            {new Utf8("1"), Integer.class, 1},

            // Boolean
            {new byte[] {byteValue}, Boolean.class, true},
            {"true", Boolean.class, true},
            {true, Boolean.class, true},

            // Byte
            {new byte[] {byteValue}, Byte.class, byteValue},
            {"42", Byte.class, byteValue},
            {byteValue, Byte.class, byteValue},
            {shortValue, Byte.class, byteValue},
            {intValue, Byte.class, byteValue},
            {longValue, Byte.class, byteValue},
            {floatValue, Byte.class, byteValue},
            {doubleValue, Byte.class, byteValue},

            // Short
            {Schema.INT16.encode(shortValue), Short.class, shortValue},
            {"42", Short.class, shortValue},
            {byteValue, Short.class, shortValue},
            {shortValue, Short.class, shortValue},
            {intValue, Short.class, shortValue},
            {longValue, Short.class, shortValue},
            {floatValue, Short.class, shortValue},
            {doubleValue, Short.class, shortValue},

            // Integer
            {Schema.INT32.encode(intValue), Integer.class, intValue},
            {"42", Integer.class, intValue},
            {byteValue, Integer.class, intValue},
            {shortValue, Integer.class, intValue},
            {intValue, Integer.class, intValue},
            {longValue, Integer.class, intValue},
            {floatValue, Integer.class, intValue},
            {doubleValue, Integer.class, intValue},
            {localDate, Integer.class, (int) numberOfDays},

            // Long
            {Schema.INT64.encode(longValue), Long.class, longValue},
            {"42", Long.class, longValue},
            {byteValue, Long.class, longValue},
            {shortValue, Long.class, longValue},
            {intValue, Long.class, longValue},
            {longValue, Long.class, longValue},
            {floatValue, Long.class, longValue},
            {doubleValue, Long.class, longValue},
            {date, Long.class, dateTimeMillis},
            {timestamp, Long.class, dateTimeMillis},
            {time, Long.class, timeMillis},
            {localDateTime, Long.class, dateTimeMillis},
            {instant, Long.class, dateTimeMillis},
            {offsetDateTime, Long.class, dateTimeMillis},
            {localTime, Long.class, timeMillis},
            {localDate, Long.class, numberOfDays},

            // Float
            {Schema.FLOAT.encode(floatValue), Float.class, floatValue},
            {"42.8", Float.class, floatValue},
            {byteValue, Float.class, 42F},
            {shortValue, Float.class, 42F},
            {intValue, Float.class, 42F},
            {longValue, Float.class, 42F},
            {floatValue, Float.class, floatValue},
            {doubleValue, Float.class, floatValue},
            {localDate, Float.class, (float) numberOfDays},

            // Double
            {Schema.DOUBLE.encode(doubleValue), Double.class, doubleValue},
            {"42.8", Double.class, doubleValue},
            {byteValue, Double.class, 42D},
            {shortValue, Double.class, 42D},
            {intValue, Double.class, 42D},
            {longValue, Double.class, 42D},
            {floatValue, Double.class, (double) floatValue},
            {doubleValue, Double.class, doubleValue},
            {date, Double.class, (double) dateTimeMillis},
            {timestamp, Double.class, (double) dateTimeMillis},
            {time, Double.class, (double) timeMillis},
            {localDateTime, Double.class, (double) dateTimeMillis},
            {instant, Double.class, (double) dateTimeMillis},
            {offsetDateTime, Double.class, (double) dateTimeMillis},
            {localTime, Double.class, timeMillisWithNanos},
            {localDate, Double.class, (double) numberOfDays},

            // Date
            {Schema.DATE.encode(date), Date.class, date},
            {"2023-01-02T23:04:05.000000006Z", Date.class, date},
            {dateTimeMillis, Date.class, date},
            {(double) dateTimeMillis, Date.class, date},
            {date, Date.class, date},
            {timestamp, Date.class, date},
            {localDateTime, Date.class, date},
            {instant, Date.class, date},
            {offsetDateTime, Date.class, date},
            {localDate, Date.class, new Date(midnightMillis)},

            // Timestamp
            {Schema.TIMESTAMP.encode(timestamp), Timestamp.class, new Timestamp(dateTimeMillis)},
            {"2023-01-02T23:04:05.000000006Z", Timestamp.class, timestamp},
            {dateTimeMillis, Timestamp.class, new Timestamp(dateTimeMillis)},
            {(double) dateTimeMillis, Timestamp.class, new Timestamp(dateTimeMillis)},
            {date, Timestamp.class, new Timestamp(dateTimeMillis)},
            {timestamp, Timestamp.class, timestamp},
            {localDateTime, Timestamp.class, timestamp},
            {instant, Timestamp.class, timestamp},
            {offsetDateTime, Timestamp.class, timestamp},
            {offsetDateTime, Timestamp.class, timestamp},
            {localDate, Timestamp.class, new Timestamp(midnightMillis)},

            // Time
            {Schema.TIME.encode(time), Time.class, time},
            {"23:04:05.000000006", Time.class, time},
            {dateTimeMillis, Time.class, new Time(dateTimeMillis)},
            {(double) dateTimeMillis, Time.class, new Time(dateTimeMillis)},
            {date, Time.class, new Time(dateTimeMillis)},
            {timestamp, Time.class, time},
            {localDateTime, Time.class, time},
            {instant, Time.class, time},
            {offsetDateTime, Time.class, time},
            {localDate, Time.class, new Time(midnightMillis)},
            {time, Time.class, time},
            {localTime, Time.class, time},

            // LocalTime
            {Schema.LOCAL_TIME.encode(localTime), LocalTime.class, localTime},
            {"23:04:05.000000006", LocalTime.class, localTime},
            {dateTimeMillis, LocalTime.class, LocalTime.of(23, 4, 5)},
            {timeMillisWithNanos, LocalTime.class, localTime},
            {date, LocalTime.class, LocalTime.of(23, 4, 5)},
            {timestamp, LocalTime.class, localTime},
            {localDateTime, LocalTime.class, localTime},
            {instant, LocalTime.class, localTime},
            {offsetDateTime, LocalTime.class, localTime},
            {localDate, LocalTime.class, LocalTime.ofSecondOfDay(0)},
            {time, LocalTime.class, LocalTime.of(23, 4, 5)},
            {localTime, LocalTime.class, localTime},

            // LocalDate
            {Schema.LOCAL_DATE.encode(localDate), LocalDate.class, localDate},
            {"2023-01-02", LocalDate.class, localDate},
            {(int) numberOfDays, LocalDate.class, localDate},
            {numberOfDays, LocalDate.class, localDate},
            {(float) numberOfDays, LocalDate.class, localDate},
            {(double) numberOfDays, LocalDate.class, localDate},
            {date, LocalDate.class, localDate},
            {timestamp, LocalDate.class, localDate},
            {localDateTime, LocalDate.class, localDate},
            {instant, LocalDate.class, localDate},
            {offsetDateTime, LocalDate.class, localDate},
            {localDate, LocalDate.class, localDate},

            // LocalDateTime
            {Schema.LOCAL_DATE_TIME.encode(localDateTime), LocalDateTime.class, localDateTime},
            {"2023-01-02T23:04:05.000000006", LocalDateTime.class, localDateTime},
            {(double) dateTimeMillis, LocalDateTime.class, localDateTimeWithoutNanos},
            {date, LocalDateTime.class, localDateTimeWithoutNanos},
            {timestamp, LocalDateTime.class, localDateTime},
            {localDateTime, LocalDateTime.class, localDateTime},
            {instant, LocalDateTime.class, localDateTime},
            {offsetDateTime, LocalDateTime.class, localDateTime},
            {localDate, LocalDateTime.class, LocalDateTime.of(localDate, LocalTime.MIDNIGHT)},

            // Instant
            {Schema.INSTANT.encode(instant), Instant.class, instant},
            {"2023-01-02T23:04:05.000000006Z", Instant.class, instant},
            {"2023-01-02", Instant.class, localDate.atStartOfDay(ZoneOffset.UTC).toInstant()},
            {dateTimeMillis, Instant.class, Instant.ofEpochMilli(dateTimeMillis)},
            {(double) dateTimeMillis, Instant.class, Instant.ofEpochMilli(dateTimeMillis)},
            {date, Instant.class, Instant.ofEpochMilli(dateTimeMillis)},
            {timestamp, Instant.class, instant},
            {localDateTime, Instant.class, instant},
            {instant, Instant.class, instant},
            {offsetDateTime, Instant.class, instant},
            {localDate, Instant.class, instant.truncatedTo(ChronoUnit.DAYS)},

            // OffsetDateTime
            {Schema.INSTANT.encode(instant), OffsetDateTime.class, offsetDateTime},
            {"2023-01-02T23:04:05.000000006Z", OffsetDateTime.class, offsetDateTime},
            {
                "2023-01-02",
                OffsetDateTime.class,
                localDate.atStartOfDay(ZoneOffset.UTC).toOffsetDateTime()
            },
            {
                dateTimeMillis,
                OffsetDateTime.class,
                OffsetDateTime.of(localDateTimeWithoutNanos, ZoneOffset.UTC)
            },
            {
                (double) dateTimeMillis,
                OffsetDateTime.class,
                OffsetDateTime.of(localDateTimeWithoutNanos, ZoneOffset.UTC)
            },
            {
                date,
                OffsetDateTime.class,
                OffsetDateTime.of(localDateTimeWithoutNanos, ZoneOffset.UTC)
            },
            {timestamp, OffsetDateTime.class, offsetDateTime},
            {localDateTime, OffsetDateTime.class, offsetDateTime},
            {offsetDateTime, OffsetDateTime.class, offsetDateTime},
            {localDate, OffsetDateTime.class, offsetDateTime.truncatedTo(ChronoUnit.DAYS)},
            {instant, OffsetDateTime.class, offsetDateTime},

            // BigDecimal
            {bigInteger, BigInteger.class, bigInteger},
            {"345781342432523452345", BigInteger.class, bigInteger},
            {Integer.MAX_VALUE, BigInteger.class, BigInteger.valueOf(Integer.MAX_VALUE)},
            {Long.MAX_VALUE, BigInteger.class, BigInteger.valueOf(Long.MAX_VALUE)},

            // BigDecimal
            {bigDecimal, BigDecimal.class, bigDecimal},
            {"435897983457.83421", BigDecimal.class, bigDecimal},
            {Integer.MAX_VALUE, BigDecimal.class, BigDecimal.valueOf(Integer.MAX_VALUE)},
            {Long.MAX_VALUE, BigDecimal.class, BigDecimal.valueOf(Long.MAX_VALUE)},
            {Float.MAX_VALUE, BigDecimal.class, BigDecimal.valueOf(Float.MAX_VALUE)},
            {Double.MAX_VALUE, BigDecimal.class, BigDecimal.valueOf(Double.MAX_VALUE)},
        };
    }

    @ParameterizedTest
    @MethodSource("conversions")
    public void testNonNullConversions(Object o, Class<?> type, Object expected) {
        Object converted = converter.coerceToType(o, type);
        assertEquals(converted.getClass(), type);
        if (type.equals(Time.class)) {
            // j.s.Time equality is weird...
            assertEquals(((Time) converted).toLocalTime(), ((Time) expected).toLocalTime());
        } else {
            if (converted instanceof byte[]) {
                assertArrayEquals((byte[]) converted, (byte[]) expected);
            } else {
                assertEquals(converted, expected);
            }
        }
    }
}
