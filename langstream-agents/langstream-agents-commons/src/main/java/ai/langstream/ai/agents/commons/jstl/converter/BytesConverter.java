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
package ai.langstream.ai.agents.commons.jstl.converter;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;

public interface BytesConverter<T> {

    BytesConverter<Boolean> BOOL = new BooleanBytesConverter();
    BytesConverter<String> STRING = new StringBytesConverter();
    BytesConverter<Byte> INT8 = new ByteBytesConverter();
    BytesConverter<Short> INT16 = new ShortBytesConverter();
    BytesConverter<Integer> INT32 = new IntegerBytesConverter();
    BytesConverter<Long> INT64 = new LongBytesConverter();
    BytesConverter<Float> FLOAT = new FloatBytesConverter();
    BytesConverter<Double> DOUBLE = new DoubleBytesConverter();
    BytesConverter<Date> DATE = new DateBytesConverter();
    BytesConverter<Timestamp> TIMESTAMP = new TimestampBytesConverter();
    BytesConverter<Time> TIME = new TimeBytesConverter();
    BytesConverter<LocalTime> LOCAL_TIME = new LocalTimeBytesConverter();
    BytesConverter<LocalDate> LOCAL_DATE = new LocalDateBytesConverter();
    BytesConverter<LocalDateTime> LOCAL_DATE_TIME = new LocalDateTimeBytesConverter();
    BytesConverter<Instant> INSTANT = new InstantBytesConverter();

    /**
     * Encode an object into a byte array.
     *
     * @param obj the object to encode
     * @return the serialized byte array
     */
    byte[] encode(T obj);

    /**
     * Decode a byte array into an object.
     *
     * @param bytes the byte array to decode
     * @return the deserialized object
     */
    default T decode(byte[] bytes) {
        return decode(bytes);
    }
}
