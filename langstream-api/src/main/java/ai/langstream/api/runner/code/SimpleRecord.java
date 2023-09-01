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
package ai.langstream.api.runner.code;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/** Basic implementation of {@link Record} interface. */
@Data
@Builder
public final class SimpleRecord implements Record {
    final Object key;
    final Object value;
    final String origin;
    final Long timestamp;
    @Builder.Default final Collection<Header> headers = Set.of();

    @Override
    public Object key() {
        return key;
    }

    @Override
    public Object value() {
        return value;
    }

    @Override
    public String origin() {
        return origin;
    }

    @Override
    public Long timestamp() {
        return timestamp;
    }

    @Override
    public Collection<Header> headers() {
        return headers;
    }

    /**
     * Copy all fields from {@link Record} to new {@link SimpleRecordBuilder}.
     *
     * @param record record to copy from
     * @return the new {@link SimpleRecordBuilder}
     */
    public static SimpleRecordBuilder copyFrom(Record record) {
        return builder()
                .key(record.key())
                .value(record.value())
                .origin(record.origin())
                .headers(record.headers())
                .timestamp(record.timestamp());
    }

    public static SimpleRecord of(Object key, Object value) {
        return builder().key(key).value(value).build();
    }

    @Data
    @AllArgsConstructor
    public static class SimpleHeader implements Header {

        public static SimpleHeader of(String key, Object value) {
            return new SimpleHeader(key, value);
        }

        final String key;
        final Object value;

        @Override
        public String key() {
            return key;
        }

        @Override
        public Object value() {
            return value;
        }

        @Override
        public String valueAsString() {
            if (value == null) {
                return null;
            } else if (value instanceof byte[] b) {
                return new String(b, StandardCharsets.UTF_8);
            } else {
                return value.toString();
            }
        }
    }
}
