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

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class LocalDateTimeBytesConverter implements BytesConverter<LocalDateTime> {

    @Override
    public byte[] encode(LocalDateTime message) {
        if (null == message) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
        buffer.putLong(message.toLocalDate().toEpochDay());
        buffer.putLong(message.toLocalTime().toNanoOfDay());
        return buffer.array();
    }

    @Override
    public LocalDateTime decode(byte[] bytes) {
        if (null == bytes) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long epochDay = buffer.getLong();
        long nanoOfDay = buffer.getLong();
        return LocalDateTime.of(LocalDate.ofEpochDay(epochDay), LocalTime.ofNanoOfDay(nanoOfDay));
    }
}
