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
import java.time.Instant;

public class InstantBytesConverter implements BytesConverter<Instant> {

    @Override
    public byte[] encode(Instant message) {
        if (null == message) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        buffer.putLong(message.getEpochSecond());
        buffer.putInt(message.getNano());
        return buffer.array();
    }

    @Override
    public Instant decode(byte[] bytes) {
        if (null == bytes) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long epochSecond = buffer.getLong();
        int nanos = buffer.getInt();
        return Instant.ofEpochSecond(epochSecond, nanos);
    }
}
