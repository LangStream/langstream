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

import java.sql.Timestamp;

public class TimestampBytesConverter implements BytesConverter<Timestamp> {

    @Override
    public byte[] encode(Timestamp obj) {
        if (null == obj) {
            return null;
        }
        Long timestamp = obj.getTime();
        return BytesConverter.INT64.encode(timestamp);
    }

    @Override
    public Timestamp decode(byte[] bytes) {
        if (null == bytes) {
            return null;
        }
        Long decode = BytesConverter.INT64.decode(bytes);
        return new Timestamp(decode);
    }
}
