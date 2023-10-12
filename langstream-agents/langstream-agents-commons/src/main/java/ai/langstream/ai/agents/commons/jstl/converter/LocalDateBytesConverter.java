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

import java.time.LocalDate;

public class LocalDateBytesConverter implements BytesConverter<LocalDate> {

    @Override
    public byte[] encode(LocalDate message) {
        if (null == message) {
            return null;
        }

        Long epochDay = message.toEpochDay();
        return BytesConverter.INT64.encode(epochDay);
    }

    @Override
    public LocalDate decode(byte[] bytes) {
        if (null == bytes) {
            return null;
        }

        Long decode = BytesConverter.INT64.decode(bytes);
        return LocalDate.ofEpochDay(decode);
    }
}
