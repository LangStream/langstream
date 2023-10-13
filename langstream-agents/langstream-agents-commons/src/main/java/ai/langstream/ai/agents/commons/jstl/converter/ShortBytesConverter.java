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

public class ShortBytesConverter implements BytesConverter<Short> {

    @Override
    public byte[] encode(Short obj) {
        if (null == obj) {
            return null;
        } else {
            return new byte[] {(byte) (obj >>> 8), obj.byteValue()};
        }
    }

    @Override
    public Short decode(byte[] bytes) {
        if (null == bytes) {
            return null;
        }
        if (bytes.length != 2) {
            throw new SerializationException(
                    "Size of data received by ShortBytesConverter is not 2");
        }
        short value = 0;
        for (byte b : bytes) {
            value <<= 8;
            value |= b & 0xFF;
        }
        return value;
    }
}
