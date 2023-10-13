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

public class DoubleBytesConverter implements BytesConverter<Double> {

    @Override
    public byte[] encode(Double obj) {
        if (null == obj) {
            return null;
        } else {
            long bits = Double.doubleToLongBits(obj);
            return new byte[] {
                (byte) (bits >>> 56),
                (byte) (bits >>> 48),
                (byte) (bits >>> 40),
                (byte) (bits >>> 32),
                (byte) (bits >>> 24),
                (byte) (bits >>> 16),
                (byte) (bits >>> 8),
                (byte) bits
            };
        }
    }

    @Override
    public Double decode(byte[] bytes) {
        if (null == bytes) {
            return null;
        }
        if (bytes.length != 8) {
            throw new SerializationException(
                    "Size of data received by DoubleBytesConverter is not 8");
        }
        long value = 0;
        for (byte b : bytes) {
            value <<= 8;
            value |= b & 0xFF;
        }
        return Double.longBitsToDouble(value);
    }
}
