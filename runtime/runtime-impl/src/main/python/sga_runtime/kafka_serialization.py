# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import struct as _struct

from confluent_kafka.serialization import Serializer, SerializationError, StringSerializer, DoubleSerializer, \
    IntegerSerializer


class BooleanSerializer(Serializer):
    """
    Serializes bool to boolean bytes.

    See Also:
        `BooleanSerializer Javadoc <https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/serialization/BooleanSerializer.java>`_
    """  # noqa: E501

    def __call__(self, obj, ctx=None):
        """
        Serializes bool as boolean bytes.

        Args:
            obj (object): object to be serialized

            ctx (SerializationContext): Metadata pertaining to the serialization
                operation

        Note:
            None objects are represented as Kafka Null.

        Raises:
            SerializerError if an error occurs during serialization

        Returns:
            boolean bytes if obj is not None, else None
        """

        if obj is None:
            return None

        return b'\x01' if obj else b'\x00'


class LongSerializer(Serializer):
    """
    Serializes int to int32 bytes.

    See Also:
        `LongSerializer Javadoc <https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/serialization/LongSerializer.java>`_
    """  # noqa: E501

    def __call__(self, obj, ctx=None):
        """
        Serializes int as int64 bytes.

        Args:
            obj (object): object to be serialized

            ctx (SerializationContext): Metadata pertaining to the serialization
                operation

        Note:
            None objects are represented as Kafka Null.

        Raises:
            SerializerError if an error occurs during serialization

        Returns:
            int64 bytes if obj is not None, else None
        """

        if obj is None:
            return None

        try:
            return _struct.pack('>q', obj)
        except _struct.error as e:
            raise SerializationError(str(e))


STRING_SERIALIZER = StringSerializer()
DOUBLE_SERIALIZER = DoubleSerializer()
INTEGER_SERIALIZER = IntegerSerializer()
LONG_SERIALIZER = LongSerializer()
BOOLEAN_SERIALIZER = BooleanSerializer()
