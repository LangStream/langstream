#
#
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

from confluent_kafka.serialization import (
    Serializer,
    SerializationError,
    StringSerializer,
    DoubleSerializer,
    IntegerSerializer,
)


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

        return b"\x01" if obj else b"\x00"


class ShortSerializer(Serializer):
    """
    Serializes int to int16 bytes.

    See Also:
        `ShortSerializer Javadoc <https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/serialization/ShortSerializer.java>`_
    """  # noqa: E501

    def __call__(self, obj, ctx=None):
        """
        Serializes int as int16 bytes.

        Args:
            obj (object): object to be serialized

            ctx (SerializationContext): Metadata pertaining to the serialization
                operation

        Note:
            None objects are represented as Kafka Null.

        Raises:
            SerializerError if an error occurs during serialization

        Returns:
            int16 bytes if obj is not None, else None
        """

        if obj is None:
            return None

        try:
            return _struct.pack(">h", obj)
        except _struct.error as e:
            raise SerializationError(str(e))


class LongSerializer(Serializer):
    """
    Serializes int to int64 bytes.

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
            return _struct.pack(">q", obj)
        except _struct.error as e:
            raise SerializationError(str(e))


class FloatSerializer(Serializer):
    """
    Serializes float to IEEE 754 binary32.

    See Also:
        `FloatSerializer Javadoc <https://docs.confluent.io/current/clients/javadocs/org/apache/kafka/common/serialization/FloatSerializer.html>`_

    """  # noqa: E501

    def __call__(self, obj, ctx=None):
        """
        Args:
            obj (object): object to be serialized

            ctx (SerializationContext): Metadata pertaining to the serialization
                operation

        Note:
            None objects are represented as Kafka Null.

        Raises:
            SerializerError if an error occurs during serialization.

        Returns:
            IEEE 764 binary32 bytes if obj is not None, otherwise None
        """

        if obj is None:
            return None

        try:
            return _struct.pack(">f", obj)
        except _struct.error as e:
            raise SerializationError(str(e))


class ByteArraySerializer(Serializer):
    """
    Serializes bytes.

    See Also:
        `ByteArraySerializer Javadoc <https://docs.confluent.io/current/clients/javadocs/org/apache/kafka/common/serialization/ByteArraySerializer.html>`_

    """  # noqa: E501

    def __call__(self, obj, ctx=None):
        """
        Args:
            obj (object): object to be serialized

            ctx (SerializationContext): Metadata pertaining to the serialization
                operation

        Note:
            None objects are represented as Kafka Null.

        Raises:
            SerializerError if an error occurs during serialization.

        Returns:
            the bytes
        """

        if obj is None:
            return None

        if not isinstance(obj, bytes):
            raise SerializationError(f"ByteArraySerializer cannot serialize {obj}")

        return obj


STRING_SERIALIZER = StringSerializer()
BOOLEAN_SERIALIZER = BooleanSerializer()
SHORT_SERIALIZER = ShortSerializer()
INTEGER_SERIALIZER = IntegerSerializer()
LONG_SERIALIZER = LongSerializer()
FLOAT_SERIALIZER = FloatSerializer()
DOUBLE_SERIALIZER = DoubleSerializer()
BYTEARRAY_SERIALIZER = ByteArraySerializer()
