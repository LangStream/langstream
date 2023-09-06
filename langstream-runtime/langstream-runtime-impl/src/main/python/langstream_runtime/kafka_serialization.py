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
    Deserializer,
    SerializationError,
    StringSerializer,
    DoubleSerializer,
    IntegerSerializer,
    StringDeserializer,
    DoubleDeserializer,
    IntegerDeserializer,
)


class BooleanSerializer(Serializer):
    """
    Serializes bool to bytes.

    See Also:
        `BooleanSerializer Javadoc <https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/serialization/BooleanSerializer.java>`_
    """  # noqa: E501

    def __call__(self, obj, ctx=None):
        """
        Serializes bool as bytes.

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

        try:
            return _struct.pack(">?", obj)
        except _struct.error as e:
            raise SerializationError(str(e))


class BooleanDeserializer(Deserializer):
    """
    Deserializes bool from bytes.

    See Also:
        `BooleanDeserializer Javadoc <https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/serialization/BooleanDeserializer.java>`_
    """  # noqa: E501

    def __call__(self, value, ctx=None):
        """
        Deserializes bool from bytes.

        Args:
            value (bytes): bytes to be deserialized

            ctx (SerializationContext): Metadata pertaining to the serialization
                operation

        Raises:
            SerializerError if an error occurs during deserialization.

        Returns:
            bool if data is not None, otherwise None
        """

        if value is None:
            return None

        try:
            return _struct.unpack(">?", value)[0]
        except _struct.error as e:
            raise SerializationError(str(e))


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


class ShortDeserializer(Deserializer):
    """
    Deserializes int from int16 bytes.

    See Also:
        `ShortDeserializer Javadoc <https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/serialization/ShortDeserializer.java>`_
    """  # noqa: E501

    def __call__(self, value, ctx=None):
        """
        Deserializes int from int16 bytes.

        Args:
            value (bytes): bytes to be deserialized

            ctx (SerializationContext): Metadata pertaining to the serialization
                operation

        Raises:
            SerializerError if an error occurs during deserialization.

        Returns:
            int if data is not None, otherwise None
        """

        if value is None:
            return None

        try:
            return _struct.unpack(">h", value)[0]
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


class LongDeserializer(Deserializer):
    """
    Deserializes int from int64 bytes.

    See Also:
        `LongDeserializer Javadoc <https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/serialization/LongDeserializer.java>`_
    """  # noqa: E501

    def __call__(self, value, ctx=None):
        """
        Deserializes int from int32 bytes.

        Args:
            value (bytes): bytes to be deserialized

            ctx (SerializationContext): Metadata pertaining to the serialization
                operation

        Raises:
            SerializerError if an error occurs during deserialization.

        Returns:
            int if data is not None, otherwise None
        """

        if value is None:
            return None

        try:
            return _struct.unpack(">q", value)[0]
        except _struct.error as e:
            raise SerializationError(str(e))


class FloatSerializer(Serializer):
    """
    Serializes float to IEEE 754 binary32 bytes.

    See Also:
        `FloatSerializer Javadoc <https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/serialization/FloatSerializer.java>`_

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


class FloatDeserializer(Deserializer):
    """
    Deserializes float from IEEE 754 binary32 bytes.

    See Also:
        `FloatDeserializer Javadoc <https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/serialization/FloatDeserializer.java>`_
    """  # noqa: E501

    def __call__(self, value, ctx=None):
        """
        Deserializes float from IEEE 754 binary32 bytes.

        Args:
            value (bytes): bytes to be deserialized

            ctx (SerializationContext): Metadata pertaining to the serialization
                operation

        Raises:
            SerializerError if an error occurs during deserialization.

        Returns:
            float if data is not None, otherwise None
        """

        if value is None:
            return None

        try:
            return _struct.unpack(">f", value)[0]
        except _struct.error as e:
            raise SerializationError(str(e))


class ByteArraySerializer(Serializer):
    """
    Serializes bytes.

    See Also:
        `ByteArraySerializer Javadoc <https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/serialization/ByteArraySerializer.java>`_

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
            raise SerializationError(
                f"ByteArraySerializer: a bytes-like object is required, not {type(obj)}"
            )

        return obj


class ByteArrayDeserializer(Deserializer):
    """
    Deserializes bytes.

    See Also:
        `ByteArrayDeserializer Javadoc <https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/serialization/ByteArrayDeserializer.java>`_
    """  # noqa: E501

    def __call__(self, value, ctx=None):
        """
        Deserializes bytes.

        Args:
            value (bytes): bytes to be deserialized

            ctx (SerializationContext): Metadata pertaining to the serialization
                operation

        Raises:
            SerializerError if an error occurs during deserialization.

        Returns:
            float if data is not None, otherwise None
        """

        if value is None:
            return None

        if not isinstance(value, bytes):
            raise SerializationError(
                f"ByteArrayDeserializer: a bytes-like object is required, "
                f"not {type(value)}"
            )

        return value


STRING_SERIALIZER = StringSerializer()
BOOLEAN_SERIALIZER = BooleanSerializer()
SHORT_SERIALIZER = ShortSerializer()
INTEGER_SERIALIZER = IntegerSerializer()
LONG_SERIALIZER = LongSerializer()
FLOAT_SERIALIZER = FloatSerializer()
DOUBLE_SERIALIZER = DoubleSerializer()
BYTEARRAY_SERIALIZER = ByteArraySerializer()

STRING_DESERIALIZER = StringDeserializer()
BOOLEAN_DESERIALIZER = BooleanDeserializer()
SHORT_DESERIALIZER = ShortDeserializer()
INTEGER_DESERIALIZER = IntegerDeserializer()
LONG_DESERIALIZER = LongDeserializer()
FLOAT_DESERIALIZER = FloatDeserializer()
DOUBLE_DESERIALIZER = DoubleDeserializer()
BYTEARRAY_DESERIALIZER = ByteArrayDeserializer()
