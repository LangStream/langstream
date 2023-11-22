from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import (
    ClassVar as _ClassVar,
    Iterable as _Iterable,
    Mapping as _Mapping,
    Optional as _Optional,
    Union as _Union,
)

DESCRIPTOR: _descriptor.FileDescriptor

class InfoResponse(_message.Message):
    __slots__ = ["json_info"]
    JSON_INFO_FIELD_NUMBER: _ClassVar[int]
    json_info: str
    def __init__(self, json_info: _Optional[str] = ...) -> None: ...

class Value(_message.Message):
    __slots__ = [
        "schema_id",
        "bytes_value",
        "boolean_value",
        "string_value",
        "byte_value",
        "short_value",
        "int_value",
        "long_value",
        "float_value",
        "double_value",
        "json_value",
        "avro_value",
    ]
    SCHEMA_ID_FIELD_NUMBER: _ClassVar[int]
    BYTES_VALUE_FIELD_NUMBER: _ClassVar[int]
    BOOLEAN_VALUE_FIELD_NUMBER: _ClassVar[int]
    STRING_VALUE_FIELD_NUMBER: _ClassVar[int]
    BYTE_VALUE_FIELD_NUMBER: _ClassVar[int]
    SHORT_VALUE_FIELD_NUMBER: _ClassVar[int]
    INT_VALUE_FIELD_NUMBER: _ClassVar[int]
    LONG_VALUE_FIELD_NUMBER: _ClassVar[int]
    FLOAT_VALUE_FIELD_NUMBER: _ClassVar[int]
    DOUBLE_VALUE_FIELD_NUMBER: _ClassVar[int]
    JSON_VALUE_FIELD_NUMBER: _ClassVar[int]
    AVRO_VALUE_FIELD_NUMBER: _ClassVar[int]
    schema_id: int
    bytes_value: bytes
    boolean_value: bool
    string_value: str
    byte_value: int
    short_value: int
    int_value: int
    long_value: int
    float_value: float
    double_value: float
    json_value: str
    avro_value: bytes
    def __init__(
        self,
        schema_id: _Optional[int] = ...,
        bytes_value: _Optional[bytes] = ...,
        boolean_value: bool = ...,
        string_value: _Optional[str] = ...,
        byte_value: _Optional[int] = ...,
        short_value: _Optional[int] = ...,
        int_value: _Optional[int] = ...,
        long_value: _Optional[int] = ...,
        float_value: _Optional[float] = ...,
        double_value: _Optional[float] = ...,
        json_value: _Optional[str] = ...,
        avro_value: _Optional[bytes] = ...,
    ) -> None: ...

class Header(_message.Message):
    __slots__ = ["name", "value"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    name: str
    value: Value
    def __init__(
        self,
        name: _Optional[str] = ...,
        value: _Optional[_Union[Value, _Mapping]] = ...,
    ) -> None: ...

class Schema(_message.Message):
    __slots__ = ["schema_id", "value"]
    SCHEMA_ID_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    schema_id: int
    value: bytes
    def __init__(
        self, schema_id: _Optional[int] = ..., value: _Optional[bytes] = ...
    ) -> None: ...

class Record(_message.Message):
    __slots__ = ["record_id", "key", "value", "headers", "origin", "timestamp"]
    RECORD_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    ORIGIN_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    record_id: int
    key: Value
    value: Value
    headers: _containers.RepeatedCompositeFieldContainer[Header]
    origin: str
    timestamp: int
    def __init__(
        self,
        record_id: _Optional[int] = ...,
        key: _Optional[_Union[Value, _Mapping]] = ...,
        value: _Optional[_Union[Value, _Mapping]] = ...,
        headers: _Optional[_Iterable[_Union[Header, _Mapping]]] = ...,
        origin: _Optional[str] = ...,
        timestamp: _Optional[int] = ...,
    ) -> None: ...

class TopicProducerWriteResult(_message.Message):
    __slots__ = ["record_id", "error"]
    RECORD_ID_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    record_id: int
    error: str
    def __init__(
        self, record_id: _Optional[int] = ..., error: _Optional[str] = ...
    ) -> None: ...

class TopicProducerResponse(_message.Message):
    __slots__ = ["topic", "schema", "record"]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    RECORD_FIELD_NUMBER: _ClassVar[int]
    topic: str
    schema: Schema
    record: Record
    def __init__(
        self,
        topic: _Optional[str] = ...,
        schema: _Optional[_Union[Schema, _Mapping]] = ...,
        record: _Optional[_Union[Record, _Mapping]] = ...,
    ) -> None: ...

class PermanentFailure(_message.Message):
    __slots__ = ["record_id", "error_message"]
    RECORD_ID_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    record_id: int
    error_message: str
    def __init__(
        self, record_id: _Optional[int] = ..., error_message: _Optional[str] = ...
    ) -> None: ...

class SourceRequest(_message.Message):
    __slots__ = ["committed_records", "permanent_failure"]
    COMMITTED_RECORDS_FIELD_NUMBER: _ClassVar[int]
    PERMANENT_FAILURE_FIELD_NUMBER: _ClassVar[int]
    committed_records: _containers.RepeatedScalarFieldContainer[int]
    permanent_failure: PermanentFailure
    def __init__(
        self,
        committed_records: _Optional[_Iterable[int]] = ...,
        permanent_failure: _Optional[_Union[PermanentFailure, _Mapping]] = ...,
    ) -> None: ...

class SourceResponse(_message.Message):
    __slots__ = ["schema", "records"]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    schema: Schema
    records: _containers.RepeatedCompositeFieldContainer[Record]
    def __init__(
        self,
        schema: _Optional[_Union[Schema, _Mapping]] = ...,
        records: _Optional[_Iterable[_Union[Record, _Mapping]]] = ...,
    ) -> None: ...

class ProcessorRequest(_message.Message):
    __slots__ = ["schema", "records"]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    schema: Schema
    records: _containers.RepeatedCompositeFieldContainer[Record]
    def __init__(
        self,
        schema: _Optional[_Union[Schema, _Mapping]] = ...,
        records: _Optional[_Iterable[_Union[Record, _Mapping]]] = ...,
    ) -> None: ...

class ProcessorResponse(_message.Message):
    __slots__ = ["schema", "results"]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    schema: Schema
    results: _containers.RepeatedCompositeFieldContainer[ProcessorResult]
    def __init__(
        self,
        schema: _Optional[_Union[Schema, _Mapping]] = ...,
        results: _Optional[_Iterable[_Union[ProcessorResult, _Mapping]]] = ...,
    ) -> None: ...

class ProcessorResult(_message.Message):
    __slots__ = ["record_id", "error", "records"]
    RECORD_ID_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    record_id: int
    error: str
    records: _containers.RepeatedCompositeFieldContainer[Record]
    def __init__(
        self,
        record_id: _Optional[int] = ...,
        error: _Optional[str] = ...,
        records: _Optional[_Iterable[_Union[Record, _Mapping]]] = ...,
    ) -> None: ...

class SinkRequest(_message.Message):
    __slots__ = ["schema", "record"]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    RECORD_FIELD_NUMBER: _ClassVar[int]
    schema: Schema
    record: Record
    def __init__(
        self,
        schema: _Optional[_Union[Schema, _Mapping]] = ...,
        record: _Optional[_Union[Record, _Mapping]] = ...,
    ) -> None: ...

class SinkResponse(_message.Message):
    __slots__ = ["record_id", "error"]
    RECORD_ID_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    record_id: int
    error: str
    def __init__(
        self, record_id: _Optional[int] = ..., error: _Optional[str] = ...
    ) -> None: ...
