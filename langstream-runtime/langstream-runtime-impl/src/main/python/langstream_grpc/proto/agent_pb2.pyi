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

class Value(_message.Message):
    __slots__ = [
        "schemaId",
        "bytesValue",
        "booleanValue",
        "stringValue",
        "byteValue",
        "shortValue",
        "intValue",
        "longValue",
        "floatValue",
        "doubleValue",
        "jsonValue",
        "avroValue",
    ]
    SCHEMAID_FIELD_NUMBER: _ClassVar[int]
    BYTESVALUE_FIELD_NUMBER: _ClassVar[int]
    BOOLEANVALUE_FIELD_NUMBER: _ClassVar[int]
    STRINGVALUE_FIELD_NUMBER: _ClassVar[int]
    BYTEVALUE_FIELD_NUMBER: _ClassVar[int]
    SHORTVALUE_FIELD_NUMBER: _ClassVar[int]
    INTVALUE_FIELD_NUMBER: _ClassVar[int]
    LONGVALUE_FIELD_NUMBER: _ClassVar[int]
    FLOATVALUE_FIELD_NUMBER: _ClassVar[int]
    DOUBLEVALUE_FIELD_NUMBER: _ClassVar[int]
    JSONVALUE_FIELD_NUMBER: _ClassVar[int]
    AVROVALUE_FIELD_NUMBER: _ClassVar[int]
    schemaId: int
    bytesValue: bytes
    booleanValue: bool
    stringValue: str
    byteValue: int
    shortValue: int
    intValue: int
    longValue: int
    floatValue: float
    doubleValue: float
    jsonValue: str
    avroValue: bytes
    def __init__(
        self,
        schemaId: _Optional[int] = ...,
        bytesValue: _Optional[bytes] = ...,
        booleanValue: bool = ...,
        stringValue: _Optional[str] = ...,
        byteValue: _Optional[int] = ...,
        shortValue: _Optional[int] = ...,
        intValue: _Optional[int] = ...,
        longValue: _Optional[int] = ...,
        floatValue: _Optional[float] = ...,
        doubleValue: _Optional[float] = ...,
        jsonValue: _Optional[str] = ...,
        avroValue: _Optional[bytes] = ...,
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
    __slots__ = ["schemaId", "value"]
    SCHEMAID_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    schemaId: int
    value: bytes
    def __init__(
        self, schemaId: _Optional[int] = ..., value: _Optional[bytes] = ...
    ) -> None: ...

class Record(_message.Message):
    __slots__ = ["recordId", "key", "value", "headers", "origin", "timestamp"]
    RECORDID_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    ORIGIN_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    recordId: int
    key: Value
    value: Value
    headers: _containers.RepeatedCompositeFieldContainer[Header]
    origin: str
    timestamp: int
    def __init__(
        self,
        recordId: _Optional[int] = ...,
        key: _Optional[_Union[Value, _Mapping]] = ...,
        value: _Optional[_Union[Value, _Mapping]] = ...,
        headers: _Optional[_Iterable[_Union[Header, _Mapping]]] = ...,
        origin: _Optional[str] = ...,
        timestamp: _Optional[int] = ...,
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
    __slots__ = ["recordId", "error", "records"]
    RECORDID_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    recordId: int
    error: str
    records: _containers.RepeatedCompositeFieldContainer[Record]
    def __init__(
        self,
        recordId: _Optional[int] = ...,
        error: _Optional[str] = ...,
        records: _Optional[_Iterable[_Union[Record, _Mapping]]] = ...,
    ) -> None: ...
