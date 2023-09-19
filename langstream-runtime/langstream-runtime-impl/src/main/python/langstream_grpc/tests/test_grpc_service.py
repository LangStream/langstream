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

import json
from io import BytesIO
from typing import List, Dict, Any

import fastavro
import grpc
import pytest
from google.protobuf import empty_pb2

from langstream_grpc.grpc_service import AgentServer
from langstream_grpc.proto.agent_pb2 import (
    ProcessorRequest,
    Record as GrpcRecord,
    Value,
    Header,
    ProcessorResponse,
    Schema,
    InfoResponse,
)
from langstream_grpc.proto.agent_pb2_grpc import AgentServiceStub
from langstream_runtime.api import Record, RecordType
from langstream_runtime.util import SingleRecordProcessor


@pytest.fixture(autouse=True)
def stub():
    config = """{
      "className": "langstream_grpc.tests.test_grpc_service.MyProcessor"
    }"""
    server = AgentServer("[::]:0", config)
    server.start()
    channel = grpc.insecure_channel("localhost:%d" % server.port)

    yield AgentServiceStub(channel=channel)

    channel.close()
    server.stop()


@pytest.mark.parametrize(
    "input_type,output_type,value,key,header",
    [
        pytest.param(
            "string_value", "string_value", "test-value", "test-key", "test-header"
        ),
        pytest.param(
            "bytes_value", "bytes_value", b"test-value", b"test-key", b"test-header"
        ),
        pytest.param("byte_value", "long_value", 42, 43, 44),
        pytest.param("short_value", "long_value", 42, 43, 44),
        pytest.param("int_value", "long_value", 42, 43, 44),
        pytest.param("float_value", "double_value", 42.0, 43.0, 44.0),
        pytest.param("double_value", "double_value", 42.0, 43.0, 44.0),
    ],
)
def test_process(input_type, output_type, value, key, header, request):
    stub = request.getfixturevalue("stub")

    record = GrpcRecord(
        record_id=42,
        key=Value(**{input_type: key}),
        value=Value(**{input_type: value}),
        headers=[
            Header(
                name="test-header",
                value=Value(**{input_type: header}),
            )
        ],
        origin="test-origin",
        timestamp=43,
    )
    response: ProcessorResponse
    for response in stub.process(iter([ProcessorRequest(records=[record])])):
        assert len(response.results) == 1
        assert response.results[0].record_id == record.record_id
        assert response.results[0].HasField("error") is False
        assert len(response.results[0].records) == 1
        result = response.results[0].records[0]
        assert result.key == Value(**{output_type: key})
        assert result.value == Value(**{output_type: value})
        assert len(result.headers) == 1
        assert result.headers[0].name == result.headers[0].name
        assert result.headers[0].value == Value(**{output_type: header})
        assert result.origin == record.origin
        assert result.timestamp == record.timestamp


def test_avro(stub):
    requests = []
    schema = {
        "type": "record",
        "name": "Test",
        "namespace": "test",
        "fields": [{"name": "field", "type": {"type": "string"}}],
    }
    canonical_schema = fastavro.schema.to_parsing_canonical_form(schema)
    requests.append(
        ProcessorRequest(
            schema=Schema(schema_id=42, value=canonical_schema.encode("utf-8"))
        )
    )

    fp = BytesIO()
    try:
        fastavro.schemaless_writer(fp, schema, {"field": "test"})
        requests.append(
            ProcessorRequest(
                records=[
                    GrpcRecord(
                        record_id=43,
                        value=Value(schema_id=42, avro_value=fp.getvalue()),
                    )
                ]
            )
        )
    finally:
        fp.close()

    responses: list[ProcessorResponse]
    responses = list(stub.process(iter(requests)))
    response_schema = responses[0]
    assert len(response_schema.results) == 0
    assert response_schema.HasField("schema")
    assert response_schema.schema.schema_id == 1
    assert response_schema.schema.value.decode("utf-8") == canonical_schema

    response_record = responses[1]
    assert len(response_record.results) == 1
    result = response_record.results[0]
    assert result.record_id == 43
    assert len(result.records) == 1
    assert result.records[0].value.schema_id == 1
    fp = BytesIO(result.records[0].value.avro_value)
    try:
        decoded = fastavro.schemaless_reader(fp, json.loads(canonical_schema))
        assert decoded == {"field": "test"}
    finally:
        fp.close()


def test_empty_record(request):
    stub = request.getfixturevalue("stub")
    record = GrpcRecord()
    for response in stub.process(iter([ProcessorRequest(records=[record])])):
        assert len(response.results) == 1
        assert response.results[0].record_id == 0
        assert response.results[0].HasField("error") is False
        assert len(response.results[0].records) == 1
        result = response.results[0].records[0]
        assert result.HasField("key") is False
        assert result.HasField("value") is False
        assert len(result.headers) == 0
        assert result.origin == ""
        assert result.HasField("timestamp") is False


def test_failing_record(request):
    stub = request.getfixturevalue("stub")
    record = GrpcRecord(origin="failing-record")
    for response in stub.process(iter([ProcessorRequest(records=[record])])):
        assert len(response.results) == 1
        assert response.results[0].HasField("error") is True
        assert response.results[0].error == "failure"


def test_info(stub):
    info: InfoResponse = stub.agent_info(empty_pb2.Empty())
    assert info.json_info == '{"test-info-key": "test-info-value"}'


class MyProcessor(SingleRecordProcessor):
    def agent_info(self) -> Dict[str, Any]:
        return {"test-info-key": "test-info-value"}

    def process_record(self, record: Record) -> List[RecordType]:
        if record.origin() == "failing-record":
            raise Exception("failure")
        return [record]
