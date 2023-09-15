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

import concurrent
from typing import List

import grpc
import pytest

from langstream_grpc.grpc_service import AgentService
from langstream_grpc.proto import agent_pb2_grpc
from langstream_grpc.proto.agent_pb2 import (
    ProcessorRequest,
    Record as GrpcRecord,
    Value,
    Header,
    ProcessorResponse,
)
from langstream_grpc.proto.agent_pb2_grpc import AgentServiceStub
from langstream_runtime.util import Record, RecordType, SingleRecordProcessor


@pytest.fixture(autouse=True)
def stub():
    thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=10)
    server = grpc.server(thread_pool)
    agent_pb2_grpc.add_AgentServiceServicer_to_server(
        AgentService(MyProcessor()), server
    )
    port = server.add_insecure_port("[::]:0")
    server.start()
    channel = grpc.insecure_channel("localhost:%d" % port)

    yield AgentServiceStub(channel=channel)

    channel.close()
    server.stop(None)
    thread_pool.shutdown(wait=True)


@pytest.mark.parametrize(
    "input_type,output_type,value,key,header",
    [
        pytest.param(
            "stringValue", "stringValue", "test-value", "test-key", "test-header"
        ),
        pytest.param(
            "bytesValue", "bytesValue", b"test-value", b"test-key", b"test-header"
        ),
        pytest.param("byteValue", "longValue", 42, 43, 44),
        pytest.param("shortValue", "longValue", 42, 43, 44),
        pytest.param("intValue", "longValue", 42, 43, 44),
        pytest.param("floatValue", "doubleValue", 42.0, 43.0, 44.0),
        pytest.param("doubleValue", "doubleValue", 42.0, 43.0, 44.0),
    ],
)
def test_process(input_type, output_type, value, key, header, request):
    stub = request.getfixturevalue("stub")

    record = GrpcRecord(
        recordId=1,
        key=Value(**{input_type: key}),
        value=Value(**{input_type: value}),
        headers=[
            Header(
                name="test-header",
                value=Value(**{input_type: header}),
            )
        ],
        origin="test-origin",
        timestamp=42,
    )
    response: ProcessorResponse
    for response in stub.process(iter([ProcessorRequest(records=[record])])):
        assert len(response.results) == 1
        assert response.results[0].recordId == record.recordId
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


def test_empty_record(request):
    stub = request.getfixturevalue("stub")
    record = GrpcRecord()
    for response in stub.process(iter([ProcessorRequest(records=[record])])):
        assert len(response.results) == 1
        assert response.results[0].recordId == 0
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


class MyProcessor(SingleRecordProcessor):
    def process_record(self, record: Record) -> List[RecordType]:
        if record.origin() == "failing-record":
            raise Exception("failure")
        return [record]
