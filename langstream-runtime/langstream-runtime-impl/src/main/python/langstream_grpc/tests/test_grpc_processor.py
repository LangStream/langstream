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
from concurrent.futures import ThreadPoolExecutor, Future
from io import BytesIO
from typing import List, Dict, Any

import fastavro
import pytest
from google.protobuf import empty_pb2

from langstream_grpc.api import Record, RecordType, Processor, AgentContext
from langstream_grpc.proto.agent_pb2 import (
    ProcessorRequest,
    Record as GrpcRecord,
    Value,
    Header,
    ProcessorResponse,
    Schema,
    InfoResponse,
)
from langstream_grpc.tests.server_and_stub import ServerAndStub


@pytest.mark.parametrize(
    "input_type,output_type,value,key,header",
    [
        pytest.param(
            "string_value", "string_value", "test-value", "test-key", "test-header"
        ),
        pytest.param(
            "bytes_value", "bytes_value", b"test-value", b"test-key", b"test-header"
        ),
        pytest.param(
            "json_value",
            "json_value",
            '{"test": "value"}',
            '{"test": "key"}',
            '{"test": "header"}',
        ),
        pytest.param("boolean_value", "boolean_value", True, False, True),
        pytest.param("boolean_value", "boolean_value", False, True, False),
        pytest.param("byte_value", "long_value", 42, 43, 44),
        pytest.param("short_value", "long_value", 42, 43, 44),
        pytest.param("int_value", "long_value", 42, 43, 44),
        pytest.param("float_value", "double_value", 42.0, 43.0, 44.0),
        pytest.param("double_value", "double_value", 42.0, 43.0, 44.0),
    ],
)
async def test_process(input_type, output_type, value, key, header):
    async with ServerAndStub(
        "langstream_grpc.tests.test_grpc_processor.MyProcessor"
    ) as server_and_stub:
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
        async for response in server_and_stub.stub.process(
            [ProcessorRequest(records=[record])]
        ):
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


async def test_avro():
    async with ServerAndStub(
        "langstream_grpc.tests.test_grpc_processor.MyProcessor"
    ) as server_and_stub:
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
        responses = [
            response async for response in server_and_stub.stub.process(requests)
        ]
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


async def test_empty_record():
    async with ServerAndStub(
        "langstream_grpc.tests.test_grpc_processor.MyProcessor"
    ) as server_and_stub:
        response: ProcessorResponse
        async for response in server_and_stub.stub.process(
            [ProcessorRequest(records=[GrpcRecord()])]
        ):
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


async def test_failing_record():
    async with ServerAndStub(
        "langstream_grpc.tests.test_grpc_processor.MyFailingProcessor"
    ) as server_and_stub:
        async for response in server_and_stub.stub.process(
            [ProcessorRequest(records=[GrpcRecord()])]
        ):
            assert len(response.results) == 1
            assert response.results[0].HasField("error") is True
            assert response.results[0].error == "failure"


@pytest.mark.parametrize("klass", ["MyFutureProcessor", "MyAsyncProcessor"])
async def test_future_record(klass):
    async with ServerAndStub(
        f"langstream_grpc.tests.test_grpc_processor.{klass}"
    ) as server_and_stub:
        response: ProcessorResponse
        async for response in server_and_stub.stub.process(
            [ProcessorRequest(records=[GrpcRecord(value=Value(string_value="test"))])]
        ):
            assert len(response.results) == 1
            assert response.results[0].HasField("error") is False
            assert len(response.results[0].records) == 1
            assert response.results[0].records[0].value.string_value == "test"


async def test_big_record():
    async with ServerAndStub(
        "langstream_grpc.tests.test_grpc_processor.MyProcessor"
    ) as server_and_stub:
        long_string = "a" * 10_000_000
        response: ProcessorResponse
        async for response in server_and_stub.stub.process(
            [
                ProcessorRequest(
                    records=[GrpcRecord(value=Value(string_value=long_string))]
                )
            ]
        ):
            assert len(response.results) == 1
            assert response.results[0].HasField("error") is False
            assert len(response.results[0].records) == 1
            assert response.results[0].records[0].value.string_value == long_string


async def test_info():
    async with ServerAndStub(
        "langstream_grpc.tests.test_grpc_processor.MyProcessor"
    ) as server_and_stub:
        info: InfoResponse = await server_and_stub.stub.agent_info(empty_pb2.Empty())
        assert info.json_info == '{"test-info-key": "test-info-value"}'


async def test_init_one_parameter():
    async with ServerAndStub(
        "langstream_grpc.tests.test_grpc_processor.ProcessorInitOneParameter",
        {"my-param": "my-value"},
    ) as server_and_stub:
        async for response in server_and_stub.stub.process(
            [ProcessorRequest(records=[GrpcRecord()])]
        ):
            assert len(response.results) == 1
            result = response.results[0].records[0]
            assert result.value.string_value == "my-value"


async def test_processor_use_context():
    async with ServerAndStub(
        "langstream_grpc.tests.test_grpc_processor.ProcessorUseContext",
        {"my-param": "my-value"},
        {"persistentStateDirectory": "/tmp/processor"},
    ) as server_and_stub:
        async for response in server_and_stub.stub.process(
            [ProcessorRequest(records=[GrpcRecord()])]
        ):
            assert len(response.results) == 1
            result = response.results[0].records[0]
            assert result.value.string_value == "directory is /tmp/processor"


class MyProcessor(Processor):
    def agent_info(self) -> Dict[str, Any]:
        return {"test-info-key": "test-info-value"}

    def process(self, record: Record) -> List[RecordType]:
        if isinstance(record.value(), str):
            return [record]
        if isinstance(record.value(), float):
            return [
                {
                    "value": record.value(),
                    "key": record.key(),
                    "headers": record.headers(),
                    "origin": record.origin(),
                    "timestamp": record.timestamp(),
                }
            ]
        return [
            (
                record.value(),
                record.key(),
                record.headers(),
                record.origin(),
                record.timestamp(),
            )
        ]


class MyFailingProcessor(Processor):
    def process(self, record: Record) -> List[RecordType]:
        raise Exception("failure")


class MyFutureProcessor(Processor):
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=10)

    def process(self, record: Record) -> Future[List[RecordType]]:
        return self.executor.submit(lambda r: [r], record)


class MyAsyncProcessor(Processor):
    async def process(self, record: Record) -> List[RecordType]:
        return [record]


class ProcessorInitOneParameter:
    def __init__(self):
        self.myparam = None

    def init(self, agent_config):
        self.myparam = agent_config["my-param"]

    def process(self, _) -> List[RecordType]:
        return [{"value": self.myparam}]


class ProcessorUseContext(Processor):
    def __init__(self):
        self.myparam = None
        self.context = None

    def init(self, agent_config, context: AgentContext):
        self.myparam = agent_config["my-param"]
        self.context = context

    def process(self, record: Record) -> List[RecordType]:
        return [
            {"value": f"directory is {self.context.get_persistent_state_directory()}"}
        ]
