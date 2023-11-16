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

from concurrent.futures import ThreadPoolExecutor, Future
from io import BytesIO

import fastavro

from langstream_grpc.api import Record, Sink
from langstream_grpc.proto.agent_pb2 import (
    Record as GrpcRecord,
    SinkRequest,
    Schema,
    Value,
    SinkResponse,
)
from langstream_grpc.tests.server_and_stub import ServerAndStub


async def test_write():
    async with ServerAndStub(
        "langstream_grpc.tests.test_grpc_sink.MySink"
    ) as server_and_stub:

        async def requests():
            schema = {
                "type": "record",
                "name": "Test",
                "namespace": "test",
                "fields": [{"name": "field", "type": {"type": "string"}}],
            }
            canonical_schema = fastavro.schema.to_parsing_canonical_form(schema)
            yield SinkRequest(
                schema=Schema(schema_id=42, value=canonical_schema.encode("utf-8"))
            )

            fp = BytesIO()
            try:
                fastavro.schemaless_writer(fp, schema, {"field": "test"})
                yield SinkRequest(
                    record=GrpcRecord(
                        record_id=43,
                        value=Value(schema_id=42, avro_value=fp.getvalue()),
                    )
                )
            finally:
                fp.close()

        responses: list[SinkResponse]
        responses = [
            response async for response in server_and_stub.stub.write(requests())
        ]

        assert len(responses) == 1
        assert responses[0].record_id == 43
        assert len(server_and_stub.server.agent.written_records) == 1
        assert (
            server_and_stub.server.agent.written_records[0].value().value["field"]
            == "test"
        )


async def test_write_error():
    async with ServerAndStub(
        "langstream_grpc.tests.test_grpc_sink.MyErrorSink"
    ) as server_and_stub:
        responses: list[SinkResponse]
        responses = [
            response
            async for response in server_and_stub.stub.write(
                [
                    SinkRequest(
                        record=GrpcRecord(
                            value=Value(string_value="test"),
                        )
                    )
                ]
            )
        ]
        assert len(responses) == 1
        assert responses[0].error == "test-error"


async def test_write_future():
    async with ServerAndStub(
        "langstream_grpc.tests.test_grpc_sink.MyFutureSink"
    ) as server_and_stub:
        responses: list[SinkResponse]
        responses = [
            response
            async for response in server_and_stub.stub.write(
                [
                    SinkRequest(
                        record=GrpcRecord(
                            record_id=42,
                            value=Value(string_value="test"),
                        )
                    )
                ]
            )
        ]
        assert len(responses) == 1
        assert responses[0].record_id == 42
        assert len(server_and_stub.server.agent.written_records) == 1
        assert server_and_stub.server.agent.written_records[0].value() == "test"


class MySink(Sink):
    def __init__(self):
        self.written_records = []

    def write(self, record: Record):
        self.written_records.append(record)


class MyErrorSink(Sink):
    def write(self, record: Record):
        raise RuntimeError("test-error")


class MyFutureSink(Sink):
    def __init__(self):
        self.written_records = []
        self.executor = ThreadPoolExecutor(max_workers=10)

    def write(self, record: Record) -> Future[None]:
        return self.executor.submit(lambda r: self.written_records.append(r), record)
