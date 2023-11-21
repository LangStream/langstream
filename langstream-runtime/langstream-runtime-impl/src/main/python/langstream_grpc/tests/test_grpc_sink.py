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
import grpc.aio
import pytest

from langstream_grpc.api import Record, Sink
from langstream_grpc.proto.agent_pb2 import (
    Record as GrpcRecord,
    SinkRequest,
    Schema,
    Value,
    SinkResponse,
)
from langstream_grpc.tests.server_and_stub import ServerAndStub


@pytest.mark.parametrize("klass", ["MySink", "MyFutureSink", "MyAsyncSink"])
async def test_write(klass):
    async with ServerAndStub(
        f"langstream_grpc.tests.test_grpc_sink.{klass}"
    ) as server_and_stub:
        write_call = server_and_stub.stub.write()

        schema = {
            "type": "record",
            "name": "Test",
            "namespace": "test",
            "fields": [{"name": "field", "type": {"type": "string"}}],
        }
        canonical_schema = fastavro.schema.to_parsing_canonical_form(schema)
        await write_call.write(
            SinkRequest(
                schema=Schema(schema_id=42, value=canonical_schema.encode("utf-8"))
            )
        )

        fp = BytesIO()
        try:
            fastavro.schemaless_writer(fp, schema, {"field": "test"})
            await write_call.write(
                SinkRequest(
                    record=GrpcRecord(
                        record_id=43,
                        value=Value(schema_id=42, avro_value=fp.getvalue()),
                    )
                )
            )
        finally:
            fp.close()

        response = await write_call.read()
        assert response.record_id == 43
        assert response.error == ""
        assert len(server_and_stub.server.agent.written_records) == 1
        assert (
            server_and_stub.server.agent.written_records[0].value().value["field"]
            == "test"
        )

        await write_call.done_writing()
        assert await write_call.read() == grpc.aio.EOF


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


class MySink(Sink):
    def __init__(self):
        self.written_records = []

    def write(self, record: Record):
        self.written_records.append(record)


class MyFutureSink(Sink):
    def __init__(self):
        self.written_records = []
        self.executor = ThreadPoolExecutor(max_workers=10)

    def write(self, record: Record) -> Future[None]:
        return self.executor.submit(lambda r: self.written_records.append(r), record)


class MyAsyncSink(MySink):
    async def write(self, record: Record):
        super().write(record)


class MyErrorSink(Sink):
    def write(self, record: Record):
        raise RuntimeError("test-error")
