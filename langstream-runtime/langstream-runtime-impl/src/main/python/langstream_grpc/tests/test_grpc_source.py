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
from typing import List

import fastavro
import grpc
import pytest

from langstream_grpc.api import Record, RecordType, Source
from langstream_grpc.proto.agent_pb2 import (
    SourceRequest,
    PermanentFailure,
)
from langstream_grpc.tests.server_and_stub import ServerAndStub
from langstream_grpc.util import AvroValue, SimpleRecord


@pytest.mark.parametrize("klass", ["MyAsyncSource"])
async def test_read(klass):
    async with ServerAndStub(
        f"langstream_grpc.tests.test_grpc_source.{klass}"
    ) as server_and_stub:
        read_call = server_and_stub.stub.read()
        response = await read_call.read()

        assert len(response.records) == 0
        assert response.HasField("schema")
        assert response.schema.schema_id == 1
        schema = response.schema.value.decode("utf-8")
        assert (
            schema
            == '{"name":"test.Test","type":"record","fields":[{"name":"field","type":"string"}]}'  # noqa: E501
        )

        response = await read_call.read()
        assert len(response.records) == 1
        record = response.records[0]
        assert record.record_id == 1
        assert record.value.schema_id == 1
        fp = BytesIO(record.value.avro_value)
        try:
            decoded = fastavro.schemaless_reader(fp, json.loads(schema))
            assert decoded == {"field": "test"}
        finally:
            fp.close()

        response = await read_call.read()
        assert len(response.records) == 1
        record = response.records[0]
        assert record.record_id == 2
        assert record.value.long_value == 42

        response = await read_call.read()
        assert len(response.records) == 1
        record = response.records[0]
        assert record.record_id == 3
        assert record.value.long_value == 43

        await read_call.done_writing()

        assert await read_call.read() == grpc.aio.EOF


@pytest.mark.parametrize("klass", ["MySource", "MyAsyncSource"])
async def test_commit(klass):
    async with ServerAndStub(
        f"langstream_grpc.tests.test_grpc_source.{klass}"
    ) as server_and_stub:
        read_call = server_and_stub.stub.read()

        # first read is a schema
        await read_call.read()

        for _ in range(3):
            response = await read_call.read()
            assert len(response.records) == 1
            await read_call.write(
                SourceRequest(committed_records=[response.records[0].record_id])
            )

        with pytest.raises(grpc.RpcError):
            await read_call.read()

        sent = server_and_stub.server.agent.sent
        committed = server_and_stub.server.agent.committed
        assert len(committed) == 2
        assert committed[0] == sent[0]
        assert committed[1].value() == sent[1]["value"]


@pytest.mark.parametrize("klass", ["MySource", "MyAsyncSource"])
async def test_permanent_failure(klass):
    async with ServerAndStub(
        f"langstream_grpc.tests.test_grpc_source.{klass}"
    ) as server_and_stub:
        read_call = server_and_stub.stub.read()

        # first read is a schema
        await read_call.read()

        response = await read_call.read()
        await read_call.write(
            SourceRequest(
                permanent_failure=PermanentFailure(
                    record_id=response.records[0].record_id, error_message="failure"
                )
            )
        )

        await read_call.done_writing()

        try:
            response = await read_call.read()
            while response != grpc.aio.EOF:
                response = await read_call.read()
            pytest.fail("call should have raised an exception")
        except grpc.RpcError as e:
            assert "failure" in e.details()

        failures = server_and_stub.server.agent.failures
        assert len(failures) == 1
        assert failures[0][0] == server_and_stub.server.agent.sent[0]
        assert str(failures[0][1]) == "failure"


async def test_read_error():
    async with ServerAndStub(
        "langstream_grpc.tests.test_grpc_source.MyErrorSource"
    ) as server_and_stub:
        read_call = server_and_stub.stub.read()
        try:
            await read_call.read()
            pytest.fail("call should have raised an exception")
        except grpc.RpcError as e:
            assert "test-error" in e.details()


class MySource(Source):
    def __init__(self):
        self.records = [
            SimpleRecord(
                value=AvroValue(
                    schema={
                        "type": "record",
                        "name": "Test",
                        "namespace": "test",
                        "fields": [{"name": "field", "type": {"type": "string"}}],
                    },
                    value={"field": "test"},
                )
            ),
            {"value": 42},
            (43,),
        ]
        self.sent = []
        self.committed = []
        self.failures = []

    def read(self) -> List[RecordType]:
        if len(self.records) > 0:
            record = self.records.pop(0)
            self.sent.append(record)
            return [record]
        return []

    def commit(self, record: Record):
        if record.value() == 43:
            raise Exception("test error")
        self.committed.append(record)

    def permanent_failure(self, record: Record, error: Exception):
        self.failures.append((record, error))
        raise error


class MyAsyncSource(MySource):
    async def read(self) -> List[RecordType]:
        return super().read()

    async def commit(self, record: Record):
        super().commit(record)

    async def permanent_failure(self, record: Record, error: Exception):
        return super().permanent_failure(record, error)


class MyErrorSource(Source):
    def read(self) -> List[RecordType]:
        raise ValueError("test-error")
