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
import queue
from io import BytesIO
from typing import List

import fastavro
import grpc
import pytest

from langstream_grpc.grpc_service import AgentServer
from langstream_grpc.proto.agent_pb2 import (
    SourceResponse,
    SourceRequest,
)
from langstream_grpc.proto.agent_pb2_grpc import AgentServiceStub
from langstream_runtime.api import Record, RecordType, Source
from langstream_runtime.util import SimpleRecord, AvroValue


@pytest.fixture(autouse=True)
def server_and_stub():
    config = """{
      "className": "langstream_grpc.tests.test_grpc_source.MySource"
    }"""
    server = AgentServer("[::]:0", config)
    server.start()
    channel = grpc.insecure_channel("localhost:%d" % server.port)

    yield server, AgentServiceStub(channel=channel)

    channel.close()
    server.stop()


def test_read(server_and_stub):
    server, stub = server_and_stub

    responses: list[SourceResponse]
    responses = list(stub.read(iter([])))

    response_schema = responses[0]
    assert len(response_schema.records) == 0
    assert response_schema.HasField("schema")
    assert response_schema.schema.schema_id == 1
    schema = response_schema.schema.value.decode("utf-8")
    assert (
        schema
        == '{"name":"test.Test","type":"record","fields":[{"name":"field","type":"string"}]}'  # noqa: E501
    )

    response_record = responses[1]
    assert len(response_schema.records) == 0
    record = response_record.records[0]
    assert record.record_id == 1
    assert record.value.schema_id == 1
    fp = BytesIO(record.value.avro_value)
    try:
        decoded = fastavro.schemaless_reader(fp, json.loads(schema))
        assert decoded == {"field": "test"}
    finally:
        fp.close()


def test_commit(server_and_stub):
    server, stub = server_and_stub
    to_commit = queue.Queue()

    def send_commit():
        committed = 0
        while committed < 2:
            try:
                commit_id = to_commit.get(True)
                yield SourceRequest(committed_records=[commit_id])
                committed += 1
            except queue.Empty:
                pass

    with pytest.raises(grpc.RpcError):
        response: SourceResponse
        for response in stub.read(iter(send_commit())):
            for record in response.records:
                to_commit.put(record.record_id)

    assert len(server.agent.committed) == 1
    assert server.agent.committed[0] == server.agent.sent[0]


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
            SimpleRecord(value=42),
        ]
        self.sent = []
        self.committed = []

    def read(self) -> List[RecordType]:
        if len(self.records) > 0:
            record = self.records.pop(0)
            self.sent.append(record)
            return [record]
        return []

    def commit(self, records: List[Record]):
        for record in records:
            if record.value() == 42:
                raise Exception("test error")
        self.committed.extend(records)
