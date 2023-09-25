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

from io import BytesIO
from typing import List, Optional

import fastavro
import grpc
import pytest

from langstream_grpc.grpc_service import AgentServer
from langstream_grpc.proto.agent_pb2 import (
    Record as GrpcRecord,
    SinkRequest,
    Schema,
    Value,
    SinkResponse,
)
from langstream_grpc.proto.agent_pb2_grpc import AgentServiceStub
from langstream_runtime.api import Record, Sink, CommitCallback


@pytest.fixture(autouse=True)
def server_and_stub():
    config = """{
      "className": "langstream_grpc.tests.test_grpc_sink.MySink"
    }"""
    server = AgentServer("[::]:0", config)
    server.start()
    channel = grpc.insecure_channel("localhost:%d" % server.port)

    yield server, AgentServiceStub(channel=channel)

    channel.close()
    server.stop()


def test_write(server_and_stub):
    server, stub = server_and_stub

    def requests():
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
    responses = list(stub.write(iter(requests())))
    assert len(responses) == 1
    assert responses[0].record_id == 43
    assert len(server.agent.written_records) == 1
    assert server.agent.written_records[0].value().value["field"] == "test"


def test_write_error(server_and_stub):
    server, stub = server_and_stub

    responses: list[SinkResponse]
    responses = list(
        stub.write(
            iter(
                [
                    SinkRequest(
                        record=GrpcRecord(
                            value=Value(string_value="test"), origin="failing-record"
                        )
                    )
                ]
            )
        )
    )
    assert len(responses) == 1
    assert responses[0].error == "test-error"


class MySink(Sink):
    def __init__(self):
        self.commit_callback: Optional[CommitCallback] = None
        self.written_records = []

    def write(self, records: List[Record]):
        for record in records:
            if record.origin() == "failing-record":
                raise RuntimeError("test-error")
        self.written_records.extend(records)
        self.commit_callback.commit(records)

    def set_commit_callback(self, commit_callback: CommitCallback):
        self.commit_callback = commit_callback
