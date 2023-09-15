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
from typing import Iterable, Union, List, Tuple, Any

from langstream_grpc.proto.agent_pb2 import (
    ProcessorRequest,
    Record as GrpcRecord,
    Value,
    Header,
    ProcessorResponse,
    ProcessorResult,
)
from langstream_grpc.proto.agent_pb2_grpc import AgentServiceServicer
from fastavro import parse_schema

from langstream_runtime.api import Source, Sink, Processor, Record
from langstream_runtime.util import SimpleRecord


class RecordWithId(SimpleRecord):
    def __init__(
        self,
        record_id,
        value,
        key=None,
        headers: List[Tuple[str, Any]] = None,
        origin: str = None,
        timestamp: int = None,
    ):
        super().__init__(value, key, headers, origin, timestamp)
        self.record_id = record_id


class AgentService(AgentServiceServicer):
    def __init__(self, agent: Union[Source, Sink, Processor]):
        self.agent = agent
        self.client_schemas = {}

    def process(self, requests: Iterable[ProcessorRequest], context):
        for request in requests:
            if request.HasField("schema"):
                schema = parse_schema(request.schema.value.decode("utf-8"))
                self.client_schemas[request.schema.schemaId] = schema
            if len(request.records) > 0:
                records = [self.from_grpc_record(record) for record in request.records]
                process_result = self.agent.process(records)
                grpc_results = []
                for source_record, result in process_result:
                    grpc_result = ProcessorResult(recordId=source_record.record_id)
                    if isinstance(result, Exception):
                        grpc_result.error = str(result)
                    else:
                        for r in result:
                            grpc_result.records.append(self.to_grpc_record(r))
                    grpc_results.append(grpc_result)

                yield ProcessorResponse(results=grpc_results)

    def from_grpc_record(self, record: GrpcRecord) -> SimpleRecord:
        return RecordWithId(
            record_id=record.recordId,
            value=self.from_grpc_value(record.value),
            key=self.from_grpc_value(record.key),
            headers=[(h.name, self.from_grpc_value(h.value)) for h in record.headers],
            origin=record.origin if record.origin != "" else None,
            timestamp=record.timestamp if record.HasField("timestamp") else None,
        )

    def from_grpc_value(self, value: Value):
        a = value.WhichOneof("type_oneof")
        if value is None or value.WhichOneof("type_oneof") is None:
            return None
        # TODO: define a python type for Avro
        # if value.HasField("avroValue"):
        #     schema = self.client_schemas[value.schemaId]
        #     return schemaless_reader(BytesIO(value.avroValue), schema)
        return getattr(value, value.WhichOneof("type_oneof"))

    def to_grpc_record(self, record: Record) -> GrpcRecord:
        return GrpcRecord(
            value=self.to_grpc_value(record.value()),
            key=self.to_grpc_value(record.key()),
            headers=[
                Header(name=name, value=self.to_grpc_value(value))
                for name, value in record.headers()
            ],
            origin=record.origin(),
            timestamp=record.timestamp(),
        )

    def to_grpc_value(self, value) -> Value:
        if value is None:
            return None
        # TODO: define a python type for Avro
        grpc_value = Value()
        if isinstance(value, bytes):
            grpc_value.bytesValue = value
        elif isinstance(value, str):
            grpc_value.stringValue = value
        elif isinstance(value, bool):
            grpc_value.booleanValue = value
        elif isinstance(value, int):
            grpc_value.longValue = value
        elif isinstance(value, float):
            grpc_value.doubleValue = value
        elif isinstance(value, dict) or isinstance(value, list):
            grpc_value.jsonValue = json.dumps(value)
        else:
            raise TypeError(f"Got unsupported type {type(value)}")
        return grpc_value
