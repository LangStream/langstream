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

from typing import List, Dict, Any, Optional

import grpc
import pytest

from langstream_grpc.api import Record, Processor, AgentContext
from langstream_grpc.proto.agent_pb2 import (
    Record as GrpcRecord,
    ProcessorRequest,
    Value,
    TopicProducerWriteResult,
)
from langstream_grpc.tests.server_and_stub import ServerAndStub


@pytest.mark.parametrize("klass", ["MyProcessor", "MyAsyncProcessor"])
async def test_topic_producer_success(klass):
    async with ServerAndStub(
        f"langstream_grpc.tests.test_grpc_topic_producer.{klass}"
    ) as server_and_stub:
        process_call = server_and_stub.stub.process()
        await process_call.write(
            ProcessorRequest(records=[GrpcRecord(value=Value(string_value="test"))])
        )

        topic_producer_call = server_and_stub.stub.get_topic_producer_records()
        topic_producer_record = await topic_producer_call.read()

        assert topic_producer_record.topic == "topic-producer-topic"
        assert topic_producer_record.record.value.string_value == "test"
        await topic_producer_call.write(
            TopicProducerWriteResult(record_id=topic_producer_record.record.record_id)
        )

        await topic_producer_call.done_writing()

        processed = await process_call.read()
        assert processed.results[0].records[0].value.string_value == "test"

        await process_call.done_writing()


@pytest.mark.parametrize("klass", ["MyProcessor", "MyAsyncProcessor"])
async def test_topic_producer_write_error(klass):
    async with ServerAndStub(
        f"langstream_grpc.tests.test_grpc_topic_producer.{klass}"
    ) as server_and_stub:
        process_call = server_and_stub.stub.process()
        await process_call.write(
            ProcessorRequest(records=[GrpcRecord(value=Value(string_value="test"))])
        )

        topic_producer_call = server_and_stub.stub.get_topic_producer_records()
        topic_producer_record = await topic_producer_call.read()

        assert topic_producer_record.topic == "topic-producer-topic"
        assert topic_producer_record.record.value.string_value == "test"
        await topic_producer_call.write(
            TopicProducerWriteResult(
                record_id=topic_producer_record.record.record_id, error="test-error"
            )
        )

        await topic_producer_call.done_writing()

        response = await process_call.read()
        assert "test-error" in response.results[0].error

        await process_call.done_writing()


async def test_topic_producer_invalid():
    async with ServerAndStub(
        "langstream_grpc.tests.test_grpc_topic_producer.MyFailingProcessor"
    ) as server_and_stub:
        process_call = server_and_stub.stub.process()
        await process_call.write(
            ProcessorRequest(records=[GrpcRecord(value=Value(string_value="test"))])
        )

        topic_producer_call = server_and_stub.stub.get_topic_producer_records()
        with pytest.raises(grpc.RpcError):
            await topic_producer_call.read()


class MyProcessor(Processor):
    def __init__(self):
        self.context: Optional[AgentContext] = None

    def init(self, config: Dict[str, Any], context: AgentContext):
        self.context = context

    def process(self, record: Record) -> List[Record]:
        self.context.get_topic_producer().write("topic-producer-topic", record).result()
        return [record]


class MyAsyncProcessor(MyProcessor):
    async def process(self, record: Record) -> List[Record]:
        await self.context.get_topic_producer().awrite("topic-producer-topic", record)
        return [record]


class MyFailingProcessor(MyProcessor):
    async def process(self, record: Record) -> List[Record]:
        await self.context.get_topic_producer().awrite(
            "topic-producer-topic", "invalid"
        )
        return [record]
