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
import logging
from typing import List, Tuple, Union, Dict, Any
from unittest.mock import patch

import pytest
import yaml

from langstream import (
    Source,
    Sink,
    CommitCallback,
    Record,
    SimpleRecord,
    SingleRecordProcessor,
)
from langstream_runtime import runtime


@patch("time.time_ns", return_value=12345000000)
def test_simple_agent(_):
    config_yaml = """
        streamingCluster:
            type: kafka
        agent:
            applicationId: testApplicationId
            agentId: testAgentId
            agentType: testAgentType
            configuration:
                className: langstream_runtime.tests.test_runtime.TestAgent
    """
    config = yaml.safe_load(config_yaml)
    agent = runtime.init_agent(config)
    agent_info = runtime.AgentInfo()
    runtime.run(config, agent=agent, agent_info=agent_info, max_loops=2)
    print(json.dumps(agent_info.worker_status(), indent=2))
    assert (
        json.dumps(agent_info.worker_status(), indent=2)
        == """[
  {
    "agent-id": "testAgentId",
    "agent-type": "testAgentType",
    "component-type": "SOURCE",
    "info": {
      "config": [
        {
          "className": "langstream_runtime.tests.test_runtime.TestAgent"
        }
      ],
      "start": 1,
      "close": 1,
      "set_commit_callback": 1,
      "records": [
        "some record 0 processed",
        "some record 1 processed"
      ]
    },
    "metrics": {
      "total-in": 0,
      "total-out": 2,
      "last-processed_at": 12345
    }
  },
  {
    "agent-id": "testAgentId",
    "agent-type": "testAgentType",
    "component-type": "PROCESSOR",
    "info": {
      "config": [
        {
          "className": "langstream_runtime.tests.test_runtime.TestAgent"
        }
      ],
      "start": 1,
      "close": 1,
      "set_commit_callback": 1,
      "records": [
        "some record 0 processed",
        "some record 1 processed"
      ]
    },
    "metrics": {
      "total-in": 2,
      "total-out": 2,
      "last-processed_at": 12345
    }
  },
  {
    "agent-id": "testAgentId",
    "agent-type": "testAgentType",
    "component-type": "SINK",
    "info": {
      "config": [
        {
          "className": "langstream_runtime.tests.test_runtime.TestAgent"
        }
      ],
      "start": 1,
      "close": 1,
      "set_commit_callback": 1,
      "records": [
        "some record 0 processed",
        "some record 1 processed"
      ]
    },
    "metrics": {
      "total-in": 2,
      "total-out": 0,
      "last-processed_at": 12345
    }
  }
]"""
    )


class TestAgent(Source, Sink, SingleRecordProcessor):
    def __init__(self):
        self.context = {
            "config": [],
            "start": 0,
            "close": 0,
            "set_commit_callback": 0,
            "records": [],
        }
        self.commit_callback = None

    def init(self, config):
        self.context["config"].append(config)

    def start(self):
        self.context["start"] += 1

    def close(self):
        self.context["close"] += 1

    def read(self):
        return [SimpleRecord("some record " + str(len(self.context["records"])))]

    def set_commit_callback(self, commit_callback: CommitCallback):
        self.context["set_commit_callback"] += 1

    def write(self, records):
        for record in records:
            self.context["records"].append(record.value())

    def process_record(self, record: Record) -> List[Record]:
        return [SimpleRecord(record.value() + " processed")]

    def agent_info(self) -> Dict[str, Any]:
        return self.context


class AbstractFailingAgent(object):
    def __init__(self, records, fail_on_content="fail-me", batch_size=1):
        self.commit_callback = None
        self.batch_size = batch_size
        self.records = records.copy()
        self.uncommitted = []
        self.fail_on_content = fail_on_content
        self.execution_count = 0

    def commit(self, records: List[Record]):
        for record in records:
            self.uncommitted.remove(record)

    def set_commit_callback(self, commit_callback: CommitCallback):
        self.commit_callback = commit_callback


class FailingProcessor(AbstractFailingAgent, SingleRecordProcessor):
    def read(self) -> List[Record]:
        result = []
        if len(self.records) > 0:
            for i in range(self.batch_size):
                removed = self.records.pop(0)
                result.append(removed)
                self.uncommitted.append(removed)
                if len(self.records) == 0:
                    break
        return result

    def process(
        self, records: List[Record]
    ) -> List[Tuple[Record, Union[List[Record], Exception]]]:
        self.execution_count += 1
        return super().process(records)

    def process_record(self, record: Record) -> List[Record]:
        logging.info(f"Processing {record.value()}", record.value())
        if record.value() in self.fail_on_content:
            raise Exception(f"Failed on {record.value()}")
        return [record]

    def write(self, records: List[Record]):
        self.commit_callback.commit(records)


class FailingSink(AbstractFailingAgent, SingleRecordProcessor):
    def read(self) -> List[Record]:
        record = SimpleRecord("source-record")
        self.uncommitted.append(record)
        return [record]

    def process_record(self, _: Record) -> List[Record]:
        result = []
        if len(self.records) > 0:
            for i in range(self.batch_size):
                removed = self.records.pop(0)
                result.append(removed)
                if len(self.records) == 0:
                    break
        return result

    def write(self, records: List[Record]):
        self.execution_count += 1
        for record in records:
            logging.info(f"Writing {record.value()}", record.value())
            if record.value() in self.fail_on_content:
                raise Exception(f"Failed on {record.value()}")
            self.commit_callback.commit([record])


@pytest.mark.parametrize(
    "retries, on_failure, agent, run_should_throw, expected_uncommitted, "
    "expected_executions",
    [
        pytest.param(
            0,
            "skip",
            FailingProcessor([SimpleRecord("fail-me")]),
            False,
            0,
            1,
            id="processor skip",
        ),
        pytest.param(
            3,
            "fail",
            FailingProcessor([SimpleRecord("fail-me")]),
            True,
            1,
            3,
            id="processor fail with retries",
        ),
        pytest.param(
            0,
            "fail",
            FailingProcessor([SimpleRecord("fail-me")]),
            True,
            1,
            1,
            id="processor fail no retries",
        ),
        pytest.param(
            0,
            "dead-letter",
            FailingProcessor([SimpleRecord("fail-me")]),
            True,
            1,
            1,
            id="processor dead-letter no dlq configured",
        ),
        pytest.param(
            0,
            "skip",
            FailingProcessor([SimpleRecord("fail-me"), SimpleRecord("process-me")]),
            False,
            0,
            2,
            id="processor some failed some good with skip",
        ),
        pytest.param(
            0,
            "skip",
            FailingProcessor([SimpleRecord("process-me"), SimpleRecord("fail-me")]),
            False,
            0,
            2,
            id="processor some good some failed with skip",
        ),
        pytest.param(
            0,
            "skip",
            FailingProcessor(
                [SimpleRecord("fail-me"), SimpleRecord("process-me")], batch_size=2
            ),
            False,
            0,
            1,
            id="processor some failed some good with skip and batching",
        ),
        pytest.param(
            0,
            "skip",
            FailingProcessor(
                [SimpleRecord("process-me"), SimpleRecord("fail-me")], batch_size=2
            ),
            False,
            0,
            1,
            id="processor some good some failed with skip and batching",
        ),
        pytest.param(
            0,
            "skip",
            FailingSink([SimpleRecord("fail-me")]),
            False,
            0,
            1,
            id="sink skip",
        ),
        pytest.param(
            3,
            "fail",
            FailingSink([SimpleRecord("fail-me")]),
            True,
            1,
            3,
            id="sink fail with retries",
        ),
        pytest.param(
            0,
            "fail",
            FailingSink([SimpleRecord("fail-me")]),
            True,
            1,
            1,
            id="sink fail no retries",
        ),
        pytest.param(
            0,
            "dead-letter",
            FailingSink([SimpleRecord("fail-me")]),
            True,
            1,
            1,
            id="sink dead-letter no dlq configured",
        ),
        pytest.param(
            0,
            "skip",
            FailingSink([SimpleRecord("fail-me"), SimpleRecord("process-me")]),
            False,
            0,
            2,
            id="sink some failed some good with skip",
        ),
        pytest.param(
            0,
            "skip",
            FailingSink([SimpleRecord("process-me"), SimpleRecord("fail-me")]),
            False,
            0,
            2,
            id="sink some good some failed with skip",
        ),
        pytest.param(
            0,
            "skip",
            FailingSink(
                [SimpleRecord("fail-me"), SimpleRecord("process-me")], batch_size=2
            ),
            False,
            0,
            1,
            id="sink some failed some good with skip and batching",
        ),
        pytest.param(
            0,
            "skip",
            FailingSink(
                [SimpleRecord("process-me"), SimpleRecord("fail-me")], batch_size=2
            ),
            False,
            0,
            1,
            id="sink some good some failed with skip and batching",
        ),
    ],
)
def test_errors_handler(
    retries,
    on_failure,
    agent: AbstractFailingAgent,
    run_should_throw,
    expected_uncommitted,
    expected_executions,
):
    config_yaml = f"""
            streamingCluster:
                type: kafka
            agent:
                applicationId: testApplicationId
                agentId: testAgentId
                errorHandlerConfiguration:
                    retries: {retries}
                    onFailure: {on_failure}
        """
    config = yaml.safe_load(config_yaml)
    if run_should_throw:
        with pytest.raises(Exception):
            runtime.run(config, agent=agent, max_loops=5)
    else:
        runtime.run(config, agent=agent, max_loops=5)
    assert len(agent.uncommitted) == expected_uncommitted
    assert agent.execution_count == expected_executions
