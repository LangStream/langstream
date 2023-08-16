#
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
import logging
from typing import List

import pytest
import yaml

from langstream.internal import runtime
from langstream.api import Source, Sink, CommitCallback, Record
from langstream.util import SimpleRecord, SingleRecordProcessor


def test_simple_agent():
    config_yaml = f"""
        streamingCluster:
            type: kafka
        agent:
            applicationId: testApplicationId
            agentId: testAgentId
            configuration:
                className: tests.test_sga_runtime.TestAgent
    """
    config = yaml.safe_load(config_yaml)
    agent = runtime.init_agent(config)
    runtime.run(config, agent=agent, max_loops=2)
    assert agent.context['config'] == [{'className': 'tests.test_sga_runtime.TestAgent'}]
    assert agent.context['start'] == 1
    assert agent.context['close'] == 1
    assert agent.context['set_commit_callback'] == 1
    assert agent.context['records'][0].value() == 'some record 0 processed'
    assert agent.context['records'][1].value() == 'some record 1 processed'


class TestAgent(Source, Sink, SingleRecordProcessor):
    def __init__(self):
        self.context = {
            'config': [],
            'start': 0,
            'close': 0,
            'set_commit_callback': 0,
            'records': []
        }
        self.commit_callback = None

    def init(self, config):
        self.context['config'].append(config)

    def start(self):
        self.context['start'] += 1

    def close(self):
        self.context['close'] += 1

    def read(self):
        return [SimpleRecord('some record ' + str(len(self.context['records'])))]

    def set_commit_callback(self, commit_callback: CommitCallback):
        self.context['set_commit_callback'] += 1

    def write(self, records):
        self.context['records'].extend(records)

    def process_record(self, record: Record) -> List[Record]:
        return [SimpleRecord(record.value() + ' processed')]


@pytest.mark.parametrize(
    'retries, on_failure, records, batch_size, run_should_throw, expected_uncommitted, expected_executions',
    [
        pytest.param(0, 'skip', [SimpleRecord('fail-me')], 1, False, 0, 1, id='skip'),
        pytest.param(3, 'fail', [SimpleRecord('fail-me')], 1, True, 1, 3, id='fail with retries'),
        pytest.param(0, 'fail', [SimpleRecord('fail-me')], 1, True, 1, 1, id='fail no retries'),
        pytest.param(0, 'skip', [SimpleRecord('fail-me'), SimpleRecord('process-me')], 1, False, 0, 2,
                     id='some failed some good with skip'),
        pytest.param(0, 'skip', [SimpleRecord('process-me'), SimpleRecord('fail-me')], 1, False, 0, 2,
                     id='some good some failed with skip'),
        pytest.param(0, 'skip', [SimpleRecord('fail-me'), SimpleRecord('process-me')], 2, False, 0, 2,
                     id='some failed some good with skip and batching'),
        pytest.param(0, 'skip', [SimpleRecord('process-me'), SimpleRecord('fail-me')], 2, False, 0, 2,
                     id='some good some failed with skip and batching'),
    ])
def test_errors_handler(retries, on_failure, records: List[Record], batch_size: int, run_should_throw,
                        expected_uncommitted, expected_executions):
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
    agent = TestErrorAgent(records, 'fail-me', batch_size=batch_size)
    if run_should_throw:
        with pytest.raises(Exception):
            runtime.run(config, agent=agent, max_loops=5)
    else:
        runtime.run(config, agent=agent, max_loops=5)
    assert len(agent.uncommitted) == expected_uncommitted
    assert agent.execution_count == expected_executions


class TestErrorAgent(Source, Sink, SingleRecordProcessor):
    def __init__(self, records, fail_on_content, batch_size=1):
        self.commit_callback = None
        self.batch_size = batch_size
        self.records = records.copy()
        self.uncommitted = []
        self.fail_on_content = fail_on_content
        self.execution_count = 0

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

    def commit(self, records: List[Record]):
        self.uncommitted.clear()

    def process_record(self, record: Record) -> List[Record]:
        logging.info(f"Processing {record.value()}", record.value())
        self.execution_count += 1
        if record.value() in self.fail_on_content:
            raise Exception(f'Failed on {record.value()}')
        return [record]

    def write(self, records: List[Record]):
        self.commit_callback.commit(records)

    def set_commit_callback(self, commit_callback: CommitCallback):
        self.commit_callback = commit_callback
