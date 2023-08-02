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

import random
import string

import yaml

from sga_runtime import sga_runtime
from sga_runtime.api import Source, Sink, Processor, CommitCallback


def test_simple_agent():
    random_value = ''.join(random.choice(string.ascii_letters) for _ in range(8))
    config_yaml = f"""
        streamingCluster:
            type: kafka
        agent:
            applicationId: testApplicationId
            agentId: testAgentId
            configuration:
                className: tests.test_sga_runtime.TestAgent
                key: {random_value}
    """
    config = yaml.safe_load(config_yaml)
    sga_runtime.run(config, 2)
    expected = {
        'config': [{'className': 'tests.test_sga_runtime.TestAgent', 'key': random_value}],
        'start': 1,
        'close': 1,
        'set_commit_callback': 1,
        'records': ['some record 0 processed', 'some record 1 processed']
    }
    assert expected == TEST_RESULTS[random_value]


TEST_RESULTS = {}


class TestAgent(Source, Sink, Processor):
    def __init__(self):
        self.context = {
            'config': [],
            'start': 0,
            'close': 0,
            'set_commit_callback': 0,
            'records': []
        }
        self.key = None
        self.commit_callback = None

    def init(self, config):
        self.context['config'].append(config)
        self.key = config['key']

    def start(self):
        self.context['start'] += 1

    def close(self):
        self.context['close'] += 1
        TEST_RESULTS[self.key] = self.context

    def read(self):
        return ['some record ' + str(len(self.context['records']))]

    def set_commit_callback(self, commit_callback: CommitCallback):
        self.context['set_commit_callback'] += 1

    def write(self, records):
        self.context['records'].extend(records)

    def process(self, records):
        return [(record, [record + ' processed']) for record in records]
