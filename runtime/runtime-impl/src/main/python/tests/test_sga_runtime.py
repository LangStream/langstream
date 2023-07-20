import random
import string

import yaml

from sga_runtime import sga_runtime


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
        'records': ['some record 0 processed', 'some record 1 processed']
    }
    assert expected == TEST_RESULTS[random_value]


TEST_RESULTS = {}


class TestAgent(object):

    def __init__(self):
        self.context = {
            'config': [],
            'start': 0,
            'close': 0,
            'records': []
        }
        self.key = None

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

    def write(self, records):
        self.context['records'].extend(records)

    @staticmethod
    def process(records):
        return [record + ' processed' for record in records]
