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

import logging, json, re
from confluent_kafka import Producer


def extract_jaas_property(prop, jaas_entry):
    re_pattern = re.compile(prop + r'\s*=\s*(?:"([^"]*)"|\'([^\']*)\'|(\S+))')
    re_match = re_pattern.search(jaas_entry)
    if re_match:
        return re_match.group(1) or re_match.group(2) or re_match.group(3)
    else:
        return None
class TestSink(object):
    def __init__(self):
        self.producer = None



    def init(self, config):
        logging.info("Init config: " + str(config))
        producer_config = json.loads(config["producerConfig"])
        logging.info("Producer config: " + str(producer_config))
        if "sasl.jaas.config" in producer_config:
            for prop in ["username", "password"]:
                if "sasl." + prop not in producer_config:
                    prop_value = extract_jaas_property(prop, producer_config["sasl.jaas.config"])
                    if prop_value:
                        producer_config["sasl." + prop] = prop_value
            del producer_config["sasl.jaas.config"]
        self.producer = Producer(producer_config)

    def write(self, record):
        logging.info("Write record: " + str(record))
        try:
            self.producer.produce(
                "ls-test-output", value=("write: " + record.value()).encode("utf-8")
            )
            self.producer.flush()
        except Exception as e:
            logging.error("Error writing records: " + str(e))
            raise e
