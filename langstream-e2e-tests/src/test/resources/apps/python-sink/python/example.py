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
from confluent_kafka import Producer


class TestSink(object):
    def __init__(self):
        self.producer = None

    def init(self, config):
        logging.info("Init config: " + str(config))
        self.producer = Producer({"bootstrap.servers": config["bootstrapServers"]})

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
