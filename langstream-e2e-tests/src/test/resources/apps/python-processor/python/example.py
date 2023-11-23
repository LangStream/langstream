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

from langstream import SimpleRecord, Processor, AgentContext
import logging, os


class Exclamation(Processor):
    def init(self, config, context: AgentContext):
        print("init", config, context)
        self.secret_value = config["secret_value"]
        self.context = context

    def process(self, record):
        logging.info("Processing record" + str(record))
        self.context.get_topic_producer().write("ls-test-topic-producer", {"value": record.value() + " test-topic-producer"}).result()
        directory = self.context.get_persistent_state_directory()
        counter_file = os.path.join(directory, "counter.txt")
        counter = 0
        if os.path.exists(counter_file):
            with open(counter_file, "r") as f:
                counter = int(f.read())
                counter += 1
        else:
            counter = 1
        with open(counter_file, 'w') as file:
            file.write(str(counter))


        if self.secret_value == "super secret value - changed":
            assert counter == 2
        else:
            assert counter == 1
        return [
            SimpleRecord(
                record.value() + "!!" + self.secret_value, headers=record.headers()
            )
        ]
