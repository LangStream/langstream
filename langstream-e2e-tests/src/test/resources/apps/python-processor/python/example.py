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

from langstream import SimpleRecord, Processor
import logging


class Exclamation(Processor):
    def init(self, config):
        print("init", config)
        self.secret_value = config["secret_value"]

    def process(self, record):
        logging.info("Processing record" + str(record))
        return [
            SimpleRecord(
                record.value() + "!!" + self.secret_value, headers=record.headers()
            )
        ]
