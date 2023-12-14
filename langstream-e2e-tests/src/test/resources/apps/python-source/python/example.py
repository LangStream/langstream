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

from langstream import SimpleRecord
import logging


class TestSource(object):
    def __init__(self):
        self.sent = False

    def read(self):
        if not self.sent:
            logging.info("Sending the record")
            self.sent = True
            return [SimpleRecord("test" * 10000)]
        return []

    def commit(self, records):
        logging.info("Commit :" + str(records))
