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

from langstream import Sink, Source, RecordType, Record
import logging
from typing import List
import time

# Example Python source that emits a record with the value "test!"
class Exclamation(Source):
    def read(self) -> List[RecordType]:
        record = {"value": "test!"}
        time.sleep(3)
        logging.info("emitting record: %s", record)
        return [record]

