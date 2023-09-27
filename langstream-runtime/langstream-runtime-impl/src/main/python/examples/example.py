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
import time


class Example(object):
    def read(self):
        time.sleep(1)
        records = ["".join(random.choice(string.ascii_letters) for _ in range(8))]
        print(f"read {records}")
        return records

    def process(self, record):
        print(f"process {record}")
        return [record]

    def write(self, record):
        print(f"write {record}")
