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
from sga_runtime.record import Record

class TestClass(object):
  def init(self, config):
      print('init', config)
      self.secret_value = config["secret_value"]

  def process(self, records):
    processed_records = []
    for record in records:
      processed_records.append(Record(record.value() + "!!" + self.secret_value))
    return processed_records
