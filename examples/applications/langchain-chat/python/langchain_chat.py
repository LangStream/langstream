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


from langstream import Processor, SimpleRecord

class LangChainChat(Processor):
    def __init__(self):
     self.loader = None
     self.bucket = None

    def init(self, config):
     self.openai_key = config.get("openai-key", "")

     def process(self, record):
      return [SimpleRecord(record.value() + "!!", headers=record.headers())]