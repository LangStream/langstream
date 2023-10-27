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

import openai
import json
from openai.embeddings_utils import get_embedding


class Embedding(object):
    def init(self, config):
        print("init", config)
        openai.api_key = config["openaiKey"]

    def process(self, record):
        embedding = get_embedding(record.value(), engine="text-embedding-ada-002")
        result = {"input": str(record.value()), "embedding": embedding}
        new_value = json.dumps(result)
        return [(new_value,)]
