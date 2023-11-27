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

from langstream import Sink, SimpleRecord
from get_recommendation import get_recommendation_for_product
import json
from util import create_raw_astra_client
import logging
import traceback

class LangChainAgent(Sink):

    def init(self, config):
        self.astra_db = create_raw_astra_client()

    def write(self, record):
        logging.info(f"Received record: {record}")
        question = record.value()
        logging.info(f"Getting question for: {question}")
        try:
            parsed = json.loads(question)
            response = get_recommendation_for_product({"name": parsed["product_name"], "description": parsed["product_description"]})
            logging.info(f"Got recommendation: {response}")
            self.astra_db.collection("recommendations").upsert({
                "id": parsed["session_id"],
                "rec": json.dumps(response)
            })
        except Exception:
            traceback.print_exc()
            raise Exception("Failed to get recommendation")

