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

import os
from langstream import Processor, SimpleRecord
from operator import itemgetter
from typing import Dict, List, Optional, Sequence, Any
from langchain.vectorstores.cassandra import Cassandra
from langchain.chat_models import ChatOpenAI, AzureChatOpenAI
from langchain.embeddings import OpenAIEmbeddings
from langchain.chains import RetrievalQA
import cassio
import logging

class LangChainChat(Processor):

    def init(self, config):
        logging.info("Initializing LangChain Chat with config %s", config)
        # the values are configured in the resources section in configuration.yaml
        self.openai_key = config["resources"]["openai"]["access-key"]
        self.astra_db_token = config["resources"]["database"]["token"]
        self.astra_db_id = config["resources"]["database"]["database-id"]
        self.astra_keyspace = config["astra-db-keyspace"]
        self.astra_table_name = config["astra-db-table"]
        self.deployment = config["deployment"]

        cassio.init(token=self.astra_db_token, database_id=self.astra_db_id)

        self.embedings_model = OpenAIEmbeddings(openai_api_key=self.openai_key)

        self.astra_vector_store = Cassandra(
            embedding=self.embedings_model,
            session=None,
            keyspace=self.astra_keyspace,
            table_name=self.astra_table_name,
        )

        self.llm = AzureChatOpenAI(
            deployment_name=self.deployment,
            streaming=True,
            temperature=0,
        )
        # ^^ key obtained from env var: AZURE_OPENAI_API_KEY
        # version obtained from env var: OPENAI_API_VERSION

        self.retriever = self.astra_vector_store.as_retriever()

        self.answer_chain = RetrievalQA.from_chain_type(
            llm=self.llm, chain_type="stuff", retriever=self.retriever
        )

    def process(self, record):
        question = record.value()
        response = self.answer_chain.run(question)
        return [SimpleRecord(response, headers=record.headers())]
