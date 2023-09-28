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

import base64
import io
from typing import Dict, Any

import openai
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from langstream import Sink, Record
from llama_index import VectorStoreIndex, Document
from llama_index.vector_stores import CassandraVectorStore


class LlamaIndexCassandraSink(Sink):
    def __init__(self):
        self.config = None
        self.session = None
        self.index = None

    def init(self, config: Dict[str, Any]):
        self.config = config
        openai.api_key = config["openaiKey"]

    def start(self):
        secure_bundle = self.config["cassandra"]["secureBundle"]
        secure_bundle = secure_bundle.removeprefix("base64:")
        secure_bundle = base64.b64decode(secure_bundle)
        cluster = Cluster(
            cloud={
                "secure_connect_bundle": io.BytesIO(secure_bundle),
                "use_default_tempdir": True,
            },
            auth_provider=PlainTextAuthProvider(
                self.config["cassandra"]["username"],
                self.config["cassandra"]["password"],
            ),
        )
        self.session = cluster.connect()

        vector_store = CassandraVectorStore(
            session=self.session,
            keyspace=self.config["cassandra"]["keyspace"],
            table=self.config["cassandra"]["table"],
            embedding_dimension=1536,
            insertion_batch_size=15,
        )

        self.index = VectorStoreIndex.from_vector_store(vector_store)

    def write(self, record: Record):
        self.index.insert(Document(text=record.value()))

    def close(self):
        if self.session:
            self.session.shutdown()
