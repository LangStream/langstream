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

from typing import List, Optional, Dict, Any

import openai
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from langstream import Sink, CommitCallback, Record
from llama_index import VectorStoreIndex, Document
from llama_index.vector_stores import CassandraVectorStore


class LlamaIndexCassandraSink(Sink):

    def __init__(self):
        self.commit_cb: Optional[CommitCallback] = None
        self.config = None
        self.session = None
        self.index = None

    def init(self, config: Dict[str, Any]):
        self.config = config
        openai.api_key = config['openai-key']

    def start(self):
        cluster = Cluster(
            cloud={
                "secure_connect_bundle": "/app-code-download/python/secure-connect-bundle.zip",
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

    def write(self, records: List[Record]):
        for record in records:
            self.index.insert(Document(text=record.value()))
            self.commit_cb.commit([record])

    def set_commit_callback(self, commit_callback: CommitCallback):
        self.commit_cb = commit_callback

    def close(self):
        if self.session:
            self.session.shutdown()


