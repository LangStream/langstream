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

import importlib
import json
import time
from typing import List, Optional

from langchain.document_loaders.base import BaseLoader
from langchain.schema import Document
from langchain.text_splitter import TextSplitter
from langstream import Source, Processor, Record, SimpleRecord

LOADERS_MODULE = importlib.import_module("langchain.document_loaders")
SPLITTERS_MODULE = importlib.import_module("langchain.text_splitter")


class LangChainDocumentRecord(SimpleRecord):
    def __init__(self, document: Document):
        super().__init__(
            value={"page_content": document.page_content, "metadata": document.metadata}
        )


class RecordLangChainDocument(Document):
    def __init__(self, record: Record):
        doc = record.value()
        if isinstance(doc, str) or isinstance(doc, bytes):
            doc = json.loads(doc)

        if not isinstance(doc, dict):
            raise ValueError(f"Invalid record value {record.value()}")

        super().__init__(
            page_content=doc["page_content"], metadata=doc.get("metadata", {})
        )


class LangChainDocumentLoaderSource(Source):
    def __init__(self):
        self.load_interval = -1
        self.loader: Optional[BaseLoader] = None
        self.first_run = True

    def init(self, config):
        self.load_interval = config.get("load-interval-seconds", -1)
        loader_class = config.get("loader-class", "")
        if not hasattr(LOADERS_MODULE, loader_class):
            raise ValueError(f"Unknown loader: {loader_class}")
        kwargs = {
            k.replace("-", "_"): v for k, v in config.get("loader-args", {}).items()
        }
        self.loader = getattr(LOADERS_MODULE, loader_class)(**kwargs)

    def read(self) -> List[Record]:
        if not self.first_run:
            if self.load_interval == -1:
                time.sleep(1)
                return []
            time.sleep(self.load_interval)
        else:
            self.first_run = False
        docs = self.loader.load()
        return [LangChainDocumentRecord(doc) for doc in docs]


class LangChainTextSplitterProcessor(Processor):
    def __init__(self):
        self.splitter: Optional[TextSplitter] = None

    def init(self, config):
        splitter_class = config.get("splitter-class", "RecursiveCharacterTextSplitter")
        if not hasattr(SPLITTERS_MODULE, splitter_class):
            raise ValueError(f"Unknown loader: {splitter_class}")
        kwargs = {
            k.replace("-", "_"): v for k, v in config.get("splitter-args", {}).items()
        }
        self.splitter = getattr(SPLITTERS_MODULE, splitter_class)(**kwargs)

    def process(self, record: Record) -> List[Record]:
        chunks = self.splitter.split_documents([RecordLangChainDocument(record)])
        return [LangChainDocumentRecord(chunk) for chunk in chunks]
