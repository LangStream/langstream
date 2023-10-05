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
import time
from typing import List, Dict

from langstream import Source

LOADERS_MODULE = importlib.import_module("langchain.document_loaders")
SPLITTERS_MODULE = importlib.import_module("langchain.text_splitter")


class LangChainDocumentLoaderSource(Source):
    def __init__(self):
        self.load_interval = -1
        self.loader = None
        self.splitter = None
        self.first_run = True

    def init(self, config):
        self.load_interval = config.get("load-interval-seconds", -1)
        loader_class = config.get("loader-class", "")
        if not hasattr(LOADERS_MODULE, loader_class):
            raise ValueError(f"Unknown loader: {loader_class}")
        kwargs = config.get("loader-args", {})
        self.loader = getattr(LOADERS_MODULE, loader_class)(**kwargs)

        splitter_class = config.get("splitter-class", "RecursiveCharacterTextSplitter")
        if not hasattr(SPLITTERS_MODULE, splitter_class):
            raise ValueError(f"Unknown loader: {splitter_class}")
        kwargs = config.get("splitter-args", {})
        self.splitter = getattr(SPLITTERS_MODULE, splitter_class)(**kwargs)

    def read(self) -> List[Dict]:
        if not self.first_run:
            if self.load_interval == -1:
                time.sleep(1)
                return []
            time.sleep(self.load_interval)
        else:
            self.first_run = False
        docs = self.loader.load_and_split(text_splitter=self.splitter)
        return [{"value": doc.page_content} for doc in docs]
