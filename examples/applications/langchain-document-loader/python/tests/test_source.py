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

import yaml

from langchain_document_loader import LangChainDocumentLoaderSource

dir_path = os.path.dirname(os.path.abspath(__file__))


def test():
    source = LangChainDocumentLoaderSource()

    config = f"""
    loader-class: TextLoader
    loader-args:
      file_path: {dir_path}/lorem.txt
    splitter-class: CharacterTextSplitter
    splitter-args:
      separator: " "
      chunk_size: 1
      chunk_overlap: 0
    """

    source.init(yaml.safe_load(config))
    records = source.read()
    assert records[0]["value"] == "Lorem"
    assert records[1]["value"] == "Ipsum"
    assert source.read() == []
