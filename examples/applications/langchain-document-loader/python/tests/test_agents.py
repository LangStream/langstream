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
from langstream import SimpleRecord

from langchain_agents import (
    LangChainDocumentLoaderSource,
    LangChainTextSplitterProcessor,
)

dir_path = os.path.dirname(os.path.abspath(__file__))


def test_document_loader():
    source = LangChainDocumentLoaderSource()

    config = f"""
    loader-class: TextLoader
    loader-args:
      file_path: {dir_path}/lorem.txt
    """

    source.init(yaml.safe_load(config))
    records = source.read()
    assert len(records) == 1
    assert records[0]["value"]["page_content"] == "Lorem Ipsum"
    assert records[0]["value"]["metadata"]["source"].endswith("lorem.txt")
    assert source.read() == []


def test_text_splitter():
    processor = LangChainTextSplitterProcessor()

    config = """
    splitter-class: CharacterTextSplitter
    splitter-args:
      separator: " "
      chunk-size: 1
      chunk-overlap: 0
    """

    processor.init(yaml.safe_load(config))
    records = processor.process(
        SimpleRecord({"page_content": "Lorem Ipsum", "metadata": {"source": "foo"}})
    )
    assert records[0]["value"]["page_content"] == "Lorem"
    assert records[0]["value"]["metadata"] == {"source": "foo"}
    assert records[1]["value"]["page_content"] == "Ipsum"
    assert records[1]["value"]["metadata"] == {"source": "foo"}
