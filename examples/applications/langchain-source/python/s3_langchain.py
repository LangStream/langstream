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

import time
from typing import List
from urllib.parse import urlparse

import boto3
from langchain.document_loaders import S3DirectoryLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter

from langstream import Source, SimpleRecord


class S3Record(SimpleRecord):
    def __init__(self, url, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = url


class S3LangChain(Source):
    def __init__(self):
        self.loader = None
        self.bucket = None

    def init(self, config):
        bucket_name = config.get("bucketName", "langstream-s3-langchain")
        endpoint_url = config.get("endpoint", "http://minio-endpoint.-not-set:9090")
        aws_access_key_id = config.get("username", "minioadmin")
        aws_secret_access_key = config.get("password", "minioadmin")
        s3 = boto3.resource(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        self.bucket = s3.Bucket(bucket_name)
        self.loader = S3DirectoryLoader(
            bucket_name,
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

    def read(self) -> List[S3Record]:
        time.sleep(1)
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=100,
            chunk_overlap=20,
            length_function=len,
            add_start_index=False,
        )
        docs = self.loader.load_and_split(text_splitter=text_splitter)
        return [
            S3Record(doc.metadata["source"], value=doc.page_content) for doc in docs
        ]

    def commit(self, records: List[S3Record]):
        objects_to_delete = [
            {"Key": f'{urlparse(record.url).path.lstrip("/")}'} for record in records
        ]
        self.bucket.delete_objects(Delete={"Objects": objects_to_delete})
