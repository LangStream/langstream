import boto3
import tempfile
import time
from langchain.docstore.document import Document
from langchain.document_loaders.base import BaseLoader
from langchain.document_loaders.unstructured import UnstructuredFileLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from sga_runtime.record import Record
from typing import List


class S3LangChain(object):
    def __init__(self):
        self.loader = None
        self.bucket = None
        self.objs_to_commit = set()

    def init(self, config):
        bucket_name = config.get('bucketName', 'sga-s3-langchain')
        endpoint_url = config.get('endpoint', 'http://minio-endpoint.-not-set:9090')
        aws_access_key_id = config.get('username', 'minioadmin')
        aws_secret_access_key = config.get('password', 'minioadmin')
        s3 = boto3.resource("s3", endpoint_url=endpoint_url, aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key)

        self.bucket = s3.Bucket(bucket_name)
        self.loader = S3DirectoryLoader(bucket_name, endpoint_url=endpoint_url, aws_access_key_id=aws_access_key_id,
                                        aws_secret_access_key=aws_secret_access_key)

    def read(self):
        time.sleep(1)
        text_splitter = RecursiveCharacterTextSplitter(
            # Set a really small chunk size, just to show.
            chunk_size=100,
            chunk_overlap=20,
            length_function=len,
            add_start_index=False,
        )
        docs = self.loader.load_and_split(text_splitter=text_splitter)
        for doc in docs:
            self.objs_to_commit.add(doc.metadata['s3_object_key'])
        return [Record(doc.page_content) for doc in docs]

    def commit(self):
        objects_to_delete = []
        for key in set(self.objs_to_commit):
            objects_to_delete.append({'Key': f'{key}'})
        self.bucket.delete_objects(Delete={'Objects': objects_to_delete})


#
# We copy the following classes from langchain and add support for setting credentials
# and endpoint URL. We also add the S3 bucket and object key names to the document metadata.
#

class S3DirectoryLoader(BaseLoader):
    """Loading logic for loading documents from s3."""

    def __init__(self, bucket: str, prefix: str = "", endpoint_url: str = None, aws_access_key_id: str = None,
                 aws_secret_access_key: str = None):
        """Initialize with bucket and key name."""
        self.bucket = bucket
        self.prefix = prefix
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    def load(self) -> List[Document]:
        """Load documents."""
        s3 = boto3.resource("s3", endpoint_url=self.endpoint_url, aws_access_key_id=self.aws_access_key_id,
                            aws_secret_access_key=self.aws_secret_access_key)
        bucket = s3.Bucket(self.bucket)
        docs = []
        for obj in bucket.objects.filter(Prefix=self.prefix):
            loader = S3FileLoader(self.bucket, obj.key, endpoint_url=self.endpoint_url,
                                  aws_access_key_id=self.aws_access_key_id,
                                  aws_secret_access_key=self.aws_secret_access_key)
            docs.extend(loader.load())
        return docs


class S3FileLoader(BaseLoader):
    """Loading logic for loading documents from s3."""

    def __init__(self, bucket: str, key: str, endpoint_url: str = None, aws_access_key_id: str = None,
                 aws_secret_access_key: str = None):
        """Initialize with bucket and key name."""
        self.bucket = bucket
        self.key = key
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    def load(self) -> List[Document]:
        """Load documents."""
        s3 = boto3.client("s3", endpoint_url=self.endpoint_url, aws_access_key_id=self.aws_access_key_id,
                          aws_secret_access_key=self.aws_secret_access_key)
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = f"{temp_dir}/{self.key}"
            s3.download_file(self.bucket, self.key, file_path)
            loader = UnstructuredFileLoader(file_path)
            docs = loader.load()
            for doc in docs:
                doc.metadata['s3_object_key'] = self.key
                doc.metadata['s3_bucket'] = self.bucket
            return docs
