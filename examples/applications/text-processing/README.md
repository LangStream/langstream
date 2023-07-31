# Preprocessing Text 

This sample application shows how to use some common NLP techniques to preprocess text data and then write the results to a Vector Database.

We have two pipelines:

The extract-text.yaml file defines a pipeline that will:

- Extract text from document files (PDF, Word...)
- Detect the language and filter out non-English documents
- Normalize the text
- Split the text into chunks

The write-to-db.yaml file defines a pipeline that will:
- Write the chunks to a Vector Database, in this case DataStax Astra DB

You could write a single pipeline file, but in this example we are keeping them as separate files
for demonstration purposes.

When you deploy the application all the files are deployed to the cluster as a single unit. 

## Prerequisites

Prepare some PDF files and upload them to a bucket in S3.

Create a table in Astra DB with the following schema.
This example assumes that you have a KEYSPACE named `documents` and a TABLE named `documents`.

```
USE documents;
CREATE TABLE IF NOT EXISTS documents (  
  filename TEXT,
  chunk_id int,
  num_tokens int,
  language TEXT,  
  text TEXT,
  embeddings_vector VECTOR<FLOAT, 1536>,
  PRIMARY KEY (filename, chunk_id)
);
CREATE CUSTOM INDEX IF NOT EXISTS ann_index 
  ON documents(embeddings_vector) USING 'StorageAttachedIndex';
```


## Deploy the SGA application

```
./bin/sga-cli apps deploy test -app examples/applications/text-processing -i examples/instances/kafka-kubernetes.yaml
```

## Write a document in the S3 bucket

```
# Activate port forwarding of  the MinIO service in order to be able to upload files to the S3 bucket
kubectl -n minio-dev port-forward minio 9000:9000 &

# Upload a document to the S3 bucket
dev/s3_upload.sh documents examples/applications/text-processing/simple.pdf
```

## Start a Consumer

Use the gateway to start a consumer that will read the output of the application.

```
./bin/sga-cli gateway consume test consume-chunks
```