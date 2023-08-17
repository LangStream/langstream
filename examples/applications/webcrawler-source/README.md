# Indexing a WebSite

This sample application shows how to use the WebCrawler Source Connector

## Prerequisites

Create a S3 bucket, it will contain only a metadata file for the WebCrawler.

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

## Configure the pipeline

Update the same file and set username, password and the other parameters.

Configure the list of the allowed web domains, this is required in order to not let the crawler escape outside of your data.
Configure the list of seed URLs, for instance with your home page.


## Deploy the LangStream application

```
./bin/langstream apps deploy test -app examples/applications/webcrawler-source -i examples/instances/kafka-kubernetes.yaml -s examples/secrets/secrets.yaml
```

## Talk with the Chat bot using the CLI
Since the application opens a gateway, we can use the gateway API to send and consume messages.

```
./bin/langstream gateway chat test -cg bot-output -pg user-input -p sessionId=$(uuidgen)
```
