# Indexing a WebSite

This sample application shows how to use the WebCrawler Source Connector

## Prerequisites


Create a table in Astra DB with the following schema.
This example assumes that you have a KEYSPACE named `documents`.

The LangStream application will create for you a table named "documents" with the following schema and index:

```
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


## Configure access to the Vector Database

Export some ENV variables in order to configure access to the database:

```
export ASTRA_TOKEN=...
export ASTRA_CLIENT_ID=...
export ASTRA_SECRET=...
export ASTRA_DATABASE=...
```

You can find the credentials in the Astra DB console when you create a Token.

The examples/secrets/secrets.yaml resolves those environment variables for you.
When you go in production you are supposed to create a dedicated secrets.yaml file for each environment.


## Configure the pipeline

Edit the file `crawler.yaml` and configure the list of the allowed web domains, this is required in order to not let the crawler escape outside your data.
Configure the list of seed URLs, for instance with your home page.

The default configuration in this example will crawl the LangStream website.

## Deploy the LangStream application

```
./bin/langstream apps deploy test -app examples/applications/webcrawler-source -i examples/instances/kafka-kubernetes.yaml -s examples/secrets/secrets.yaml
```

## Talk with the Chat bot using the CLI
Since the application opens a gateway, we can use the gateway API to send and consume messages.

```
./bin/langstream gateway chat test -cg bot-output -pg user-input -p sessionId=$(uuidgen)
```

Responses are streamed to the output-topic. If you want to inspect the history of the raw answers you can
consume from the log-topic using the llm-debug gateway:

```
./bin/langstream gateway consume test llm-debug
```

