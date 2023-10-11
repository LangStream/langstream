# Indexing a WebSite

This sample application shows how to use the WebCrawler Source Connector and use Milvus.io as a Vector Database.

## Prerequisites

Create a S3 bucket, it will contain only a metadata file for the WebCrawler.

Start a Milvus.io instance, you can use the following Helm chart:

The LangStream application will create for you a collection named "documents" in "default" database.

```
documents (  
  filename string,
  chunk_id int,
  num_tokens int,
  language string,  
  text string 
)
```


## Configure access to the Vector Database

Export some ENV variables in order to configure access to the database:

```bash
export MILVUS_HOST=...
export MILVUS_PORT=...
export MILVUS_USERNAME=...
export MILVUS_PASSWORD=...
```


The examples/secrets/secrets.yaml resolves those environment variables for you.
When you go in production you are supposed to create a dedicated secrets.yaml file for each environment.

## Configure an S3 bucket to store the status of the Crawler

The Web Crawling Source Connector requires an S3 bucket to store the status of the crawler.
It doesn't copy the contents of the web pages, it only stores some metadata.

If you are using AWS S3, you can use the following environment variables:

```
export S3_BUCKET_NAME...  
export S3_ENDPOINT=https://s3.amazonaws.com      
export S3_ACCESS_KEY=...
export S3_SECRET=...
```

The default configuration uses the internal MinIO service deployed in the local Kubernetes cluster,
this is useful for testing purposes only and it works only when you deployed LangStream locally.


## Configure the pipeline

Edit the file `crawler.yaml` and configure the list of the allowed web domains, this is required in order to not let the crawler escape outside your data.
Configure the list of seed URLs, for instance with your home page.

The default configuration in this example will crawl the LangStream website.

## Deploy the LangStream application

```
./bin/langstream apps deploy test -app examples/applications/query_milvus -i examples/instances/kafka-kubernetes.yaml -s examples/secrets/secrets.yaml
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

