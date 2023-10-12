# Indexing a WebSite using Apache Solr as Vector Database

This sample application shows how to use the WebCrawler Source Connector and use [Apache Solr](https://solr.apache.org) as a Vector Database.

## Prerequisites

Launch Apache Solr locally in docker

```
docker run --rm  -p 8983:8983 --rm solr:9.3.0 -c
```

You can now open your browser at http://localhost:8983/ and you will see the Solr admin page.

The '-c' parameter launches Solr in "Cloud" mode, that allows you to dynamically create collections.


The LangStream application will create for you a collection named "documents".

It create a new data type "vector" following this guide:
https://solr.apache.org/guide/solr/latest/query-guide/dense-vector-search.html

## Configure access to the Vector Database

In order to allow LangStream that runs in docker to connect to the Solr instance running in your host, you need to configure the SOLR_HOST environment variable.

```bash
SOLR_HOST=host.docker.internal
```


The examples/secrets/secrets.yaml resolves environment variables for you.
When you go in production you are supposed to create a dedicated secrets.yaml file for each environment.


## Configure the pipeline

Edit the file `crawler.yaml` and configure the list of the allowed web domains, this is required in order to not let the crawler escape outside your data.
Configure the list of seed URLs, for instance with your home page.

The default configuration in this example will crawl the LangStream website.

## Run the LangStream application locally on docker

```
./bin/langstream docker run test -app examples/applications/query_solr  -s examples/secrets/secrets.yaml
```

## Talk with the Chat bot using the UI

By default the langstream CLI opens a UI in your browser. You can use that to chat with the bot.

## Talk with the Chat bot using the CLI
Since the application opens a gateway, we can use the gateway API to send and consume messages.

```
./bin/langstream gateway chat test -cg bot-output -pg user-input -p sessionId=$(uuidgen)
```


