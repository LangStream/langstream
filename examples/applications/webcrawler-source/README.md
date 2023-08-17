# Indexing a WebSite

This sample application shows how to use the WebCrawler Source Connector

## Prerequisites

Create a S3 bucket, it will contain only a metadata file for the WebCrawler.

## Configure the pipeline

Update the same file and set username, password and the other parameters.

Configure the list of the allowed web domains, this is required in order to not let the crawler escape outside of your data.
Configure the list of seed URLs, for instance with your home page.


## Deploy the LangStream application

```
./bin/langstream apps deploy test -app examples/applications/webcrawler-source -i examples/instances/kafka-kubernetes.yaml
```

## Start a Consumer
```
kubectl -n kafka run kafka-consumer -ti --image=quay.io/strimzi/kafka:0.35.1-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic output-topic --from-beginning
```

The content of the files will be displayed on the Kafka consumer terminal.
