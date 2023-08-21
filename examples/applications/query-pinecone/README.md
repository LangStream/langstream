# Querying a Pinecone Index

This sample application shows how to perform queries against a Pinecone index.

## Prerequisites

Create an index on Pinecone.


## Preparing the Pinecone index

Create your Pinecone index following the official documentation.
https://docs.pinecone.io/docs/quickstart

Then you have to fill in the secrets.yaml file with the Pinecone API key and the index name.

Please ensure that when you create the index you set the dimension to 1536 that is the default vector size for 
the embedding-ada-002 model we are using to compute the embeddings.

You also have to set your OpenAI API keys in the secrets.yaml file. 

## Deploy the LangStream application

```
./bin/langstream apps deploy test -app examples/applications/query-pinecone -i examples/instances/kafka-kubernetes.yaml -s /path/to/secrets.yaml
```

## Start a Producer
```
kubectl -n kafka run kafka-producer -ti --image=quay.io/strimzi/kafka:0.35.1-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-producer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic input-topic
```

Insert a JSON with "id", "name" and "description":

```
Hello
```


## Start a Consumer

Start a Kafka Consumer on a terminal

```
kubectl -n kafka run kafka-consumer -ti --image=quay.io/strimzi/kafka:0.35.1-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic output-topic --from-beginning
```

