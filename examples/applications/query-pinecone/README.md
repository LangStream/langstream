# Querying a Pinecone Index

This sample application shows how to perform queries against a Pinecone index.

## Prerequisites

Create an index on Pinecone.


## Preparing the Pinecone index

Create your Pinecone index following the official documentation.
https://docs.pinecone.io/docs/quickstart

Please ensure that when you create the index you set the dimension to 1536 that is the default vector size for 
the embedding-ada-002 model we are using to compute the embeddings.

```python
import pinecone;
pinecone.init(api_key="xxxxxx",environment="xxxx")
pinecone.create_index("example-index", dimension=1536)
pinecone.list_indexes()
```

You also have to set your OpenAI API keys in the secrets.yaml file. 

## Configure access to the Vector Database

Export some ENV variables in order to configure access to the database:

```
export PINECONE_SERVICE = ...
export PINECONE_ACCESS_KEY...
export PINECONE_PROJECT_NAME...
export PINECONE_ENVIRONMENT=...
export PINECONE_INDEX_NAME=...
```

The examples/secrets/secrets.yaml resolves those environment variables for you.
When you go in production you are supposed to create a dedicated secrets.yaml file for each environment.


## Deploy the LangStream application

```
./bin/langstream apps deploy test -app examples/applications/query-pinecone -i examples/instances/kafka-kubernetes.yaml -s examples/secrets/secrets.yaml
```

## Start a Producer to index a document

Let's start a produce that sends messages to the vectors-topic:

```
kubectl -n kafka run kafka-producer -ti --image=quay.io/strimzi/kafka:0.35.1-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-producer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic vectors-topic
```

Insert a JSON with "id" and "document" and a genre:

```json
{"id": "myid", "document": "Hello", "genre": "comedy"}
```

The Write pipeline will compute the embeddings on the "document" field and then write a Vector into Pinecone.

## Start a Producer to Trigger a query

```
kubectl -n kafka run kafka-producer-question -ti --image=quay.io/strimzi/kafka:0.35.1-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-producer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic input-topic
```

Insert a JSON with a "question":

```json
{"question": "Hello"}
```


## Start a Consumer

Start a Kafka Consumer on a terminal

```
kubectl -n kafka run kafka-consumer -ti --image=quay.io/strimzi/kafka:0.35.1-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic output-topic --from-beginning
```

