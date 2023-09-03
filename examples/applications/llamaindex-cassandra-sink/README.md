# Using LlamaIndex in a Python agent

This sample application shows how to use LlamaIndex in a Python agent.
The application is a Sink that uses LlamaIndex to compute embeddings from the messages it receives and stores them in a DataStax Astra DB Vector Database.

## Prerequisites

Create a DataStax Astra DB Vector Database.
Get an API key for OpenAI.

## Configure the pipeline

Get the credentials to access your AstraDB Vector Database:
- username
- password
- [Secure Connect Bundle](https://awesome-astra.github.io/docs/pages/astra/download-scb/#c-procedure)

Encode the Secure Connect Bundle in base64.
```
cat secure-connect-<your database>.zip | base64
```

Update the secrets.yaml file with those values.

Update the secrets.yaml with the OpenAI API key.

## Deploy the LangStream application

```
./bin/langstream apps deploy test -app examples/applications/llamaindex-cassandra-sink -i examples/instances/kafka-kubernetes.yaml -s examples/secrets/secrets.yaml
```

## Start a Producer
```
kubectl -n kafka run kafka-producer -ti --image=quay.io/strimzi/kafka:0.35.1-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-producer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic input-topic
```

Send a message with the content of a document that you want to index:
```
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec euismod, nisl eget ultricies ultrices, nunc nisl aliquam nunc, vitae aliqua.
```

## Verify the data on Astra DB

Query Astra DB to see the results

```
SELECT body_blob FROM ks1.vs_ll_openai;
```
