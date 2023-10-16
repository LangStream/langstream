# Querying a Pinecone Index

This sample application shows how to perform queries against a Pinecone index.

## Prerequisites

Run OpenSearch locally.
```
docker network create n1
docker run --rm -it --network n1 --name opensearch -p 9200:9200 -p 9600:9600 -e "discovery.type=single-node" -e "plugins.security.disabled=true" opensearchproject/opensearch:latest
```


Or if you want to use Amazon AWS OpenSearch Serverless:
```
export OPENSEARCH_USERNAME=<aws-access-key-id>
export OPENSEARCH_PASSWORD=<aws-secret-access-key>
export OPENSEARCH_HOST=xxxx.<region>.aoss.amazonaws.com
export OPENSEARCH_REGION=<region>
```

Note that the you need to create a AWS OpenSearch collection.
This examples uses both document IDs and vector.

This is not supported by the current Vector Search type collection so you either remove the document IDs or you use the new Vector Search type collection.

## Configure access to the Vector Database

Configure OpenAI access key to generate embeddings:

```
export OPEN_AI_ACCESS_KEY= ...
```


## Deploy the LangStream application

```
langstream docker run test -app examples/applications/query-opensearch -i examples/instances/kafka-kubernetes.yaml -s examples/secrets/secrets.yaml --docker-args --network --docker-args n1
```

or with AWS OpenSearch:

```
langstream docker run test -app examples/applications/query-opensearch -i examples/instances/kafka-kubernetes.yaml -s examples/secrets/secrets.yaml
```

## Fill the index

Let's start a produce that sends messages to the vectors-topic:

```
langstream gateway produce test fill-index -v "My cat eats carrots" 
langstream gateway produce test fill-index -v "My dog is called Jim"
```

## Search 

Search via k-NN (k-Nearest Neighbors):

```
langstream gateway chat test -g chat
$ > Food
My cat eats carrots
```

