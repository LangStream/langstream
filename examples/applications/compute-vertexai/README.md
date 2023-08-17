# Using the Google Vertex AI API

This sample application shows how to use the Vertex AI API to compute embeddings and perform chat completions.

The Vertex AI Service Provider currently supports both the 'compute-ai-embeddings' and the 'ai-chat-completions' agents.

This example shows how to use the 'compute-ai-embeddings' agent.

## Prerequisites

Get a Google Cloud account and create a project, enable the Vertex AI API and create a service account.

Download the Service Account JSON file and put it into the `secrets.yaml` file.

```
secrets:  
- name: vertex-ai
  data:
      url: https://us-central1-aiplatform.googleapis.com  
      serviceAccountJson: xxx
      region: us-central1
      project: myproject
```

Alternatively, if you have already configured gcloud locally you can get a token from the gcloud CLI:

```
gcloud auth print-access-token
```

In this case you can put the token in the `secrets.yaml` file:

```
secrets:  
- name: vertex-ai
  data:
      url: https://us-central1-aiplatform.googleapis.com  
      token: xxx
      region: us-central1
      project: myproject
```

## Deploy the LangStream application

```
./bin/langstream apps deploy test -app examples/applications/compute-vertex -i examples/instances/kafka-kubernetes.yaml -s examples/secrets/secrets.yaml
```

## Start a Producer
```
kubectl -n kafka run kafka-producer -ti --image=quay.io/strimzi/kafka:0.35.1-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-producer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic input-topic
```

Insert a JSON with "id", "name" and "description":

```
{"id": 10, "name": "test", "description": "test"}
```


## Start a Consumer

Start a Kafka Consumer on a terminal

```
kubectl -n kafka run kafka-consumer -ti --image=quay.io/strimzi/kafka:0.35.1-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic output-topic --from-beginning
```

