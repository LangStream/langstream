# Querying a JDBC Database

This sample application shows how to use the Hugging Face APIs


## Prerequisites

Get an access-key from Hugging Face: https://huggingface.co/api

## Deploy the LangStream application

Set the access-key into the `examples/secrets/secrets.yaml` file.

```
./bin/langstream apps deploy test -app examples/applications/compute-hugging-face -i examples/instances/kafka-kubernetes.yaml  -s examples/secrets/secrets.yaml
```

## Start a Producer
```
kubectl -n kafka run kafka-producer -ti --image=quay.io/strimzi/kafka:0.35.1-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-producer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic input-topic
```

Insert a prompt

```
hello I am a model of
```


## Start a Consumer

Start a Kafka Consumer on a terminal

```
kubectl -n kafka run kafka-consumer -ti --image=quay.io/strimzi/kafka:0.35.1-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic output-topic --from-beginning
```

