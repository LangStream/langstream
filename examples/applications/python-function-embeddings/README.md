# Python Processor

This sample application shows how to execute embeddings using the OpenAI python library.

## Create OpenAI Secret

```
export OPEN_AI_ACCESS_KEY=xxxx

echo """
secrets:
  - name: open-ai
    id: open-ai
    data:
      access-key: $OPEN_AI_ACCESS_KEY
""" > /tmp/secrets.yaml
```
## 
```
./bin/sga-cli apps deploy test -app examples/applications/python-function-embeddings -i examples/instances/kafka-kubernetes.yaml -s /tmp/secrets.yaml
```

## Start a Producer
```
kubectl -n kafka run kafka-producer -ti --image=quay.io/strimzi/kafka:0.35.1-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-producer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic input-topic
```

Insert a String:

```
> Hello World
```


## Start a Consumer

Start a Kafka Consumer on a terminal

```
kubectl -n kafka run kafka-consumer -ti --image=quay.io/strimzi/kafka:0.35.1-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic output-topic --from-beginning
```

You should see the message with the embeddings.

