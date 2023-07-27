# Python Processor

This sample application shows how to execute a Python processor code.
The code in `example.py` adds an exclamation mark to the end of a string message.

## Deploy the SGA application

```
./bin/sga-cli apps deploy test -app examples/applications/python-function-exclamation -i examples/instances/kafka-kubernetes.yaml
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

You should see the message with an exclamation mark at the end:

```
Hello World!
``` 

