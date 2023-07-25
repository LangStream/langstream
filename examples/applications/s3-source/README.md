# Reading from S3

This sample application shows how to use the S3 Source Connector

## Prerequisites

Create a S3 bucket.

## Configure the pipeline

Update the same file and set username, password and the other parameters.

## Deploy the SGA application

```
./bin/sga-cli apps deploy test -app examples/applications/s3-source -i examples/instances/kafka-kubernetes.yaml
```

## Start a Consumer
```
kubectl -n kafka run kafka-consumer -ti --image=quay.io/strimzi/kafka:0.35.1-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic output-topic --from-beginning
```

## Upload a file to S3

For instance using the AWS CLI in another terminal:
```
aws s3 cp <some file> s3://sga-source
```
The content of the file will be displayed on the Kafka consumer terminal.
