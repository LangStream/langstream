# LangChain Source

This sample application shows how to use LangChain in a Python agent.
The application is a Source that uses LangChain to load documents from S3 and then uses a LangChain document transformer to chunk the document and send the chunks to a Kafka topic.

## Prerequisites

Create a S3 bucket.

## Configure the pipeline

Update the pipeline file and set the bucket name, s3 URL username, password and the other parameters.

## Deploy the LangStream application

```
./bin/langstream apps deploy test -app examples/applications/langchain-source -i examples/instances/kafka-kubernetes.yaml -s -s examples/secrets/secrets.yaml
```

## Start a Consumer
```
kubectl -n kafka run kafka-consumer -ti --image=quay.io/strimzi/kafka:0.35.1-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic output-topic --from-beginning
```

## Upload a file to S3

For instance using the AWS CLI in another terminal:
```
aws s3 cp <some file> s3://langstream-langchain-source
```
The chunks of the file will be displayed on the Kafka consumer terminal.