# Preprocessing Text 

This sample application shows how to use some common NLP techniques to preprocess text data:

- Extract text from document files (PDF, Word...)
- Detect the language and filter out non-English documents
- Normalize the text
- Split the text into chunks

## Prerequisites

Prepare some PDF files and upload them to a bucket in S3.

## Deploy the SGA application

```
./bin/sga-cli apps deploy test -app examples/applications/text-processing -i examples/instances/kafka-kubernetes.yaml
```

## Write a document in the S3 bucket

```
# Activate port forwarding of  the MinIO service in order to be able to upload files to the S3 bucket
kubectl -n minio-dev port-forward minio 9000:9000 &

# Upload a document to the S3 bucket
dev/s3_upload.sh documents examples/applications/text-processing/simple.pdf
```

## Start a Consumer

Start a Kafka Consumer on a terminal and see the results.

```
kubectl -n kafka run kafka-consumer -ti --image=quay.io/strimzi/kafka:0.35.1-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic output-topic --from-beginning
```