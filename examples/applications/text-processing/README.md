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

Use the gateway to start a consumer that will read the output of the application.

```
./bin/sga-cli gateway consume test consume-output
```