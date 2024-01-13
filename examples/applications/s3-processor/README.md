# Preprocessing Text 

This sample application shows how to read a single file from an S3 bucket and process in a pipeline.

The pipeline will:


- Read a file from and S3 bucket which is specified in the value of the message sent to the input topic
- Extract text from document files (PDF, Word...)
- Split the text into chunks
- Write the chunks to the output topic

## Prerequisites

Prepare some PDF files and upload them to a bucket in S3.

## Deploy the LangStream application

```
./bin/langstream docker run test -app examples/applications/s3-processor -s examples/secrets/secrets.yaml --docker-args="-p9900:9000"
```

Please note that here we are adding --docker-args="-p9900:9000" to expose the S3 API on port 9900.


## Write some documents in the S3 bucket

```
# Upload a document to the S3 bucket
dev/s3_upload.sh localhost http://localhost:9900 documents README.md
dev/s3_upload.sh localhost http://localhost:9900 documents examples/applications/s3-source/simple.pdf
```

## Interact with the pipeline

Now you can use the developer UI to specify which document to read from S3 and process. To process the simple.pdf file, simply send `simple.pdf` as the message.