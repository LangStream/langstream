# Preprocessing Text 

This sample application shows how to use some common NLP techniques to preprocess text data and then write the results to a Vector Database.

We have two pipelines:

The extract-text.yaml file defines a pipeline that will:

- Extract text from document files (PDF, Word...)
- Detect the language and filter out non-English documents
- Normalize the text
- Split the text into chunks
- Write the chunks to a Vector Database, in this case DataStax Astra DB

## Prerequisites

Prepare some PDF files and upload them to a bucket in S3.

## Deploy the LangStream application

```
./bin/langstream docker run test -app examples/applications/s3-source -s examples/secrets/secrets.yaml --docker-args="-p9900:9000"
```

Please note that here we are adding --docker-args="-p9900:9000" to expose the S3 API on port 9900.


## Write some documents in the S3 bucket

```
# Upload a document to the S3 bucket
dev/s3_upload.sh localhost http://localhost:9900 documents README.md
dev/s3_upload.sh localhost http://localhost:9900 documents examples/applications/s3-source/simple.pdf
```

## Interact with the Chatbot

Now you can use the developer UI to ask questions to the chatbot about your documents.

If you have uploaded the README file then you should be able to ask "what is LangStream ?"