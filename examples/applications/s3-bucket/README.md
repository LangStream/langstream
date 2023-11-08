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
./bin/langstream docker run test -app examples/applications/s3-source -s examples/secrets/secrets.yaml
```

## Write a document in the S3 bucket

```
# Upload a document to the S3 bucket
dev/s3_upload.sh documents examples/applications/s3-source/simple.pdf
```

## Interact with the Chatbot

Now you can use the developer UI to ask questions to the chatbot about your documents.