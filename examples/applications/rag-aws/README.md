# Indexing a WebSite

This sample application shows how to use the WebCrawler Source Connector

## Prerequisites

- Create a S3 bucket.
- Setup OpenSearch Serverless in your AWS account.
- Setup Bedrock models. 


## Configure access to the Vector Database

Export some ENV variables in order to configure access to the database:

```
export BEDROCK_ACCESS_KEY=...
export BEDROCK_SECRET_KEY=...

export OPENSEARCH_USERNAME=<aws-access-key-id>
export OPENSEARCH_PASSWORD=<aws-secret-access-key>
export OPENSEARCH_HOST=xxxx.<region>.aoss.amazonaws.com
export OPENSEARCH_REGION=<region>

export S3_BUCKET_NAME=..
export S3_ACCESS_KEY=..
export S3_SECRET=..
export S3_ENDPOINT=https://s3.amazonaws.com
```


## Deploy the LangStream application

```
./bin/langstream docker run test -app examples/applications/rag-aws -s examples/secrets/secrets.yaml
```
