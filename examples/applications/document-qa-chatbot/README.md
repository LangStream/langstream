# Building a Chat bot that answers to questions over a corpus of Documents 

This sample application builds a simple chat bot that answers questions over a corpus of documents.
The documents are retrieved from a Vector Database (in this case DataStax Astra DB).

Please use the text-processing Application to fill the Vector Database and propertly extract the text and
compute embeddings.

This application assumes that you are using OpenAI as the LLM.

## Prerequisites

Set up the text-processing application and fill the Vector Database with documents.
From this point you should have a table 'documents' on your Vector Database with the following schema:

```
USE documents;
CREATE TABLE IF NOT EXISTS documents (  
  filename TEXT,
  chunk_id int,
  num_tokens int,
  language TEXT,  
  text TEXT,
  embeddings_vector VECTOR<FLOAT, 1536>,
  PRIMARY KEY (filename, chunk_id)
);
CREATE CUSTOM INDEX IF NOT EXISTS ann_index 
  ON documents(embeddings_vector) USING 'StorageAttachedIndex';
```

## Prepare the secrets file

As you would have already set up you should have a secrets.yaml file that contains the credentials to access the Vector Database
and OpenAI APIs.


## Deploy the SGA application

```
./bin/sga-cli apps deploy chat-bot -app examples/applications/document-qa-chatbot -i examples/instances/kafka-kubernetes.yaml -s examples/secrets/secrets.yaml
```

## Talk with the Chat bot using the CLI
Since the application opens a gateway, we can use the gateway API to send and consume messages.

```
./bin/sga-cli gateway chat chat-bot -cg bot-output -pg user-input -p sessionId=$(uuidgen)
```


