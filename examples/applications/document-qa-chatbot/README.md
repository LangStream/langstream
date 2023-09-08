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

## Configure your OpenAI API Key

Export some ENV variables in order to configure access to Open AI:

```
export OPENAI_ACCESS_KEY=...
export OPENAI_URL=...
export OPENAI_PROVIDER=azure
```

## Configure access to the Vector Database

Export some ENV variables in order to configure access to the database:

```
export ASTRA_TOKEN=...
export ASTRA_CLIENT_ID=...
export ASTRA_SECRET=...
export ASTRA_DATABASE=...
```

You can find the credentials in the Astra DB console when you create a Token.

The examples/secrets/secrets.yaml resolves those environment variables for you.
When you go in production you are supposed to create a dedicated secrets.yaml file for each environment.


## Deploy the LangStream application

```
./bin/langstream apps deploy chat-bot -app examples/applications/document-qa-chatbot -i examples/instances/kafka-kubernetes.yaml -s examples/secrets/secrets.yaml
```

## Talk with the Chat bot using the CLI
Since the application opens a gateway, we can use the gateway API to send and consume messages.

```
./bin/langstream gateway chat chat-bot -cg bot-output -pg user-input -p sessionId=$(uuidgen)
```



## Debug what you are sending to the LLM

```
./bin/langstream gateway consume chat-bot debug
```


