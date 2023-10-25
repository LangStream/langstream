# Simple Chat-bot using LangChain

This sample application shows how to use LangChain to build a chat-bot.

These are the main components:
- DataStax Astra DB to store the documents to support the chat-bot
- Cassio.org as Driver to connect to Astra DB (compatible with Cassandra) to connect to the Vector Database
- OpenAI as LLM for the chat-completion and for computing the embeddings.

The application is made of two pipelines:
- The Chat bot pipeline, that is built using the LangChain SDK
- The Vector Database pipeline, that is built using the native LangStream agents

This is the table schema used by the application:

```
CREATE TABLE documents.documents (
    row_id text PRIMARY KEY,
    attributes_blob text,
    body_blob text,
    metadata_s map<text, text>,
    vector vector<float, 1536>
    
    ) ..
```


## Configure you OpenAI API Key

Export to the ENV the access key to OpenAI

```
export OPEN_AI_ACCESS_KEY=...
```

The default [secrets file](../../secrets/secrets.yaml) reads from the ENV. Check out the file to learn more about
the default settings, you can change them by exporting other ENV variables.

## Configure access to the Vector Database

Export some ENV variables in order to configure access to the database:

```
export ASTRA_LANGCHAIN_TOKEN=...
export ASTRA_LANGCHAIN_CLIENT_ID=...
export ASTRA_LANGCHAIN_SECRET=...
export ASTRA_LANGCHAIN_DATABASE=...
export ASTRA_LANGCHAIN_DATABASE_ID=...
export ASTRA_LANGCHAIN_KEYSPACE=...
export ASTRA_LANGCHAIN_TABLE=...
```

You can find the credentials in the Astra DB console when you create a Token.

The table is automatically created by the application if it doesn't exist (this is performed by Cassio.org).

The examples/secrets/secrets.yaml resolves those environment variables for you.
When you go in production you are supposed to create a dedicated secrets.yaml file for each environment.

## Deploy the LangStream application

```
./bin/langstream docker run test -app examples/applications/langchain-chatbot -s examples/secrets/secrets.yaml
```

Then you can talk with the chat-bot using the UI.