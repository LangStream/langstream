# Website Crawler with RAG Chatbot and Memory

This sample application shows how to use the webcrawler source to crawl a website and store the results
in a vector database. It includes a chatbot that uses Retrievel Augmented Generation (RAG) pattern to
query the vector database for relevant documents to include in the prompt. 

The chatbot also uses conversational memory for both the retrieval stage and the response generation stage.
Before the retrieval, the user question is reworked to take into account the previous conversation. This 
context-aware question is then used to query the vector database. By using a question with context, the
quality of the retrieval is improved.

The response generation stage includes the conversational memory and the updated question in order
to improve the quality of the response.

## Prerequisites

Create a S3 bucket, it will contain only a metadata file for the WebCrawler.

Create a database in Astra DB. The required tables (one for the vector embeddings, one for the chat history) will be created automatically as 
specified in the `assets` part of the configuration. Note that the conversation history table is indexed by
session and timestamp to allow for a query to get the last N messages in the conversation. The table also
has TTL set to 1 hour so the table doesn't not grow indefinitely.

The gateway in this example is configured for GitHub authentication. You will need to a GitHub OAuth application
to connect to the gateway. See the `gateway-authentication` example for details.


## Configure the pipeline

Update the same file and set username, password and the other parameters in the `secrets.yaml` file.

Many of the parameters, such as the website to crawl and the names of the tables, are defined as globals
in the `instances.yaml` file. Here is an example:

```
instance:
  globals:
    assistantType: "LangStream project"
    vectorDb: "sgai"
    vectorKeyspace: "chatbot"
    vectorTable: "langstreamdocs"
    chatTable: "lsdocshistory"
    vectorIndex: "annlangstream"
    chunksTopic: "langstream-chunks"
    questionsTopic: "langstream-questions"
    answersTopic: "langstream-answers"
    chatModelName: "gpt-35-turbo"
    logTopic: "langstream-logs"
    seedUrl1: "https://docs.langstream.ai/"
    seedUrl2: "https://langstream.ai/"
    allowedUrl1: "https://docs.langstream.ai/"
    allowedUrl2: "https://langstream.ai/"
    codeBucket: "langstream-code-from-git"
```

Configure the list of the allowed web domains, this is required in order to not let the crawler escape outside of your data.
Configure the list of seed URLs, for instance with your home page.


## Deploy the LangStream application

```
./bin/langstream apps deploy test -app examples/applications/chatbot-rag-memory -i examples/instances/kafka-kubernetes.yaml -s examples/secrets/secrets.yaml
```

## Talk with the Chat bot using the CLI
Since the application opens a gateway, we can use the gateway API to send and consume messages.

```
./bin/langstream gateway chat test -cg bot-output -pg user-input -p sessionId=$(uuidgen)
```
