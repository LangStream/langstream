# Running your own Chat bot using Ollama.ai

This sample application shows how to build a chat bot over the content of a website.
In this case you are going to crawl the LangStream.ai documentation website.

The Chat bot will be able to help you with LangStream.

In this example we are using [HerdDB](ps://github.com/diennea/herddb) as a vector database using the JDBC driver,
but you can use any Vector databases.

As LLM we are using [Ollama](https://ollama.ai), that is a service that runs on your machine. 


## Install Ollama


Follow the instructions on the Ollama.ai website to install Ollama.

Then start Ollama with the llama2 model

```
ollama run llama2
```


## Deploy the LangStream application in docker

The default docker runner starts Minio, Kafka and HerdDB, so you can run the application locally.

```
./bin/langstream docker run test -app examples/applications/ollama-chatbot -s examples/secrets/secrets.yaml
```

## Talk with the Chat bot using the CLI
Since the application opens a gateway, we can use the gateway API to send and consume messages.

```
./bin/langstream gateway chat test -cg bot-output -pg user-input -p sessionId=$(uuidgen)
```
