# LangChain Source

This sample application shows how to use LangChain in a Python agent.
The application is a Source that uses a configurable LangChain to load documents and then uses a configurable LangChain text splitter to chunk the document and send the chunks to a Kafka topic.

## Configure the pipeline

Update the pipeline file and set the loader and splitter class names and their parameters.
Set the load-interval-seconds to the number of seconds between each load of documents. 
The load-interval-seconds value -1 (default) means load once.

## Deploy the LangStream application

```
./bin/langstream apps deploy test -app examples/applications/langchain-document-loader -i examples/instances/kafka-kubernetes.yaml
```

## Consume from the Gateway Consumer
```
./bin/langstream gateway consume test consume-output
```
