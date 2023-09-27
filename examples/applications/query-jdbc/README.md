# Querying a JDBC Database

This sample application shows how to perform queries against a JDBC database.

In this example we are using [HerdDB](https://github.com/diennea/herddb) as a database, but you can use any JDBC database.

HerdDB supports basic Vector search scalar functions, so it is perfect for supporting the RAG pattern.


## Deploy the LangStream application

```
./bin/langstream apps deploy test -app examples/applications/query-jdbc -i examples/instances/kafka-kubernetes.yaml -s examples/secrets/secrets.yaml
```

## Talk with the Chat bot using the CLI
Since the application opens a gateway, we can use the gateway API to send and consume messages.

```
./bin/langstream gateway chat test -cg bot-output -pg user-input -p sessionId=$(uuidgen)
```

Responses are streamed to the output-topic. If you want to inspect the history of the raw answers you can
consume from the log-topic using the llm-debug gateway:

```
./bin/langstream gateway consume test llm-debug
```
