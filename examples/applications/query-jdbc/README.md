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
./bin/langstream gateway chat test -cg consume-output -pg produce-input -p sessionId=$(uuidgen)
```

Insert a JSON with "id", "name" and "description":

```
{"id": 1, "name": "test", "description": "test"}
```

