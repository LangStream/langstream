# Writing to a Cassandra or Datastax Astra DB Database 

This sample application shows how to write to Astra DB using the 'vector-db-sink' agent.
https://github.com/datastax/kafka-sink

## Prerequisites

Install Cassandra or create a DataStax Astra DB Database

## Table creation

The sample application creates a table named "products" inside a keyspace named 'products' with the following schema:

```
CREATE TABLE IF NOT EXISTS products (
  id int PRIMARY KEY,
  name TEXT,
  description TEXT
);
```

This is handled by the 'cassandra-table' assets in the pipeline.yaml file.

## Configure the pipeline

Update the secrets.yaml file and set the Astra credentials and the database name:
- clientId
- secret
- token
- database

You can find the credentials in the Astra DB console when you create a Token.

## Deploy the LangStream application

./bin/langstream apps deploy test -app examples/applications/astradb-sink -i examples/instances/kafka-kubernetes.yaml -s examples/secrets/secrets.yaml


## Produce a message

```
./bin/langstream gateway produce test produce-input -v '{"id": 10, "name": "test", "description": "test"}'
```

## Verify the data on Cassandra

Query Cassandra to see the results using the Astra DB console or the cqlsh tool:

```
SELECT * FROM products.products;
```

