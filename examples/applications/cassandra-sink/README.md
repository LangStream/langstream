# Writing to a Cassandra or Datastax Astra DB Database 

This sample application shows how to use the DataStax Cassandra Sink Connector
https://github.com/datastax/kafka-sink

## Prerequisites

Install Cassandra or create a DataStax Astra DB Database

## Create a "products" table

```
CREATE TABLE IF NOT EXISTS products (
  id int PRIMARY KEY,
  name TEXT,
  description TEXT
);
```

Insert some data:
```
INSERT INTO products (id, name, description) 
VALUES (
   1,
   'Coded Cleats',
   'ChatGPT integrated sneakers that talk to you'
   )
```

## Configure the pipeline

Update the same file and set username, password and the other parameters.

## Deploy the SGA application

./bin/sga-cli apps deploy test -app examples/applications/cassandra-sink -i examples/instances/kafka-kubernetes.yaml


## Start a Producer
```
kubectl -n kafka run kafka-producer -ti --image=quay.io/strimzi/kafka:0.35.1-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-producer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic input-topic
```

Insert a JSON with "id", "name" and "description":

```
{"id": 10, "name": "test", "description": "test"}
```


## Verify the data on Cassandra

Query Cassandra to see the results

```
SELECT * FROM products;
```

