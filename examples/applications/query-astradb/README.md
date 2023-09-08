# Querying a DataStax AstraDB Database

This sample application shows how to perform queries against a DataStax AstraDB Database,
that is a Cassandra database with Vector Search support.


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

## Create a "products" table

You can follow this documentation in order to create a table with Vector Search support:
https://docs.datastax.com/en/astra-serverless/docs/vector-search/cql.html

Enter the AstraDB Database CQLSH shell.

Create the table:
```
CREATE TABLE IF NOT EXISTS mykeyspace.products (
  id int PRIMARY KEY,
  name TEXT,
  description TEXT,
  item_vector VECTOR<FLOAT, 5> //create a 5-dimensional embedding
);
```

Insert some data:
`INSERT INTO mykeyspace.products (id, name, description, item_vector)
VALUES (
1, //id
'Coded Cleats', //name
'ChatGPT integrated sneakers that talk to you', //description
[0.1, 0.15, 0.3, 0.12, 0.05] //item_vector
);`

## Deploy the LangStream application

```
./bin/langstream apps deploy test -app examples/applications/query-cassandra -i examples/instances/kafka-kubernetes.yaml -s examples/secrets/secrets.yaml
```

## Start a Producer
```
kubectl -n kafka run kafka-producer -ti --image=quay.io/strimzi/kafka:0.35.1-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-producer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic input-topic
```

Insert a JSON with "id", "name" and "description":

```
{"id": 10, "name": "test", "description": "test"}
```


## Start a Consumer

Start a Kafka Consumer on a terminal

```
kubectl -n kafka run kafka-consumer -ti --image=quay.io/strimzi/kafka:0.35.1-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic output-topic --from-beginning
```

