# Querying a DataStax AstraDB Database

This sample application shows how to perform queries against a DataStax AstraDB Database,
that is a Cassandra database with Vector Search support.


## Get AstraDB Credentials 

Get the credentials to access your AstraDB Database:
- username
- password
- Secure connect Bundle

Encode the Secure connect Bundle in base64.

Update the secrets.yaml file with those values.

## Create a "products" table

Enter the PG shell:

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

## Deploy the SGA application

```
./bin/sga-cli apps deploy test -app examples/applications/query-cassandra -i examples/instances/kafka-kubernetes.yaml -s examples/secrets/secrets.yaml
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

