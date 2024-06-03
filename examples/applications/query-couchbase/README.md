# Querying a Couchbase Cluster

This sample application shows how to perform queries against a Coushbase Cluster.

## Prerequisites

Create an Cluster and bucket on Couchbase.


## Preparing the Couchbase Cluster

Create your Couchbase Cluster following the official documentation.
https://docs.couchbase.com/cloud/index.html

Create a new bucket with any name you desire.

You also have to set your OpenAI API keys in the secrets.yaml file. 

## Configure access to the Vector Database

Export some ENV variables in order to configure access to the database:

```
export COUCHBASE_BUCKET_NAME=...
export COUCHBASE_USERNAME=...
export COUCHBASE_PASSWORD=...
export COUCHBASE_CONNECTION_STRING=...
```

Use the same bucket name you created.
For the username and password you will need to create a new 'Database Access' user. You can find this in the Settings/Database Access tab.
(Ensure you give the user the 'Data Read/Write' role in the bucket you created.)
You can find the connection string from the Couchbase web interface in the Connect tab.

```
couchbases://cb.shnnjztaidekg6i.cloud.couchbase.com
```

The above is an example of a connection string.

## Create a scope

Navigate to Databases > Data tools tab.
On the right hand side hover over the bucket of your choice and click the ellipsis that appears when you hover over it and create a scope.
Do the same but with the scope you just created to create a collection.

## Create a search index in the database in the Couchbase Capella UI

1. **Select Your Cluster:**
   - From the dashboard, select the cluster where you want to create the search index.

2. **Open the "Search" Tab:**
   - In the cluster's overview page, go to the "Search" tab which can be found in the sidebar.

3. **Create a New Search Index:**
   - Click on the "Create Search Index" button.
   - Choose "Full Text Search" from the index type options.

4. **Configure the Index:**
   - Name the index 'vector-search'.
   - Select the bucket that you want to index.
   - Add a new type mapping by selecting the 'Embeddings' field. Ensure the type and dimensions are set to vector and 1536 respectively.

5. **Save and Deploy the Index:**
   - After configuring the index settings, click on the "Create Index" button to deploy your new search index.

6. **Verify the Index Creation:**
   - Once created, the index should appear in the list of indexes. You can manage or modify the index from this interface as needed.


## Deploy the LangStream application

```
./bin/langstream apps deploy test -app examples/applications/query-couchbase -i examples/instances/kafka-kubernetes.yaml -s examples/secrets/secrets.yaml
```
Using the docker image:

```
./bin/langstream docker run test -app examples/applications/query-couchbase -s examples/secrets/secrets.yaml
```

## Send a message using the gateway to upload a document

```
bin/langstream gateway produce test write-topic -v "{\"id\":\"myid\",\"document\":\"Kafkaesque: extremely unpleasant, frightening, and confusing, and similar to situations described in the novels of Franz Kafka.\"}" -p sessionId=$(uuidgen)

```
You can view the uploaded document in the example scope and default collection of the bucket you selected.

## Start a chat using the gateway to query the document

```
 bin/langstream gateway chat test -pg produce-input -cg consume-output -p sessionId=$(uuidgen)
 ```

 Send a JSON string with at matching question:

```
{"question": "What's the definition of Kafkaesque?"}
```



