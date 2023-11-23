# Indexing a WebSite

This sample application shows how to use the WebCrawler Source Connector and Astra Vector DB.

## Collections

This application creates a collection named "documents" in your DB.
You can change the name of the collection in the file `configuration.yaml`.

## Configure access to the Vector Database

Export some ENV variables in order to configure access to the database:

```
export ASTRA_VECTOR_DB_TOKEN=AstraCS:...
export ASTRA_VECTOR_DB_ENDPOINT=https://....astra.datastax.com
```

You can find the credentials in the Astra DB console.

The examples/secrets/secrets.yaml resolves those environment variables for you.
When you go in production you are supposed to create a dedicated secrets.yaml file for each environment.

## Configure the pipeline

You can edit the file `crawler.yaml` and configure the list of the allowed web domains, this is required in order to not let the crawler escape outside your data.
Configure the list of seed URLs, for instance with your home page.

The default configuration in this example will crawl the LangStream website.

## Deploy the LangStream application

```
./bin/langstream docker run test -app examples/applications/webcrawler-astra-vector-db  -s examples/secrets/secrets.yaml
```

## Talk with the Chat bot

If the application launches successfully, you can talk with the chat bot using UI.

You can also use the CLI:

```
./bin/langstream gateway chat test -cg bot-output -pg user-input -p sessionId=$(uuidgen)
```
