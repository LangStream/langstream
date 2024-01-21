# Computing text embeddings with Open AI

This sample application shows how to use Open AI to compute text embeddings by calling the API.

## Configure you OpenAI API Key

Export to the ENV the access key to OpenAI

```
export OPEN_AI_ACCESS_KEY=...
```

## Deploy the LangStream application

```
./bin/langstream docker run test -app examples/applications/compute-openai-embeddings -s examples/secrets/secrets.yaml
```

## Talk with the Chat bot using the CLI
Since the application opens a gateway, we can use the gateway API to send and consume messages.

```
./bin/langstream gateway chat test -cg output -pg input -p sessionId=$(uuidgen)
```

