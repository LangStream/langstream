# OpenAI Chat Completions

This sample application shows how to execute Chat Completions using the OpenAI library using the Azure OpenAI API endpoint.

## Configure you OpenAI API Key

Export some ENV variables in order to configure access to Open AI:

```
export OPEN_AI_ACCESS_KEY=...
export OPEN_AI_URL=...
export OPEN_AI_PROVIDER=azure
```


## Deploy the LangStream application
```
./bin/langstream apps deploy test -app examples/applications/openai-completions -i examples/instances/kafka-kubernetes.yaml -s examples/secrets/secrets.yaml
```

## Produce a message
Since the application opens a gateway, we can use the gateway API to send and consume messages using the use gateway `chat` feature:
```
./bin/langstream gateway chat test -cg consume-output -pg produce-input -p sessionId=$(uuidgen)
```

Responses are streamed to the output-topic. If you want to inspect the history of the raw answers you can
consume from the history-topic:

```
./bin/langstream gateway consume test consume-history
```


