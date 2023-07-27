# OpenAI Completions

This sample application shows how to execute completion using the OpenAI library using the Azure OpenAI API endpoint.

## Create OpenAI Secret

```
export AZURE_URL=xxxx
export OPEN_AI_ACCESS_KEY=xxxx

echo """
secrets:
  - name: open-ai
    id: open-ai
    data:
      url: $AZURE_URL
      access-key: $OPEN_AI_ACCESS_KEY
""" > /tmp/secrets.yaml
```
## 
```
./bin/sga-cli apps deploy test -app examples/applications/openai-completions -i examples/instances/kafka-kubernetes.yaml -s /tmp/secrets.yaml
```

## Produce a message
Since the application opens a gateway, we can use the gateway API to send and consume messages.

```
session="$(uuidgen)"
./bin/sga-cli gateway produce test produce-input -p sessionId="$session" -v "Barack Obama"
./bin/sga-cli gateway consume test consume-output -p sessionId="$session"
```

Another approach to test values is to use gateway `chat` CLI feature:
```
./bin/sga-cli gateway chat test -cg consume-output -pg produce-input -p sessionId=$(uuidgen)
```
