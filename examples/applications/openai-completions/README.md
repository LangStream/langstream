# OpenAI Completions

This sample application shows how to execute completion using the OpenAI library using the Azure OpenAI API endpoint.

## Create OpenAI Secret

If you want to enable Google authentication in the gateways, also set the `GOOGLE_CLIENT_ID` environment variable.

```
export AZURE_URL=xxxx
export OPEN_AI_ACCESS_KEY=xxxx

export GOOGLE_CLIENT_ID=xxx

echo """
secrets:
  - name: open-ai
    id: open-ai
    data:
      url: $AZURE_URL
      access-key: $OPEN_AI_ACCESS_KEY
  - name: google
    id: google
    data:
      client-id: $GOOGLE_CLIENT_ID
""" > /tmp/secrets.yaml
```
## 
```
./bin/langstream apps deploy test -app examples/applications/openai-completions -i examples/instances/kafka-kubernetes.yaml -s /tmp/secrets.yaml
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


## Use authenticated gateway
This example shows how to create a gateway that requires authentication. The application uses the [Sign In with Google](https://developers.google.com/identity/gsi/web/guides/overview) feature.

In this case there's no need to create a session since the session will be per-user.

```
google_token=xxx
./bin/langstream gateway produce test produce-input-auth -c "$google_token" -v "Who was the Presitent of the US in 19990 ?"
./bin/langstream gateway consume test consume-output-auth -c "$google_token"
```






```


