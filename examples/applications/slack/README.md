# Slack channel writer

This sample application shows how to write to a slack channel

Export the slack incoming webhook (token) (iwebhooks are locked to slack apps channels at creation):
export SLACK_TOKEN="<token>"

## Deploy the LangStream application
```
langstream docker run slack -app examples/applications/slack -i examples/instances/kafka-docker.yaml  -s examples/secrets/secrets.yaml
```

## write

Since the application opens a gateway, we can use the gateway API to send and consume messages using the use gateway `chat` feature:
```
langstream gateway produce slack produce-input -p sessionId=$(uuidgen) -v "hello"
```

