# Slack channel witer

This sample application shows how to iwrite to a slack channel


## Deploy the LangStream application
```
langstream docker run slack -app examples/applications/slack -i examples/instances/kafka-docker.yaml  -s examples/secrets/secrets.yaml --docker-args -u --docker-args 0
```

## write

Since the application opens a gateway, we can use the gateway API to send and consume messages using the use gateway `chat` feature:
```
langstream gateway produce slack produce-input -p sessionId=$(uuidgen) -v "hello"
```

This is currently working because the slack message and token are hardcoded and need to be referenced by variables.

