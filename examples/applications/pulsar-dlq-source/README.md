# Listen to messages on Pulsar DLQ topics

This sample application listens for new messages on all DLQ topics in a Pulsar
namespace and outputs them to a topic. As new DLQ topics are created, it automatically
subscribes to them and listens for messages.

## Start a Pulsar container locally


Access the pulsar admin interface:

```
docker exec -it pulsar-standalone /bin/bash
```


## Deploy the LangStream application

Run the application using the built-in Pulsar and exposing its ports.

```
./bin/langstream docker run test --use-pulsar=true --docker-args=-p --docker-args=6650:6650 -app examples/applications/pulsar-dlq-source
```

## Send messages to DLQ topics

Use the sample script to simulate failed messages and put them in the DLQ.

```
pip install pulsar-client
./python3 simulate_dlq.py
```

