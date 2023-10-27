# Python Sink

This sample application shows how to execute a Python sink.
The code in `example.py` adds an exclamation mark to the end of a string message.
This is a Sink, it doesn't write to a topic, but only to the logs.

## Deploy the LangStream application

```
./bin/langstream docker run test -app examples/applications/python/python-sink-exclamation
```

## Talk with the Chat bot using the UI
Since the application opens a gateway, we can use the gateway API to send and consume messages.

You can use the UI to send the messages and see the output in the logs.