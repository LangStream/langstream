# Python Source

This sample application shows how to execute a Python source.
The code in `example.py` emits a string message every second, the contest is "test!"

## Deploy the LangStream application

```
./bin/langstream docker run test -app examples/applications/python/python-source-exclamation
```

## Talk with the Chat bot using the UI
Since the application opens a gateway, we can use the gateway API to send and consume messages.

You can use the UI to see the output of the source.