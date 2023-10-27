# Python Processor

This sample application shows how to execute a Python processor code.
The code in `example.py` adds an exclamation mark to the end of a string message.

## Deploy the LangStream application

```
./bin/langstream docker run test -app examples/applications/python/python-processor-exclamation
```

## Talk with the Chat bot using the CLI
Since the application opens a gateway, we can use the gateway API to send and consume messages.

```
./bin/langstream gateway chat test -cg consume-output -pg produce-input -p sessionId=$(uuidgen)
```

# How to import the dependencies

If you want to try to import the requirements.txt file you can use this command

```
./bin/langstream python load-pip-requirements -app examples/applications/python-processor-exclamation
```

This step is not needed to run the sample application, but you can use use this sample application
to get started with your own Python processor code.
