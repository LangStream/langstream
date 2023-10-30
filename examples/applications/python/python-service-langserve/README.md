# LangServe application

This sample application shows how to execute a LangServe application.

## Configure you OpenAI API Key

Export to the ENV the access key to OpenAI

```
export OPEN_AI_ACCESS_KEY=...
```

The default [secrets file](../../secrets/secrets.yaml) reads from the ENV. Check out the file to learn more about
the default settings, you can change them by exporting other ENV variables.


## Deploy the LangStream application

```
./bin/langstream docker run test -app examples/applications/python/python-service-langserve
```

## Interact with the application


TODO: Add instructions on how to interact with the application.
```
```