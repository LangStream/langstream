# LangServe application

This sample application shows how to execute a LangServe application.

## Configure you OpenAI API Key

Export to the ENV the access key to OpenAI

```bash
export OPEN_AI_ACCESS_KEY=...
```

The default [secrets file](../../../secrets/secrets.yaml) reads from the ENV. Check out the file to learn more about
the default settings, you can change them by exporting other ENV variables.


# Configure LangSmith (optional)

You can also connect to LangSmith by setting some environment variables

```bash
export LANGSMITH_API_URL=https://api.smith.langchain.com
export LANGSMITH_APIKEY=xxxxx
```

## Deploy the LangStream application

```
./bin/langstream docker run test -app examples/applications/python/langserve-service -s examples/secrets/secrets.yaml --start-broker=false
```

## Interact with the application

Sample request

```bash
curl --location --request POST 'http://localhost:8000/chain/invoke/' \
    --header 'Content-Type: application/json' \
    --data-raw '{
        "input": {
            "topic": "cats"
        }
    }'
```

Sample response:

```json
{
  "output":
   {
     "content": "Why don't cats play poker in the wild?\n\nToo many cheetahs!",
     "additional_kwargs":{},
     "type":"ai",
     "example":false
   },
   "callback_events":[]
}
```