# Simple Chat-bot using LangChain

This sample application shows how to use LangChain to build a chat-bot.

## Configure you OpenAI API Key

Export to the ENV the access key to OpenAI

```
export OPEN_AI_ACCESS_KEY=...
```

The default [secrets file](../../secrets/secrets.yaml) reads from the ENV. Check out the file to learn more about
the default settings, you can change them by exporting other ENV variables.

## Deploy the LangStream application

```
./bin/langstream docker run test -app examples/applications/langchain-chatbot -s examples/secrets/secrets.yaml
```

Then you can talk with the chat-bot using the UI.